(ns jdbc.chdb.owned-statement
  "Experimental internal owned ASCII statement for explicit owned requests.
  Ownership is enforced at the supported API, not against reflection, private
  Var access or unsafe FFI. Caller input must not race with snapshot creation."
  (:require [clojure.java.io :as io] [jolt.scheme :as scheme] [jolt.ffi :as ffi]))

(def ^:private max-snapshot-bytes (* 64 1024 1024))
(def ^:private statement-seal (Object.))

(defmacro ^:private kernel-source [resource]
  (if-let [url (io/resource resource)] (slurp url)
    (throw (ex-info "Missing owned-statement resource" {}))))
(def ^:private snapshot-source (kernel-source "jdbc/chdb/owned_ascii_snapshot.ss"))
(def ^:private wal-source (kernel-source "jdbc/chdb/owned_ascii_wal.ss"))
(def ^:private placeholder-source (kernel-source "jdbc/chdb/owned_ascii_placeholder.ss"))
(def ^:private snapshot-kernel (delay (scheme/eval-string snapshot-source)))
(def ^:private wal-kernel (delay (scheme/eval-string wal-source)))
(def ^:private placeholder-kernel (delay (scheme/eval-string placeholder-source)))

(deftype ^:private OwnedAsciiStatement [seal payload size]
  Object
  (toString [_] "#<owned ASCII SQL statement>"))

(defn statement? [value]
  (and (instance? OwnedAsciiStatement value)
       (identical? statement-seal (.-seal value))))

(defn- require-statement! [value]
  (when-not (statement? value)
    (throw (ex-info "Invalid owned statement" {:type ::invalid-statement})))
  value)

(defn try-snapshot
  "Copy a caller byte array into private storage. Nil declines non-ASCII,
  non-byte or >64MiB inputs; it is never a truncation or mutation permission.
  Input must not be mutated during this call. It may be changed after return.
  Does not admit, classify, execute or confirm persistence."
  [bytes]
  (when (bytes? bytes)
    (let [snapshot (@snapshot-kernel bytes max-snapshot-bytes)]
      (when (bytes? snapshot)
        (OwnedAsciiStatement. statement-seal snapshot (alength snapshot))))))

(defn byte-count [statement]
  (.-size (require-statement! statement)))

(defn text
  "Materialize independent immutable text only for an explicit legacy consumer.
  No text or query cache is retained by this module."
  [statement]
  (String. (.-payload (require-statement! statement)) "UTF-8"))

(defn code-placeholder?
  "Read the private ASCII snapshot with the same lexical quote/comment rules
  as ordinary SQL preparation. No text materialization or mutation permission."
  [statement]
  (@placeholder-kernel (.-payload (require-statement! statement))))

(defn prepared-wal
  "Prepare fresh, independent V1 JSONL chunks from the private ASCII snapshot.
  This is encoding only, not permission to stage or publish recovery data."
  [statement]
  (let [[total chunks] (@wal-kernel (.-payload (require-statement! statement)))
        valid? (and (integer? total) (pos? total) (vector? chunks) (seq chunks)
                    (every? (fn [[bytes used]]
                              (and (bytes? bytes) (integer? used)
                                   (<= 1 used (alength bytes) 65536))) chunks)
                    (= total (reduce + 0 (map second chunks))))]
    (when-not valid?
      (throw (ex-info "Invalid owned WAL chunks" {:type ::invalid-wal-chunks})))
    {:byte-count total :chunks chunks}))

(defn with-query-buffer
  "Copy the same private snapshot into scoped native storage for f.
  The pointer is borrowed only during f, as in native/with-query-buffer. It must
  not be retained or mutated; f remains an explicit trusted low-level consumer.
  No managed payload alias escapes. This function does not execute anything."
  [statement f]
  (require-statement! statement)
  (let [size (.-size statement)]
    (ffi/with-alloc [pointer (max 1 size)]
      (when (pos? size) (ffi/write-array pointer (.-payload statement)))
      (f {:pointer pointer :length size}))))
