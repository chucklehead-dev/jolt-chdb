(ns jdbc.chdb.owned-statement
  "Experimental internal owned ASCII statement for explicit owned requests.
  Ownership is enforced at the supported API, not against reflection, private
  Var access or unsafe FFI. Caller input must not race with snapshot creation."
  (:require [clojure.java.io :as io] [jolt.scheme :as scheme] [jolt.ffi :as ffi]
            [jdbc.chdb.byte-copy :as byte-copy]
            [jdbc.chdb.byte-output :as byte-output]
            [jdbc.chdb.byte-record :as byte-record]))

(def ^:private max-snapshot-bytes (* 64 1024 1024))
(def ^:private max-precomputed-input-bytes (quot (- (* 128 1024 1024) 11) 6))
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

(deftype ^:private PreparedWalRecord [size chunks]
  byte-record/ByteRecord
  (byte-count [_] size)
  (write-to! [_ output]
    (doseq [[bytes used] chunks]
      (byte-output/write-private-prefix! output bytes used))
    nil)
  Object
  (toString [_] "#<prepared WAL record>"))

(deftype ^:private OwnedAsciiStatement [seal payload size prepared]
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
        (OwnedAsciiStatement. statement-seal snapshot (alength snapshot) nil)))))

(defn byte-count [statement]
  (.-size (require-statement! statement)))

(defn text
  "Materialize independent immutable text only for an explicit legacy consumer.
  No decoded String is retained. Optional prepared WAL bytes belong only to
  their opaque statement, not to a process-global query cache."
  [statement]
  (String. (.-payload (require-statement! statement)) "UTF-8"))

(defn code-placeholder?
  "Read the private ASCII snapshot with the same lexical quote/comment rules
  as ordinary SQL preparation. No text materialization or mutation permission."
  [statement]
  (@placeholder-kernel (.-payload (require-statement! statement))))

(defn- encode-wal
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

(defn prepared-wal
  "Return fresh, independent V1 JSONL chunks from the private ASCII snapshot.
  Cached records copy their bytes; no mutable backing alias is exposed."
  [statement]
  (if-let [prepared (.-prepared (require-statement! statement))]
    {:byte-count (.-size prepared)
     :chunks (mapv (fn [[bytes used]] [(aclone bytes) used]) (.-chunks prepared))}
    (encode-wal statement)))

(defn precompute-wal
  "Return an opaque statement with private WAL preparation completed now.
  No admission, native operation, storage, or acknowledgement occurs. The
  snapshot is unchanged and the original statement remains usable. Repeated
  preparation is idempotent. Requests whose worst-case encoded size exceeds
  128MiB retain the ordinary path rather than creating a retained cache."
  [statement]
  (require-statement! statement)
  (if (or (.-prepared statement)
          (> (.-size statement) max-precomputed-input-bytes))
    statement
    (let [{:keys [byte-count chunks]} (encode-wal statement)]
      (OwnedAsciiStatement. statement-seal (.-payload statement) (.-size statement)
                            (PreparedWalRecord. byte-count chunks)))))

(defn prepared-wal-for-write
  "Internal writer view: an opaque immutable record when precomputed, or fresh
  legacy chunks otherwise. Public prepared-wal always returns independent bytes."
  [statement]
  (or (.-prepared (require-statement! statement)) (encode-wal statement)))

(defn precomputed-retained-byte-count
  "Observe private SQL and cached WAL array capacities without copying bytes.
  Return nil unless WAL was actually precomputed; this never starts encoding.
  Includes unused chunk capacity, not only bytes that will be written. Object
  metadata, projection temporaries, native query copies and total RSS are NOT
  included. This observation can shrink an already acquired preparation budget;
  it is not a substitute for reserving BEFORE encoding starts."
  [statement]
  (let [statement (require-statement! statement)]
    (when-let [prepared (.-prepared statement)]
      (reduce (fn [total chunk] (+ total (alength (first chunk))))
              (.-size statement) (.-chunks prepared)))))

(defn precomputed-input-byte-limit
  "Largest snapshot eligible for cached WAL preparation under its worst-case
  expansion guard. This is not a total heap/RSS limit."
  []
  max-precomputed-input-bytes)

(defn with-query-buffer
  "Copy the same private snapshot into scoped native storage for f.
  The pointer is borrowed only during f, as in native/with-query-buffer. It must
  not be retained or mutated; f remains an explicit trusted low-level consumer.
  No managed payload alias escapes. This function does not execute anything."
  [statement f]
  (require-statement! statement)
  (let [size (.-size statement)
        ;; Reserved Jolt primitive paired with ffi/free, not ffi/alloc's
        ;; zeroed public wrapper. Every logical byte is copied before f can
        ;; observe the buffer; no managed pointer or backing alias is lent.
        pointer (ffi/__alloc (max 1 size))]
    (try
      (if (pos? size)
        (when-not (= size (byte-copy/write-prefix! pointer (.-payload statement) size))
          (throw (ex-info "Private query copy did not complete" {:type ::incomplete-copy})))
        ;; Preserve the earlier one-byte zeroed empty-buffer sentinel too.
        (ffi/write pointer :uint8 0))
      (f {:pointer pointer :length size})
      (finally (ffi/free pointer)))))
