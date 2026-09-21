(ns jdbc.chdb.durable.wal
  "Byte-exact Durable V1 WAL line encoding.

  The portable encoder is the compatibility oracle.  A Jolt build which
  provides `String.toDurableWalBytes` may use its allocation-reducing native
  implementation only after it produces the same bytes for the complete
  Durable record corpus.  The probe is deliberately behavioral: a compiler
  version or an advertised method name is not a correctness contract."
  (:require [clojure.data.json :as json])
  (:import [java.io ByteArrayOutputStream OutputStreamWriter]))

(defn- portable-line
  [sql]
  ;; Keep the exact established streaming map path as the compatibility
  ;; oracle. In particular, do not substitute write-str: old Jolt's known
  ;; OutputStreamWriter defect is separately gated at writer construction, and
  ;; this fallback must not silently create a third encoding behavior.
  (let [output (ByteArrayOutputStream.)
        text-output (OutputStreamWriter. output "UTF-8")]
    (json/write {"sql" sql} text-output)
    (.append text-output "\n")
    (.flush text-output)
    (.toByteArray output)))

#?(:jolt
   (defn- native-line
     [^String sql]
     ;; This is intentionally compiled only for Jolt.  Older runtimes lower
     ;; it through ordinary dispatch and the delayed behavioral probe catches
     ;; the missing method; the JVM/Babashka path never reads this form.
     (.toDurableWalBytes sql)))

(def ^:private probe-sql
  ;; Empty/ASCII, JSON quote/slash/backslash escaping, every named JSON
  ;; control escape, BMP, and astral Unicode.
  [""
   "SELECT 1"
   "SELECT \"quote\" / slash \\ backslash"
   (str "controls" (char 0) (char 8) (char 9) (char 10) (char 12) (char 13))
   "β€"
   "😀"])

#?(:jolt
   (defn- probe-native-line? []
     (try
       (every? (fn [sql]
                 (= (vec (portable-line sql))
                    (vec (native-line sql))))
               probe-sql)
       (catch Throwable _ false))))

(def ^:private native-line-capable?
  #?(:jolt (delay (probe-native-line?))
     :clj (delay false)))

(defn native-line-enabled?
  "True only when the active Jolt runtime has demonstrated native byte parity.

  This is observability for tests and diagnostics, not a promise that a
  particular compiler/version owns the primitive."
  []
  @native-line-capable?)

(defn line
  "Encode one complete SQL JSONL record as UTF-8 bytes.

  The return bytes are the sole input to the existing size accounting, spool,
  immutable publication, and replay paths."
  [sql]
  #?(:jolt (if (native-line-enabled?)
             (native-line sql)
             (portable-line sql))
     :clj (portable-line sql)))

(defn portable-line-bytes
  "Expose the established portable oracle for cross-host corpus tests only."
  [sql]
  (portable-line sql))

#?(:jolt
   (defn native-line-bytes
     "Invoke the candidate native implementation directly for parity tests.
     Callers must handle unavailable old runtimes."
     [sql]
     (native-line sql)))
