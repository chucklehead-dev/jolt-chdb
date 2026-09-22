(ns jdbc.chdb.durable.wal
  "Byte-exact Durable V1 WAL line encoding.

  The portable encoder is the compatibility oracle.  A Jolt build which
  provides `String.toDurableWalBytes` may use its allocation-reducing native
  implementation only after it produces the same bytes for the complete
  Durable record corpus.  The probe is deliberately behavioral: a compiler
  version or an advertised method name is not a correctness contract."
  (:require [clojure.data.json :as json])
  (:import [java.io StringWriter]))

(defn- portable-line
  [sql]
  ;; `OutputStreamWriter.append(CharSequence, start, end)` is not correct on
  ;; stock Jolt 0.8.10. data.json may use that overload while emitting a run,
  ;; so do not put its output through that writer. StringWriter keeps the
  ;; serializer's characters in managed memory; the one final UTF-8 conversion
  ;; supplies the exact byte record used for accounting, persistence and replay.
  (let [output (StringWriter.)]
    (json/write {"sql" sql} output)
    (.append output "\n")
    (.getBytes (.toString output) "UTF-8")))

#?(:jolt
   (defn- native-line
     [^String sql]
     ;; This is intentionally compiled only for Jolt.  Older runtimes lower
     ;; it through ordinary dispatch and the delayed behavioral probe catches
     ;; the missing method; the JVM/Babashka path never reads this form.
     (.toDurableWalBytes sql)))

(defn- native-probe-sql []
  ;; Keep native selection behavioral and broad. The test corpus independently
  ;; specifies the expected JSONL octets for these representative boundaries;
  ;; this production check then requires a candidate primitive to match the
  ;; established managed fallback for every one of them.
  (let [boundaries [(char 0) (char 1) (char 7) (char 8) (char 9)
                    (char 10) (char 12) (char 13) (char 31) (char 32)
                    (char 34) (char 47) (char 92) (char 126) (char 127)
                    (char 128) (char 255) (char 2047) (char 2048)
                    (char 55295) (char 57344) (char 8232) (char 8233)
                    (char 65535)]
        prefixes [0 1 2 63 64 127 128 1023]]
    (concat
     ["" "SELECT 1" "SELECT \"quote\" / slash \\ backslash"
      (str "controls" (char 0) (char 8) (char 9) (char 10) (char 12) (char 13))
      (apply str boundaries) "β€😀" "quote\"slash/backslash\\"]
     (map (fn [length]
            (str (apply str (repeat length "a")) "\"/\\β😀"))
          prefixes)
     (for [index (range 128)]
       (str "generated-" index "-"
            (apply str
                   (map (fn [offset]
                          (nth boundaries
                               (mod (+ (* 17 index) (* 11 offset))
                                    (count boundaries))))
                        (range (inc (mod index 17)))))
            (if (zero? (mod index 3)) "😀" "β"))))))

#?(:jolt
   (defn- probe-native-line? []
     (try
       (every? (fn [sql]
                 (= (vec (portable-line sql))
                    (vec (native-line sql))))
               (native-probe-sql))
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
