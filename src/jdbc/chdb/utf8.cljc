(ns jdbc.chdb.utf8
  "Internal, behavior-qualified UTF-8 decoding and sizing with explicit decline."
  #?(:jolt (:require [clojure.java.io :as io] [jolt.scheme :as scheme])))

#?(:jolt
   (do
     (defmacro ^:private native-source []
       (when-let [resource (io/resource "jdbc/chdb/utf8_decode.ss")]
         (slurp resource)))
     (def ^:private source (native-source))
     (defn- qualified-decoder [decode]
       (let [valid ["" "plain\n\u0000" "β€😀" "legitimate\ufffd" "mid\ufeffbom"]
             invalid [[128] [192 175] [224 128 175] [237 160 128]
                      [244 144 128 128] [245 128 128 128] [194] [226 130]
                      [226 130 65] [239 187 191 65]]]
         (when (and (every? #(= % (decode (.getBytes % "UTF-8"))) valid)
                    (every? #(false? (decode (byte-array (map unchecked-byte %)))) invalid))
           decode)))
     (def ^:private native-decoder
       (delay
         (try
           (when (string? source)
             (qualified-decoder (scheme/eval-string source)))
           (catch Throwable _ nil))))))

#?(:jolt
   (do
     (defmacro ^:private length-source []
       (when-let [resource (io/resource "jdbc/chdb/utf8_length.ss")]
         (slurp resource)))
     (def ^:private length-code (length-source))
     (def ^:private native-length
       (delay
         (try
           (when (string? length-code)
             (let [size (scheme/eval-string length-code)]
               (when (every? #(= (alength (.getBytes % "UTF-8")) (size %))
                             ["" "ASCII\n\u0000" "\u007f\u0080\u07ff\u0800"
                              "\ud7ff\ue000\uffff" "β€😀" "\ufeff"])
                 size)))
           (catch Throwable _ nil))))))

(defn byte-count
  "Exact encoded UTF-8 size of an immutable String. A qualified Jolt source
  kernel avoids allocating an otherwise unused byte array. Unsupported hosts
  or declined characters use the same host codec as payload encoding; no AOT
  capability or changed malformed-character semantics are claimed."
  [text]
  #?(:jolt (if-let [size @native-length]
             (let [result (size text)]
               (if (false? result) (alength (.getBytes text "UTF-8")) result))
             (alength (.getBytes text "UTF-8")))
     :clj (alength (.getBytes ^String text "UTF-8"))))

(defn native-enabled?
  "True only after source-mode behavioral qualification, not an AOT claim."
  [] #?(:jolt (some? @native-decoder) :clj false))

(defn try-decode
  "Return an independent String only for a qualified well-formed input.
  Nil means declined: the caller must use its existing codec and error behavior.
  Input is borrowed read-only during the call and must not be mutated concurrently.
  Unsupported hosts/backings, malformed input and leading BOMs decline."
  [bytes]
  #?(:jolt (when-let [decode @native-decoder]
             (let [text (decode bytes)] (when (string? text) text)))
     :clj nil))
