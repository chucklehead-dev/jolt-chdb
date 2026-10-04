(ns jdbc.chdb.utf8
  "Internal, behavior-qualified strict UTF-8 decoding with explicit decline."
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
