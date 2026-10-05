(ns jdbc.chdb-utf8-test
  (:require [jdbc.chdb.utf8 :as utf8]
            [clojure.test :refer [deftest is run-tests]]
            #?(:jolt [clojure.java.io :as io])
            #?(:jolt [jolt.scheme :as scheme])))

(deftest exact-utf8-byte-count-preserves-host-codec
  (doseq [text ["" "ASCII\n\u0000" "\u007f\u0080\u07ff\u0800"
               "\ud7ff\ue000\uffff" "éβ€😀" "\ufeff" "a\ufeffb"
               (apply str (repeat 200 "xé😀"))]]
    (is (= (alength (.getBytes text "UTF-8")) (utf8/byte-count text)))))

#?(:jolt
   (do
     (deftest exact-native-length-resource-and-decline
       (is (= (slurp (io/resource "jdbc/chdb/utf8_length.ss"))
              @#'jdbc.chdb.utf8/length-code))
       (is (ifn? (force @#'jdbc.chdb.utf8/native-length)))
       (let [size (scheme/eval-string (slurp (io/resource "jdbc/chdb/utf8_length.ss")))]
         (is (false? (size 42)))
         (doseq [codepoint [0 127 128 2047 2048 55295 57344 65535 65536 1114111]]
           (let [text (str (char codepoint))]
             (is (= (alength (.getBytes text "UTF-8")) (size text))))))
       (with-redefs [jdbc.chdb.utf8/native-length (delay (constantly false))]
         (is (= 11 (utf8/byte-count "éβ€😀")))))

     (deftest current-native-resource-is-selected
       (is (= (slurp (io/resource "jdbc/chdb/utf8_decode.ss")) @#'jdbc.chdb.utf8/source))
       (is (true? (utf8/native-enabled?))))

     (deftest exact-scalar-corpus
       (doseq [text ["" "plain\n\r\t\u0000" "β€😀" "\ufffd" "x\ufeffy"
                    (apply str (map char [0 127 128 2047 2048 55295 57344 65533 65535]))]]
         (is (= text (utf8/try-decode (.getBytes text "UTF-8"))))))

     (deftest malformed-and-leading-bom-decline
       (doseq [values [[128] [255] [192 175] [193 191] [224 128 175]
                       [237 160 128] [244 144 128 128] [245 128 128 128]
                       [194] [224 160] [240 144 128] [226 130 65]
                       [239 187 191] [239 187 191 65]]
               prefix [[] [65 66]]
               :when (or (empty? prefix) (not= [239 187 191] (take 3 values)))]
         (is (nil? (utf8/try-decode (byte-array (map unchecked-byte (concat prefix values))))))))

     (deftest qualification-rejects-live-bad-decoder
       (let [native (deref @#'jdbc.chdb.utf8/native-decoder)]
         (is (some? native))
         (is (nil? (#'jdbc.chdb.utf8/qualified-decoder
                    (fn [bytes] (or (native bytes) "accepted malformed input")))))))

     (deftest output-ownership-and-backing-decline
       (let [bytes (.getBytes "before" "UTF-8") text (utf8/try-decode bytes)]
         (aset-byte bytes 0 (byte 120))
         (is (= "before" text))
         (is (= "xefore" (utf8/try-decode bytes))))
       (let [bytes (.getBytes (apply str (repeat 65536 "x")) "UTF-8")
             text (utf8/try-decode bytes)]
         (System/gc)
         (aset-byte bytes 0 (byte 121))
         (is (= 65536 (count text)))
         (is (= \x (first text))))
       (let [boxed ((scheme/eval-string "(lambda () (make-jolt-array (vector 65 66) 'byte))"))]
         (is (nil? (utf8/try-decode boxed))))
       (doseq [value [nil "x" [] (int-array 1)]]
         (is (nil? (utf8/try-decode value))))
       (with-redefs [jdbc.chdb.utf8/native-decoder (delay nil)]
         (is (false? (utf8/native-enabled?)))
         (is (nil? (utf8/try-decode (.getBytes "fallback" "UTF-8"))))))))

(deftest unsupported-host-declines
  #?(:jolt (is (nil? (utf8/try-decode nil)))
     :clj (do (is (false? (utf8/native-enabled?)))
              (is (nil? (utf8/try-decode (byte-array [65 66])))))))

(defn -main [& _]
  (let [r (run-tests 'jdbc.chdb-utf8-test)]
    (System/exit (if (zero? (+ (:fail r) (:error r))) 0 1))))
