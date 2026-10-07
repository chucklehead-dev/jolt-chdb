(ns jdbc.chdb.direct-bytes-test
  (:require [clojure.test :refer [deftest is testing]]
            [clojure.data.json :as json]
            [jolt.scheme :as scheme]
            [clojure.java.io :as io]))

(def kernel (scheme/eval-string (slurp (io/resource "jdbc/chdb/direct_bytes.ss"))))
(defn encode [rows limit] (kernel rows limit nil))
(defn guarded-encode [rows limit] (kernel rows limit @#'json/native-writer-stock))
(def decode (scheme/eval-string "(lambda (x) (utf8->string (na-bytearray->bv x)))"))
(defn reference [rows] (apply str (map #(str (json/write-str %) "\n") rows)))

(deftest exact-default-wire
  (doseq [rows [[] [nil true false] [0 1 -1 Long/MIN_VALUE Long/MAX_VALUE 18446744073709551615N]
                ["" "ascii/a\\b\"c" "é😀\n\t\r\b\f" "\u0000\u001f\u2028\u2029"]
                [[1 [] [true nil]] {"a" "é" "b" [1 2]}]
                [{"a" 1 "b" 2 "c" 3 "d" 4 "e" 5 "f" 6 "g" 7 "h" 8 "i" 9}]
                [0.0 -0.0 1.25 1.0e-7 1.0e21]]]
    (let [expected (reference rows) n (count (.getBytes expected "UTF-8"))]
      (is (= expected (decode (encode rows n))))
      (is (= expected (decode (guarded-encode rows n))))
      (when (pos? n) (is (thrown? Throwable (encode rows (dec n)))))))
  (let [rows [(apply str (repeat 10000 "é😀/"))]]
    (is (= (reference rows) (decode (encode rows 1000000))))))

(deftest explicit-diagnostic-boundary
  (doseq [rows [[{:keyword "unsupported"}] [(list 1 2)] [Double/NaN]]]
    (is (thrown? Throwable (encode rows 100000))))
  (is (= "[1]\n" (decode (encode [[1]] 100))))
  (is (= "[2]\n" (decode (encode [[2]] 100)))))

(deftest guarded-dispatch-is-live
  (with-redefs [json/-write (fn [& _] (throw (ex-info "must not invoke" {})))]
    (is (thrown? Throwable (guarded-encode [[1]] 100))))
  (is (= "[1]\n" (decode (guarded-encode [[1]] 100)))))

(deftest malformed-stock-is-not-read-positionally
  (doseq [stock [[] [1] (vec (repeat 11 nil))]]
    (is (thrown? Throwable (kernel [] 0 stock)))))

(deftest repeated-numeric-and-escape-boundaries
  (doseq [n (range -40 41)]
    (let [rows [[n (* n 1000000000) {"value" (str "é/😀\n" n)}]]]
      (is (= (reference rows) (decode (guarded-encode rows 10000)))))))
