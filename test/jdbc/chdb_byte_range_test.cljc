(ns jdbc.chdb-byte-range-test
  (:require [jdbc.chdb.byte-range :as scan]
            [clojure.test :refer [deftest is run-tests]]
            #?(:jolt [jolt.scheme :as scheme])))

(deftest independent-byte-and-range-corpus
  (doseq [n [0 1 2 63 64 65 65536]
          target [0 10 127 128 255]
          hit [-1 0 (dec n)]
          start [0 (quot n 2) n]
          :let [bytes (byte-array n (unchecked-byte 42))]]
    (when (<= 0 hit (dec n)) (aset-byte bytes hit (unchecked-byte target)))
    (is (= (if (<= start hit (dec n)) hit n)
           (scan/index-of-byte bytes target start n)))))

(deftest invalid-ranges-and-targets-reject
  (doseq [[target start end] [[-1 0 1] [256 0 1] [10 -1 1]
                            [10 1 0] [10 0 2] [10 0.5 1]]]
    (is (= ::scan/invalid-range
           (try (scan/index-of-byte (byte-array 1) target start end) nil
                (catch Exception error (:type (ex-data error))))))))

(deftest invalid-input-types-reject
  (doseq [bytes [nil [] "x" (int-array 1)]]
    (is (= ::scan/invalid-range
           (try (scan/index-of-byte bytes 10 0 1) nil
                (catch Exception error (:type (ex-data error))))))))

(deftest all-unsigned-targets-and-subranges
  (let [bytes (byte-array (map unchecked-byte (range 256)))]
    (doseq [target (range 256) start [0 1 127 128 255 256]]
      (is (= (if (< target start) 256 target)
             (scan/index-of-byte bytes target start 256))))))

(deftest first-match-respects-both-range-boundaries
  ;; Search only the selected window, including repeated matches and a match
  ;; exactly at END. The immutable positions are an independent result oracle.
  (let [values [10 128 10 255 0 10 128 255 10]
        bytes (byte-array (map unchecked-byte values))]
    (doseq [target [0 10 127 128 255]
            start (range (inc (count values)))
            end (range start (inc (count values)))]
      (is (= (or (first (keep-indexed
                        (fn [index value]
                          (when (and (<= start index) (< index end)
                                     (= target value))
                            index))
                        values))
                 end)
             (scan/index-of-byte bytes target start end))))))

(deftest scan-does-not-retain-or-mutate-input
  (let [bytes (byte-array (map unchecked-byte [128 10 255]))]
    (is (= 1 (scan/index-of-byte bytes 10 0 3)))
    (is (= [-128 10 -1] (vec bytes)))
    (aset-byte bytes 1 (byte 11))
    (is (= 3 (scan/index-of-byte bytes 10 0 3)))
    (is (= 1 (scan/index-of-byte bytes 11 0 3)))))

#?(:jolt
   (deftest selected-and-boxed-backing-paths-are-live
     (is (true? (scan/native-enabled?)))
     (let [boxed ((scheme/eval-string
                   "(lambda () (make-jolt-array (vector -128 10 -1) 'byte))"))
           kernel (deref (deref (var jdbc.chdb.byte-range/native-search)))]
       (is (false? (kernel boxed 10 0 3)) "boxed backing really declines")
       (is (false? (kernel (byte-array 1) 10 0 2)) "invalid raw range declines")
       (is (= 1 (scan/index-of-byte boxed 10 0 3)))
       (is (= 2 (scan/index-of-byte boxed 255 0 3))))
     (with-redefs [jdbc.chdb.byte-range/native-search (delay nil)]
       (is (false? (scan/native-enabled?)))
       (is (= 1 (scan/index-of-byte (byte-array [0 10]) 10 0 2))))))

(defn -main [& _]
  (let [r (run-tests 'jdbc.chdb-byte-range-test)]
    (System/exit (if (zero? (+ (:fail r) (:error r))) 0 1))))
