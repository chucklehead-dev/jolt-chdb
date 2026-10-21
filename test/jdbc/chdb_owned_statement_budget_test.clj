(ns jdbc.chdb-owned-statement-budget-test
  (:require [clojure.test :refer [deftest is run-tests]]
            [jdbc.chdb.owned-statement :as owned]))

(deftest observation-neither-prepares-nor-copies-public-wal
  (is (= (quot (- (* 128 1024 1024) 11) 6) (owned/precomputed-input-byte-limit)))
  (let [statement (owned/try-snapshot (.getBytes "INSERT INTO t VALUES (1)" "UTF-8"))]
    (is (nil? (owned/precomputed-retained-byte-count statement)))
    (let [prepared (owned/precompute-wal statement)
          expected (+ (owned/byte-count prepared)
                      (reduce + (map #(alength (first %))
                                     (:chunks (owned/prepared-wal prepared)))))]
      (with-redefs [owned/prepared-wal (fn [_] (throw (ex-info "unexpected defensive copy" {})))
                    owned/precompute-wal (fn [_] (throw (ex-info "unexpected preparation" {})))]
        (is (= expected (owned/precomputed-retained-byte-count prepared)))
        (is (nil? (owned/precomputed-retained-byte-count statement)))))))

(deftest chunk-capacity-not-used-length-determines-retained-byte-count
  ;; Zero bytes take the worst-case six-byte JSON escape. This is encoding-only
  ;; input, never executed as SQL. Exercise small-buffer and full-chunk edges.
  (doseq [n [0 1 243 244 245 65520 65524 65525 65526 131072]]
    (let [prepared (owned/precompute-wal (owned/try-snapshot (byte-array n)))
          {:keys [byte-count chunks]} (owned/prepared-wal prepared)
          actual (owned/precomputed-retained-byte-count prepared)
          expected (+ n (reduce + (map #(alength (first %)) chunks)))]
      (is (= (+ (* 6 n) 11) byte-count))
      (is (= expected actual))
      (is (> actual (+ n byte-count)) "chunk padding remains retained"))))

(deftest arbitrary-values-are-not-retained-statements
  (doseq [value [nil {} (byte-array 1) "SELECT 1"]]
    (is (thrown? clojure.lang.ExceptionInfo
                 (owned/precomputed-retained-byte-count value)))))

(defn -main [& _]
  (let [r (run-tests 'jdbc.chdb-owned-statement-budget-test)]
    (when-not (and (= 3 (:test r)) (pos? (:pass r)))
      (throw (ex-info "Owned-statement budget test inventory did not execute" {})))
    (System/exit (if (zero? (+ (:fail r) (:error r))) 0 1))))
