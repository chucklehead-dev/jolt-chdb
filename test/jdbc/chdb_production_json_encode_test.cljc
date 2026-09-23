(ns jdbc.chdb-production-json-encode-test
  (:require [clojure.test :refer [deftest is run-tests]]
            [jdbc.chdb-durable-log-fixture :as fixture]
            [jdbc.chdb-production-json-encode :as bench]))

(deftest production-clickstack-fixture-is-exact
  (let [rows (fixture/rows-512)]
    (is (= 512 (count rows)))
    (is (= (mapv #(fixture/log-row % false) (range 512)) rows))
    (is (= 16 (count (first rows))))
    (is (= "1700000000.000000000" (get-in rows [0 "Timestamp"])))
    (is (= "1700000000.511000000" (get-in rows [511 "Timestamp"])))
    (is (= "00000000000000000000000000000001" (get-in rows [0 "TraceId"])))
    (is (= "00000000000000000000000000000200" (get-in rows [511 "TraceId"])))
    (is (= "oscope.benchmark" (get-in rows [0 "ResourceAttributes" "service.name"])))
    (is (= "15" (get-in rows [511 "LogAttributes" "benchmark.bucket"])))
    (is (= 0 (get-in rows [0 "TraceFlags"])))
    (is (= 1 (get-in rows [511 "TraceFlags"])))
    (is (= "benchmark.request" (get-in rows [0 "EventName"])))))

(deftest host-encoder-preserves-all-production-values
  (doseq [profile #?(:bb [:cheshire-production]
                     :jolt [:data-json-production :ordered-four]
                     :clj [:data-json-production])]
    (let [report (bench/report! profile 1 0)]
      (is (= :encode-only (:scope report)))
      (is (= 512 (get-in report [:fixture :rows])))
      (is (= :passed (get-in report [:fixture :decoded-value-parity])))
      (is (= "148d3d2971109c0f56598b0a7e3b1de2b2e339cfd7abc1b5dfa5b8d87b122ee7"
             (get-in report [:fixture :semantic-sha256])))
      (is (= #?(:bb 345252 :default 348836)
             (get-in report [:fixture :utf8-bytes])))
      (is (= #?(:bb "7a68762d95230e12d9001c5b9e0d90500babc10dcea8e7d6943e5fe9b632d604"
                :default "3aaa66f49724fd83d501f3f42cd9f167857f50a64008b66b2999fa2ce81a0793")
             (get-in report [:fixture :utf8-sha256])))
      (is (= (get-in report [:fixture :utf8-sha256])
             (get-in report [:fixture :utf8-array-sha256])))
      (is (= :verification-only (get-in report [:measurement :status]))))))

(defn -main [& _]
  (let [{:keys [fail error]} (run-tests 'jdbc.chdb-production-json-encode-test)]
    (System/exit (if (zero? (+ fail error)) 0 1))))
