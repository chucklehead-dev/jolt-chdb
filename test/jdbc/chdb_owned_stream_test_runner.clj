(ns jdbc.chdb-owned-stream-test-runner
  (:require [clojure.test :as test]
            [jdbc.chdb-owned-compact-stream-test]
            [jdbc.chdb-owned-stream-option-test]
            [jdbc.chdb-owned-stream-fixture-test]))

(defn -main [& _]
  (let [result (test/run-tests 'jdbc.chdb-owned-compact-stream-test
                               'jdbc.chdb-owned-stream-option-test
                               'jdbc.chdb-owned-stream-fixture-test)]
    (System/exit (if (zero? (+ (:fail result) (:error result))) 0 1))))
