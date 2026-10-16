(ns jdbc.chdb-durable-throughput-exit-test
  (:require [clojure.test :refer [deftest is run-tests]]
            [jdbc.chdb-durable-throughput-checks :as aggregate]
            [jdbc.chdb-durable-throughput-test :as throughput]
            [jdbc.chdb-durable-throughput-metrics-test :as metrics]))

(defn- observed [failing-group]
  (let [prior-throughput @throughput/failures prior-metrics @metrics/failures
        quiet (fn [] nil)]
    (try
      (with-redefs-fn
        {#'throughput/run-checks! #(reset! throughput/failures (if (= :throughput failing-group) 1 0))
         #'metrics/run-checks! #(reset! metrics/failures (if (= :metrics failing-group) 1 0))
         #'throughput/worker-contract-checks! quiet
         #'throughput/commit-inclusive-checks! quiet
         #'throughput/commit-inclusive-result-check! quiet
         #'throughput/counter-ledger-contract-checks! quiet
         #'throughput/worker-failure-receipt-checks! quiet
         #'throughput/worker-launch-command-checks! quiet
         #'throughput/orchestration-contract-checks! quiet}
        (fn []
          (try (aggregate/run-checks!) :passed
               (catch Throwable e {:failures (:failures (ex-data e))}))))
      (finally
        (reset! throughput/failures prior-throughput)
        (reset! metrics/failures prior-metrics)))))

(deftest aggregate-rejects-each-childs-private-failure-counter
  (is (= {:failures 1} (observed :throughput)))
  (is (= {:failures 1} (observed :metrics))))

(deftest aggregate-accepts-successful-children
  (is (= :passed (observed :none))))

(defn -main [& _]
  (let [result (run-tests 'jdbc.chdb-durable-throughput-exit-test)]
    (when (pos? (+ (:fail result) (:error result)))
      (throw (ex-info "Throughput aggregate exit controls failed" {})))))
