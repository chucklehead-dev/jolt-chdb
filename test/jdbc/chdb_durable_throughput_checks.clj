(ns jdbc.chdb-durable-throughput-checks
  "Shared aggregate entrypoint: use each custom suite's throwing CLI contract."
  (:require [jdbc.chdb-durable-throughput-test :as throughput]
            [jdbc.chdb-durable-throughput-metrics-test :as metrics]))

(defn run-checks! []
  ;; run-checks! in these namespaces only increments namespace-local counters.
  ;; Their -main functions reset/check those counters and run all subgroups.
  (throughput/-main)
  (metrics/-main))
