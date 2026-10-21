(require '[clojure.edn :as edn])
(defn read-edn [path] (edn/read-string (slurp path)))
(defn pct [values q]
  (nth (vec (sort values)) (max 0 (dec (long (Math/ceil (* q (count values))))))))
(let [root "/home/chuck/ai-src/evidence/stock-batch-geometry-screen-20261019/"
      results (mapv
                (fn [[batch lanes]]
                  (let [prefix (str root batch "-" lanes)
                        w (read-edn (str prefix ".writer.edn"))
                        r (read-edn (str prefix ".reader.edn"))
                        v (read-edn (str prefix ".validation.edn"))
                        events (:batches w)
                        labels [[:p50 0.50] [:p90 0.90] [:p95 0.95] [:p99 0.99]]]
                    (assert (and (:verified v) (:passed r)))
                    (assert (= (* batch 35) (:rows r) (:recovered-rows v)))
                    {:batch-limit batch :lanes lanes :physical-rows (:rows r)
                     :measured-rows (:rows w) :callbacks (count events)
                     :observed-callback-rows (vec (sort (distinct (map :rows events))))
                     :confirmed-window-rows-per-second (:confirmed-rows-per-second w)
                     :estimated-bytes-per-row
                     (/ (double (get-in w [:counter-delta :scheme-heap-bytes-allocated])) (:rows w))
                     :callback-ms (into {} (for [[label q] labels]
                                            [label (/ (pct (map :nanos events) q) 1e6)]))
                     ;; Inverse of quantiles of time/row, not upper throughput
                     ;; quantiles, and not sustained/arrival latency evidence.
                     :callback-rate-at-slow-percentile
                     (into {} (for [[label q] labels]
                                [label (/ 1e9 (pct (map #(/ (double (:nanos %)) (:rows %)) events) q))]))
                     :process-gc-count (get-in w [:counter-delta :gc-count])
                     :process-gc-real-ms (/ (get-in w [:counter-delta :process-gc-real-nanos]) 1e6)
                     :total-queue-capacity (:total-queue-capacity w)
                     :independent-recovery-passed true}))
                [[5000 1] [10000 1] [10000 2] [5000 2]])]
  (prn {:scope :stock-sdk-local-durable-batch-geometry-screen
        :runtime :official-stock-0.8.23 :engine "26.9.0"
        :source "a189afb1fff8fc674348ce2118363bd5549b2d99"
        :screen-order [[5000 1] [10000 1] [10000 2] [5000 2]]
        :physical-rows-checked (reduce + (map :physical-rows results))
        :results results :tail-qualified false :scaling-ratio-qualified false
        :note "One process per geometry, no repetitions. Callback rate is inverse time/row at slow percentiles, not sustained throughput quantiles or end-to-end span latency."}))
