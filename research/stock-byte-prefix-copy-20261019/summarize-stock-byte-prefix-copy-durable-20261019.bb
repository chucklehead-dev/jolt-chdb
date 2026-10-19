(require '[clojure.edn :as edn])
(defn read-edn [path] (edn/read-string (slurp path)))
(defn percentile [values q]
  (nth (vec (sort values)) (max 0 (dec (long (Math/ceil (* q (count values))))))))
(let [root "/home/chuck/ai-src/evidence/stock-byte-prefix-copy-durable-abba-20261019/"
      runs (mapv (fn [[i arm]]
                   (let [base (str root i "-" arm)
                         w (read-edn (str base ".writer.edn"))
                         r (read-edn (str base ".reader.edn"))
                         v (read-edn (str base ".validation.edn"))]
                     (assert (= arm (:byte-prefix-copy-arm w)))
                     (assert (= (= arm "ordinary") (:legacy-copy-exercised w)))
                     (assert (= (= arm "prefix") (:candidate-production-method w)))
                     (assert (and (:verified v) (:passed r) (= 175000 (:rows r))))
                     (assert (= 32 (count (:batches w))))
                     {:ordinal i :arm arm :writer w}))
                 [[0 "ordinary"] [1 "prefix"] [2 "prefix"] [3 "ordinary"]])
      summary (into {}
                (for [arm ["ordinary" "prefix"]
                      :let [writers (map :writer (filter #(= arm (:arm %)) runs))
                            callbacks (mapcat :batches writers)
                            millis (map #(/ (- (:end-nanos %) (:start-nanos %)) 1e6) callbacks)]]
                  [(keyword arm)
                   {:processes (count writers) :callbacks (count callbacks)
                    :individual-confirmed-rows-per-second (mapv :confirmed-rows-per-second writers)
                    :pooled-confirmed-rows-per-second
                    (/ (* 1e9 (reduce + (map :rows writers)))
                       (reduce + (map :confirmed-window-nanos writers)))
                    :estimated-bytes-per-row
                    (double (/ (reduce + (map #(get-in % [:counter-delta :scheme-heap-bytes-allocated]) writers))
                               (reduce + (map :rows writers))))
                    :callback-ms (into {} (for [[label q] [[:p50 0.5] [:p90 0.9] [:p95 0.95] [:p99 0.99]]]
                                           [label (percentile millis q)]))}]))
      rates (get-in summary [:ordinary :individual-confirmed-rows-per-second])
      drift (* 100 (/ (abs (- (first rates) (second rates))) (first rates)))
      change (* 100 (dec (/ (get-in summary [:prefix :pooled-confirmed-rows-per-second])
                            (get-in summary [:ordinary :pooled-confirmed-rows-per-second]))))]
  (prn {:scope :stock-local-sdk-byte-prefix-copy-abba :physical-rows-checked 700000
        :runtime :official-stock-0.8.23 :batch-size 5000 :summary summary
        :control-window-drift-percent drift :apparent-pooled-rate-change-percent change
        :throughput-speedup-qualified false :tail-qualified false
        :note "Short controlled research. Compare control drift with rate difference; allocation and latency are distinct. Do not infer production tails."}))
