(require '[clojure.edn :as edn] '[clojure.string :as str])
(load-file "/home/chuck/ai-src/evidence/validate-stock-batch-geometry-20261019.bb")
(let [prefix "/home/chuck/ai-src/evidence/stock-output-prefix-durable-abba-20261019/0-ordinary"
      writer (assoc (edn/read-string (slurp (str prefix ".writer.edn")))
                    :research-selector :batch-geometry-only)
      reader (edn/read-string (slurp (str prefix ".reader.edn")))
      check (fn [w r batch lanes]
              (with-out-str (validate! w r batch lanes)))
      rejects? (fn [w r batch lanes intended-check]
                 (try (check w r batch lanes) false
                      (catch AssertionError error
                        (str/includes? (.getMessage error) intended-check))))]
  (assert (:verified (edn/read-string (check writer reader 5000 1))))
  ;; Synthetic receipt geometry checks only, not evidence of real 10k recovery.
  (let [twice (fn [stats] (-> stats (update :attempted-span-count * 2)
                                   (update :exported-span-count * 2)))
        larger (-> writer (assoc :rows 320000 :batch-size 10000 :trace-count 1250
                                  :expected-lane-counts [320000])
                          (update :stats #(mapv twice %))
                          (update :batches #(mapv (fn [event] (update event :rows * 2)) %)))
        recovered (assoc reader :rows 350000 :groups 10000 :trace-count 1250)]
    (assert (:verified (edn/read-string (check larger recovered 10000 1))))
    (assert (rejects? larger (assoc recovered :groups 5000) 10000 1 ":groups")))
  (doseq [[w r batch lanes intended-check]
          [[writer reader 10000 1 ":batch-size"]
           [writer reader 5000 2 ":distinct-exporter-count"]
           [(assoc writer :rows 159999) reader 5000 1 ":rows"]
           [(assoc-in writer [:stats 0 :dropped-count] 1) reader 5000 1 ":dropped-count"]
           [(assoc-in writer [:batches 0 :ok] false) reader 5000 1 ":ok"]
           [(assoc writer :per-trace-callback-order-checked false) reader 5000 1 ":per-trace-callback-order-checked"]
           [writer (assoc reader :rows 174999) 5000 1 ":groups"]
           [writer (assoc reader :per-record-multiplicity 34) 5000 1 ":per-record-multiplicity"]
           [writer (assoc reader :full-physical-rows-equal false) 5000 1 ":full-physical-rows-equal"]]]
    (assert (rejects? w r batch lanes intended-check)))
  (prn {:validator-positive-controls 2 :rejected-negative-controls 10 :passed true}))
