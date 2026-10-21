(require '[clojure.edn :as edn])
(defn read-edn [path] (edn/read-string (slurp path)))
(defn pct [xs q]
  (nth (vec (sort xs)) (max 0 (dec (long (Math/ceil (* q (count xs))))))))
(let [[prefix] *command-line-args*
      p (read-edn (str prefix ".worker-profile.edn"))
      w (read-edn (str prefix ".writer.edn"))
      v (read-edn (str prefix ".validation.edn"))
      _ (assert (:verified v))
      events (:events p)
      rows (:rows w)
      summary (into (sorted-map)
                (for [[phase xs] (group-by :phase events)
                      :let [cpu? (every? :process-cpu-ticks xs)
                            counters? (every? :counters xs)]]
                  [phase {:samples (count xs)
                          :total-inclusive-ms (/ (reduce + (map :nanos xs)) 1e6)
                          :p50-ms (/ (pct (map :nanos xs) 0.5) 1e6)
                          :p95-ms (/ (pct (map :nanos xs) 0.95) 1e6)
                          :process-cpu-seconds (when cpu?
                                                 (/ (reduce + (mapcat :process-cpu-ticks xs))
                                                    (double (:ticks-per-second p))))
                          :estimated-bytes-per-row (when counters?
                                                     (/ (double (reduce + (map #(get-in % [:counters :scheme-heap-bytes-allocated]) xs))) rows))}]))]
  (prn {:scope :single-lane-private-wal-phase-diagnostic-summary :arm (:arm p)
        :confirmed-window-rows-per-second (:confirmed-rows-per-second w)
        :physical-rows-checked (:recovered-rows v) :phase-summary summary
        :nested-and-inclusive true :process-cpu-coarse-non-atomic true
        :performance-qualified false
        :note "Diagnostic overhead included; nested phase totals overlap. Do not add parent/child totals or compare profile window rates as a performance win."}))
