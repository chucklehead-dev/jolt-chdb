(require '[jdbc.chdb.owned-statement :as owned] '[jdbc.chdb.byte-record :as record]
         '[jolt.host :as host])
(defn pct [xs q]
  (nth (vec (sort xs)) (max 0 (dec (long (Math/ceil (* q (count xs))))))))
(let [context {:prepare-statement @(ns-resolve 'jdbc.chdb.owned-statement 'precompute-wal)}
      results (mapv
                (fn [n]
                  (let [statement (owned/try-snapshot (byte-array n (byte 97)))
                        run (fn [mode]
                              (let [start (System/nanoTime)
                                    result (if (= mode :direct) (owned/precompute-wal statement)
                                               ((:prepare-statement context) statement))
                                    elapsed (- (System/nanoTime) start)]
                                (assert (= (+ n 11) (record/byte-count (owned/prepared-wal-for-write result))))
                                {:mode mode :nanos elapsed}))
                        _ (doseq [mode [:direct :captured :captured :direct]] (run mode))
                        samples (mapv #(run (nth [:direct :captured :captured :direct] (mod % 4))) (range 32))]
                    {:bytes n :samples samples
                     :summary (into {} (for [mode [:direct :captured]
                                              :let [xs (map :nanos (filter #(= mode (:mode %)) samples))]]
                                          [mode (into {} (for [[label q] [[:p50 0.5] [:p90 0.9] [:p95 0.95] [:p99 0.99]]]
                                                           [label (/ (pct xs q) 1e6)]))]))}))
                [65536 8388608])]
  (spit (first *command-line-args*)
        (pr-str {:scope :direct-versus-captured-private-preparation-call
                 :runtime :official-stock-0.8.23 :scheme (host/scheme-version)
                 :durable-io false :results results}))
  (doseq [result results] (prn (dissoc result :samples))))
