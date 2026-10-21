(require '[jolt.host :as host] '[jdbc.chdb.byte-output :as output])
(import '[java.io FileOutputStream] '[java.nio.file Files] '[java.nio.file.attribute FileAttribute])
(defn sample [] {:live (host/bytes-allocated) :gc (host/gc-bytes) :count (host/gc-count)})
(defn percentile [xs q]
  (nth (vec (sort xs)) (max 0 (dec (long (Math/ceil (* q (count xs))))))))
(let [results
      (mapv
        (fn [n]
          (let [input (byte-array n (byte 65)) reps (if (= n 65536) 64 1)
                expected (byte-array (* n reps) (byte 65))
                path (Files/createTempFile "jchdb-output-component-" ".bin" (into-array FileAttribute []))]
            (try
              (let [run (fn [mode]
                          (with-open [out (FileOutputStream. (.toFile path))]
                            (let [before (sample) start (System/nanoTime)]
                              (dotimes [_ reps]
                                (if (= mode :ordinary) (.write out input 0 n)
                                    (output/write-prefix! out input n)))
                              (let [elapsed (- (System/nanoTime) start) after (sample)]
                                {:mode mode :nanos-per-write (/ elapsed (double reps))
                                 :estimated-bytes-per-write
                                 (/ (- (+ (:live after) (:gc after)) (+ (:live before) (:gc before))) (double reps))
                                 :gc-count (- (:count after) (:count before))}))))
                    check! (fn [] (assert (java.util.Arrays/equals expected (Files/readAllBytes path))))
                    _ (doseq [mode [:ordinary :prefix :prefix :ordinary]] (run mode) (check!))
                    observations (mapv (fn [i]
                                         (let [result (run (nth [:ordinary :prefix :prefix :ordinary] (mod i 4)))]
                                           (check!) (assoc result :ordinal i))) (range 32))]
                {:bytes n :repetitions reps :observations observations
                 :summary (into {} (for [mode [:ordinary :prefix]
                                         :let [xs (filter #(= mode (:mode %)) observations)]]
                                     [mode {:samples (count xs)
                                            :write-ms (into {} (for [[label q] [[:p50 0.5] [:p90 0.9] [:p95 0.95] [:p99 0.99]]]
                                                                 [label (/ (percentile (map :nanos-per-write xs) q) 1e6)]))
                                            :median-estimated-bytes-per-write
                                            (percentile (map :estimated-bytes-per-write xs) 0.5)}]))})
              (finally (Files/deleteIfExists path))))) [65536 8388608])]
  (spit (first *command-line-args*)
    (pr-str {:scope :stock-output-prefix-component :runtime :official-stock-0.8.23
             :full-file-readback true :explicit-flush-and-close-outside-timing true
             :fsync-included false :durable-io false :tail-qualified false :results results}))
  (doseq [result results] (prn (dissoc result :observations))))
