(require '[jolt.ffi :as ffi] '[jolt.host :as host]
         '[jdbc.chdb.byte-copy :as copy])
(defn sample [] {:live (host/bytes-allocated) :gc (host/gc-bytes) :count (host/gc-count)})
(defn estimated-allocation [before after]
  (- (+ (:live after) (:gc after)) (+ (:live before) (:gc before))))
(defn percentile [values q]
  (nth (vec (sort values)) (max 0 (dec (long (Math/ceil (* q (count values))))))))
(let [results
      (mapv
        (fn [n]
          (let [input (byte-array n (byte 65)) reps (if (= n 65536) 64 1)]
            (ffi/with-alloc [pointer (+ n 2)]
              (ffi/write pointer :byte 91)
              (ffi/write pointer :byte 92 (inc n))
              (let [run (fn [mode]
                          (dotimes [_ reps]
                            (assert (= n (if (= mode :ordinary)
                                           (ffi/write-array (+ pointer 1) input 0 n)
                                           (copy/write-prefix! (+ pointer 1) input n))))))
                    check! (fn []
                             (assert (= 91 (ffi/read pointer :byte)))
                             (assert (= 92 (ffi/read pointer :byte (inc n))))
                             (assert (java.util.Arrays/equals input (ffi/read-array (+ pointer 1) :byte n))))
                    _ (doseq [mode [:ordinary :prefix :prefix :ordinary]] (run mode) (check!))
                    observations
                    (mapv (fn [i]
                            (let [mode (nth [:ordinary :prefix :prefix :ordinary] (mod i 4))
                                  before (sample) start (System/nanoTime)
                                  _ (run mode) elapsed (- (System/nanoTime) start) after (sample)]
                              (check!)
                              {:ordinal i :mode mode :nanos-per-copy (/ elapsed (double reps))
                               :estimated-bytes-per-copy (/ (estimated-allocation before after) (double reps))
                               :gc-count (- (:count after) (:count before))})) (range 32))]
                {:bytes n :repetitions reps :observations observations
                 :summary (into {} (for [mode [:ordinary :prefix]
                                         :let [xs (filter #(= mode (:mode %)) observations)]]
                                     [mode {:samples (count xs)
                                            :copy-ms (into {} (for [[label q] [[:p50 0.5] [:p90 0.9] [:p95 0.95] [:p99 0.99]]]
                                                                [label (/ (percentile (map :nanos-per-copy xs) q) 1e6)]))
                                            :median-estimated-bytes-per-copy
                                            (percentile (map :estimated-bytes-per-copy xs) 0.5)}]))}))))
        [65536 8388608])]
  (spit (first *command-line-args*)
    (pr-str {:scope :stock-byte-prefix-copy-component :runtime :official-stock-0.8.23
             :full-readback-and-guards true :native-execution false :durable-execution false
             :counter-snapshots-non-atomic true :tail-qualified false :results results}))
  (doseq [result results] (prn (dissoc result :observations))))
