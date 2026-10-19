;; Single-lane diagnostic only. Do not use this current-context holder across
;; concurrent exporters. Native/caller phase intervals are inclusive and nested.
(require '[jdbc.chdb :as driver] '[jdbc.chdb.native :as native]
         '[jdbc.chdb.owned-statement :as owned]
         '[jdbc.chdb.durable :as durable] '[jdbc.chdb.durable.control :as control]
         '[jdbc.chdb.durable.digest :as digest] '[clojure.edn :as edn]
         '[clojure.string :as str] '[jolt.codec.experimental :as codec]
         '[otel.exporter.chdb :as e] '[otel.exporter.chdb-benchmark :as bench])
(defn profile-process-cpu []
  (let [stat (slurp "/proc/self/stat")
        fields (str/split (subs stat (+ 2 (.lastIndexOf stat ")"))) #"\s+")]
    [(parse-long (nth fields 11)) (parse-long (nth fields 12))]))
(let [[root arm] *command-line-args* context (atom nil) calls (atom 0) events (atom [])
      _ (assert (contains? #{"ordinary" "cached"} arm))
      sample #'bench/counter-sample delta #'bench/counter-delta
      record! (fn [event] (assert (< (count @events) 1024)) (swap! events conj event))
      around (fn [phase f]
               (fn [& args]
                 (if-let [stamp @context]
                   (let [thread (.getName (Thread/currentThread))
                         cpu-before (profile-process-cpu) before (sample) start (System/nanoTime)
                         result (apply f args) end (System/nanoTime) after (sample)
                         cpu-after (profile-process-cpu)]
                     (record! (assoc stamp :phase phase :status :complete :thread thread
                                     :nanos (- end start) :counters (delta before after)
                                     :process-cpu-ticks (mapv - cpu-after cpu-before)))
                     result)
                   (apply f args))))
      writer-dbspec durable/writer-dbspec compact @#'e/execute-closed-compact-rows!
      prepare owned/precompute-wal
      observer (fn [event]
                 (when-let [stamp @context]
                   (assert (contains? #{:wal-prepare :wal-append :wal-join
                                        :wal-immutable-put :wal-immutable-verify :wal-head-cas}
                                      (:phase event)))
                   (record! (merge stamp (select-keys event [:phase :status :calls :nanos :bytes])
                                   {:thread (.getName (Thread/currentThread))}))))]
  (with-redefs-fn
    {#'durable/writer-dbspec
     (fn [options]
       (assert (or (nil? (get-in options [:operations :writer-phase!]))
                   (identical? observer (get-in options [:operations :writer-phase!]))))
       (writer-dbspec (assoc-in options [:operations :writer-phase!] observer)))
     #'e/execute-closed-compact-rows!
     (fn [& args]
       (let [call (swap! calls inc)]
         (assert (<= call 35)) (assert (nil? @context))
         (reset! context (when (> call 3) {:ordinal (- call 4)}))
         (try (apply (around :compact-total compact) args)
              (finally (reset! context nil)))))
     #'owned/precompute-wal (around :private-wal-precompute (if (= arm "cached") prepare identity))
     #'codec/encode-prefixed-records (around :projection-and-encoding codec/encode-prefixed-records)
     #'durable/execute-owned-and-flush! (around :native-and-confirm durable/execute-owned-and-flush!)
     #'native/classify-query-buffer! (around :native-classify native/classify-query-buffer!)
     #'driver/execute-owned-compact-json-stream-with-query-buffer
     (around :native-stream driver/execute-owned-compact-json-stream-with-query-buffer)
     #'control/publish-wal-file! (around :wal-publish control/publish-wal-file!)
     #'control/commit-reference! (around :head-commit control/commit-reference!)
     #'digest/sha256-file (around :file-digest digest/sha256-file)}
    (fn []
      (with-bindings {#'*command-line-args* [root]}
        (load-file "/home/chuck/ai-src/evidence/stock-library-codec-durable-writer-20261019.clj"))))
  (assert (= 35 @calls))
  (doseq [phase [:compact-total :projection-and-encoding :private-wal-precompute :native-and-confirm
                 :native-classify :native-stream :wal-publish :head-commit
                 :wal-prepare :wal-append :wal-join]]
    (let [observed (filter #(= phase (:phase %)) @events)]
      (assert (= 32 (count observed)))
      (assert (= (vec (range 32)) (mapv :ordinal observed)))
      (assert (every? #(= :complete (:status %)) observed))))
  (spit (str root "/worker-profile.edn")
        (pr-str {:scope :single-lane-private-wal-phase-diagnostic :arm arm
                 :runtime :official-stock-0.8.23 :samples 32 :events @events
                 :ticks-per-second (parse-long (System/getenv "BENCH_CLK_TCK"))
                 :row-conversion-and-execution-unchanged true
                 :counter-snapshots-non-atomic true :nested-and-inclusive true
                 :tail-qualified false})))
