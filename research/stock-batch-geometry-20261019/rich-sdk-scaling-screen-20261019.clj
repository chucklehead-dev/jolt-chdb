;; Research only: partitioned ended spans, not a new supported SDK processor.
;; One serial SDK worker per DISTINCT exporter; one borrowed connection owner.
(require '[db.jdbc] '[jdbc.core :as jdbc] '[clojure.java.io :as io]
         '[clojure.string :as str]
         '[jdbc.chdb.durable :as durable] '[jdbc.chdb.native :as native]
         '[clojure.data.json.jolt-native :as json-native]
         '[otel.sdk.export :as export] '[otel.trace :as trace]
         '[otel.sdk.lifecycle :as lifecycle]
         '[otel.exporter.chdb :as e]
         '[otel.exporter.chdb-ordinary-transport-benchmark :as fixture]
         '[otel.exporter.chdb-benchmark :as bench]
         '[jdbc.chdb-typed-storage-support :as storage])

(defn ensure! [condition label]
  (when-not condition (throw (ex-info "Partitioned SDK screen failed" {:label label}))))

(load-file "/home/chuck/ai-src/evidence/trace-affinity-scaling-helpers-20261019.clj")

;; Linux-only research metadata. stat utime/stime cover the entire process's
;; threads (including native workers), unlike a calling-carrier CPU sample.
;; Two coarse non-atomic snapshots, never per-span/hot callback instrumentation.
(defn process-sample []
  (let [stat (slurp "/proc/self/stat")
        fields (str/split (subs stat (+ 2 (.lastIndexOf stat ")"))) #"\s+")
        status (slurp "/proc/self/status")
        kib (fn [label]
              (parse-long (second (re-find (re-pattern (str "(?m)^" label ":\\s+(\\d+) kB$")) status))))]
    {:user-ticks (parse-long (nth fields 11)) :system-ticks (parse-long (nth fields 12))
     :thread-count (parse-long (nth fields 17))
     :rss-kib (kib "VmRSS") :highwater-kib (kib "VmHWM")}))

(let [[root lanes-text arrival] *command-line-args*
      lanes (parse-long lanes-text) n 5000 rounds 32 total (* n rounds)
      _ (ensure! (contains? #{1 2 4} lanes) :bounded-lanes)
      _ (ensure! (contains? #{"saturated" "paced"} arrival) :bounded-arrival)
      _ (ensure! (= :local (storage/arm)) :local-only)
      queue-capacity (quot (* 4 n) lanes)
      _ (ensure! (<= n queue-capacity) :batch-within-per-lane-queue)
      ticks-per-second (parse-long (System/getenv "BENCH_CLK_TCK"))
      _ (ensure! (and (integer? ticks-per-second) (pos? ticks-per-second)) :os-clock-ticks)
      spans (otel.research.trace-affinity/group-traces
             (mapv (fn [span]
                    (update (assoc-in span [:span-context :trace-flags] 1) :attributes into
                            (map (fn [i] [(str "extra-" i)
                                          (case (mod i 3) 0 "value" 1 false i)]))
                            (range 12)))
                  (with-redefs [fixture/batch-size n] (fixture/spans "typed-compact-prefix"))))
      trace-groups (vec (group-by #(get-in % [:span-context :trace-id]) spans))
      _ (ensure! (= 625 (count trace-groups)) :multiple-siblings-per-trace)
      expected (into {} (map-indexed
                         (fn [index [id siblings]]
                           [id {:index index :lane (otel.research.trace-affinity/lane (first siblings) lanes)
                                :span-ids (mapv #(get-in % [:span-context :span-id]) siblings)}])
                         trace-groups))
      cursors (int-array (count trace-groups))
      expected-lane-counts (mapv (fn [lane]
                                   (* rounds (count (filter #(= lane (otel.research.trace-affinity/lane % lanes)) spans))))
                                 (range lanes))
      _ (ensure! (every? pos? expected-lane-counts) :all-lanes-positive-input)
      stream-calls (atom 0) actual-stream native/chdb-stream-insert-n
      batches (atom [])]
  (ensure! (every? #(trace/sampled? (:span-context %)) spans) :sampled-fixture)
  (ensure! (every? #(trace/valid? (:span-context %)) spans) :valid-fixture-contexts)
  (native/ensure-loaded!)
  (ensure! (= "26.9.0" (native/chdb-version)) :native-version)
  (with-redefs [native/chdb-stream-insert-n
                (fn [& args] (swap! stream-calls inc) (apply actual-stream args))]
    (let [conn (jdbc/connection
                      (durable/writer-dbspec
                        (merge (storage/options root "writer")
                               {:owner "typed-compact-prefix" :instance "writer-1"
                                :database "otel" :owned-compact-stream? true
                                :lease-ttl-ms 900000 :heartbeat-interval-ms 300000
                               :operations {:json-reader (json-native/load-reader!)}})))]
      (let [{:keys [capability]} (fixture/setup conn)
            _ (ensure! (= :committed (:status (durable/checkpoint! conn))) :schema-checkpoint)
            owners (atom []) processors (atom [])]
        (try
          ;; Record each acquired owner immediately so partial setup cleans up.
          (dotimes [lane lanes]
            (let [owner (e/exporter {:connection conn :durable? true :create-schema? false
                                     :datetime64-wire :raw-ticks :max-insert-bytes 67108864
                                     :signals #{:spans} :typed-span-descriptors capability
                                     :insert-format :json-compact-each-row
                                     :json-backend :native-guarded-byte-batch
                                     :owned-statement-output? true})]
              (swap! owners conj owner)))
          (dotimes [_ 3]
            (ensure! (true? (export/export-spans! (first @owners) spans)) :warmup))
          (dotimes [lane lanes]
            (let [owner (nth @owners lane)
                  borrowed (reify export/SpanExporter
                             (export-spans! [_ values]
                               (ensure! (< (count @batches) 256) :bounded-scalar-events)
                               ;; Bounded source oracle: one private counter
                               ;; per trace, owned by that trace's serial lane.
                               ;; Verification overhead is included in window
                               ;; wall time, outside callback export timing.
                               (doseq [span values]
                                 (let [entry (get expected (get-in span [:span-context :trace-id]))
                                       index (:index entry) cursor (aget cursors index)]
                                   (ensure! (= lane (:lane entry)) :trace-affinity)
                                   (ensure! (= (nth (:span-ids entry) (mod cursor 8))
                                               (get-in span [:span-context :span-id])) :per-trace-order)
                                   (aset cursors index (inc cursor))))
                               (let [start (System/nanoTime)
                                     ok (export/export-spans! owner values)
                                     end (System/nanoTime)]
                                 (swap! batches conj {:lane lane :rows (count values)
                                                      :ok (true? ok)
                                                      :start-nanos start :end-nanos end
                                                      :nanos (- end start)})
                                 ok))
                             (flush-exporter! [_] (export/flush-exporter! owner))
                             (shutdown-exporter! [_] true))]
              (swap! processors conj
                     (export/batch-processor borrowed
                       {:max-queue-size queue-capacity :max-export-batch-size n
                        :schedule-delay-ms 100}))))
          (System/gc)
          (let [os-before (process-sample)
                before (#'bench/counter-sample) start (System/nanoTime)
                deadline (+ start 120000000000) throttled (atom 0)
                pacing-wait (atom 0) max-lateness (atom 0)]
            (dotimes [round rounds]
              (dotimes [index n]
                (let [span (nth spans index)]
                  (when (and (= arrival "paced") (zero? (mod index 100)))
                    (let [begin (System/nanoTime)
                          due (+ start (* (+ (* round n) index) 50000))]
                      (loop []
                        (when (< (System/nanoTime) due) (Thread/sleep 1) (recur)))
                      (let [now (System/nanoTime)]
                        (swap! pacing-wait + (- now begin))
                        (swap! max-lateness max (- now due)))))
                  (let [lane (otel.research.trace-affinity/lane span lanes)
                        processor (nth @processors lane)]
                    (when (= arrival "saturated")
                      (loop []
                        (ensure! (< (System/nanoTime) deadline) :producer-deadline)
                        (when (>= (export/queue-size processor) queue-capacity)
                          (let [begin (System/nanoTime)]
                            (Thread/sleep 1)
                            (swap! throttled + (- (System/nanoTime) begin)))
                          (recur))))
                    ;; Paced mode does not wait for queue capacity. Any overflow
                    ;; is a failed gate, not silently slowed offered load.
                    (export/on-end processor span)))))
            (let [producer-done (System/nanoTime)]
              (loop []
                (let [stats (mapv export/batch-processor-stats @processors)]
                  (when (< (reduce + (map #(+ (:exported-span-count %) (:failed-span-count %) (:dropped-count %)) stats)) total)
                    (ensure! (< (System/nanoTime) deadline) :completion-deadline)
                    (Thread/sleep 1)
                    (recur))))
              (let [end (System/nanoTime) after (#'bench/counter-sample)
                    os-after (process-sample)
                    stats (mapv export/batch-processor-stats @processors)]
                ;; Preserve a categorical incomplete receipt before failing an
                ;; overloaded open-loop arm. Such a run is not qualified.
                (spit (str root "/progress.edn")
                      (pr-str {:stats stats :lanes lanes :arrival arrival
                               :offered-rows total :elapsed-nanos (- end start)}))
                (ensure! (every? #(zero? (:failed-span-count %)) stats) :no-failed-export)
                (ensure! (every? #(zero? (:dropped-count %)) stats) :no-overflow)
                (ensure! (= expected-lane-counts (mapv :exported-span-count stats)) :per-lane-routing-counts)
                (ensure! (every? #(= (* rounds 8) (aget cursors %)) (range (count trace-groups))) :per-trace-counts)
                (ensure! (= total (reduce + (map :attempted-span-count stats))
                            (reduce + (map :exported-span-count stats))
                            (reduce + (map :rows @batches))) :row-accounting)
                (ensure! (every? #(zero? (:queue-size %)) stats) :empty-queues)
                (ensure! (every? :ok @batches) :confirmed-callbacks)
                (doseq [[_ events] (group-by :lane @batches)]
                  (let [ordered (sort-by :start-nanos events)]
                    (ensure! (every? (fn [[a b]] (<= (:end-nanos a) (:start-nanos b)))
                                     (partition 2 1 ordered)) :serial-per-exporter)))
                (ensure! (= (+ 3 (count @batches)) @stream-calls) :actual-native-streams)
                (ensure! (= (* n (+ 3 rounds))
                            (:n (jdbc/fetch-one conn "SELECT count() AS n FROM otel_traces"))) :writer-count)
                (spit (str root "/writer.edn")
                      (pr-str {:scope :trace-affinity-sdk-consumer-research-not-public-mode
                               :routing :fixed-lifetime-trace-hash :trace-count (count trace-groups)
                               :siblings-per-trace 8 :per-trace-callback-order-checked true
                               :per-trace-counts-checked true :expected-lane-counts expected-lane-counts
                               :arrival arrival :target-arrival-rows-per-second (when (= arrival "paced") 20000)
                               :pacing-wait-nanos @pacing-wait :maximum-arrival-lateness-nanos @max-lateness
                               :lanes lanes :distinct-exporter-count (count @owners)
                               :serial-worker-per-exporter true :shared-connection-owner-count 1
                               :batch-size n :rounds rounds :rows total :warmups 3
                               :queue-capacity-per-lane queue-capacity
                               :total-queue-capacity (* lanes queue-capacity)
                               :schedule-delay-ms 100 :stats stats :batches @batches
                               :stream-calls @stream-calls
                               :sdk-source (str (io/resource "otel/sdk/export.clj"))
                               :load-generator-capacity-wait-nanos @throttled
                               :producer-nanos (- producer-done start)
                               :confirmed-window-nanos (- end start)
                               :confirmed-rows-per-second (/ (double (* total 1000000000)) (- end start))
                               :counter-delta (#'bench/counter-delta before after)
                               :os-process-window
                               {:ticks-per-second ticks-per-second
                                :user-ticks-delta (- (:user-ticks os-after) (:user-ticks os-before))
                                :system-ticks-delta (- (:system-ticks os-after) (:system-ticks os-before))
                                :before os-before :after os-after
                                :non-atomic-snapshots true
                                :highwater-covers-process-lifetime true}
                               :force-flush-before-measurement false :tail-qualified false})))))
          (finally
            ;; Quarantine rather than close a connection whose SDK users lack
            ;; stable retirement proof. The outer bounded process will fail;
            ;; its native store is retained rather than counted/cleaned as pass.
            (let [errors (atom [])]
              (doseq [processor @processors]
                (try (ensure! (true? (export/shutdown! processor)) :processor-shutdown)
                     (catch Throwable error (swap! errors conj error))))
              (ensure! (every? #(= :confirmed (:sdk-quiescence
                                                (lifecycle/component-settlement %)))
                               @processors) :all-sdk-users-settled-before-release)
              (let [released (atom 0)]
                (doseq [owner @owners]
                  (try (ensure! (true? (export/shutdown-exporter! owner)) :exporter-shutdown)
                       (swap! released inc)
                       (catch Throwable error (swap! errors conj error))))
                (ensure! (= (count @owners) @released) :all-exporters-released-before-close)
                (.close conn))
              (when (seq @errors) (throw (first @errors))))))))))
