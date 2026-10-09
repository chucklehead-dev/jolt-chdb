(require '[jdbc.chdb-stream-storage :as storage]
         '[clojure.string :as str] '[db.jdbc] '[jdbc.core :as jdbc]
         '[jdbc.chdb.durable :as durable] '[jdbc.chdb.durable.local-posix :as local]
         '[jdbc.chdb.durable.backend :as backend] '[jdbc.chdb.durable.control :as control]
         '[jdbc.chdb.durable.head :as head]
         '[jdbc.chdb.durable.digest :as digest]
         '[jdbc.chdb :as chdb] '[jdbc.chdb.owned-statement :as owned]
         '[clojure.data.json.jolt-native :as json-native]
         '[clojure.data.json :as json]
         '[jdbc.chdb.json-each-row :as codec] '[jdbc.chdb.native :as native]
         '[otel.sdk.export :as export] '[otel.exporter.chdb :as e]
         '[otel.exporter.chdb-ordinary-transport-benchmark :as fixture]
         '[otel.exporter.chdb-benchmark :as bench])

(defn benchmark-compact-query [table columns]
  (with-bindings {#'e/*timestamp-wire* :raw-ticks}
    (@#'e/compact-insert-query table columns)))

(def owned-output? (= "true" (System/getenv "CHDB_BENCH_OWNED_OUTPUT")))
(def stream-arm (or (System/getenv "SDK_STREAM_ARM") "owned"))
(def stream-header (atom nil))
(def stream-calls (atom 0))
(def sample-counters? (= "true" (System/getenv "CHDB_BENCH_SAMPLE_COUNTERS")))

(defn check! [ok label]
  (when-not ok (throw (ex-info "Typed compact qualification failed" {:label label}))))
(defn opts [root phase]
  (storage/options root phase #(System/getenv %)))
(defn records [n]
  (let [shape (nth *command-line-args* 5 "small")
        _ (check! (contains? #{"small" "wide16"} shape) :known-attribute-shape)
        spans (with-redefs [fixture/batch-size n] (fixture/spans "typed-compact-prefix"))]
    (if (= shape "small") spans
      (mapv (fn [span]
              (update span :attributes into
                      (map (fn [i] [(str "extra-" i)
                                    (case (mod i 3) 0 "value" 1 false i)]))
                      (range 12))) spans))))

(def ^:dynamic *phase-samples* nil)
(def ^:dynamic *components?* false)
(def ^:dynamic *window* nil)
(defn observed-open-options [& values]
  (let [result (apply merge values)
        operations (apply merge (keep :operations values))
        result (if (seq operations) (assoc result :operations operations) result)]
    (when *window*
      (check! (and (ifn? (:renew-control! operations))
                   (ifn? (:create-checkpoint! operations))) :window-observers-installed))
    (when *phase-samples*
      (check! (ifn? (:writer-phase! operations)) :phase-observer-installed))
    result))
(defn component-report [label f]
  (dotimes [_ 2] (f))
  (let [before (@#'bench/counter-sample)
        times (mapv (fn [_] (let [start (System/nanoTime)] (f) (- (System/nanoTime) start))) (range 10))
        after (@#'bench/counter-sample)]
    {:stage label :latency (@#'bench/latency-summary times)
     :counter-delta (@#'bench/counter-delta before after)}))
(defn observed [stage f]
  (let [samples *phase-samples*]
   (fn [& args]
    (let [before (@#'bench/counter-sample)
          start (System/nanoTime)
          result (apply f args)
          elapsed (- (System/nanoTime) start)
          after (@#'bench/counter-sample)]
      (swap! samples conj {:stage stage :nanos elapsed
                                 :counter-delta (@#'bench/counter-delta before after)})
      result))))

(defn statistics-observed [f]
  (let [samples *phase-samples*]
    (fn [& args]
      ;; Collect on the thread that owns/consumes the result. The public
      ;; collector snapshots scalars before destruction; no pointer escapes.
      (let [measurement (chdb/with-query-statistics #(apply f args))]
        (doseq [statistics (:queries measurement)]
          (swap! samples conj {:stage :native-query-statistics :statistics statistics}))
        (:result measurement)))))

(def export-callers (Long/parseLong (or (System/getenv "SDK_EXPORT_CALLERS") "2")))
(def caller-window-wall (atom nil))
(defn run-caller-window! [samples f]
  (check! (contains? #{1 2} export-callers) :bounded-export-callers)
  (if (= 1 export-callers)
    (let [start (System/nanoTime) result (mapv f (range samples))]
      (reset! caller-window-wall (- (System/nanoTime) start)) result)
    (let [gate (java.util.concurrent.CountDownLatch. 1)
          results (mapv (fn [_] (atom nil)) (range export-callers))
          threads (mapv
                   (fn [lane]
                     (Thread.
                       (bound-fn []
                         (try
                           (.await gate)
                           (reset! (nth results lane)
                             {:values (mapv (fn [i] [i (f i)])
                                           (range lane samples export-callers))})
                           (catch Throwable error
                             (reset! (nth results lane) {:error error}))))))
                   (range export-callers))]
      (doseq [thread threads] (.start thread))
      (let [start (System/nanoTime)]
        (.countDown gate)
        (doseq [thread threads] (.join thread))
        (reset! caller-window-wall (- (System/nanoTime) start)))
      (doseq [result results]
        (when-let [error (:error @result)] (throw error)))
      (let [entries (sort-by first (mapcat #(get @% :values) results))]
        (check! (= (vec (range samples)) (mapv first entries)) :all-callers-confirmed)
        (mapv second entries)))))

(defn writer! [root n samples expected-prefix?]
  (let [spans (records n)
        reader-mode (or (System/getenv "CHDB_BENCH_METADATA_READER") "stock")
        _ (check! (contains? #{"stock" "native"} reader-mode) :metadata-reader-mode)
        _ (check! (nil? json/*experimental-native-reader*) :global-reader-unmodified)
        reader-calls (atom 0)
        selected-reader (when (= reader-mode "native") (json-native/load-reader!))
        reader (when selected-reader
                 (fn [& args] (swap! reader-calls inc) (apply selected-reader args)))]
    (with-open [conn (jdbc/connection
                     (durable/writer-dbspec
                      (observed-open-options (opts root "writer")
                             (when reader {:operations {:json-reader reader}})
                             {:owner "typed-compact-prefix" :instance "writer-1"
                              :owned-compact-stream? (= stream-arm "stream")
                              :database "otel" :lease-ttl-ms (if *window* 60000 900000)
                              :heartbeat-interval-ms (if *window* 1000 300000)}
                             (when *window*
                               (let [window *window* defaults (@#'durable/default-open-operations)
                                     backup (:create-checkpoint! defaults)
                                     renew (:renew-control! defaults)]
                                 {:clock-skew-ms 1000 :checkpoint-wal-reference-threshold 128
                                  :operations {:renew-control!
                                               (fn [& args]
                                                 (let [r (apply renew args)]
                                                   (swap! window update :renewals inc) r))
                                               :create-checkpoint!
                                               (fn [& args]
                                                 (let [start (System/nanoTime) r (apply backup args)]
                                                   (swap! window update :checkpoints conj
                                                          {:batch (:current-batch @window)
                                                           :nanos (- (System/nanoTime) start)}) r))}}))
                             (when *phase-samples*
                               (let [samples *phase-samples*]
                                 {:operations {:writer-phase!
                                               (fn [event]
                                                 (swap! samples conj
                                                        (assoc event :stage (:phase event))))}})))))]
      (let [{:keys [capability fields]} (fixture/setup conn)
            _ (check! (= :committed (:status (durable/checkpoint! conn))) :schema-checkpoint)
            owner (e/exporter {:connection conn :durable? true :create-schema? false :datetime64-wire :raw-ticks
                               :signals #{:spans} :typed-span-descriptors capability
                               :insert-format :json-compact-each-row
                               :json-backend :native-guarded-byte-batch
                               :owned-statement-output? owned-output?})]
        (try
          (let [calls (atom 0) sql (atom nil) direct-calls (atom 0)
                row-fn @#'e/span-row
                old codec/encode-limited-prefixed-text!
                old-owned codec/encode-limited-prefixed-statement!
                state @(:state owner)
                _ (check! (ifn? (:typed-span-projector state)) :confirmed-projector)
                _ (check! (get-in state [:compact-plans "otel_traces"]) :confirmed-compact-plan)]
            ;; Positive branch witness and exact legacy SQL oracle, untimed;
            ;; no mutation or ack claim from this admission spy.
            (with-redefs [codec/encode-limited-prefixed-text!
                          (fn [& args] (swap! calls inc) (apply old args))
                          codec/encode-limited-prefixed-statement!
                          (fn [& args] (swap! calls inc) (apply old-owned args))
                          e/span-row (fn [& args]
                                       (when (and (= 3 (count args)) (true? (nth args 2))
                                                  (nil? (nth args 1)))
                                         (swap! direct-calls inc))
                                       (apply row-fn args))
                          durable/execute-and-flush!
                          (fn [_ text] (reset! sql text) {:status :committed})
                          durable/execute-owned-and-flush!
                          (fn [_ statement]
                            (check! (owned/statement? statement) :owned-output-witness)
                            (reset! sql (owned/text statement)) {:status :committed})]
              (check! (true? (export/export-spans! owner spans)) :witness-call))
            (check! (= expected-prefix? (pos? @calls)) :selected-prefix-path)
            (check! (= (if (:typed-span-vector-plan state) n 0) @direct-calls) :direct-vector-witness)
            (let [cols (:span-insert-columns state)
                  rows (with-bindings {#'e/*timestamp-wire* (:timestamp-wire state)}
                         (mapv #(@#'e/span-row % (:typed-span-projector state)) spans))
                  _ (reset! stream-header (str (benchmark-compact-query "otel_traces" cols)
                                              " FORMAT JSONCompactEachRow\n"))
                  expected (with-bindings {#'e/*json-backend* :native-guarded-byte-batch}
                             (str (benchmark-compact-query "otel_traces" cols)
                                  " FORMAT JSONCompactEachRow\n"
                                  (@#'e/insert-payload :json-compact-each-row cols rows)))
                  digest (fixture/digest expected)]
              (check! (= expected @sql) :exact-sql)
              (when *components?*
                (let [row-fn @#'e/span-row projector (:typed-span-projector state)
                      compact-fn @#'e/compact-rows payload-fn @#'e/insert-payload
                      make-rows #(mapv (fn [span] (row-fn span projector)) spans)
                      prepared-rows (make-rows)
                      make-compact #(vec (compact-fn cols prepared-rows))
                      prepared-compact (make-compact)
                      vector-projector (get-in state [:typed-span-vector-plan :vector-projector])
                      make-direct #(mapv (fn [span]
                                           (into (row-fn span nil true) (vector-projector span))) spans)
                      prepared-direct (when vector-projector (make-direct))
                      prefix (str (benchmark-compact-query "otel_traces" cols)
                                  " FORMAT JSONCompactEachRow\n")]
                  (with-bindings {#'e/*json-backend* :native-guarded-byte-batch}
                    (check! (= expected (@#'e/json-each-row-payload prepared-compact prefix)) :prepared-vector-sql)
                    (spit (str root "/components.edn")
                          (pr-str {:sql-characters (count expected) :batch-size n :sql-sha256 digest
                                   :reports [(component-report :physical-row-construction make-rows)
                                             (component-report :typed-projector-only
                                                               #(mapv projector spans))
                                             (component-report :base-named-row
                                                               #(mapv (fn [s] (row-fn s nil)) spans))
                                             (component-report :base-compact-row
                                                               #(mapv (fn [s] (row-fn s nil true)) spans))
                                             (component-report :compact-vector-projection make-compact)
                                             (component-report :prepared-vector-json
                                                               #(@#'e/json-each-row-payload prepared-compact prefix))
                                             (when vector-projector
                                               (component-report :actual-typed-vector-projector
                                                                 #(mapv vector-projector spans)))
                                             (when vector-projector
                                               (component-report :actual-direct-typed-rows make-direct))
                                             (when vector-projector
                                               (component-report :actual-prepared-owned-json-bytes
                                                                 #(json-native/write-prefixed-batch-bytes!
                                                                    prefix prepared-direct 67108864)))]})))))
              (dotimes [_ 3] (check! (true? (export/export-spans! owner spans)) :warmup))
              (System/gc)
              (when *window* (reset! *window* {:renewals 0 :checkpoints []}))
              (when *phase-samples* (reset! *phase-samples* []))
              (let [sample-counters (atom [])
                    before (@#'bench/counter-sample)
                    times (run-caller-window! samples (fn [i]
                                  (when *window* (swap! *window* assoc :current-batch i))
                                  (let [sample-before (when sample-counters? (@#'bench/counter-sample))
                                        start (System/nanoTime)]
                                    (check! (true? (export/export-spans! owner spans)) :confirmed-export)
                                    (let [ns (- (System/nanoTime) start)]
                                      (when sample-counters?
                                        (swap! sample-counters conj
                                               {:sample i :started-at-nanos start :confirmed-at-nanos (System/nanoTime) :counter-delta
                                                (@#'bench/counter-delta sample-before (@#'bench/counter-sample))}))
                                      ns))))
                    after (@#'bench/counter-sample)
                    report {:scope (if (= :s3 (storage/kind #(System/getenv %)))
                                   :s3-typed-public-export :local-posix-typed-public-export-not-aws)
                            :native-execution-arm stream-arm :stream-calls @stream-calls
                            :timestamp-wire (:timestamp-wire state)
                            :metadata-reader {:mode reader-mode :calls @reader-calls
                                              :startup-reader :stock :recovery-reader :stock}
                            :owned-statement-output? owned-output?
                            :attribute-shape (nth *command-line-args* 5 "small")
                            :batch-size n :samples samples :warmups 3
                            :prefix-delegated? (pos? @calls) :sql-sha256 digest
                            :direct-typed-rows-witness @direct-calls
                            :export-callers export-callers
                            :confirmed-window-nanos @caller-window-wall
                            :confirmed-wall-rows-per-second
                            (/ (double (* n samples 1000000000)) @caller-window-wall)
                            :counter-interpretation {:parent-cpu-excludes-encoders true
                                                     :per-call-gc-and-allocation-overlap true}
                            :samples-nanos times
                            :sample-counter-deltas (when sample-counters? @sample-counters)
                            :phase-samples (when *phase-samples* @*phase-samples*)
                            :checkpoint-renewal-window (when *window* (dissoc @*window* :current-batch))
                            :latency (@#'bench/latency-summary times)
                            :counter-delta (@#'bench/counter-delta before after)
                            :rows-per-second (/ (double (* n samples 1000000000)) (reduce + times))
                            :tail-qualified? false}]
                ;; Keep scalar diagnostic evidence even when a later gate
                ;; fails; only the final overwrite marks the gate successful.
                (spit (str root "/writer.edn")
                      (pr-str (assoc report :gate-status :pending)))
                (check! (= (* n (+ 3 samples))
                           (:n (jdbc/fetch-one conn "SELECT count() AS n FROM otel_traces")))
                        :writer-count)
                (check! (if reader (pos? @reader-calls) (zero? @reader-calls))
                        :metadata-reader-exercised)
                (check! (nil? json/*experimental-native-reader*) :global-reader-unmodified)
                (check! (= (if (= stream-arm "stream") (+ 3 samples) 0) @stream-calls) :stream-execution-witness)
                (when *window*
                  (check! (>= (:renewals @*window*) 2) :automatic-renewals)
                  (check! (>= (count (:checkpoints @*window*)) 2) :automatic-checkpoints))
                (spit (str root "/writer.edn") (pr-str (assoc report :gate-status :passed)))
                (prn (dissoc report :counter-delta :latency :samples-nanos :phase-samples :sample-counter-deltas)))))
          (finally (check! (export/shutdown-exporter! owner) :exporter-shutdown)))))))

(defn reader! [root n samples]
  (check! (nil? json/*experimental-native-reader*) :fresh-stock-reader)
  (with-open [conn (jdbc/connection (durable/snapshot-dbspec (opts root "reader")))]
    (let [fields (:fields (fixture/approved))
          cols (remove #{"Timestamp"} (fixture/columns fields))
          query (str "SELECT " (str/join ", " (map fixture/sql-column cols))
                     ", toString(toUnixTimestamp64Nano(Timestamp)) AS ticks, count() AS copies"
                     " FROM otel_traces GROUP BY ALL ORDER BY TraceId")
          actual (jdbc/fetch conn query {:max-rows (inc n)})
          expected (mapv (fn [span]
                           (let [row (merge (@#'e/span-row span nil)
                                            (into {} (mapcat (fn [f]
                                                               [[(get-in f [:physical :value-column])
                                                                 (get-in span [:attributes (:key f)])]
                                                                [(get-in f [:physical :status-column]) 3]]) fields)))]
                             (assoc (into {} (map (fn [col] [(keyword (str/lower-case col)) (get row col)]) cols))
                                    :ticks (str (:start-time-unix-nano span)) :copies (+ 3 samples))))
                         (records n))]
      (check! (= expected actual) :fresh-full-physical-row-equality)
      (check! (= (* n (+ 3 samples)) (:n (jdbc/fetch-one conn "SELECT count() AS n FROM otel_traces")))
              :fresh-count)
      (doseq [f fields]
        (check! (= (if (= :boolean (:type f)) "Bool" "Int64")
                   (:t (jdbc/fetch-one conn (str "SELECT toTypeName("
                                                (fixture/sql-column (get-in f [:physical :value-column]))
                                                ") AS t FROM otel_traces LIMIT 1")))) :physical-type))
      (spit (str root "/reader.edn")
            (pr-str {:passed true :groups n :rows (* n (+ 3 samples))
                     :full-physical-rows-equal true :typed-values-status true}))
      (println :fresh-reader-green :rows (* n (+ 3 samples))))))

(let [[phase root size samples prefix] *command-line-args*
      n (Long/parseLong size) samples (Long/parseLong samples)]
  (check! (and (<= 1 n 10000) (<= 1 samples 300)) :bounded-workload)
  ;; The window oracle requires two 128-reference automatic checkpoints.
  ;; Reject undersized scenarios before opening/mutating a native writer.
  (when (contains? #{"window" "query-window"} phase)
    (check! (>= samples 256) :checkpoint-window-duration))
  (native/ensure-loaded!)
  (check! (= "26.9.0" (native/chdb-version)) :native-version)
  (try
   (let [original native/chdb-stream-insert-n]
    (with-redefs [native/chdb-stream-insert-n
                  (fn [& args]
                    (swap! stream-calls inc)
                    (apply original args))]
    (case phase "writer" (writer! root n samples (= prefix "true"))
                "window" (binding [*window* (atom {:renewals 0 :checkpoints []})]
                           (writer! root n samples (= prefix "true")))
                "components" (binding [*components?* true]
                               (writer! root n samples (= prefix "true")))
                "profile" (binding [*phase-samples* (atom [])]
                            (with-redefs [codec/encode-limited-prefixed-text!
                                          (observed :projection-and-encoding codec/encode-limited-prefixed-text!)
                                          durable/execute-and-flush!
                                          (observed :native-execute-and-persist durable/execute-and-flush!)]
                              (writer! root n samples (= prefix "true"))))
                "head-profile" (binding [*phase-samples* (atom [])]
                                 (with-redefs [chdb/with-owned-result
                                               (statistics-observed @#'chdb/with-owned-result)
                                               codec/encode-limited-prefixed-text!
                                               (observed :projection-and-encoding codec/encode-limited-prefixed-text!)
                                               codec/encode-limited-prefixed-statement!
                                               (observed :projection-and-encoding codec/encode-limited-prefixed-statement!)
                                               json-native/write-prefixed-batch-bytes!
                                               (observed :projection-and-json-bytes json-native/write-prefixed-batch-bytes!)
                                               owned/try-snapshot (observed :owned-snapshot owned/try-snapshot)
                                               owned/prepared-wal (observed :owned-wal-chunks owned/prepared-wal)
                                               owned/code-placeholder? (observed :owned-placeholder owned/code-placeholder?)
                                               durable/execute-and-flush!
                                               (observed :native-execute-and-persist durable/execute-and-flush!)
                                               durable/execute-owned-and-flush!
                                               (observed :native-execute-and-persist durable/execute-owned-and-flush!)
                                               head/encode (observed :head-encode head/encode)
                                               head/decode (observed :head-decode head/decode)
                                               head/single-json-value-bounds
                                               (observed :head-scan @#'head/single-json-value-bounds)
                                               head/validate! (observed :head-validate head/validate!)
                                               chdb/prepare-query (observed :query-prepare chdb/prepare-query)
                                               native/classify-query-buffer!
                                               (observed :native-classify native/classify-query-buffer!)
                                               native/chdb-query-with-params-n
                                               (observed :native-query native/chdb-query-with-params-n)
                                               digest/sha256-bytes (observed :digest-bytes digest/sha256-bytes)
                                               digest/hash+count-input-stream
                                               (observed :digest-stream digest/hash+count-input-stream)
                                               local/c-fsync (observed :posix-fsync local/c-fsync)]
                                   (writer! root n samples (= prefix "true"))))
                "query-window" (binding [*phase-samples* (atom [])
                                         *window* (atom {:renewals 0 :checkpoints []})]
                                 (with-redefs [chdb/with-owned-result
                                               (statistics-observed @#'chdb/with-owned-result)]
                                   (writer! root n samples (= prefix "true"))))
                "reader" (reader! root n samples))))
    (catch Throwable _
      ;; Credential-bearing provider errors/causes must never enter logs.
      (println :qualification-failed)
      (System/exit 1))))
