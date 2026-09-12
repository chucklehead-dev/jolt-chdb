(ns jdbc.chdb-durable-throughput
  "Current-main, production-path Durable JSONEachRow throughput probe.

  The report separates row encoding, pre-encoded Durable admission, the
  persisted flush boundary, and ordinary non-Durable execution. It uses a
  ClickStack-compatible log shape and verifies every run by reopening the
  immutable Durable snapshot. This is a manual benchmark, not a CI gate."
  (:require [clojure.data.json :as json]
            [clojure.string :as str]
            [db.jdbc]
            [db.jdbc-shim :as shim]
            [jdbc.chdb :as chdb]
            [jdbc.chdb.durable :as durable]
            [jdbc.chdb.durable.backend :as backend]
            [jdbc.chdb.durable.digest :as digest]
            [jdbc.chdb.durable.local-posix :as local]
            [jdbc.chdb.durable.policy :as policy]
            [jdbc.chdb.durable.s3 :as s3]
            [jdbc.chdb.durable.writer :as writer]
            [jdbc.chdb-durable-throughput-metrics :as provider-metrics]
            [jdbc.chdb.native :as native]
            [jdbc.core :as jdbc]
            [jdbc.proto :as proto]
            [jolt.ffi :as ffi]
            [jolt.host :as host])
  (:import [java.io File]))

(def ^:private logs-ddl
  "CREATE TABLE otel_logs (
     Timestamp DateTime64(9), TraceId String, SpanId String, TraceFlags UInt8,
     SeverityText String, SeverityNumber UInt8, ServiceName String, Body String,
     ResourceSchemaUrl String, ResourceAttributes Map(String, String),
     ScopeSchemaUrl String, ScopeName String, ScopeVersion String,
     ScopeAttributes Map(String, String), LogAttributes Map(String, String),
     EventName String
   ) ENGINE=MergeTree ORDER BY (toStartOfFiveMinutes(Timestamp), ServiceName, Timestamp)")

(def ^:private insert-prefix "INSERT INTO otel_logs FORMAT JSONEachRow\n")
(def ^:private insert-prefix-bytes (alength (.getBytes insert-prefix "UTF-8")))
(def ^:private base-nanos 1700000000000000000)

(defn- padded-hex [width n]
  (let [value (format "%x" n)]
    (str (apply str (repeat (- width (count value)) "0")) value)))

(defn- log-row [index question-mark?]
  {"Timestamp" (format "%d.%09d"
                       (quot (+ base-nanos (* index 1000000)) 1000000000)
                       (mod (+ base-nanos (* index 1000000)) 1000000000))
   "TraceId" (padded-hex 32 (inc index))
   "SpanId" (padded-hex 16 (+ 1000000 index))
   "TraceFlags" (mod index 2)
   "SeverityText" (if (zero? (mod index 20)) "ERROR" "INFO")
   "SeverityNumber" (if (zero? (mod index 20)) 17 9)
   "ServiceName" "oscope.benchmark"
   "Body" (str "request completed route=/api/items/" (mod index 64)
               " status=" (if (zero? (mod index 20)) 500 200)
               (if question-mark? " query=ready?" ""))
   "ResourceSchemaUrl" "https://opentelemetry.io/schemas/1.27.0"
   "ResourceAttributes" {"service.name" "oscope.benchmark"
                         "deployment.environment.name" "benchmark"}
   "ScopeSchemaUrl" ""
   "ScopeName" "oscope.benchmark"
   "ScopeVersion" "1.0"
   "ScopeAttributes" {"library.language" "clojure"}
   "LogAttributes" {"http.request.method" "GET"
                    "http.response.status_code"
                    (str (if (zero? (mod index 20)) 500 200))
                    "benchmark.bucket" (str (mod index 16))}
   "EventName" "benchmark.request"})

(defn- encode-batch-production [rows]
  (let [start (System/nanoTime)
        payload (apply str (map #(str (json/write-str %) "\n") rows))
        payload-byte-array (.getBytes payload "UTF-8")
        payload-bytes (alength payload-byte-array)]
    {:sql (str insert-prefix payload)
     :payload-bytes payload-bytes
     :statement-bytes (+ insert-prefix-bytes payload-bytes)
     ;; Execution-only evidence input. Callers report only the scalar sizes
     ;; above and never retain this array in progress or result maps.
     ::payload-byte-array payload-byte-array
     :nanos (- (System/nanoTime) start)}))

(defn- encode-batch-profiled [rows]
  ;; Keep the exporter production shape: data.json runs inside the lazy chunks
  ;; consumed by apply-str, followed by one UTF-8 size pass over the payload.
  (let [json-nanos (atom 0)
        start (System/nanoTime)
        payload
        (apply str
               (map (fn [row]
                      (let [json-start (System/nanoTime)
                            encoded (json/write-str row)]
                        (swap! json-nanos + (- (System/nanoTime) json-start))
                        (str encoded "\n")))
                    rows))
        payload-bytes (alength (.getBytes payload "UTF-8"))
        total-nanos (- (System/nanoTime) start)]
    {:sql (str insert-prefix payload)
     :payload-bytes payload-bytes
     :statement-bytes (+ insert-prefix-bytes payload-bytes)
     :json-nanos @json-nanos
     :materialize-nanos (- total-nanos @json-nanos)
     :nanos total-nanos}))

(defn- percentile [ordered fraction]
  (nth ordered (max 0 (dec (long (Math/ceil (* fraction (count ordered))))))))

(defn- ms [nanos] (/ (double nanos) 1000000.0))

(defn- latency-summary [samples]
  (let [ordered (vec (sort samples))]
    {:count (count ordered)
     :p50-supported? (>= (count ordered) 2)
     :p95-supported? (>= (count ordered) 20)
     :p99-qualification? (>= (count ordered) 100)
     :total-ms (ms (reduce + 0 ordered))
     :p50-ms (ms (percentile ordered 0.50))
     :p95-ms (ms (percentile ordered 0.95))
     :p99-ms (ms (percentile ordered 0.99))
     :max-ms (ms (peek ordered))}))

(defn- counter-sample []
  ;; These host functions sample separately; this is intentionally not called
  ;; an atomic counter snapshot.
  {:sample-nano-time (System/nanoTime)
   :calling-thread-cpu-nanos (host/cpu-nanos)
   :real-nanos (host/real-nanos)
   :gc-count (host/gc-count)
   :gc-cpu-nanos (host/gc-cpu-nanos)
   :gc-real-nanos (host/gc-real-nanos)
   :gc-bytes (host/gc-bytes)
   :live-scheme-heap-bytes (host/bytes-allocated)
   :current-memory-bytes (host/current-memory-bytes)
   :maximum-memory-bytes (host/maximum-memory-bytes)})

(defn- runtime-memory-observation []
  ;; These are absolute Chez/Jolt allocator readings, not process RSS. Keep
  ;; them absolute so recovery runs from separate processes remain attributable
  ;; without pretending that an endpoint delta is a peak or plateau oracle.
  {:sample-nano-time (System/nanoTime)
   :live-scheme-heap-bytes (host/bytes-allocated)
   :reserved-from-os-bytes (host/current-memory-bytes)
   :peak-reserved-from-os-bytes (host/maximum-memory-bytes)})

(def ^:private recovery-memory-limitations
  {:plateau-or-growth-oracle :not-supported
   :reason :two-endpoint-jolt-allocator-samples-do-not-establish-process-rss-plateau
   :external-peak-rss-required
   "Run each selector in a fresh process under GNU /usr/bin/time -v and retain Maximum resident set size (kbytes)."})

(def ^:private report-contract
  {:relative-trial-paths-to :configuration-result
   :provenance-paths [[:runtime :jolt-version]
                      [:runtime :jolt-source-sha]
                      [:runtime :jolt-executable :sha256]
                      [:runtime :native-library :sha256]
                      [:runtime :git :head]
                      [:runtime :git :parent]
                      [:runtime :git :tree]
                      [:runtime :git :status]
                      [:runtime :started-at]]
   :recovery-memory-paths
   [[:recovery :memory :immediately-before-reader-open]
    [:recovery :memory :after-open-and-reconciliation]]
   :wal-size-semantics
   {:wal-growth-total [:wal-growth :total-bytes]
    :maximum-row-payload [:maximum-row-payload-bytes]
    :maximum-input-batch [:maximum-batch-statement-bytes]}})

(defn- wal-size-observation [pending measured]
  (cond->
   {:wal-growth {:total-bytes (:pending-wal-bytes pending)
                 :records (:pending-statements pending)}
    :maximum-batch-payload-bytes (:maximum-batch-payload-bytes measured)
    :maximum-batch-statement-bytes (:maximum-batch-statement-bytes measured)}
    (contains? measured :maximum-row-payload-bytes)
    (assoc :maximum-row-payload-bytes
           (:maximum-row-payload-bytes measured))))

(defn- maximum-serialized-row-payload-bytes
  "Scan row boundaries in the encoder's already-produced UTF-8 byte array.

  This runs only after the timed admission sample and never calls data.json a
  second time, converts strings, splits text, or allocates row substrings. The
  production SQL value admitted to Durable remains unchanged."
  [encoded]
  (provider-metrics/maximum-json-each-row-bytes
   (::payload-byte-array encoded)))

(def ^:private successful-flush-statuses #{:committed :reconciled})

(defn- successful-flush? [result]
  (contains? successful-flush-statuses (:status result)))

(defn- recovery-memory-observation [before after]
  {:immediately-before-reader-open before
   :after-open-and-reconciliation after
   :limitations recovery-memory-limitations})

(def ^:private counter-keys
  [:sample-nano-time :calling-thread-cpu-nanos :real-nanos :gc-count
   :gc-cpu-nanos :gc-real-nanos :gc-bytes :live-scheme-heap-bytes
   :current-memory-bytes :maximum-memory-bytes])

(defn- counter-delta [before after]
  (let [deltas (into {} (map (fn [key] [key (- (get after key) (get before key))]))
                     counter-keys)]
    (assoc deltas :scheme-heap-bytes-allocated
           (- (+ (:live-scheme-heap-bytes after) (:gc-bytes after))
              (+ (:live-scheme-heap-bytes before) (:gc-bytes before))))))

(defn- record-stage! [metrics stage nanos bytes]
  (swap! metrics update stage
         (fn [entry]
           (-> (or entry {:calls 0 :nanos 0 :bytes 0})
               (update :calls inc)
               (update :nanos + nanos)
               (update :bytes + (or bytes 0))))))

(defn- timed-stage [metrics stage bytes f]
  (let [start (System/nanoTime)
        value (f)]
    (record-stage! metrics stage (- (System/nanoTime) start) bytes)
    value))

(defn- stage-report [metrics]
  (into {}
        (map (fn [[stage {:keys [calls nanos bytes]}]]
               [stage {:calls calls :total-ms (ms nanos) :bytes bytes
                       :mean-ms (if (zero? calls) 0.0 (ms (/ nanos calls)))}]))
        @metrics))

(defn- instrumented-backend [delegate metrics]
  (reify backend/ObjectBackend
    (get-bytes [_ key]
      (timed-stage metrics :backend/get-bytes 0 #(backend/get-bytes delegate key)))
    (get-with-etag [_ key]
      (timed-stage metrics :backend/get-with-etag 0
                   #(backend/get-with-etag delegate key)))
    (put-file-if-absent! [_ key path]
      (timed-stage metrics :backend/put-file-if-absent 0
                   #(backend/put-file-if-absent! delegate key path)))
    (put-bytes-if-absent! [_ key bytes]
      (timed-stage metrics :backend/put-bytes-if-absent (alength bytes)
                   #(backend/put-bytes-if-absent! delegate key bytes)))
    (replace-if-match! [_ key bytes etag]
      (timed-stage metrics :backend/replace-if-match (alength bytes)
                   #(backend/replace-if-match! delegate key bytes etag)))
    (download-to-file! [_ key path]
      (timed-stage metrics :backend/download-to-file 0
                   #(backend/download-to-file! delegate key path)))))

(defn- timed-operations [metrics]
  ;; Do not replace :publish-wal!. The production writer operation closes over
  ;; its complete retry options, including the lease-aware :stopped? predicate.
  ;; Publication remains visible through the timed immutable PUT and head-CAS
  ;; backend operations without changing that control contract.
  {:classification-sql!
   (fn [sql params]
     (timed-stage metrics :classification-sql 0
                  #(chdb/classification-sql sql params)))
   :prepare-query!
   (fn [sql params]
     (timed-stage metrics :prepare-query 0
                  #(chdb/prepare-query sql params)))
   :classify!
   (fn [handle sql database]
     (timed-stage metrics :native-classify 0
                  #(native/classify-query! handle sql database)))
   :execute-native!
   (fn [handle sql params]
     (timed-stage metrics :native-execute 0
                  #(chdb/execute-any handle sql params)))
   :execute-prepared-native!
   (fn [handle prepared]
     (timed-stage metrics :native-execute 0
                  #(chdb/execute-prepared-any handle prepared)))})

(defn- instrumentation-contract! []
  (let [operations (timed-operations (atom {}))]
    (when (contains? operations :publish-wal!)
      (throw (ex-info
              "benchmark must retain the production retry-aware WAL publisher"
              {:operation :publish-wal!})))
    {:publish-wal-operation :production
     :retry-options :writer-owned
     :publication-observation [:backend/put-bytes-if-absent
                               :backend/replace-if-match]}))

(defn- delete-tree! [^File root]
  (when (.exists root)
    (doseq [child (reverse (file-seq root))]
      (.delete ^File child))))

(defn- durable-handle [connection]
  (:handle (shim/driver-context (proto/connection connection) :chdb-durable)))

(def ^:private empty-expected-aggregates
  {:n 0 :flags 0 :severity_sum 0 :body_bytes 0 :question_bodies 0
   :min_trace nil :max_trace nil :min_span nil :max_span nil})

(defn- lesser-string [left right]
  (if (or (nil? left) (neg? (compare right left))) right left))

(defn- greater-string [left right]
  (if (or (nil? left) (pos? (compare right left))) right left))

(defn- accumulate-expected-row [expected row question-mark?]
  (let [trace-id (get row "TraceId")
        span-id (get row "SpanId")]
    (-> expected
        (update :n inc)
        (update :flags + (get row "TraceFlags"))
        (update :severity_sum + (get row "SeverityNumber"))
        (update :body_bytes + (alength (.getBytes (get row "Body") "UTF-8")))
        (update :question_bodies + (if question-mark? 1 0))
        (update :min_trace lesser-string trace-id)
        (update :max_trace greater-string trace-id)
        (update :min_span lesser-string span-id)
        (update :max_span greater-string span-id))))

(defn- accumulate-expected-batch [expected rows question-mark?]
  (reduce #(accumulate-expected-row %1 %2 question-mark?) expected rows))

(defn- verify-counts! [connection expected label]
  (let [actual (jdbc/fetch-one
                connection
                (str "SELECT count() n, sum(TraceFlags) flags, "
                     "sum(SeverityNumber) severity_sum, "
                     "sum(length(Body)) body_bytes, "
                     "countIf(position(Body, '?') > 0) question_bodies, "
                     "min(TraceId) min_trace, max(TraceId) max_trace, "
                     "min(SpanId) min_span, max(SpanId) max_span "
                     "FROM otel_logs"))]
    (when-not (= expected actual)
      (throw (ex-info "benchmark row reconciliation failed"
                      {:label label :expected expected :actual actual})))
    actual))

(defn- reduce-row-batches
  "Constructs exactly one batch at a time and does not retain consumed batches.

  row-fn is explicit so the focused contract test can causally observe that a
  10k-row configuration reaches its consumer once per batch rather than only
  after constructing the complete approximately 50k-row workload."
  [row-fn batch-size batches question-mark? start initial reduce-batch]
  (reduce (fn [acc batch]
            (let [batch-start (+ start (* batch batch-size))
                  rows (mapv #(row-fn % question-mark?)
                             (range batch-start (+ batch-start batch-size)))]
              (reduce-batch acc batch rows)))
          initial
          (range batches)))

(defn- one-row-batch [batch-size question-mark? start]
  (reduce-row-batches log-row batch-size 1 question-mark? start nil
                      (fn [_ _ rows] rows)))

(defn- add-counter-delta [total delta]
  (merge-with + total delta))

(def ^:private provider-kinds #{:local-posix :aws-s3})

(defn- local-trial-context [{:keys [trial]}]
  (let [root-file (File/createTempFile "jolt-chdb-throughput-" "")
        _ (.delete root-file)
        _ (.mkdirs root-file)]
    {:namespace-backend (local/local-backend (.getAbsolutePath root-file))
     :object-id (str "bench-" trial "-" (random-uuid))
     :provider-kind :local-posix
     :cleanup! #(delete-tree! root-file)}))

(defn- checked-trial-context!
  "Validate a backend factory result without ever reporting its identity."
  [context]
  (when-not (and (satisfies? backend/ObjectBackend
                             (:namespace-backend context))
                 (string? (:object-id context))
                 (contains? provider-kinds (:provider-kind context))
                 (or (= :local-posix (:provider-kind context))
                     (and (= :aws-s3 (:provider-kind context))
                          (string? (:provider-region context))))
                 (fn? (:cleanup! context))
                 (or (nil? (:writer-options context))
                     (map? (:writer-options context)))
                 (or (nil? (:provider-metrics context))
                     (map? (:provider-metrics context))))
    (throw (ex-info "throughput backend factory returned an invalid context"
                    {:type ::invalid-backend-context})))
  context)

(defn- trial-context!
  [{:keys [backend-context!] :as options}]
  (checked-trial-context!
   ((or backend-context! local-trial-context) options)))

(defn- required-env [name]
  (let [value (System/getenv name)]
    (when-not (and (string? value) (not (str/blank? value)))
      (throw (ex-info "required S3 throughput configuration is absent"
                      {:type ::missing-s3-configuration
                       :option name})))
    value))

(defn- checked-region [region]
  (when-not (and (string? region)
                 (re-matches #"[a-z]{2}(-gov)?-[a-z]+-[0-9]" region))
    (throw (ex-info "S3 throughput region is invalid"
                    {:type ::invalid-s3-region})))
  region)

(defn- aws-trial-context
  [{:keys [trial target-wal-bytes encode-included?]}]
  (let [metrics (provider-metrics/recorder)
        region (checked-region (required-env "JOLT_CHDB_S3_REGION"))
        transport-options {:connect-timeout-ms 10000
                           :timeout-ms 300000
                           :max-response-bytes (* 128 1024 1024)}
        request-function
        (requiring-resolve 'jdbc.chdb.durable.s3-curl/request-function)
        request! (provider-metrics/instrument-transport
                  metrics (request-function transport-options))
        namespace
        (s3/s3-backend
         (merge
          transport-options
          {:endpoint (required-env "JOLT_CHDB_S3_ENDPOINT")
           :bucket (required-env "JOLT_CHDB_S3_BUCKET")
           :prefix (required-env "JOLT_CHDB_S3_PREFIX")
           :region region
           :access-key (required-env "JOLT_CHDB_S3_ACCESS_KEY")
           :secret-key (required-env "JOLT_CHDB_S3_SECRET_KEY")
           :session-token (required-env "JOLT_CHDB_S3_SESSION_TOKEN")
           :max-attempts 3
           :request! request!}))
        observed (provider-metrics/instrument-backend metrics namespace)]
    {:namespace-backend observed
     :object-id (str "curve-" target-wal-bytes "-" trial "-"
                     (if encode-included? "encoding" "preencoded") "-"
                     (random-uuid))
     :provider-kind :aws-s3
     :provider-region region
     :provider-metrics metrics
     :writer-options {:lease-ttl-ms 21600000
                      :heartbeat-interval-ms 7200000}
     ;; V1 and its CI role deliberately have no delete permission. Lifecycle
     ;; expiration owns eventual cleanup of this unique workflow prefix.
     :cleanup! (fn [] nil)}))

(defn- report-configuration [configuration]
  ;; A provider factory closes over namespace and authentication configuration.
  ;; Use an allowlist rather than redacting known secret names: it is
  ;; execution-only and must never enter bounded EDN or progress output.
  (select-keys configuration
               [:label :selector :batch-size :batches :warmup-batches :trials
                :question-mark? :provider-kind :target-wal-bytes]))

(defn- collect-measured-batches!
  "Run fixed batches or stop after measured pending WAL reaches the target.

  `step!` owns one complete production-path admission. `status!` is sampled
  after that admission, so target selection never estimates WAL from row
  counts. The writer's frozen segment guard remains the final authority and is
  checked again here before returning evidence."
  [{:keys [batches target-wal-bytes]} step! status!]
  (when-not (or (and (integer? batches) (pos? batches)
                     (nil? target-wal-bytes))
                (and (integer? target-wal-bytes)
                     (pos? target-wal-bytes)
                     (<= target-wal-bytes (* 127 1024 1024))
                     (nil? batches)))
    (throw (ex-info "trial requires fixed batches or one bounded WAL target"
                    {:type ::invalid-measurement-boundary})))
  (let [checked-boundary
        (fn [completed pending]
          (when-not (= completed (:pending-statements pending))
            (throw (ex-info "Durable pending WAL count mismatch"
                            {:type ::pending-wal-count-mismatch
                             :expected completed
                             :actual (:pending-statements pending)})))
          (when (> (:pending-wal-bytes pending) writer/max-wal-segment-bytes)
            (throw (ex-info "Durable pending WAL crossed its frozen limit"
                            {:type ::wal-limit-crossed})))
          {:batches completed :pending pending})]
    (if batches
      ;; Preserve the established local timing path: do not insert status calls
      ;; between fixed batches.
      (do
        (dotimes [batch batches] (step! batch))
        (checked-boundary batches (status!)))
      (loop [batch 0]
        (step! batch)
        (let [completed (inc batch)
              boundary (checked-boundary completed (status!))]
          (if (>= (get-in boundary [:pending :pending-wal-bytes])
                  target-wal-bytes)
            boundary
            (recur completed)))))))

(defn- durable-uninstrumented-trial
  [{:keys [batch-size batches warmup-batches question-mark? encode-included?
           target-wal-bytes trial] :as options}]
  (let [{:keys [namespace-backend object-id provider-kind provider-region
                writer-options cleanup! provider-metrics]}
        (trial-context! options)
        store namespace-backend
        expected (atom empty-expected-aggregates)
        trial-result (atom nil)
        configuration
        (merge {:namespace-backend store :object-id object-id
                :owner "durable-throughput-benchmark"
                :database "benchmark" :lease-ttl-ms 300000
                :heartbeat-interval-ms 100000}
               writer-options)]
    (try
      (with-open [connection (jdbc/connection (durable/writer-dbspec configuration))]
        (jdbc/execute! connection logs-ddl)
        (durable/flush! connection)
        (reduce-row-batches
         log-row batch-size warmup-batches question-mark? 0 nil
         (fn [_ _ rows]
           (swap! expected accumulate-expected-batch rows question-mark?)
           (jdbc/execute! connection (:sql (encode-batch-production rows)))))
        (durable/flush! connection)
        (System/gc)
        (when provider-metrics
          (provider-metrics/set-phase! provider-metrics :admission))
        (let [measured
              (atom
               (cond-> {:samples [] :payload-bytes 0 :statement-bytes 0
                        :maximum-batch-payload-bytes 0
                        :maximum-batch-statement-bytes 0
                        :counter-deltas {}}
                 target-wal-bytes (assoc :maximum-row-payload-bytes 0)))
              boundary
              (collect-measured-batches!
               options
               (fn [batch]
                 (let [start (+ (* batch-size warmup-batches)
                                (* batch batch-size))
                       rows (mapv #(log-row % question-mark?)
                                  (range start (+ start batch-size)))]
                   (swap! expected accumulate-expected-batch rows question-mark?)
                   (let [preencoded (when-not encode-included?
                                      (encode-batch-production rows))
                         before (counter-sample)
                         batch-start (System/nanoTime)
                         encoded (if encode-included?
                                   (encode-batch-production rows)
                                   preencoded)
                         _ (jdbc/execute! connection (:sql encoded))
                         elapsed (- (System/nanoTime) batch-start)
                         after (counter-sample)
                         maximum-row-bytes
                         (when target-wal-bytes
                           (maximum-serialized-row-payload-bytes encoded))]
                     (swap! measured
                            (fn [acc]
                              (cond->
                               (-> acc
                                   (update :samples conj elapsed)
                                   (update :payload-bytes +
                                           (:payload-bytes encoded))
                                   (update :statement-bytes +
                                           (:statement-bytes encoded))
                                   (update :maximum-batch-payload-bytes max
                                           (:payload-bytes encoded))
                                   (update :maximum-batch-statement-bytes max
                                           (:statement-bytes encoded))
                                   (update :counter-deltas add-counter-delta
                                           (counter-delta before after)))
                                maximum-row-bytes
                                (update :maximum-row-payload-bytes max
                                        maximum-row-bytes)))))))
               #(writer/status (durable-handle connection)))
              measured @measured
              batches (:batches boundary)
              samples (:samples measured)
              ingest-nanos (reduce + 0 samples)
              pending (:pending boundary)
              _ (when provider-metrics
                  (provider-metrics/set-phase! provider-metrics :flush))
              flush-before (counter-sample)
              flush-start (System/nanoTime)
              flush-result (durable/flush! connection)
              flush-nanos (- (System/nanoTime) flush-start)
              flush-after (counter-sample)]
          (when-not (successful-flush? flush-result)
            (throw (ex-info "Durable measured flush did not persist"
                            {:type ::flush-failed})))
          (reset! trial-result
                  (merge
                   {:trial trial
                    :provider-kind provider-kind
                    :provider-region provider-region
                    :mode (if encode-included?
                            :durable-encode-included
                            :durable-preencoded)
                    :instrumented? false
                    :measured-rows (* batch-size batches)
                    :batch-size batch-size :batches batches
                    :question-mark-every-row? question-mark?
                    :batch-latency (latency-summary samples)
                    ::batch-latency-samples samples
                    :ingest-ms (ms ingest-nanos)
                    :ingest-rows-per-second
                    (/ (double (* batch-size batches 1000000000)) ingest-nanos)
                    :flush-ms (ms flush-nanos)
                    :flush-cadence-batches batches
                    :flush-cadence-rows (* batch-size batches)
                    :flush-cadence-bytes (:pending-wal-bytes pending)
                    :persisted-rate-semantics
                    :one-flush-amortized-over-all-measured-batches
                    :persisted-ms (ms (+ ingest-nanos flush-nanos))
                    :persisted-rows-per-second
                    (/ (double (* batch-size batches 1000000000))
                       (+ ingest-nanos flush-nanos))
                    :ingest-counters (:counter-deltas measured)
                    :flush-counters (counter-delta flush-before flush-after)
                    :pending-before-flush pending
                    :payload-bytes (:payload-bytes measured)
                    :statement-bytes (:statement-bytes measured)}
                   (wal-size-observation pending measured)
                   (when target-wal-bytes
                     {:target-wal-bytes target-wal-bytes
                      :target-reached? true
                      :aggregation-required
                      (provider-metrics/aggregation-requirements
                       (/ (double (* batch-size batches 1000000000))
                          ingest-nanos)
                       (ms flush-nanos))})))
          (when provider-metrics
            (provider-metrics/set-phase! provider-metrics :close))))
      (when provider-metrics
        (provider-metrics/set-phase! provider-metrics :recovery))
      (let [memory-before (runtime-memory-observation)
            recovery-start (System/nanoTime)
            recovered
            (with-open [reader (jdbc/connection
                                (durable/snapshot-dbspec
                                 {:namespace-backend store :object-id object-id}))]
              (let [actual (verify-counts! reader @expected :durable-recovery)]
                {:actual actual
                 :memory-after (runtime-memory-observation)}))]
        (let [result
              (assoc @trial-result
                     :recovery {:result (:actual recovered)
                                :ms (ms (- (System/nanoTime) recovery-start))
                                :expected @expected
                                :memory (recovery-memory-observation
                                         memory-before
                                         (:memory-after recovered))})]
          (if provider-metrics
            (let [report (provider-metrics/report provider-metrics)]
              (provider-metrics/assert-uncontended-flush-control! report)
              (provider-metrics/assert-transport-coverage! report)
              (assoc result :provider-metrics report))
            result)))
      (finally
        (cleanup!)))))

(defn- durable-trial
  [{:keys [batch-size batches warmup-batches question-mark? encode-included?
           trial] :as options}]
  (let [{:keys [namespace-backend object-id provider-kind cleanup!]}
        (trial-context! options)
        metrics (atom {})
        raw-store namespace-backend
        store (instrumented-backend raw-store metrics)
        expected (atom empty-expected-aggregates)
        trial-result (atom nil)
        configuration {:namespace-backend store :object-id object-id
                       :owner "durable-throughput-benchmark"
                       :database "benchmark" :lease-ttl-ms 300000
                       :heartbeat-interval-ms 100000
                       :operations (timed-operations metrics)}]
    (try
      (with-open [connection (jdbc/connection (durable/writer-dbspec configuration))]
        (jdbc/execute! connection logs-ddl)
        (durable/flush! connection)
        (reduce-row-batches
         log-row batch-size warmup-batches question-mark? 0 nil
         (fn [_ _ rows]
           (swap! expected accumulate-expected-batch rows question-mark?)
           (jdbc/execute! connection (:sql (encode-batch-production rows)))))
        (durable/flush! connection)
        (reset! metrics {})
        (System/gc)
        (let [measured
              (reduce-row-batches
               log-row batch-size batches question-mark?
               (* batch-size warmup-batches)
               {:samples [] :payload-bytes 0 :statement-bytes 0
                :maximum-batch-payload-bytes 0
                :maximum-batch-statement-bytes 0
                :counter-deltas {}}
               (fn [acc _ rows]
                 (swap! expected accumulate-expected-batch rows question-mark?)
                 (let [preencoded (when-not encode-included?
                                    (encode-batch-production rows))
                       before (counter-sample)
                       batch-start (System/nanoTime)
                       encoded
                       (if encode-included?
                         (let [result (encode-batch-profiled rows)]
                           (record-stage! metrics :data-json
                                          (:json-nanos result)
                                          (:payload-bytes result))
                           (record-stage! metrics :exporter-materialization
                                          (:materialize-nanos result)
                                          (:payload-bytes result))
                           result)
                         preencoded)
                       _ (jdbc/execute! connection (:sql encoded))
                       elapsed (- (System/nanoTime) batch-start)
                       after (counter-sample)]
                   (-> acc
                       (update :samples conj elapsed)
                       (update :payload-bytes + (:payload-bytes encoded))
                       (update :statement-bytes + (:statement-bytes encoded))
                       (update :maximum-batch-payload-bytes max
                               (:payload-bytes encoded))
                       (update :maximum-batch-statement-bytes max
                               (:statement-bytes encoded))
                       (update :counter-deltas add-counter-delta
                               (counter-delta before after))))))
              samples-nanos (:samples measured)
              ingest-nanos (reduce + 0 samples-nanos)
              pending (writer/status (durable-handle connection))
              _ (when-not (= batches (:pending-statements pending))
                  (throw (ex-info "Durable pending WAL count mismatch"
                                  {:expected batches :actual pending})))
              flush-before (counter-sample)
              flush-start (System/nanoTime)
              flush-result (durable/flush! connection)
              flush-nanos (- (System/nanoTime) flush-start)
              flush-after (counter-sample)
              _ (when-not (successful-flush? flush-result)
                  (throw (ex-info "Durable measured flush did not persist"
                                  {:type ::flush-failed})))
              stage-values @metrics
              expected-stage-calls
              (cond-> {:prepare-query batches :native-classify batches
                       :native-execute batches
                       :backend/put-bytes-if-absent 1
                       :backend/replace-if-match 1}
                encode-included?
                (assoc :data-json batches :exporter-materialization batches))
              actual-stage-calls
              (into {} (map (fn [[stage entry]] [stage (:calls entry)]))
                    stage-values)
              _ (doseq [[stage expected-calls] expected-stage-calls]
                  (when-not (= expected-calls (get actual-stage-calls stage 0))
                    (throw (ex-info "benchmark stage count mismatch"
                                    {:stage stage :expected expected-calls
                                     :actual actual-stage-calls}))))
              _ (when-not (= (:pending-wal-bytes pending)
                             (get-in stage-values
                                     [:backend/put-bytes-if-absent :bytes]))
                  (throw (ex-info "published WAL byte count mismatch"
                                  {:pending pending :stages stage-values})))]
            (reset! trial-result
                    (merge
                     {:trial trial
                      :provider-kind provider-kind
                      :mode (if encode-included?
                              :durable-encode-included
                              :durable-preencoded)
                      :measured-rows (* batch-size batches)
                      :batch-size batch-size :batches batches
                      :question-mark-every-row? question-mark?
                      :batch-latency (latency-summary samples-nanos)
                      :ingest-ms (ms ingest-nanos)
                      :ingest-rows-per-second
                      (/ (double (* batch-size batches 1000000000)) ingest-nanos)
                      :flush-ms (ms flush-nanos)
                      :persisted-ms (ms (+ ingest-nanos flush-nanos))
                      :persisted-rows-per-second
                      (/ (double (* batch-size batches 1000000000))
                         (+ ingest-nanos flush-nanos))
                      :ingest-counters (:counter-deltas measured)
                      :flush-counters (counter-delta flush-before flush-after)
                      :pending-before-flush pending
                      :stages (stage-report metrics)
                      :payload-bytes (:payload-bytes measured)
                      :statement-bytes (:statement-bytes measured)}
                     (wal-size-observation pending measured)))))
      ;; Closing above releases the writer. This open must recover solely from
      ;; the persisted checkpoint/WAL objects and exact manifest order.
      (let [memory-before (runtime-memory-observation)
            recovery-start (System/nanoTime)
            recovered
            (with-open [reader (jdbc/connection
                                (durable/snapshot-dbspec
                                 {:namespace-backend store :object-id object-id
                                  :operations (timed-operations metrics)}))]
              (let [actual (verify-counts! reader @expected :durable-recovery)]
                {:actual actual
                 :memory-after (runtime-memory-observation)}))
            recovery-nanos (- (System/nanoTime) recovery-start)]
        (assoc @trial-result
               :recovery {:result (:actual recovered) :ms (ms recovery-nanos)
                          :expected @expected
                          :memory (recovery-memory-observation
                                   memory-before (:memory-after recovered))}
               :stages-through-recovery (stage-report metrics)))
      (finally
        (cleanup!)))))

(defn- native-trial
  [{:keys [batch-size batches warmup-batches question-mark? trial]}]
  (let [expected (atom empty-expected-aggregates)]
    (with-open [connection (jdbc/connection "chdb::memory:")]
      (jdbc/execute! connection logs-ddl)
      (reduce-row-batches
       log-row batch-size warmup-batches question-mark? 0 nil
       (fn [_ _ rows]
         (swap! expected accumulate-expected-batch rows question-mark?)
         (jdbc/execute! connection (:sql (encode-batch-production rows)))))
      (System/gc)
      (let [measured
            (reduce-row-batches
             log-row batch-size batches question-mark?
             (* batch-size warmup-batches)
             {:samples [] :payload-bytes 0 :statement-bytes 0
              :maximum-batch-payload-bytes 0
              :maximum-batch-statement-bytes 0
              :counter-deltas {}}
             (fn [acc _ rows]
               (swap! expected accumulate-expected-batch rows question-mark?)
               (let [encoded (encode-batch-production rows)
                     before (counter-sample)
                     batch-start (System/nanoTime)
                     _ (jdbc/execute! connection (:sql encoded))
                     elapsed (- (System/nanoTime) batch-start)
                     after (counter-sample)]
                 (-> acc
                     (update :samples conj elapsed)
                     (update :payload-bytes + (:payload-bytes encoded))
                     (update :statement-bytes + (:statement-bytes encoded))
                     (update :maximum-batch-payload-bytes max
                             (:payload-bytes encoded))
                     (update :maximum-batch-statement-bytes max
                             (:statement-bytes encoded))
                     (update :counter-deltas add-counter-delta
                             (counter-delta before after))))))
            samples (:samples measured)
            elapsed (reduce + 0 samples)
            actual (verify-counts! connection @expected :ordinary-native)]
        {:trial trial :mode :ordinary-native-preencoded
         :measured-rows (* batch-size batches)
         :batch-size batch-size :batches batches
         :question-mark-every-row? question-mark?
         :batch-latency (latency-summary samples)
         ::batch-latency-samples samples
         :ingest-ms (ms elapsed)
         :ingest-rows-per-second
         (/ (double (* batch-size batches 1000000000)) elapsed)
         :counters (:counter-deltas measured)
         :payload-bytes (:payload-bytes measured)
         :statement-bytes (:statement-bytes measured)
         :maximum-batch-payload-bytes
         (:maximum-batch-payload-bytes measured)
         :maximum-batch-statement-bytes
         (:maximum-batch-statement-bytes measured)
         :reconciliation {:result actual :expected @expected}}))))

(defn- isolated-stage-profile [batch-size question-mark? repetitions]
  (let [rows (one-row-batch batch-size question-mark? 0)
        encoded (encode-batch-production rows)
        sql (:sql encoded)
        wal-line (ns-resolve 'jdbc.chdb.durable.writer 'wal-line)
        validate-size (ns-resolve 'jdbc.chdb.durable.writer
                                  'validate-statement-size!)
        consume (fn [value]
                  (cond
                    (bytes? value) (alength value)
                    (string? value) (count value)
                    (number? value) value
                    (map? value) (or (:statement-bytes value) (count value))
                    :else (count (str value))))
        raw-native-insert!
        (fn [handle]
          (native/with-live-handle
           handle
           (fn [connection]
             (with-open [arena (ffi/confined-arena)]
               (let [query (ffi/alloc arena (inc (* 4 (count sql))))
                     query-length (ffi/write-bytes query sql)
                     format-text "JSONCompactEachRowWithNamesAndTypes"
                     format (ffi/alloc arena (inc (* 4 (count format-text))))
                     format-length (ffi/write-bytes format format-text)
                     result (native/chdb-query-with-params-n
                             connection query query-length format format-length
                             ffi/null ffi/null ffi/null ffi/null 0)]
                 (when (ffi/null? result)
                   (throw (ex-info "raw native benchmark returned null" {})))
                 (try
                   (when-let [message (not-empty (native/chdb-result-error result))]
                     (throw (ex-info "raw native benchmark insert failed"
                                     {:message message})))
                   (native/chdb-result-rows-written result)
                   (finally
                     (native/chdb-destroy-query-result result))))))))
        timed-many
        (fn [f]
          (dotimes [_ 3] (consume (f)))
          (System/gc)
          (let [accumulator (atom 0)
                before (counter-sample)
                samples (mapv (fn [_]
                                (let [start (System/nanoTime)]
                                  (swap! accumulator + (consume (f)))
                                  (- (System/nanoTime) start)))
                              (range repetitions))
                after (counter-sample)]
            {:latency (latency-summary samples)
             :consumed-accumulator @accumulator
             :counters (counter-delta before after)}))]
    (let [handle (native/open! ":memory:")]
      (try
        (chdb/execute-any handle logs-ddl [])
        (raw-native-insert! handle)
        {:batch-size batch-size :repetitions repetitions
         :question-mark-every-row? question-mark?
         :payload-bytes (:payload-bytes encoded)
         :statement-bytes (:statement-bytes encoded)
         :exporter-encoding-total (timed-many #(encode-batch-production rows))
         :placeholder-rewrite (timed-many #(chdb/classification-sql sql []))
         :statement-size-utf8 (timed-many #(validate-size sql))
         :wal-data-json (timed-many #(wal-line sql))
         :native-core-insert (timed-many #(raw-native-insert! handle))}
        (finally
          (native/close! handle))))))

(defn- trial-rate-summary [trials key elapsed-key]
  (let [rates (vec (map key trials))
        ordered (vec (sort rates))
        total-rows (reduce + 0 (map :measured-rows trials))
        total-ms (reduce + 0.0 (map elapsed-key trials))]
    {:trials (count trials)
     :total-rows total-rows :total-ms total-ms
     :aggregate-rows-per-second (/ (* total-rows 1000.0) total-ms)
     :trial-rate-min (first ordered)
     :trial-rate-p50 (percentile ordered 0.50)
     :trial-rate-p95-exploratory (percentile ordered 0.95)
     :trial-rate-max (peek ordered)}))

(defn- first-prefixed-line [path prefix]
  (try
    (some #(when (str/starts-with? % prefix) (str/trim %))
          (str/split-lines (slurp path)))
    (catch Throwable _ nil)))

(defn- positive-id-env [name]
  (let [value (System/getenv name)]
    (when-not (and (string? value) (re-matches #"[1-9][0-9]*" value))
      (throw (ex-info "hosted run identity is invalid"
                      {:type ::invalid-hosted-identity :option name})))
    value))

(defn- hosted-run-metadata []
  (when (System/getenv "BENCH_WORKFLOW_ID")
    (when-not (and (= "durable-aws-qualification"
                      (System/getenv "BENCH_WORKFLOW_ID"))
                   (= "workflow_dispatch" (System/getenv "BENCH_EVENT"))
                   (= "Linux" (System/getenv "BENCH_RUNNER_OS"))
                   (= "X64" (System/getenv "BENCH_RUNNER_ARCH")))
      (throw (ex-info "hosted run identity is invalid"
                      {:type ::invalid-hosted-identity})))
    {:workflow :durable-aws-qualification
     :event :workflow-dispatch
     :runner-os :linux
     :runner-arch :x64
     :run-id (positive-id-env "BENCH_RUN_ID")
     :run-attempt (positive-id-env "BENCH_RUN_ATTEMPT")}))

(defn- provider-metadata []
  (when (System/getenv "BENCH_PROVIDER_KIND")
    (when-not (= "aws-s3" (System/getenv "BENCH_PROVIDER_KIND"))
      (throw (ex-info "provider identity is invalid"
                      {:type ::invalid-provider-identity})))
    {:kind :aws-s3
     :region (checked-region (required-env "JOLT_CHDB_S3_REGION"))}))

(defn- evidence-canaries []
  (->> ["JOLT_CHDB_S3_ENDPOINT" "JOLT_CHDB_S3_BUCKET"
        "JOLT_CHDB_S3_PREFIX" "JOLT_CHDB_S3_ACCESS_KEY"
        "JOLT_CHDB_S3_SECRET_KEY" "JOLT_CHDB_S3_SESSION_TOKEN"
        "BENCH_SQL_CANARY" "BENCH_PAYLOAD_CANARY"]
       (keep #(some-> (System/getenv %) not-empty))
       vec))

(defn- runtime-metadata []
  (let [jolt-executable (System/getenv "BENCH_JOLT_BIN")
        jolt-file (when jolt-executable (File. jolt-executable))
        library (System/getenv "JOLT_CHDB_LIB")
        library-file (when library (File. library))]
    {:name :jolt :jolt-version (System/getenv "BENCH_JOLT_VERSION")
     :jolt-source-sha (System/getenv "BENCH_JOLT_SOURCE_SHA")
     :jolt-executable
     {:file-name (when jolt-file (.getName jolt-file))
      :bytes (when (and jolt-file (.isFile jolt-file)) (.length jolt-file))
      :sha256 (when (and jolt-file (.isFile jolt-file))
                (digest/sha256-file (.toPath jolt-file)))}
     :scheme-version (host/scheme-version)
     :machine-type (host/machine-type)
     :native-chdb (native/durable-capability)
     :native-library {:file-name (when library-file (.getName library-file))
                      :bytes (when (and library-file (.isFile library-file))
                               (.length library-file))
                      :sha256 (when (and library-file (.isFile library-file))
                                (digest/sha256-file (.toPath library-file)))}
     :benchmark-harness
     {:path "bench/jdbc/chdb_durable_throughput.clj"
      :sha256 (digest/sha256-file
               (.toPath (File. "bench/jdbc/chdb_durable_throughput.clj")))
      :metrics-sha256
      (digest/sha256-file
       (.toPath (File. "bench/jdbc/chdb_durable_throughput_metrics.clj")))}
     :git {:head (System/getenv "BENCH_GIT_HEAD")
           :parent (System/getenv "BENCH_GIT_PARENT")
           :tree (System/getenv "BENCH_GIT_TREE")
           :status (System/getenv "BENCH_GIT_STATUS")}
     :started-at (System/getenv "BENCH_STARTED_AT")
     :hosted-run (hosted-run-metadata)
     :provider (provider-metadata)
     :os-name (System/getProperty "os.name")
     :os-version (System/getProperty "os.version")
     :os-arch (System/getProperty "os.arch")
     :kernel (first-prefixed-line "/proc/version" "Linux")
     :cpu-model (first-prefixed-line "/proc/cpuinfo" "model name")
     :mem-total (first-prefixed-line "/proc/meminfo" "MemTotal:")
     :mem-available (first-prefixed-line "/proc/meminfo" "MemAvailable:")
     :proc-loadavg (try (str/trim (slurp "/proc/loadavg"))
                        (catch Throwable _ nil))}))

(def ^:dynamic *progress!* (fn [_ _] nil))

(def ^:private supported-profiles
  #{:smoke :probe :scale :qualification :diagnostic :s3-curve
    :scale-512 :scale-1000 :scale-5000 :scale-10000
    :recovery-512-10 :recovery-512-25 :recovery-512-50})

(def ^:private scale-configurations
  {512 [100 2]
   1000 [50 2]
   5000 [10 1]
   10000 [5 1]})

(def ^:private scale-profile-batch-size
  {:scale-512 512 :scale-1000 1000 :scale-5000 5000 :scale-10000 10000})

(def ^:private recovery-profile-batches
  {:recovery-512-10 10 :recovery-512-25 25 :recovery-512-50 50})

(defn- scale-configuration [batch-size]
  (let [[batches warmup-batches] (get scale-configurations batch-size)]
    {:label (keyword (str "scale-batched-" batch-size))
     :selector (keyword (str "scale-" batch-size))
     :batch-size batch-size :batches batches
     :warmup-batches warmup-batches :trials 5
     :question-mark? false
     :modes [:durable-encode-included :durable-preencoded
             :ordinary-native-preencoded]}))

(defn- recovery-configuration [batches]
  (let [selector (keyword (str "recovery-512-" batches))]
    {:label selector
     :selector selector
     :batch-size 512 :batches batches :warmup-batches 0 :trials 1
     :question-mark? false :modes [:durable-preencoded]}))

(defn- s3-curve-configuration [target-wal-bytes]
  {:label (keyword (str "s3-wal-" target-wal-bytes))
   :batch-size 512
   :target-wal-bytes target-wal-bytes
   :warmup-batches 0
   :trials 1
   :question-mark? false
   :provider-kind :aws-s3
   :backend-context! aws-trial-context
   :modes [:durable-encode-included :durable-preencoded]})

(defn- isolated-selector-profile? [profile]
  (or (contains? scale-profile-batch-size profile)
      (contains? recovery-profile-batches profile)))

(defn- validate-profile-configs! [profile configurations]
  (when (isolated-selector-profile? profile)
    (when-not (and (= 1 (count configurations))
                   (= profile (:selector (first configurations))))
      (throw (ex-info "selector must resolve to exactly its requested configuration"
                      {:type ::invalid-selector-resolution
                       :profile profile
                       :resolved-selectors (mapv :selector configurations)}))))
  configurations)

(defn- parse-profile! [profile-text]
  (let [profile (keyword (or profile-text "smoke"))]
    (when-not (contains? supported-profiles profile)
      (throw (ex-info "unknown Durable throughput profile"
                      {:type ::unknown-profile
                       :profile profile
                       :supported (vec (sort supported-profiles))})))
    profile))

(defn- profile-configs [profile]
  (case profile
    :smoke
    [{:label :smoke-batched :batch-size 32 :batches 2
      :warmup-batches 1 :trials 1 :question-mark? false
      :modes [:durable-encode-included :durable-preencoded
              :ordinary-native-preencoded]}]

    :probe
    [{:label :probe-batched-512 :batch-size 512 :batches 2
      :warmup-batches 1 :trials 1 :question-mark? false
      :modes [:durable-encode-included :durable-preencoded
              :ordinary-native-preencoded]}]

    :scale
    (mapv scale-configuration [512 1000 5000 10000])

    :s3-curve
    (mapv s3-curve-configuration provider-metrics/wal-target-bytes)

    :qualification
    [{:label :batched-512 :batch-size 512 :batches 100
      :warmup-batches 2 :trials 5 :question-mark? false
      :modes [:durable-encode-included :durable-preencoded
              :ordinary-native-preencoded]}
     {:label :batched-512-question-mark :batch-size 512 :batches 100
      :warmup-batches 2 :trials 5 :question-mark? true
      :modes [:durable-preencoded]}
     {:label :single-row :batch-size 1 :batches 128
      :warmup-batches 16 :trials 5 :question-mark? false
      :modes [:durable-encode-included :durable-preencoded
              :ordinary-native-preencoded]}]

    (cond
      (contains? scale-profile-batch-size profile)
      [(scale-configuration (get scale-profile-batch-size profile))]

      (contains? recovery-profile-batches profile)
      [(recovery-configuration (get recovery-profile-batches profile))]

      :else
      (throw (ex-info "profile does not use the Durable trial runner"
                      {:type ::unsupported-run-profile
                       :profile profile
                       :supported (vec (sort (disj supported-profiles
                                                   :diagnostic)))})))))

(defn- require-qualification-provenance! [profile runtime]
  (when (or (contains? #{:scale :qualification :s3-curve} profile)
            (isolated-selector-profile? profile))
    (let [required
          (cond->
           {:jolt-version (:jolt-version runtime)
            :jolt-source-sha (:jolt-source-sha runtime)
            :jolt-executable-sha256
            (get-in runtime [:jolt-executable :sha256])
            :started-at (:started-at runtime)
            :native-library-sha256
            (get-in runtime [:native-library :sha256])
            :git-head (get-in runtime [:git :head])
            :git-parent (get-in runtime [:git :parent])
            :git-tree (get-in runtime [:git :tree])
            :git-status (get-in runtime [:git :status])}
            (= :s3-curve profile)
            (assoc
             :metrics-harness-sha256
             (get-in runtime [:benchmark-harness :metrics-sha256])
             :workflow (get-in runtime [:hosted-run :workflow])
             :event (get-in runtime [:hosted-run :event])
             :runner-os (get-in runtime [:hosted-run :runner-os])
             :runner-arch (get-in runtime [:hosted-run :runner-arch])
             :run-id (get-in runtime [:hosted-run :run-id])
             :run-attempt (get-in runtime [:hosted-run :run-attempt])
             :provider-kind (get-in runtime [:provider :kind])
             :provider-region (get-in runtime [:provider :region])))
          missing (->> required
                       (keep (fn [[field value]]
                               (when (or (nil? value)
                                         (and (string? value)
                                              (str/blank? value)))
                                 field)))
                       vec)]
      (when (seq missing)
        (throw (ex-info "scale and qualification profiles require complete provenance"
                        {:type ::missing-provenance :missing missing})))
      (when-not (re-matches #"[0-9a-f]{40}" (:jolt-source-sha runtime))
        (throw (ex-info "scale and qualification profiles require a full Jolt source SHA"
                        {:type ::invalid-provenance
                         :field :jolt-source-sha})))
      (when-not (pos? (get-in runtime [:native-library :bytes] 0))
        (throw (ex-info "scale and qualification profiles require a nonempty native library"
                        {:type ::missing-provenance
                         :missing [:native-library-bytes]})))
      (when-not (pos? (get-in runtime [:jolt-executable :bytes] 0))
        (throw (ex-info "scale and qualification profiles require a nonempty Jolt executable"
                        {:type ::missing-provenance
                         :missing [:jolt-executable-bytes]})))
      (when-not (= "clean" (:git-status required))
        (throw (ex-info "scale and qualification profiles require a clean worktree"
                        {:type ::dirty-provenance
                         :git-status (:git-status required)}))))))

(defn- run-config [configuration]
  (let [{:keys [trials modes]} configuration
        measured-results
        (reduce
         (fn [acc trial]
           (let [rotation (mod (dec trial) (count modes))
                 ordered-modes (vec (concat (drop rotation modes)
                                            (take rotation modes)))]
             (reduce
              (fn [acc mode]
                (let [result
                      (case mode
                        :durable-encode-included
                        (durable-uninstrumented-trial
                         (assoc configuration :trial trial
                                :encode-included? true))
                        :durable-preencoded
                        (durable-uninstrumented-trial
                         (assoc configuration :trial trial
                                :encode-included? false))
                        :ordinary-native-preencoded
                        (native-trial (assoc configuration :trial trial)))]
                  (*progress!* :uninstrumented-trial
                               (select-keys
                                result
                                [:trial :provider-kind :provider-region
                                 :mode :batch-size :batches :target-wal-bytes
                                 :question-mark-every-row? :batch-latency
                                 :ingest-ms :ingest-rows-per-second :flush-ms
                                 :persisted-ms :persisted-rows-per-second
                                 :payload-bytes :statement-bytes
                                 :maximum-batch-payload-bytes
                                 :maximum-row-payload-bytes
                                 :maximum-batch-statement-bytes
                                 :wal-growth :aggregation-required :recovery
                                 :provider-metrics]))
                  (conj acc result)))
              acc ordered-modes)))
         [] (range 1 (inc trials)))
        results (mapv #(dissoc % ::batch-latency-samples) measured-results)]
    {:configuration (report-configuration configuration)
     :results results
     :summaries
     (into {}
           (map (fn [mode]
                  (let [selected (filterv #(= mode (:mode %)) measured-results)
                        batch-latency
                        (latency-summary
                         (mapcat ::batch-latency-samples selected))]
                    [mode
                     (if (= mode :ordinary-native-preencoded)
                       (assoc
                        (trial-rate-summary selected :ingest-rows-per-second
                                            :ingest-ms)
                        :batch-latency-across-trials batch-latency)
                       {:admission
                        (trial-rate-summary selected :ingest-rows-per-second
                                            :ingest-ms)
                        :persisted
                        (trial-rate-summary selected :persisted-rows-per-second
                                            :persisted-ms)
                        :batch-latency-across-trials batch-latency})])))
           modes)}))

(defn run! [profile]
  (let [configs (validate-profile-configs! profile (profile-configs profile))
        instrumentation-contract (instrumentation-contract!)
        smoke? (= profile :smoke)
        probe? (= profile :probe)
        isolated-selector? (or (isolated-selector-profile? profile)
                               (= :s3-curve profile))]
    (let [runtime (runtime-metadata)
          _ (require-qualification-provenance! profile runtime)
          _ (*progress!* :started {:runtime runtime :profile profile})
          configuration-results (mapv run-config configs)
          _ (*progress!* :uninstrumented-complete
                         {:configurations
                          (mapv #(select-keys % [:configuration :summaries])
                                configuration-results)})
          isolated (when-not isolated-selector?
                     (isolated-stage-profile (if smoke? 32 512) false
                                             (cond smoke? 8 probe? 2 :else 20)))
          _ (when isolated
              (*progress!* :isolated-stages-complete isolated))
          instrumented
          (when-not isolated-selector?
            (durable-trial {:batch-size (if smoke? 32 512)
                            :batches (if smoke? 2 1)
                            :warmup-batches 1 :trials 1 :trial 1
                            :question-mark? false :encode-included? false}))
          _ (when instrumented
              (*progress!* :instrumented-complete instrumented))]
    {:schema-version 2
     :profile profile
     :process-scope (cond
                      (= :s3-curve profile)
                      :independent-remote-object-per-target-and-mode
                      isolated-selector? :one-selected-configuration
                      :else :multi-configuration-with-controls)
     :runtime runtime
     :output-contract report-contract
     :instrumentation-contract instrumentation-contract
     :workload {:shape :clickstack-otel-log-jsoneachrow
                :invariants [:exact-recovered-count :exact-trace-flags-sum
                             :pending-wal-statement-count
                             :one-measured-flush-commit
                             :immutable-snapshot-replay]
                :ordering "prepare and size-check WAL, then native execute, then append; flush afterward"
                :checkpoint {:status :not-applicable
                             :reason :materialized-sql-uses-v1-statement-wal}}
     :memory-measurement recovery-memory-limitations
     :supplementary-controls
     (if isolated-selector?
       {:status :not-run
        :reason :preserve-selector-process-attribution-for-peak-rss}
       {:status :included})
     :isolated-stages isolated
     :instrumented-control instrumented
     :configurations configuration-results})))

(defn- diagnostic! [output]
  (let [root-file (File/createTempFile "jolt-chdb-throughput-diagnostic-" "")
        _ (.delete root-file)
        _ (.mkdirs root-file)
        store (local/local-backend (.getAbsolutePath root-file))
        object-id (str "diagnostic-" (random-uuid))
        warmup-rows (one-row-batch 512 false 0)
        measured-rows (one-row-batch 512 false 512)
        warmup (encode-batch-production warmup-rows)
        measured (encode-batch-production measured-rows)
        expected (-> empty-expected-aggregates
                     (accumulate-expected-batch warmup-rows false)
                     (accumulate-expected-batch measured-rows false))
        progress (atom {:schema-version 2 :profile :diagnostic
                        :runtime (runtime-metadata)
                        :configuration {:batch-size 512 :measured-batches 1
                                        :warmup-batches 1
                                        :mode :durable-preencoded
                                        :backend :local-posix}
                        :payload {:rows 512
                                  :payload-bytes (:payload-bytes measured)
                                  :statement-bytes (:statement-bytes measured)
                                  :maximum-batch-payload-bytes
                                  (:payload-bytes measured)
                                  :maximum-row-payload-bytes
                                  (maximum-serialized-row-payload-bytes measured)
                                  :maximum-batch-statement-bytes
                                  (:statement-bytes measured)}
                        :memory-measurement recovery-memory-limitations
                        :phases []})
        emit! (fn [phase data]
                (let [event {:phase phase :epoch-ms (System/currentTimeMillis)
                             :data data}]
                  (swap! progress update :phases conj event)
                  (spit output (str (pr-str @progress) "\n"))
                  (println (pr-str event))))]
    (try
      (emit! :prepared {:warmup-statement-bytes (:statement-bytes warmup)})
      (let [connection (jdbc/connection
                        (durable/writer-dbspec
                         {:namespace-backend store :object-id object-id
                          :owner "durable-throughput-diagnostic"
                          :database "benchmark" :lease-ttl-ms 300000
                          :heartbeat-interval-ms 100000}))]
        (try
          (emit! :opened {})
          (jdbc/execute! connection logs-ddl)
          (emit! :ddl-executed {})
          (durable/flush! connection)
          (emit! :ddl-flushed {})
          (jdbc/execute! connection (:sql warmup))
          (emit! :warmup-executed {})
          (durable/flush! connection)
          (emit! :warmup-flushed {})
          (System/gc)
          (let [before (counter-sample)
                start (System/nanoTime)]
            (jdbc/execute! connection (:sql measured))
            (let [elapsed (- (System/nanoTime) start)
                  after (counter-sample)
                  status (writer/status (durable-handle connection))]
              (when-not (= 1 (:pending-statements status))
                (throw (ex-info "diagnostic pending WAL mismatch"
                                {:status status})))
              (emit! :measured-execute
                     (merge
                      {:elapsed-ms (ms elapsed)
                       :rows-per-second (/ 512000000000.0 elapsed)
                       :counters (counter-delta before after)
                       :pending status}
                      (wal-size-observation
                       status
                       {:maximum-batch-payload-bytes (:payload-bytes measured)
                        :maximum-row-payload-bytes
                        (maximum-serialized-row-payload-bytes measured)
                        :maximum-batch-statement-bytes
                        (:statement-bytes measured)})))))
          (let [before (counter-sample)
                start (System/nanoTime)
                result (durable/flush! connection)
                elapsed (- (System/nanoTime) start)
                after (counter-sample)]
            (when-not (successful-flush? result)
              (throw (ex-info "diagnostic flush did not persist"
                              {:type ::flush-failed})))
            (emit! :measured-flush {:elapsed-ms (ms elapsed)
                                    :counters (counter-delta before after)}))
          (finally
            (.close connection)
            (emit! :writer-closed {}))))
      (let [memory-before (runtime-memory-observation)
            start (System/nanoTime)
            recovered
            (with-open [reader (jdbc/connection
                                (durable/snapshot-dbspec
                                 {:namespace-backend store
                                  :object-id object-id}))]
              (let [actual (verify-counts! reader expected
                                           :diagnostic-recovery)]
                {:actual actual
                 :memory-after (runtime-memory-observation)}))]
        (emit! :reopened-and-reconciled
               {:elapsed-ms (ms (- (System/nanoTime) start))
                :expected expected :actual (:actual recovered)
                :memory (recovery-memory-observation
                         memory-before (:memory-after recovered))}))
      (emit! :complete {:status :ok})
      @progress
      (catch Throwable error
        (emit! :failed {:class (str (class error))
                        :message (ex-message error)
                        :type (:type (ex-data error))})
        (throw error))
      (finally
        (delete-tree! root-file)))))

(defn -main [& [profile-text output]]
  (let [profile (parse-profile! profile-text)
        output (or output (str "target/profiles/durable-throughput-"
                               (name profile) ".edn"))]
    (try
      (.mkdirs (.getParentFile (File. output)))
      (if (= profile :diagnostic)
        (do (diagnostic! output)
            (println (pr-str {:status :ok :profile profile :output output})))
        (let [progress (atom {:schema-version 2 :profile profile :phases []})
              emit! (fn [phase data]
                      (let [event
                            {:phase phase :epoch-ms (System/currentTimeMillis)
                             :data data}]
                        (swap! progress update :phases conj event)
                        (spit output (str (pr-str @progress) "\n"))
                        (println (pr-str
                                  (select-keys event [:phase :epoch-ms])))))
              report (binding [*progress!* emit!] (run! profile))
              final-report (assoc report :phase-log (:phases @progress))]
          (when (= :s3-curve profile)
            (provider-metrics/assert-redacted!
             final-report "" "" (evidence-canaries)))
          (spit output (str (pr-str final-report) "\n"))
          (println (pr-str {:status :ok :profile profile :output output
                           :configurations
                           (mapv (fn [result]
                                   {:configuration (:configuration result)
                                    :summaries (:summaries result)})
                                 (:configurations report))}))))
      (catch Throwable error
        (if (= :s3-curve profile)
          (provider-metrics/throw-redacted-failure! error)
          (throw error))))))
