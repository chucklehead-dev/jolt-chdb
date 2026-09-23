(ns jdbc.chdb-durable-throughput
  "Current-main, production-path Durable JSONEachRow throughput probe.

  The report separates row encoding, pre-encoded Durable admission, the
  persisted flush boundary, and ordinary non-Durable execution. It uses a
  ClickStack-compatible log shape and verifies every run by reopening the
  immutable Durable snapshot. This is a manual benchmark, not a CI gate."
  (:require [clojure.edn :as edn]
            [clojure.data.json :as json]
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
            [jolt.host :as host]
            [jolt.process :as process])
  (:import [java.io File]
           [java.nio.file Files]))

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

(def ^:private attribution-stage-keys
  #{:data-json :exporter-materialization :prepare-query :native-classify
    :native-execute :wal-prepare :wal-append
    :backend/put-file-if-absent :backend/download-to-file
    :backend/get-with-etag :backend/replace-if-match})

(def ^:private attribution-events-key ::attribution-events)

(defn- stage-metrics []
  ;; Jolt atoms are deliberately not IObj values, so metadata cannot be used
  ;; as a side channel here. Keep the bounded event ledger under a private map
  ;; key instead, and explicitly omit it from every retained/reportable view.
  (atom {attribution-events-key (atom [])}))

(defn- attribution-events [metrics]
  (get @metrics attribution-events-key))

(defn- stage-values [metrics]
  (dissoc @metrics attribution-events-key))

(defn- expected-writer-stage-calls
  "Return the exact measured writer-stage cardinalities for one configuration.

  File-WAL publication verifies a created immutable object before returning
  from publication. A confirmed commit reuses that exact publication proof,
  so the file-WAL selectors expect one verification. Absent or mismatched
  proofs still require commit-time verification. The old byte-WAL path verifies
  at commit time and remains outside the file-WAL selectors."
  [{:keys [selector encode-included?]} batches]
  (cond-> {:prepare-query batches :native-classify batches
           :native-execute batches
           :backend/put-file-if-absent 1
           :backend/replace-if-match 1}
    (contains? #{:stage-512 :stage-smoke} selector)
    (assoc :wal-prepare batches
           :wal-append batches
           :wal-join 1
           :wal-immutable-put 1
           :wal-immutable-verify 1
           :wal-head-cas 1)
    encode-included?
    (assoc :data-json batches :exporter-materialization batches)))

(defn- reset-stage-metrics! [metrics]
  (let [events (attribution-events metrics)]
    (reset! metrics {attribution-events-key events})
    (reset! events []))
  metrics)

(defn- record-attribution-event! [metrics event stage]
  (when (contains? attribution-stage-keys stage)
    (when-let [events (attribution-events metrics)]
      (swap! events conj [event stage]))))

(defn- record-recovery-phase!
  [metrics {:keys [phase status calls nanos bytes]}]
  (swap! metrics update phase
         (fn [entry]
           (-> (or entry {:calls 0 :nanos 0 :bytes 0 :statuses {}})
               (update :calls + calls)
               (update :nanos + nanos)
               (update :bytes + bytes)
               (update-in [:statuses status] (fnil + 0) calls)))))

(defn- record-writer-phase!
  "Record a settled writer phase in the benchmark's bounded causal ledger."
  [metrics event]
  (let [phase (:phase event)]
    (record-attribution-event! metrics :start phase)
    (try
      (record-recovery-phase! metrics event)
      (finally
        (record-attribution-event! metrics :finish phase)))))

(defn- timed-stage [metrics stage bytes f]
  (record-attribution-event! metrics :start stage)
  (try
    (let [start (System/nanoTime)
          value (f)]
      (record-stage! metrics stage (- (System/nanoTime) start) bytes)
      value)
    (finally
      (record-attribution-event! metrics :finish stage))))

(defn- stage-report [metrics]
  (into {}
        (map (fn [[stage {:keys [calls nanos bytes statuses]}]]
               [stage
                (cond-> {:calls calls :total-ms (ms nanos) :bytes bytes
                         :mean-ms
                         (if (zero? calls) 0.0 (ms (/ nanos calls)))}
                  statuses (assoc :statuses statuses))]))
        (stage-values metrics)))

(def ^:private admission-attribution-stages
  [[:encoding/data-json :data-json]
   [:encoding/materialization :exporter-materialization]
   [:writer/prepare-query :prepare-query]
   [:writer/native-classify :native-classify]
   [:writer/native-execute :native-execute]
   [:writer/wal-prepare :wal-prepare]
   [:writer/wal-append :wal-append]])

(def ^:private flush-attribution-stages
  [[:backend/put-immutable-wal :backend/put-file-if-absent]
   [:backend/verify-immutable-wal :backend/download-to-file]
   [:backend/read-head :backend/get-with-etag]
   [:backend/compare-and-swap-head :backend/replace-if-match]])

(def ^:private attribution-limitations
  {:admission
   [:driver-and-queue-dispatch
    :writer-policy-and-lease-checks
    :timer-bookkeeping]
   :flush
   [:driver-and-queue-dispatch
    :writer-lease-checks
    :wal-join-and-clear
    :publication-and-commit-control
    :timer-bookkeeping]})

(defn- stage-attribution!
  "Build one exact, non-overlapping benchmark-only timing partition.

  `total-nanos` is the enclosing admission or flush stopwatch. The listed
  operation seams are known to execute sequentially on the writer request.
  Their measured durations are therefore disjoint components; everything else
  inside the enclosing stopwatch is retained as the explicit residual. This
  stays in the benchmark harness: neither Durable operations nor their queue,
  lease, WAL, or publication semantics gain an observer."
  [boundary total-nanos metrics events stage-spec]
  (when-not (and (integer? total-nanos) (not (neg? total-nanos)))
    (throw (ex-info "invalid benchmark timing total"
                    {:type ::invalid-stage-attribution})))
  (when-not (seq events)
    (throw (ex-info "benchmark stage causality evidence is missing"
                    {:type ::invalid-stage-attribution})))
  (let [_ (loop [remaining events active #{}]
            (if-let [[event stage] (first remaining)]
              (case event
                :start
                (if (or (not (contains? attribution-stage-keys stage))
                        (seq active))
                  (throw (ex-info "benchmark stages overlap or are malformed"
                                  {:type ::invalid-stage-attribution}))
                  (recur (next remaining) #{stage}))
                :finish
                (if (contains? active stage)
                  (recur (next remaining) #{})
                  (throw (ex-info "benchmark stage finish is malformed"
                                  {:type ::invalid-stage-attribution})))
                (throw (ex-info "benchmark stage event is malformed"
                                {:type ::invalid-stage-attribution})))
              (when (seq active)
                (throw (ex-info "benchmark stage did not settle"
                                {:type ::invalid-stage-attribution})))))
        categories
        (into (array-map)
              (map (fn [[category stage]]
                     [category (long (get-in metrics [stage :nanos] 0))]))
              stage-spec)
        _ (when (some neg? (vals categories))
            (throw (ex-info "invalid benchmark stage duration"
                            {:type ::invalid-stage-attribution})))
        accounted-nanos (reduce + 0 (vals categories))
        _ (when (> accounted-nanos total-nanos)
            ;; A stage becoming nested/overlapping would turn a residual into
            ;; false attribution. Fail the manual diagnostic rather than
            ;; silently clamp the negative remainder to zero.
            (throw (ex-info "benchmark stage durations exceed enclosing timer"
                            {:type ::invalid-stage-attribution})))
        unattributed-nanos (- total-nanos accounted-nanos)
        categories (assoc categories :writer/unattributed unattributed-nanos)]
    {:boundary boundary
     :total-nanos total-nanos
     :total-ms (ms total-nanos)
     :accounted-nanos accounted-nanos
     :unattributed-nanos unattributed-nanos
     :partition-verified? (= total-nanos (reduce + 0 (vals categories)))
     :causal-order-verified? true
     :unattributed-semantics (get attribution-limitations boundary)
     :categories
     (into (array-map)
           (map (fn [[category nanos]]
                  [category {:total-nanos nanos :total-ms (ms nanos)}]))
           categories)}))

(defn- instrumented-backend [delegate metrics]
  (reify backend/ObjectBackend
    (get-bytes [_ key]
      (timed-stage metrics :backend/get-bytes 0 #(backend/get-bytes delegate key)))
    (get-with-etag [_ key]
      (timed-stage metrics :backend/get-with-etag 0
                   #(backend/get-with-etag delegate key)))
    (put-file-if-absent! [_ key path]
      ;; The Phase 2 writer supplies its sealed WAL file.  Measure its scalar
      ;; file size, not content, so stage-byte accounting remains comparable
      ;; to the old byte-array publication without retaining payload bytes.
      (timed-stage metrics :backend/put-file-if-absent (Files/size path)
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

(defn- timed-operations
  ([metrics]
   (timed-operations metrics false))
  ([metrics writer-phase-attribution?]
  ;; Do not replace :publish-wal!. The production writer operation closes over
  ;; its complete retry options, including the lease-aware :stopped? predicate.
  ;; Publication remains visible through the timed immutable PUT and head-CAS
  ;; backend operations without changing that control contract.
   (cond->
    {:recovery-phase!
     (fn [event]
       (record-recovery-phase! metrics event))
     :classification-sql!
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
                    #(chdb/execute-prepared-any handle prepared)))}
     ;; This is intentionally stage-selector-only: it is an observation-only
     ;; scalar side channel, not a production operation override.
     writer-phase-attribution?
     (assoc :writer-phase!
            (fn [event]
              (record-writer-phase! metrics event))))))

(defn- instrumentation-contract! []
  (let [operations (timed-operations (atom {}))]
    (when (or (contains? operations :publish-wal!)
              (contains? operations :publish-wal-file!))
      (throw (ex-info
              "benchmark must retain the production retry-aware WAL publisher"
              {:operation :publish-wal-file!})))
    {:publish-wal-operation :production
     :retry-options :writer-owned
     :publication-observation [:backend/put-file-if-absent
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
                      :heartbeat-interval-ms 7200000
                      :max-attempts 4}
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

(def ^:dynamic *writer-role?* false)
(def ^:dynamic *worker-descriptor* nil)

(def ^:private measurement-boundaries
  {:admission :jdbc-return-not-crash-safe-ack
   :persisted :successful-committed-or-reconciled-flush
   :readback :independent-process-aggregate-reconciliation
   :typed-value-equivalence :not-qualified
   :checkpoint :not-qualified
   :tail-throughput :not-qualified-by-selector})

(defn- durable-uninstrumented-trial
  [{:keys [batch-size batches warmup-batches question-mark? encode-included?
           target-wal-bytes trial] :as options}]
  (let [{:keys [namespace-backend object-id provider-kind provider-region
                writer-options cleanup! provider-metrics]}
        (trial-context! options)
        store namespace-backend
        expected (atom empty-expected-aggregates)
        trial-result (atom nil)
        flush-outcome (atom nil)
        configuration
        (merge {:namespace-backend store :object-id object-id
                :scratch-parent (:root *worker-descriptor*)
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
          (reset! flush-outcome (:status flush-result))
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
      {:result @trial-result :expected @expected
         :flush-outcome @flush-outcome
         :provider-observations
         (when provider-metrics
           {:logical @(:logical provider-metrics)
            :transport @(:transport provider-metrics)})}
      (finally
        (cleanup!)))))

(defn- durable-trial
  [{:keys [batch-size batches warmup-batches question-mark? encode-included?
           trial] :as options}]
  (let [{:keys [namespace-backend object-id provider-kind cleanup!]}
        (trial-context! options)
        metrics (stage-metrics)
        raw-store namespace-backend
        store (instrumented-backend raw-store metrics)
        expected (atom empty-expected-aggregates)
        trial-result (atom nil)
        configuration {:namespace-backend store :object-id object-id
                       :scratch-parent (:root *worker-descriptor*)
                       :owner "durable-throughput-benchmark"
                       :database "benchmark" :lease-ttl-ms 300000
                       :heartbeat-interval-ms 100000
                       :operations (timed-operations
                                    metrics
                                    (contains? #{:stage-512 :stage-smoke}
                                               (:selector options)))}]
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
        (reset-stage-metrics! metrics)
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
                           (record-attribution-event! metrics :start :data-json)
                           (record-stage! metrics :data-json
                                          (:json-nanos result)
                                          (:payload-bytes result))
                           (record-attribution-event! metrics :finish :data-json)
                           (record-attribution-event! metrics :start
                                                      :exporter-materialization)
                           (record-stage! metrics :exporter-materialization
                                          (:materialize-nanos result)
                                          (:payload-bytes result))
                           (record-attribution-event! metrics :finish
                                                      :exporter-materialization)
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
              stage-values (stage-values metrics)
              expected-stage-calls (expected-writer-stage-calls options batches)
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
                                     [:backend/put-file-if-absent :bytes]))
                  (throw (ex-info "published WAL byte count mismatch"
                                  {:pending pending :stages stage-values})))
              admission-attribution
              (stage-attribution! :admission ingest-nanos stage-values
                                  @(attribution-events metrics)
                                  admission-attribution-stages)
              flush-attribution
              (stage-attribution! :flush flush-nanos stage-values
                                  @(attribution-events metrics)
                                  flush-attribution-stages)]
            (reset! trial-result
                    (merge
                     {:trial trial
                      :provider-kind provider-kind
                      :mode (if encode-included?
                              :durable-encode-included
                              :durable-preencoded)
                      :instrumented? true
                      :measured-rows (* batch-size batches)
                      :batch-size batch-size :batches batches
                      :question-mark-every-row? question-mark?
                      :batch-latency (latency-summary samples-nanos)
                      ::batch-latency-samples samples-nanos
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
                      ;; These exact stopwatch partitions intentionally keep
                      ;; the residual broad rather than attributing it to an
                      ;; unobserved Durable implementation detail.
                      :admission-attribution admission-attribution
                      :flush-attribution flush-attribution
                      :payload-bytes (:payload-bytes measured)
                      :statement-bytes (:statement-bytes measured)}
                     (wal-size-observation pending measured)))))
      ;; The bounded causality ledger is parent-local diagnostic state. A
      ;; cross-process handoff needs only scalar stage observations for the
      ;; reader's recovery report.
      {:result @trial-result :expected @expected
       :stage-observations (stage-values metrics)}
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
    :matched-local-512 :matched-local-1000 :matched-local-5000 :matched-local-10000
    :matched-aws-512 :matched-aws-1000 :matched-aws-5000 :matched-aws-10000
    :scale-512 :scale-1000 :scale-5000 :scale-10000
    :stage-512 :stage-smoke
    :recovery-512-10 :recovery-512-25 :recovery-512-50})

(def ^:private scale-configurations
  {512 [100 2]
   1000 [50 2]
   5000 [10 1]
   10000 [5 1]})

(def ^:private scale-profile-batch-size
  {:scale-512 512 :scale-1000 1000 :scale-5000 5000 :scale-10000 10000})

(def ^:private matched-profiles
  {:matched-local-512 [:local-posix 512] :matched-local-1000 [:local-posix 1000]
   :matched-local-5000 [:local-posix 5000] :matched-local-10000 [:local-posix 10000]
   :matched-aws-512 [:aws-s3 512] :matched-aws-1000 [:aws-s3 1000]
   :matched-aws-5000 [:aws-s3 5000] :matched-aws-10000 [:aws-s3 10000]})

(def ^:private encoding-inclusive-512-acceptance-profiles
  #{:qualification :scale-512 :matched-local-512 :matched-aws-512})

(def ^:private encoding-inclusive-512-acceptance-target
  {:sample-count 500 :p50-max-ms 20.48 :p99-max-ms 25.60})

(defn- finite-number? [value]
  (and (number? value)
       (not (Double/isNaN (double value)))
       (not (Double/isInfinite (double value)))))

(defn encoding-inclusive-512-acceptance
  "Pure, structured acceptance evidence for the Durable encoding-inclusive
  512-row path.  This intentionally judges only the selected summary, never
  raw samples or a provider descriptor.  A malformed/incomplete report is
  not a passing result: it is explicitly `:not-qualified` so the persisted
  receipt says why it did not establish the performance target."
  [profile configurations]
  (if-not (contains? encoding-inclusive-512-acceptance-profiles profile)
    {:status :not-applicable}
    (let [target encoding-inclusive-512-acceptance-target
          candidates
          (filterv (fn [{:keys [configuration summaries]}]
                     (and (= 512 (get-in configuration [:batch-size]))
                          (contains? summaries :durable-encode-included)))
                   configurations)]
      (if-not (= 1 (count candidates))
        {:status :not-qualified :reason :missing-or-ambiguous-encoding-inclusive-512
         :target target}
        (let [latency (get-in (first candidates)
                              [:summaries :durable-encode-included
                               :batch-latency-across-trials])
              observed (select-keys latency [:count :p99-qualification?
                                             :p50-ms :p99-ms])
              qualified-shape?
              (and (= (:sample-count target) (:count latency))
                   (true? (:p99-qualification? latency))
                   (finite-number? (:p50-ms latency))
                   (finite-number? (:p99-ms latency)))]
          (cond
            (not qualified-shape?)
            {:status :not-qualified :reason :incomplete-latency-qualification
             :target target :observed observed}

            (and (<= (:p50-ms latency) (:p50-max-ms target))
                 (<= (:p99-ms latency) (:p99-max-ms target)))
            {:status :passed :target target :observed observed}

            :else
            {:status :failed :reason :latency-target-missed
             :target target :observed observed}))))))

(defn- require-encoding-inclusive-512-acceptance! [acceptance]
  ;; Called only after the final report has been written.  The exception is a
  ;; stable category, while the report retains the bounded numerical receipt.
  (when (contains? #{:failed :not-qualified} (:status acceptance))
    (throw (ex-info "Durable encoding-inclusive 512 acceptance target missed"
                    {:type (if (= :failed (:status acceptance))
                             ::acceptance-target-missed
                             ::acceptance-not-qualified)}))))

(defn- aws-matched-profile? [profile]
  (= :aws-s3 (first (get matched-profiles profile))))

(defn- remote-s3-profile? [profile]
  (or (= :s3-curve profile)
      (aws-matched-profile? profile)))

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

(defn- matched-configuration [provider batch-size]
  (when-not (and (contains? provider-kinds provider)
                (contains? scale-configurations batch-size))
    (throw (ex-info "unknown matched benchmark configuration"
                    {:type ::invalid-matched-configuration})))
  (assoc (scale-configuration batch-size)
         :label (keyword (str "matched-" (name provider) "-" batch-size))
         :provider-kind provider
         :backend-context! (case provider :local-posix local-trial-context
                                         :aws-s3 aws-trial-context)
         :modes [:durable-encode-included :durable-preencoded]))

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
  (or (contains? matched-profiles profile)
      (contains? scale-profile-batch-size profile)
      (contains? #{:stage-512 :stage-smoke} profile)
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
      (contains? matched-profiles profile)
      [(assoc (apply matched-configuration (get matched-profiles profile)) :selector profile)]

      (contains? scale-profile-batch-size profile)
      [(scale-configuration (get scale-profile-batch-size profile))]

      (= :stage-512 profile)
      [(assoc (scale-configuration 512)
              :label :stage-512
              :selector :stage-512
              :trials 1
              :instrumented? true
              :modes [:durable-preencoded])]

      (= :stage-smoke profile)
      [{:label :stage-smoke
        :selector :stage-smoke
        :batch-size 32 :batches 1 :warmup-batches 1 :trials 1
        :instrumented? true :question-mark? false
        :modes [:durable-preencoded]}]

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
            (remote-s3-profile? profile)
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

(defn- worker-options [options]
  ;; The selector is a bounded, non-secret execution attribute. Stage
  ;; selectors must cross the process boundary so the real child installs its
  ;; observation-only WAL/control phase hook; otherwise stage reports silently
  ;; omit those child phases while claiming an instrumented profile.
  (select-keys options [:selector :batch-size :batches :warmup-batches :question-mark?
                       :encode-included? :target-wal-bytes :trial]))

(defn- provider-descriptor! [options root]
  (let [factory (:backend-context! options)]
    (when (and factory (not (identical? factory aws-trial-context))
               (not (identical? factory local-trial-context)))
      (throw (ex-info "custom benchmark factories cannot cross worker boundaries"
                      {:type ::nonserializable-provider})))
    {:provider-kind (if (identical? factory aws-trial-context) :aws-s3 :local-posix)
     :root root :object-id (str "bench-worker-" (random-uuid))}))

(defn- reconstruct-context! [descriptor options]
  (case (:provider-kind descriptor)
    :local-posix
    {:namespace-backend (local/local-backend (:root descriptor))
     :object-id (:object-id descriptor) :provider-kind :local-posix
     :cleanup! (fn [] nil)}
    :aws-s3
    ;; Authentication is read only by the existing constructor from inherited
    ;; environment. Neither request nor receipt contains provider credentials.
    (assoc (aws-trial-context options) :object-id (:object-id descriptor))
    (throw (ex-info "unsupported benchmark provider" {:type ::invalid-worker-provider}))))

(defn- await-worker [child milliseconds]
  (try
    (let [result (deref child milliseconds ::timeout)]
      (when (and (map? result) (integer? (:exit result))) result))
    (catch Throwable _ nil)))

(defn- runtime-identity [runtime]
  (select-keys runtime [:jolt-executable :native-library :benchmark-harness :scheme-version]))

(defn- read-owned-edn! [path]
  (let [forms (edn/read-string (str "[" (slurp path) "]"))]
    (when-not (= 1 (count forms))
      (throw (ex-info "missing or duplicate worker result"
                      {:type ::invalid-worker-receipt})))
    (first forms)))

(defn- require-worker-receipt! [receipt role token request]
  (when-not (and (map? receipt) (= 1 (:schema-version receipt))
                 (= role (:role receipt)) (= token (:token receipt))
                 (map? (:value receipt)) (map? (:runtime receipt))
                 (= (:inventory receipt) (select-keys request [:kind :options :descriptor]))
                 (= (runtime-identity (:runtime receipt)) (:runtime-identity request)))
    (throw (ex-info "invalid benchmark worker receipt" {:type ::invalid-worker-receipt})))
  receipt)

(defn- require-worker-completion! [initial settled]
  (when-not (and initial settled
                 (integer? (:exit initial)) (zero? (:exit initial))
                 (integer? (:exit settled)) (zero? (:exit settled)))
    (throw (ex-info "benchmark worker did not complete successfully"
                    {:type ::worker-failed :terminal? (boolean settled)
                     :primary (if initial :nonzero-exit :wait-failed)}))))

(def ^:private worker-failure-stages
  #{:worker-bootstrap :writer-trial :reader-recovery :native-trial
    :worker-receipt})

(defn- worker-stage-for-role [role]
  (case role
    :writer :writer-trial
    :reader :reader-recovery
    :native :native-trial
    :worker-bootstrap))

(defn- worker-failure-receipt? [receipt role token]
  ;; This is deliberately a much narrower shape than a success receipt.  A
  ;; failed child may have been interrupted in an arbitrary implementation
  ;; layer, so its retained cross-process evidence is only a closed category
  ;; and stage.  In particular, it must never copy an exception message,
  ;; exception data, request path, backend descriptor, SQL, or provider state.
  (and (map? receipt)
       (= #{:schema-version :role :token :status :failure} (set (keys receipt)))
       (= 1 (:schema-version receipt))
       (= role (:role receipt))
       (= token (:token receipt))
       (= :failed (:status receipt))
       (= #{:category :stage} (set (keys (:failure receipt))))
       (= :controlled-worker-failure (get-in receipt [:failure :category]))
       (contains? worker-failure-stages (get-in receipt [:failure :stage]))))

(defn- child-failure-evidence [result-file role token]
  ;; Treat all absent, malformed, and forged receipts identically.  The parent
  ;; already owns the child role; it need not expose an untrusted file's
  ;; contents to diagnose a terminal subprocess failure.
  (try
    (let [receipt (read-owned-edn! result-file)]
      (if (worker-failure-receipt? receipt role token)
        (:failure receipt)
        {:category :unclassified-child-failure
         :stage (worker-stage-for-role role)}))
    (catch Throwable _
      {:category :unclassified-child-failure
       :stage (worker-stage-for-role role)})))

(defn- require-worker-terminal! [initial settled result-file role token]
  (try
    (require-worker-completion! initial settled)
    nil
    (catch Throwable error
      ;; Preserve the established parent-side exception category and primary
      ;; control semantics.  The two added scalars are diagnostic evidence,
      ;; not a serialization of the child's throwable.
      (throw (ex-info "benchmark worker did not complete successfully"
                      (merge (ex-data error)
                             {:failure (child-failure-evidence
                                        result-file role token)})
                      error)))))

(defn- require-worker-marker! [lines role token]
  (when-not (= 1 (count (filter #(= (str ":durable-bench-worker-complete " role " " token) %)
                               lines)))
    (throw (ex-info "missing or duplicate benchmark receipt"
                    {:type ::invalid-worker-receipt}))))

(defn- require-worker-scope! [request request-path result-path]
  (let [{:keys [role token kind descriptor]} request
        root (:root descriptor)]
    (when-not (and (contains? #{:writer :reader :native} role)
                   (contains? #{:uninstrumented :instrumented :diagnostic :native} kind)
                   (= (= role :native) (= kind :native))
                   (string? token) (re-matches #"[0-9a-f-]{36}" token)
                   (string? root) (.isAbsolute (File. root))
                   (contains? provider-kinds (:provider-kind descriptor))
                   (string? (:object-id descriptor))
                   (re-matches #"bench-worker-[0-9a-f-]{36}" (:object-id descriptor))
                   (= request-path (.getAbsolutePath (File. root (str (name role) "-request.edn"))))
                   (= result-path (.getAbsolutePath (File. root (str (name role) "-result.edn")))))
      (throw (ex-info "invalid owned benchmark worker scope" {:type ::invalid-worker-receipt}))))
  nil)

(defn- redact-batch-samples [value]
  ;; Batch samples are needed only by the parent while it computes the final
  ;; summary. Worker receipts and reader requests are retained evidence, so
  ;; they must not retain that raw series after its aggregate has been formed.
  (if (contains? value :result)
    (update value :result dissoc ::batch-latency-samples)
    (dissoc value ::batch-latency-samples)))

(defn- redact-worker-receipt! [root role receipt]
  (spit (File. root (str (name role) "-result.edn"))
        (str (pr-str (update receipt :value redact-batch-samples)) "\n")))

(defn- write-worker-failure-receipt! [result-path role token stage]
  (try
    (spit result-path
          (str (pr-str {:schema-version 1
                        :role role
                        :token token
                        :status :failed
                        :failure {:category :controlled-worker-failure
                                  :stage stage}})
               "\n"))
    (catch Throwable _
      ;; The original failure remains authoritative.  The parent classifies a
      ;; missing receipt as an unclassified terminal child failure.
      nil)))

(defn- worker-command! [wrapper executable request-file result-file]
  ;; The outer selector already requires this absolute wrapper as part of its
  ;; provenance contract.  Keep worker processes on that same selected Jolt
  ;; invocation rather than embedding a workstation path: hosted CI has no
  ;; `/home/chuck/...` checkout, and a different child runtime would invalidate
  ;; the writer/reader identity check anyway.
  (let [wrapper-file (when wrapper (File. wrapper))
        executable-file (when executable (File. executable))]
    (when-not (and wrapper-file (.isAbsolute wrapper-file)
                   (.isFile wrapper-file) (.canExecute wrapper-file))
      (throw (ex-info "absolute benchmark wrapper is required"
                      {:type ::missing-worker-wrapper})))
    (when-not (and executable-file (.isAbsolute executable-file)
                   (.isFile executable-file) (.canExecute executable-file))
      (throw (ex-info "absolute benchmark executable is required"
                      {:type ::missing-worker-executable})))
    [wrapper executable "-Srepro" "-M:durable-throughput" "--worker"
     (.getAbsolutePath request-file) (.getAbsolutePath result-file)]))

(defn- run-worker! [root role request]
  (let [wrapper (System/getenv "JOLT_WRAPPER")
        executable (System/getenv "BENCH_JOLT_BIN")
        token (str (random-uuid))
        request-file (File. root (str (name role) "-request.edn"))
        result-file (File. root (str (name role) "-result.edn"))
        log-file (File. root (str (name role) ".log"))
        error-file (File. root (str (name role) ".err"))
        _ (spit request-file (pr-str (assoc request :role role :token token)))
        child (process/process
               (worker-command! wrapper executable request-file result-file)
               {:out log-file :err error-file})
        initial (await-worker child 600000)
        _ (when-not initial
            (try (process/destroy-tree child) (catch Throwable _ nil)))
        settled (or initial (await-worker child 5000))]
    (spit (File. root (str (name role) "-initial-exit.edn"))
          (pr-str (select-keys (or initial {}) [:exit])))
    (spit (File. root (str (name role) "-settled-exit.edn"))
          (pr-str (select-keys (or settled {}) [:exit])))
    ;; Never continue to a reader/later trial on uncertain settlement, even
    ;; if a partial result file or a success marker exists.
    (require-worker-terminal! initial settled result-file role token)
    (let [receipt (read-owned-edn! result-file)]
      (require-worker-marker! (str/split-lines (slurp log-file)) role token)
      (require-worker-receipt! receipt role token request))))

(defn- reader-value! [request]
  (let [{:keys [descriptor options handoff]} request
        context (reconstruct-context! descriptor options)
        batches (get-in handoff [:result :batches])
        _ (when-not (and (integer? batches) (pos? batches)
                          (or (:target-wal-bytes options) (= batches (:batches options))))
            (throw (ex-info "invalid worker batch inventory" {:type ::invalid-worker-receipt})))
        independent-expected
        (reduce-row-batches log-row (:batch-size options)
                            (+ (:warmup-batches options) batches)
                            (:question-mark? options) 0 empty-expected-aggregates
                            (fn [expected _ rows]
                              (accumulate-expected-batch expected rows (:question-mark? options))))
        _ (when-not (= independent-expected (:expected handoff))
            (throw (ex-info "writer corpus inventory differs" {:type ::invalid-worker-receipt})))
        metrics (atom (or (:stage-observations handoff) {}))
        store (if (:stage-observations handoff)
                (instrumented-backend (:namespace-backend context) metrics)
                (:namespace-backend context))
        recorder (:provider-metrics context)
        _ (when recorder
            (doseq [key [:logical :transport]]
              (reset! (get recorder key)
                      (get-in handoff [:provider-observations key] {})))
            (provider-metrics/set-phase! recorder :recovery))
        before (runtime-memory-observation)
        start (System/nanoTime)
        recovered
        (with-open [reader (jdbc/connection
                            (durable/snapshot-dbspec
                             (cond-> {:namespace-backend store
                                      :scratch-parent (:root descriptor)
                                      :object-id (:object-id context)}
                               (:stage-observations handoff)
                               (assoc :operations (timed-operations metrics)))))]
          {:actual (verify-counts! reader independent-expected :durable-recovery)
           :memory-after (runtime-memory-observation)})
        recovery {:result (:actual recovered) :expected independent-expected
                  :ms (ms (- (System/nanoTime) start))
                  :memory (recovery-memory-observation before (:memory-after recovered))}
        report (when recorder (provider-metrics/report recorder))]
    (when report
      (provider-metrics/assert-uncontended-flush-control! report (:flush-outcome handoff))
      (provider-metrics/assert-transport-coverage! report))
    (cond-> {:recovery recovery}
      report (assoc :provider-metrics report)
      (:stage-observations handoff) (assoc :stages-through-recovery (stage-report metrics)))))

(declare diagnostic! write-worker-failure-receipt!)

(defn- worker-main! [request-path result-path]
  (let [{:keys [role token descriptor options kind] :as request}
        (read-owned-edn! request-path)
        stage (atom :worker-bootstrap)]
    (try
      (require-worker-scope! request request-path result-path)
      (when-not (= (runtime-identity (runtime-metadata)) (:runtime-identity request))
        (throw (ex-info "benchmark worker source provenance differs"
                        {:type ::invalid-worker-receipt})))
      (let [value
            (case role
              :writer
              (do
                (reset! stage :writer-trial)
                (let [context (reconstruct-context! descriptor options)]
                  (binding [*writer-role?* true *worker-descriptor* descriptor]
                    (if (= kind :diagnostic)
                      (diagnostic! (str (:root descriptor) "/diagnostic-progress.edn"))
                    ((case kind
                       :instrumented durable-trial
                       :uninstrumented durable-uninstrumented-trial)
                     (assoc options :backend-context! (fn [_] context)))))))
              :reader
              (do (reset! stage :reader-recovery)
                  (reader-value! request))
              :native
              (do (reset! stage :native-trial)
                  (native-trial options))
              (throw (ex-info "invalid benchmark worker role" {:type ::invalid-worker-role})))
            receipt {:schema-version 1 :role role :token token :value value
                     :inventory (select-keys request [:kind :options :descriptor])
                     :runtime (runtime-metadata)}]
        (reset! stage :worker-receipt)
        (spit result-path (str (pr-str receipt) "\n"))
        (println :durable-bench-worker-complete role token))
      (catch Throwable error
        (write-worker-failure-receipt! result-path role token @stage)
        ;; Do not translate the child exception. Its original type/cause is
        ;; still what determines the child process's control flow and exit.
        (throw error)))))

(defn- persistent-evidence-root! []
  (let [root (System/getenv "BENCH_PERSISTENT_RECEIPT_ROOT")]
    (when-not (and root (.isAbsolute (File. root)) (.isDirectory (File. root)))
      (throw (ex-info "owned persistent benchmark receipt root required"
                      {:type ::missing-persistent-receipt-root})))
    (File. root)))

(defn- owned-trial! [kind options]
  (let [options (if (= kind :diagnostic)
                  {:batch-size 512 :batches 1 :warmup-batches 1 :question-mark? false}
                  options)
        root (File/createTempFile "jolt-chdb-throughput-workers-" "" (persistent-evidence-root!))
        _ (when-not (and (.delete root) (.mkdirs root))
            (throw (ex-info "cannot create owned benchmark evidence"
                            {:type ::worker-evidence-failed})))
        descriptor (provider-descriptor! options (.getAbsolutePath root))
        request {:kind kind :options (worker-options options) :descriptor descriptor
                 :runtime-identity (runtime-identity (runtime-metadata))}]
    (if (= kind :native)
      (let [receipt (run-worker! root :native request)]
        (redact-worker-receipt! root :native receipt)
        (assoc (:value receipt) :worker-evidence (.getAbsolutePath root)
               :worker-runtime {:native (:runtime receipt)}))
      (let [writer-receipt (run-worker! root :writer request)
            handoff (:value writer-receipt)
            reader-handoff (redact-batch-samples handoff)
            _ (redact-worker-receipt! root :writer writer-receipt)
            reader-receipt (run-worker! root :reader
                                        (assoc request :handoff reader-handoff))]
        (assoc (cond-> (merge (:result handoff) (:value reader-receipt))
                 (= kind :diagnostic)
                 (update :phases into
                         [{:phase :reopened-and-reconciled
                           :data (:recovery (:value reader-receipt))}
                          {:phase :complete :data {:status :ok}}]))
               :measurement-boundaries measurement-boundaries
               :worker-evidence (.getAbsolutePath root)
               :worker-runtime {:writer (:runtime writer-receipt)
                                :reader (:runtime reader-receipt)})))))

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
                        (owned-trial! :uninstrumented
                         (assoc configuration :trial trial
                                :encode-included? true))
                        :durable-preencoded
                        (owned-trial! (if (:instrumented? configuration)
                                        :instrumented
                                        :uninstrumented)
                         (assoc configuration :trial trial
                                :encode-included? false))
                        :ordinary-native-preencoded
                        (owned-trial! :native (assoc configuration :trial trial)))]
                  (*progress!* (if (:instrumented? configuration)
                                 :instrumented-trial
                                 :uninstrumented-trial)
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
        stage-selector? (contains? #{:stage-512 :stage-smoke} profile)
        isolated-selector? (or (isolated-selector-profile? profile)
                               (= :s3-curve profile))]
    (let [runtime (runtime-metadata)
          _ (require-qualification-provenance! profile runtime)
          _ (*progress!* :started {:runtime runtime :profile profile})
          configuration-results (mapv run-config configs)
          _ (*progress!* (if stage-selector?
                           :instrumented-complete
                           :uninstrumented-complete)
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
            (owned-trial! :instrumented {:batch-size (if smoke? 32 512)
                            :batches (if smoke? 2 1)
                            :warmup-batches 1 :trials 1 :trial 1
                            :question-mark? false :encode-included? false}))
          _ (when instrumented
              (*progress!* :instrumented-complete instrumented))]
    {:schema-version 2
     :profile profile
     :process-scope (cond
                      (remote-s3-profile? profile)
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
        :reason (if stage-selector?
                  :stage-selector-is-diagnostic-only
                  :preserve-selector-process-attribution-for-peak-rss)}
       {:status :included})
     :isolated-stages isolated
     :instrumented-control instrumented
     :configurations configuration-results})))

(defn- diagnostic! [output]
  (let [root-file (if *worker-descriptor*
                    (File. (:root *worker-descriptor*))
                    (File/createTempFile "jolt-chdb-throughput-diagnostic-" ""))
        _ (when-not *worker-descriptor* (.delete root-file))
        _ (when-not *worker-descriptor* (.mkdirs root-file))
        store (local/local-backend (.getAbsolutePath root-file))
        object-id (or (:object-id *worker-descriptor*) (str "diagnostic-" (random-uuid)))
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
                          :scratch-parent (:root *worker-descriptor*)
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
      (if *writer-role?*
        {:result (assoc @progress :batches 1) :expected expected}
        (do
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
      @progress))
      (catch Throwable error
        (emit! :failed {:category :controlled-worker-failure})
        (throw error))
      (finally
        (when-not *worker-descriptor* (delete-tree! root-file))))))

(defn- persist-final-report-and-enforce! [output final-report acceptance]
  ;; Keep this small ordering boundary independently testable: callers retain
  ;; the complete bounded receipt even when the acceptance result fails.
  (spit output (str (pr-str final-report) "\n"))
  (require-encoding-inclusive-512-acceptance! acceptance))

(defn -main [& [profile-text output worker-result]]
  (if (= profile-text "--worker")
    (try
      (worker-main! output worker-result)
      (catch Throwable _
        (println :durable-bench-worker-failed :controlled-error)
        (System/exit 1)))
  (let [profile (parse-profile! profile-text)
        output (or output (str "target/profiles/durable-throughput-"
                               (name profile) ".edn"))]
    (try
      (.mkdirs (.getParentFile (File. output)))
      (if (= profile :diagnostic)
        (do (spit output (str (pr-str (owned-trial! :diagnostic {})) "\n"))
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
              acceptance (encoding-inclusive-512-acceptance
                          profile (:configurations report))
              final-report (cond-> (assoc report :phase-log (:phases @progress))
                             (not= :not-applicable (:status acceptance))
                             (assoc :encoding-inclusive-512-acceptance acceptance))]
          (when (remote-s3-profile? profile)
            (provider-metrics/assert-redacted!
             final-report "" "" (evidence-canaries)))
          ;; Receipt first: an unmet gate remains inspectable, with the same
          ;; provenance, redaction, timing, and RSS artifacts as a pass.
          (persist-final-report-and-enforce! output final-report acceptance)
          (println (pr-str {:status :ok :profile profile :output output
                           :configurations
                           (mapv (fn [result]
                                   {:configuration (:configuration result)
                                    :summaries (:summaries result)})
                                 (:configurations report))}))))
      (catch Throwable error
        (if (remote-s3-profile? profile)
          (provider-metrics/throw-redacted-failure! error)
          (throw error)))))))
