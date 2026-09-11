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
            [jdbc.chdb.durable.control :as control]
            [jdbc.chdb.durable.digest :as digest]
            [jdbc.chdb.durable.local-posix :as local]
            [jdbc.chdb.durable.policy :as policy]
            [jdbc.chdb.durable.writer :as writer]
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
        payload-bytes (alength (.getBytes payload "UTF-8"))]
    {:sql (str insert-prefix payload)
     :payload-bytes payload-bytes
     :statement-bytes (+ insert-prefix-bytes payload-bytes)
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
  {:classification-sql!
   (fn [sql params]
     (timed-stage metrics :classification-sql 0
                  #(chdb/classification-sql sql params)))
   :classify!
   (fn [handle sql database]
     (timed-stage metrics :native-classify 0
                  #(native/classify-query! handle sql database)))
   :execute-native!
   (fn [handle sql params]
     (timed-stage metrics :native-execute 0
                  #(chdb/execute-any handle sql params)))
   :publish-wal!
   (fn [store token payload]
     (timed-stage metrics :publish-wal (alength payload)
                  #(control/publish-wal-bytes! store token payload)))})

(defn- delete-tree! [^File root]
  (when (.exists root)
    (doseq [child (reverse (file-seq root))]
      (.delete ^File child))))

(defn- durable-handle [connection]
  (:handle (shim/driver-context (proto/connection connection) :chdb-durable)))

(defn- expected-trace-flags [rows]
  (reduce + 0 (map #(get % "TraceFlags") rows)))

(defn- expected-aggregates [rows question-mark?]
  (let [trace-ids (mapv #(get % "TraceId") rows)
        span-ids (mapv #(get % "SpanId") rows)
        ordered-traces (vec (sort trace-ids))
        ordered-spans (vec (sort span-ids))]
    {:n (count rows)
     :flags (expected-trace-flags rows)
     :severity_sum (reduce + 0 (map #(get % "SeverityNumber") rows))
     :body_bytes (reduce + 0 (map #(alength (.getBytes (get % "Body") "UTF-8")) rows))
     :question_bodies (if question-mark? (count rows) 0)
     :min_trace (first ordered-traces) :max_trace (peek ordered-traces)
     :min_span (first ordered-spans) :max_span (peek ordered-spans)}))

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

(defn- make-batches [batch-size batches question-mark? start]
  (mapv (fn [batch]
          (mapv #(log-row % question-mark?)
                (range (+ start (* batch batch-size))
                       (+ start (* (inc batch) batch-size)))))
        (range batches)))

(defn- encode-all [row-batches]
  (mapv encode-batch-production row-batches))

(defn- durable-uninstrumented-trial
  [{:keys [batch-size batches warmup-batches question-mark? encode-included?
           trial]}]
  (let [root-file (File/createTempFile "jolt-chdb-throughput-" "")
        _ (.delete root-file)
        _ (.mkdirs root-file)
        store (local/local-backend (.getAbsolutePath root-file))
        object-id (str "bench-" trial "-" (random-uuid))
        warmup-rows (make-batches batch-size warmup-batches question-mark? 0)
        measured-rows (make-batches batch-size batches question-mark?
                                    (* batch-size warmup-batches))
        preencoded (when-not encode-included? (encode-all measured-rows))
        warmup-encoded (encode-all warmup-rows)
        all-rows (vec (concat (apply concat warmup-rows)
                              (apply concat measured-rows)))
        expected (expected-aggregates all-rows question-mark?)
        trial-result (atom nil)
        configuration {:namespace-backend store :object-id object-id
                       :owner "durable-throughput-benchmark"
                       :database "benchmark" :lease-ttl-ms 300000
                       :heartbeat-interval-ms 100000}]
    (try
      (with-open [connection (jdbc/connection (durable/writer-dbspec configuration))]
        (jdbc/execute! connection logs-ddl)
        (durable/flush! connection)
        (doseq [{:keys [sql]} warmup-encoded] (jdbc/execute! connection sql))
        (durable/flush! connection)
        (System/gc)
        (let [payload-total (atom 0)
              statement-total (atom 0)
              before (counter-sample)
              start (System/nanoTime)
              samples
              (mapv
               (fn [index]
                 (let [batch-start (System/nanoTime)
                       encoded (if encode-included?
                                 (encode-batch-production
                                  (nth measured-rows index))
                                 (nth preencoded index))]
                   (swap! payload-total + (:payload-bytes encoded))
                   (swap! statement-total + (:statement-bytes encoded))
                   (jdbc/execute! connection (:sql encoded))
                   (- (System/nanoTime) batch-start)))
               (range batches))
              ingest-nanos (- (System/nanoTime) start)
              after (counter-sample)
              pending (writer/status (durable-handle connection))
              _ (when-not (= batches (:pending-statements pending))
                  (throw (ex-info "Durable pending WAL count mismatch"
                                  {:expected batches :actual pending})))
              flush-before (counter-sample)
              flush-start (System/nanoTime)
              flush-result (durable/flush! connection)
              flush-nanos (- (System/nanoTime) flush-start)
              flush-after (counter-sample)]
          (when-not (= :committed (:status flush-result))
            (throw (ex-info "Durable measured flush did not commit"
                            {:result flush-result})))
          (reset! trial-result
                  {:trial trial
                   :mode (if encode-included?
                           :durable-encode-included
                           :durable-preencoded)
                   :instrumented? false
                   :measured-rows (* batch-size batches)
                   :batch-size batch-size :batches batches
                   :question-mark-every-row? question-mark?
                   :batch-latency (latency-summary samples)
                   :ingest-ms (ms ingest-nanos)
                   :ingest-rows-per-second
                   (/ (double (* batch-size batches 1000000000)) ingest-nanos)
                   :flush-ms (ms flush-nanos)
                   :flush-cadence-batches batches
                   :flush-cadence-rows (* batch-size batches)
                   :persisted-rate-semantics
                   :one-flush-amortized-over-all-measured-batches
                   :persisted-ms (ms (+ ingest-nanos flush-nanos))
                   :persisted-rows-per-second
                   (/ (double (* batch-size batches 1000000000))
                      (+ ingest-nanos flush-nanos))
                   :ingest-counters (counter-delta before after)
                   :flush-counters (counter-delta flush-before flush-after)
                   :pending-before-flush pending
                   :payload-bytes @payload-total
                   :statement-bytes @statement-total})))
      (let [recovery-start (System/nanoTime)
            actual
            (with-open [reader (jdbc/connection
                                (durable/snapshot-dbspec
                                 {:namespace-backend store :object-id object-id}))]
              (verify-counts! reader expected :durable-recovery))]
        (assoc @trial-result
               :recovery {:result actual
                          :ms (ms (- (System/nanoTime) recovery-start))
                          :expected expected}))
      (finally
        (delete-tree! root-file)))))

(defn- durable-trial
  [{:keys [batch-size batches warmup-batches question-mark? encode-included?
           trial]}]
  (let [root-file (File/createTempFile "jolt-chdb-throughput-" "")
        _ (.delete root-file)
        _ (.mkdirs root-file)
        metrics (atom {})
        raw-store (local/local-backend (.getAbsolutePath root-file))
        store (instrumented-backend raw-store metrics)
        object-id (str "bench-" trial "-" (random-uuid))
        warmup-rows (make-batches batch-size warmup-batches question-mark? 0)
        measured-rows (make-batches batch-size batches question-mark?
                                    (* batch-size warmup-batches))
        preencoded (when-not encode-included? (encode-all measured-rows))
        warmup-encoded (encode-all warmup-rows)
        all-rows (vec (concat (apply concat warmup-rows)
                              (apply concat measured-rows)))
        expected (expected-aggregates all-rows question-mark?)
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
        (doseq [{:keys [sql]} warmup-encoded] (jdbc/execute! connection sql))
        (durable/flush! connection)
        (reset! metrics {})
        (System/gc)
        (let [before (counter-sample)
              start (System/nanoTime)
              samples
              (let [payload-total (atom 0)
                    statement-total (atom 0)]
                (mapv (fn [index]
                      (let [batch-start (System/nanoTime)
                            encoded (if encode-included?
                                      (let [result (encode-batch-profiled
                                                    (nth measured-rows index))]
                                        (record-stage! metrics :data-json
                                                       (:json-nanos result)
                                                       (:payload-bytes result))
                                        (record-stage! metrics :exporter-materialization
                                                       (:materialize-nanos result)
                                                       (:payload-bytes result))
                                        result)
                                      (nth preencoded index))]
                        (swap! payload-total + (:payload-bytes encoded))
                        (swap! statement-total + (:statement-bytes encoded))
                        (jdbc/execute! connection (:sql encoded))
                        {:nanos (- (System/nanoTime) batch-start)
                         :payload-total payload-total
                         :statement-total statement-total}))
                    (range batches)))
              ingest-nanos (- (System/nanoTime) start)
              after (counter-sample)
              pending (writer/status (durable-handle connection))
              _ (when-not (= batches (:pending-statements pending))
                  (throw (ex-info "Durable pending WAL count mismatch"
                                  {:expected batches :actual pending})))
              flush-before (counter-sample)
              flush-start (System/nanoTime)
              flush-result (durable/flush! connection)
              flush-nanos (- (System/nanoTime) flush-start)
              flush-after (counter-sample)
              _ (when-not (= :committed (:status flush-result))
                  (throw (ex-info "Durable measured flush did not commit"
                                  {:result flush-result})))
              samples-nanos (mapv :nanos samples)
              payload-bytes @(-> samples first :payload-total)
              statement-bytes @(-> samples first :statement-total)
              stage-values @metrics
              expected-stage-calls
              (cond-> {:classification-sql batches :native-classify batches
                       :native-execute batches :publish-wal 1
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
                             (get-in stage-values [:publish-wal :bytes]))
                  (throw (ex-info "published WAL byte count mismatch"
                                  {:pending pending :stages stage-values})))]
            (reset! trial-result
                    {:trial trial
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
                     :ingest-counters (counter-delta before after)
                     :flush-counters (counter-delta flush-before flush-after)
                     :pending-before-flush pending
                     :stages (stage-report metrics)
                     :payload-bytes payload-bytes
                     :statement-bytes statement-bytes})))
      ;; Closing above releases the writer. This open must recover solely from
      ;; the persisted checkpoint/WAL objects and exact manifest order.
      (let [recovery-start (System/nanoTime)
            actual
            (with-open [reader (jdbc/connection
                                (durable/snapshot-dbspec
                                 {:namespace-backend store :object-id object-id
                                  :operations (timed-operations metrics)}))]
              (verify-counts! reader expected :durable-recovery))
            recovery-nanos (- (System/nanoTime) recovery-start)]
        (assoc @trial-result
               :recovery {:result actual :ms (ms recovery-nanos)
                          :expected expected}
               :stages-through-recovery (stage-report metrics)))
      (finally
        (delete-tree! root-file)))))

(defn- native-trial
  [{:keys [batch-size batches warmup-batches question-mark? trial]}]
  (let [warmup-rows (make-batches batch-size warmup-batches question-mark? 0)
        measured-rows (make-batches batch-size batches question-mark?
                                    (* batch-size warmup-batches))
        warmup-encoded (encode-all warmup-rows)
        measured-encoded (encode-all measured-rows)
        all-rows (vec (concat (apply concat warmup-rows)
                              (apply concat measured-rows)))
        expected (expected-aggregates all-rows question-mark?)]
    (with-open [connection (jdbc/connection "chdb::memory:")]
      (jdbc/execute! connection logs-ddl)
      (doseq [{:keys [sql]} warmup-encoded] (jdbc/execute! connection sql))
      (System/gc)
      (let [before (counter-sample)
            start (System/nanoTime)
            samples
            (mapv (fn [{:keys [sql]}]
                    (let [batch-start (System/nanoTime)]
                      (jdbc/execute! connection sql)
                      (- (System/nanoTime) batch-start)))
                  measured-encoded)
            elapsed (- (System/nanoTime) start)
            after (counter-sample)
            actual (verify-counts! connection expected :ordinary-native)]
        {:trial trial :mode :ordinary-native-preencoded
         :measured-rows (* batch-size batches)
         :batch-size batch-size :batches batches
         :question-mark-every-row? question-mark?
         :batch-latency (latency-summary samples)
         :ingest-ms (ms elapsed)
         :ingest-rows-per-second
         (/ (double (* batch-size batches 1000000000)) elapsed)
         :counters (counter-delta before after)
         :payload-bytes (reduce + 0 (map :payload-bytes measured-encoded))
         :statement-bytes (reduce + 0 (map :statement-bytes measured-encoded))
         :reconciliation {:result actual :expected expected}}))))

(defn- isolated-stage-profile [batch-size question-mark? repetitions]
  (let [rows (first (make-batches batch-size 1 question-mark? 0))
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

(defn- runtime-metadata []
  (let [library (System/getenv "JOLT_CHDB_LIB")
        library-file (when library (File. library))]
  {:name :jolt :jolt-version (System/getenv "BENCH_JOLT_VERSION")
   :scheme-version (host/scheme-version)
   :machine-type (host/machine-type)
   :native-chdb (native/durable-capability)
   :native-library {:path library
                    :bytes (when (and library-file (.isFile library-file))
                             (.length library-file))
                    :sha256 (when (and library-file (.isFile library-file))
                              (digest/sha256-file (.toPath library-file)))}
   :benchmark-harness
   {:path "bench/jdbc/chdb_durable_throughput.clj"
    :sha256 (digest/sha256-file
             (.toPath (File. "bench/jdbc/chdb_durable_throughput.clj")))}
   :git {:head (System/getenv "BENCH_GIT_HEAD")
         :parent (System/getenv "BENCH_GIT_PARENT")
         :tree (System/getenv "BENCH_GIT_TREE")}
   :started-at (System/getenv "BENCH_STARTED_AT")
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

(defn- run-config [configuration]
  (let [{:keys [trials modes]} configuration
        results
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
                                [:trial :mode :batch-size :batches
                                 :question-mark-every-row? :batch-latency
                                 :ingest-ms :ingest-rows-per-second :flush-ms
                                 :persisted-ms :persisted-rows-per-second
                                 :payload-bytes :statement-bytes :recovery]))
                  (conj acc result)))
              acc ordered-modes)))
         [] (range 1 (inc trials)))]
    {:configuration (dissoc configuration :modes)
     :results results
     :summaries
     (into {}
           (map (fn [mode]
                  (let [selected (filterv #(= mode (:mode %)) results)]
                    [mode
                     (if (= mode :ordinary-native-preencoded)
                       (trial-rate-summary selected :ingest-rows-per-second
                                           :ingest-ms)
                       {:admission
                        (trial-rate-summary selected :ingest-rows-per-second
                                            :ingest-ms)
                        :persisted
                        (trial-rate-summary selected :persisted-rows-per-second
                                            :persisted-ms)})])))
           modes)}))

(defn run! [profile]
  (let [smoke? (= profile :smoke)
        probe? (= profile :probe)
        configs
        (cond
          smoke?
          [{:label :smoke-batched :batch-size 32 :batches 2
            :warmup-batches 1 :trials 1 :question-mark? false
            :modes [:durable-encode-included :durable-preencoded
                    :ordinary-native-preencoded]}]

          probe?
          [{:label :probe-batched-512 :batch-size 512 :batches 2
            :warmup-batches 1 :trials 1 :question-mark? false
            :modes [:durable-encode-included :durable-preencoded
                    :ordinary-native-preencoded]}]

          :else
          [{:label :batched-512 :batch-size 512 :batches 40
            :warmup-batches 2 :trials 5 :question-mark? false
            :modes [:durable-encode-included :durable-preencoded
                    :ordinary-native-preencoded]}
           {:label :batched-512-question-mark :batch-size 512 :batches 40
            :warmup-batches 2 :trials 5 :question-mark? true
            :modes [:durable-preencoded]}
           {:label :single-row :batch-size 1 :batches 128
            :warmup-batches 16 :trials 5 :question-mark? false
            :modes [:durable-encode-included :durable-preencoded
                    :ordinary-native-preencoded]}])]
    (let [runtime (runtime-metadata)
          _ (*progress!* :started {:runtime runtime :profile profile})
          configuration-results (mapv run-config configs)
          _ (*progress!* :uninstrumented-complete
                         {:configurations
                          (mapv #(select-keys % [:configuration :summaries])
                                configuration-results)})
          isolated (isolated-stage-profile (if smoke? 32 512) false
                                           (cond smoke? 8 probe? 2 :else 20))
          _ (*progress!* :isolated-stages-complete isolated)
          instrumented
          (durable-trial {:batch-size (if smoke? 32 512)
                          :batches (if smoke? 2 1)
                          :warmup-batches 1 :trials 1 :trial 1
                          :question-mark? false :encode-included? false})
          _ (*progress!* :instrumented-complete instrumented)]
    {:schema-version 1
     :profile profile
     :runtime runtime
     :workload {:shape :clickstack-otel-log-jsoneachrow
                :invariants [:exact-recovered-count :exact-trace-flags-sum
                             :pending-wal-statement-count
                             :one-measured-flush-commit
                             :immutable-snapshot-replay]
                :ordering "prepare and size-check WAL, then native execute, then append; flush afterward"
                :checkpoint {:status :not-applicable
                             :reason :materialized-sql-uses-v1-statement-wal}}
     :isolated-stages isolated
     :instrumented-control instrumented
     :configurations configuration-results})))

(defn- diagnostic! [output]
  (let [root-file (File/createTempFile "jolt-chdb-throughput-diagnostic-" "")
        _ (.delete root-file)
        _ (.mkdirs root-file)
        store (local/local-backend (.getAbsolutePath root-file))
        object-id (str "diagnostic-" (random-uuid))
        warmup-rows (first (make-batches 512 1 false 0))
        measured-rows (first (make-batches 512 1 false 512))
        warmup (encode-batch-production warmup-rows)
        measured (encode-batch-production measured-rows)
        all-rows (vec (concat warmup-rows measured-rows))
        expected (expected-aggregates all-rows false)
        progress (atom {:schema-version 1 :profile :diagnostic
                        :runtime (runtime-metadata)
                        :configuration {:batch-size 512 :measured-batches 1
                                        :warmup-batches 1
                                        :mode :durable-preencoded
                                        :backend :local-posix}
                        :payload {:rows 512
                                  :payload-bytes (:payload-bytes measured)
                                  :statement-bytes (:statement-bytes measured)}
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
                     {:elapsed-ms (ms elapsed)
                      :rows-per-second (/ 512000000000.0 elapsed)
                      :counters (counter-delta before after)
                      :pending status})))
          (let [before (counter-sample)
                start (System/nanoTime)
                result (durable/flush! connection)
                elapsed (- (System/nanoTime) start)
                after (counter-sample)]
            (when-not (= :committed (:status result))
              (throw (ex-info "diagnostic flush did not commit" {:result result})))
            (emit! :measured-flush {:elapsed-ms (ms elapsed)
                                    :counters (counter-delta before after)}))
          (finally
            (.close connection)
            (emit! :writer-closed {}))))
      (let [start (System/nanoTime)
            actual
            (with-open [reader (jdbc/connection
                                (durable/snapshot-dbspec
                                 {:namespace-backend store
                                  :object-id object-id}))]
              (verify-counts! reader expected :diagnostic-recovery))]
        (emit! :reopened-and-reconciled
               {:elapsed-ms (ms (- (System/nanoTime) start))
                :expected expected :actual actual}))
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
  (let [profile (keyword (or profile-text "smoke"))
        output (or output (str "target/profiles/durable-throughput-"
                               (name profile) ".edn"))]
    (.mkdirs (.getParentFile (File. output)))
    (if (= profile :diagnostic)
      (do (diagnostic! output)
          (println (pr-str {:status :ok :profile profile :output output})))
      (let [progress (atom {:schema-version 1 :profile profile :phases []})
            emit! (fn [phase data]
                    (let [event {:phase phase :epoch-ms (System/currentTimeMillis)
                                 :data data}]
                      (swap! progress update :phases conj event)
                      (spit output (str (pr-str @progress) "\n"))
                      (println (pr-str (select-keys event [:phase :epoch-ms])))))
            report (binding [*progress!* emit!] (run! profile))]
        (spit output (str (pr-str (assoc report :phase-log (:phases @progress)))
                          "\n"))
        (println (pr-str {:status :ok :profile profile :output output
                         :configurations
                         (mapv (fn [result]
                                 {:configuration (:configuration result)
                                  :summaries (:summaries result)})
                               (:configurations report))}))))))
