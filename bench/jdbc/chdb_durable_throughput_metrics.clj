(ns jdbc.chdb-durable-throughput-metrics
  "Redacted logical-backend and transport-attempt metrics for manual profiles.

  This namespace deliberately retains only fixed operation/phase labels,
  scalar counts, request/response body byte counts, status categories, and
  monotonic durations. Logical `:results` use the closed keyword categories
  below; transport `:results` preserve only numeric HTTP statuses from 100
  through 599, collapsing any other response status to `:invalid-response`.
  Body byte counts exclude transport framing, headers, and TLS overhead. This
  namespace must never retain a request, response, exception, URL, header,
  ETag, object key, SQL string, or payload body."
  (:require [clojure.string :as str]
            [jdbc.chdb.durable.backend :as backend])
  (:import [java.nio.file Files Paths]))

(def ^:private phases #{:setup :admission :flush :close :recovery})
(def ^:private operations
  #{:get :get-with-etag :put-file-if-absent :put-bytes-if-absent
    :replace-if-match :download-to-file})
(def ^:private result-categories
  #{:ok :missing :created :replaced :precondition-failed :ambiguous
    :downloaded :not-found :authentication :permission :throttled
    :provider :transport :timeout :invalid-response :error})

(def wal-target-bytes
  "Independent S3 curve points. The final target leaves one MiB below the
  frozen 128 MiB WAL segment limit."
  [419430
   (* 3 1024 1024)
   (* 12 1024 1024)
   (* 48 1024 1024)
   (* 64 1024 1024)
   (* 96 1024 1024)
   (* 127 1024 1024)])

(defn maximum-json-each-row-bytes
  "Return the longest LF-delimited row in an existing UTF-8 payload array.

  The scan neither copies the array nor decodes, splits, or re-encodes rows."
  [payload]
  (when-not (bytes? payload)
    (throw (ex-info "JSONEachRow payload must be bytes"
                    {:type ::invalid-payload-bytes})))
  (let [length (alength payload)]
    (loop [index 0
           row-start 0
           maximum 0]
      (if (< index length)
        (if (= 10 (bit-and 255 (aget payload index)))
          (recur (inc index) (inc index) (max maximum (- index row-start)))
          (recur (inc index) row-start maximum))
        (max maximum (- length row-start))))))

(defn recorder
  "Create one phase-aware recorder. A recorder contains no provider identity."
  []
  {:phase (atom :setup)
   :logical (atom {})
   :transport (atom {})})

(defn set-phase!
  "Select a fixed report phase without accepting caller-provided labels."
  [metrics phase]
  (when-not (contains? phases phase)
    (throw (ex-info "unsupported throughput metrics phase"
                    {:type ::invalid-phase})))
  (reset! (:phase metrics) phase)
  nil)

(defn- checked-operation [operation]
  (if (contains? operations operation) operation :error))

(defn- checked-count [value]
  (if (and (integer? value) (not (neg? value))) value 0))

(defn- finite-number? [value]
  (and (number? value)
       (= value value)
       (not= value ##Inf)
       (not= value ##-Inf)))

(defn- request-body-bytes [request]
  (checked-count (get-in request [:request-body :byte-count])))

(defn- response-body-bytes [response]
  (cond
    (bytes? (:body response)) (alength (:body response))
    (bytes? (:bytes response)) (alength (:bytes response))
    (integer? (:byte-count response)) (checked-count (:byte-count response))
    :else 0))

(defn- safe-category [value]
  (if (contains? result-categories value) value :error))

(defn- throwable-category [error]
  (safe-category
   (or (:category (ex-data error))
       (some-> (:type (ex-data error)) name keyword))))

(defn- logical-category [value]
  (cond
    (nil? value) :missing
    (map? value) (safe-category (or (:status value) :ok))
    :else :ok))

(defn- transport-category [response]
  (let [status (:status response)]
    (if (and (integer? status) (<= 100 status 599))
      status
      :invalid-response)))

(defn- record! [target phase operation elapsed request-count response-count result]
  (swap! target update-in [phase (checked-operation operation)]
         (fn [entry]
           (-> (or entry {:calls 0 :request-body-bytes 0
                          :response-body-bytes 0
                          :latencies-nanos [] :results {}})
               (update :calls inc)
               (update :request-body-bytes + (checked-count request-count))
               (update :response-body-bytes + (checked-count response-count))
               (update :latencies-nanos conj (max 0 elapsed))
               (update-in [:results result] (fnil inc 0))))))

(defn instrument-transport
  "Wrap the S3 `request!` boundary and record each actual transport attempt.

  The wrapper intentionally derives its observation before dropping all
  references to the request/response. It records neither provider identity nor
  opaque response metadata."
  [metrics request!]
  (fn [request]
    (let [phase @(:phase metrics)
          operation (checked-operation (:operation request))
          sent-bytes (request-body-bytes request)
          start (System/nanoTime)]
      (try
        (let [response (request! request)]
          (record! (:transport metrics) phase operation
                   (- (System/nanoTime) start) sent-bytes
                   (response-body-bytes response) (transport-category response))
          response)
        (catch Throwable error
          (record! (:transport metrics) phase operation
                   (- (System/nanoTime) start) sent-bytes 0
                   (throwable-category error))
          (throw error))))))

(defn- regular-file-bytes [path]
  (try
    (Files/size (Paths/get (str path) (into-array String [])))
    (catch Throwable _ 0)))

(defn- observe-logical [metrics operation request-count f]
  (let [phase @(:phase metrics)
        start (System/nanoTime)]
    (try
      (let [value (f)]
        (record! (:logical metrics) phase operation
                 (- (System/nanoTime) start) request-count
                 (response-body-bytes (if (map? value) value {:body value}))
                 (logical-category value))
        value)
      (catch Throwable error
        (record! (:logical metrics) phase operation
                 (- (System/nanoTime) start) request-count 0
                 (throwable-category error))
        (throw error)))))

(defn instrument-backend
  "Wrap an ObjectBackend while preserving its exact calls and return values."
  [metrics delegate]
  (reify backend/ObjectBackend
    (get-bytes [_ key]
      (observe-logical metrics :get 0 #(backend/get-bytes delegate key)))
    (get-with-etag [_ key]
      (observe-logical metrics :get-with-etag 0
                       #(backend/get-with-etag delegate key)))
    (put-file-if-absent! [_ key path]
      (observe-logical metrics :put-file-if-absent (regular-file-bytes path)
                       #(backend/put-file-if-absent! delegate key path)))
    (put-bytes-if-absent! [_ key bytes]
      (observe-logical metrics :put-bytes-if-absent
                       (if (bytes? bytes) (alength bytes) 0)
                       #(backend/put-bytes-if-absent! delegate key bytes)))
    (replace-if-match! [_ key bytes etag]
      (observe-logical metrics :replace-if-match
                       (if (bytes? bytes) (alength bytes) 0)
                       #(backend/replace-if-match! delegate key bytes etag)))
    (download-to-file! [_ key path]
      (observe-logical metrics :download-to-file 0
                       #(backend/download-to-file! delegate key path)))))

(defn- percentile [ordered fraction]
  (nth ordered (max 0 (dec (long (Math/ceil (* fraction (count ordered))))))))

(defn- latency-report [samples]
  (let [ordered (vec (sort samples))
        count (count ordered)
        nanos (reduce + 0 ordered)]
    {:count count
     :p50-ms (/ (double (percentile ordered 0.50)) 1000000.0)
     :p95-ms (/ (double (percentile ordered 0.95)) 1000000.0)
     :max-ms (/ (double (peek ordered)) 1000000.0)
     :total-ms (/ (double nanos) 1000000.0)
     :p50-supported? (>= count 2)
     :p95-supported? (>= count 20)}))

(defn- summarized [observations]
  (into {}
        (map (fn [[phase phase-values]]
               [phase
                (into {}
                      (map (fn [[operation entry]]
                             [operation
                              (merge
                               (select-keys entry
                                            [:calls :request-body-bytes
                                             :response-body-bytes :results])
                               {:latency
                                (latency-report (:latencies-nanos entry))})]))
                      phase-values)]))
        observations))

(defn report
  "Return the bounded scalar-only report, including retry amplification."
  [metrics]
  (let [logical (summarized @(:logical metrics))
        transport (summarized @(:transport metrics))
        keys (set (concat
                   (for [[phase values] logical operation (keys values)]
                     [phase operation])
                   (for [[phase values] transport operation (keys values)]
                     [phase operation])))
        amplification
        (into {}
              (map (fn [[phase operation]]
                     (let [logical-calls
                           (get-in logical [phase operation :calls] 0)
                           attempts (get-in transport [phase operation :calls] 0)]
                       [[phase operation]
                        {:logical-calls logical-calls
                         :transport-attempts attempts
                         :extra-attempts (max 0 (- attempts logical-calls))}])))
              keys)]
    {:schema-version 1
     :retry-semantics :observed-inside-production-s3-loop
     :logical logical
     :transport transport
     :retry-amplification amplification}))

(defn aggregation-requirements
  "Calculate aggregation needed for persisted rates of 20k and 25k rows/s.

  Admission throughput A and flush latency L both consume wall time, so a
  target R requires N >= R*L/(1-R/A). When A is not greater than R, no finite
  aggregation can reach the persisted target."
  [admission-rows-per-second flush-ms]
  (when-not (and (finite-number? admission-rows-per-second)
                 (pos? admission-rows-per-second))
    (throw (ex-info "admission rate must be a positive number"
                    {:type ::invalid-admission-rate})))
  (when-not (and (finite-number? flush-ms) (not (neg? flush-ms)))
    (throw (ex-info "flush latency must be a nonnegative number"
                    {:type ::invalid-flush-latency})))
  (into {}
        (map (fn [rate]
               (if (<= admission-rows-per-second rate)
                 [rate {:possible? false
                        :reason :admission-rate-not-above-target}]
                 (let [rows
                       (long
                        (Math/ceil
                         (/ (* rate (/ (double flush-ms) 1000.0))
                            (- 1.0 (/ rate
                                      (double admission-rows-per-second))))))]
                   [rate {:possible? true
                          :rows rows
                          :batches (long (Math/ceil (/ (double rows) 512.0)))}]))))
        [20000 25000]))

(defn assert-uncontended-flush-control!
  "Require the logical-call shape of one successful production WAL flush.

  The fixed operation labels distinguish the three ETag-bearing head reads
  from the immutable WAL verification GET without inspecting object keys."
  [bounded-report]
  (let [actual
        (into {}
              (map (fn [[operation observation]]
                     [operation (:calls observation)]))
              (get-in bounded-report [:logical :flush] {}))
        expected {:get-with-etag 3
                  :put-bytes-if-absent 1
                  :get 1
                  :replace-if-match 1}]
    (when-not (= expected actual)
      (throw (ex-info "uncontended flush request shape mismatch"
                      {:type ::flush-control-mismatch}))))
  nil)

(defn assert-transport-coverage!
  "Fail if a logical S3 call has no corresponding transport attempt."
  [bounded-report]
  (when (some (fn [[_ {:keys [logical-calls transport-attempts]}]]
                (or (< transport-attempts logical-calls)
                    (and (zero? logical-calls) (pos? transport-attempts))))
              (:retry-amplification bounded-report))
    (throw (ex-info "transport attempt coverage mismatch"
                    {:type ::transport-coverage-mismatch})))
  nil)

(defn throw-redacted-failure!
  "Replace an arbitrary provider/runtime failure without retaining its data."
  [_]
  (throw (ex-info "Durable S3 throughput failed"
                  {:type ::throughput-failed})))

(defn assert-redacted!
  "Fail quietly when any canary appears in bounded evidence or captured logs.

  The exception intentionally identifies neither the matching value nor its
  location, so the redaction test itself cannot echo sensitive material."
  [bounded-report captured-stdout captured-stderr canaries]
  (let [haystack (str (pr-str bounded-report) "\n"
                      (or captured-stdout "") "\n" (or captured-stderr ""))]
    (when (some #(and (string? %) (not (str/blank? %))
                      (str/includes? haystack %))
                canaries)
      (throw (ex-info "throughput evidence failed redaction contract"
                      {:type ::redaction-contract}))))
  nil)
