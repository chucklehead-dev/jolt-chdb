(ns jdbc.chdb-durable-confirmed-10000
  "Manual, OIDC-gated S3 qualification of public confirmed 10k-row commits.

  This is deliberately separate from the amortized-flush throughput profiles.
  SQL construction is outside each timed execute-and-flush! call."
  (:require [clojure.edn :as edn]
            [jdbc.chdb.durable :as durable]
            [jdbc.chdb.durable.digest :as digest]
            [jdbc.chdb-durable-throughput :as throughput]
            [jdbc.chdb-durable-throughput-metrics :as metrics]
            [jdbc.core :as jdbc]
            [jolt.process :as process])
  (:import [java.io File]))

(def ^:private batch-rows 10000)
(def ^:private warmups 3)
(def ^:private measured 100)
(def ^:private total-calls (+ warmups measured))
(def ^:private expected-rows (* batch-rows total-calls))
(def ^:private max-statement-bytes (* 16 1024 1024))
(def ^:private max-cumulative-statement-bytes (* 2 1024 1024 1024))
(def ^:private max-transport-requests 2000)
(def ^:private max-transport-body-bytes (* 2 1024 1024 1024))
(def ^:private max-object-write-attempts 600)
(def ^:private reader-timeout-ms (* 30 60 1000))

(defn- fail! [type]
  ;; No caller-provided values may enter an exception or CI log.
  (throw (ex-info "confirmed S3 qualification failed" {:type type})))

(defn- valid-confirmed-prefix? [run-id attempt prefix]
  (and (string? run-id) (re-matches #"[1-9][0-9]*" run-id)
       (string? attempt) (re-matches #"[1-9][0-9]*" attempt)
       (= prefix (str "ci/jolt-chdb/" run-id "-" attempt
                      "/confirmed-10000"))))

(defn- require-confirmed-prefix! []
  (let [run-id (System/getenv "BENCH_RUN_ID")
        attempt (System/getenv "BENCH_RUN_ATTEMPT")
        prefix (System/getenv "JOLT_CHDB_S3_PREFIX")]
    (when-not (valid-confirmed-prefix? run-id attempt prefix)
      (fail! ::invalid-prefix))
    {:run-id run-id :attempt attempt}))

(defn- statement! [ordinal]
  (let [start (* ordinal batch-rows)
        rows (mapv #(#'throughput/log-row % false)
                   (range start (+ start batch-rows)))
        encoded (#'throughput/encode-batch-production rows)
        sql (:sql encoded)
        size (:statement-bytes encoded)]
    (when-not (and (pos? size) (<= size max-statement-bytes))
      (fail! ::statement-cap))
    {:sql sql :bytes size
     :sha256 (digest/sha256-bytes (.getBytes sql "UTF-8"))
     :expected (#'throughput/accumulate-expected-batch
                @#'throughput/empty-expected-aggregates rows false)}))

(def ^:private full-row-fingerprint-query
  (str "SELECT toString(sum(cityHash64(tuple("
       "Timestamp, TraceId, SpanId, TraceFlags, SeverityText, "
       "SeverityNumber, ServiceName, Body, ResourceSchemaUrl, "
       "ResourceAttributes, ScopeSchemaUrl, ScopeName, ScopeVersion, "
       "ScopeAttributes, LogAttributes, EventName)))) AS full_row_digest "
       "FROM otel_logs"))

(defn- fingerprint! [connection]
  (let [value (:full_row_digest
               (jdbc/fetch-one connection full-row-fingerprint-query))]
    (when-not (and (string? value) (re-matches #"[0-9]+" value))
      (fail! ::invalid-fingerprint))
    value))

(defn- transport-totals [report]
  (reduce (fn [total entry]
            (-> total
                (update :requests + (:calls entry))
                (update :body-bytes + (:request-body-bytes entry))))
          {:requests 0 :body-bytes 0}
          (for [[_phase operations] (:transport report)
                [_operation entry] operations]
            entry)))

(defn- merge-expected [left right]
  (let [lower (fn [a b] (if (or (nil? a) (neg? (compare b a))) b a))
        upper (fn [a b] (if (or (nil? a) (pos? (compare b a))) b a))]
    (-> (merge-with + left (select-keys right
                                       [:n :flags :severity_sum :body_bytes
                                        :question_bodies]))
        (assoc :min_trace (lower (:min_trace left) (:min_trace right))
               :max_trace (upper (:max_trace left) (:max_trace right))
               :min_span (lower (:min_span left) (:min_span right))
               :max_span (upper (:max_span left) (:max_span right))))))

(defn- resource-totals [provider-report]
  (let [{:keys [requests body-bytes]} (transport-totals provider-report)
        writes (reduce + 0
                       (for [[_phase operations] (:logical provider-report)
                             [operation entry] operations
                             :when (contains? #{:put-file-if-absent
                                                :put-bytes-if-absent
                                                :replace-if-match} operation)]
                         (:calls entry)))]
    {:requests requests :transport-body-bytes body-bytes
     :object-write-attempts writes}))

(defn- check-resource-bounds! [provider-report cumulative-bytes]
  ;; Provider requests happen inside each public call. These counters are
  ;; post-call audit gates, not preventive per-request throttles; the fixed
  ;; 3+100-call workload is the preventive cardinality bound.
  (let [{:keys [requests transport-body-bytes object-write-attempts]
         :as totals} (resource-totals provider-report)]
    (when-not (and (<= cumulative-bytes max-cumulative-statement-bytes)
                   (<= requests max-transport-requests)
                   (<= transport-body-bytes max-transport-body-bytes)
                   (<= object-write-attempts max-object-write-attempts))
      (fail! ::resource-budget))
    totals))

(defn- reader-main! [request-path result-path]
  (let [{:keys [schema object-id expected digest]} (edn/read-string (slurp request-path))
        _ (require-confirmed-prefix!)
        _ (when-not (and (= schema 1)
                         (string? object-id)
                         (re-matches #"confirmed-10000-[0-9a-f-]{36}" object-id)
                         (= expected-rows (:n expected))
                         (string? digest))
            (fail! ::invalid-reader-request))
        context (#'throughput/aws-trial-context
                 {:trial 0 :target-wal-bytes nil :encode-included? false})
        provider (:provider-metrics context)
        store (:namespace-backend context)]
    (metrics/set-phase! provider :recovery)
    (with-open [reader (jdbc/connection
                       (durable/snapshot-dbspec
                        {:namespace-backend store :object-id object-id
                         :scratch-parent (.getParent (File. request-path))}))]
      (#'throughput/verify-counts! reader expected :confirmed-10000-readback)
      (when-not (= digest (fingerprint! reader))
        (fail! ::reader-digest-mismatch)))
    (let [provider-report (metrics/report provider)
          budget (check-resource-bounds! provider-report 0)
          result {:schema 1 :status :complete :rows expected-rows
                  :full-row-digest digest :provider provider-report
                  :resources budget}]
      (metrics/assert-redacted! result "" "" (#'throughput/evidence-canaries))
      (spit result-path (str (pr-str result) "\n")))))

(defn- run-reader! [root object-id expected digest]
  (let [request (File. root "reader-request.edn")
        result (File. root "reader-result.edn")
        stdout (File. root "reader.stdout")
        stderr (File. root "reader.stderr")
        wrapper (System/getenv "JOLT_WRAPPER")
        executable (System/getenv "BENCH_JOLT_BIN")]
    (when-not (and wrapper executable
                   (.isAbsolute (File. wrapper)) (.canExecute (File. wrapper))
                   (.isAbsolute (File. executable)) (.canExecute (File. executable)))
      (fail! ::missing-reader-runtime))
    (spit request (pr-str {:schema 1 :object-id object-id
                           :expected expected :digest digest}))
    (let [command (into (if (= wrapper executable)
                          [executable]
                          [wrapper executable])
                        ["-Srepro" "-M:durable-confirmed-10000"
                         "--reader" (.getAbsolutePath request)
                         (.getAbsolutePath result)])
          child (process/process
                 command
                 {:out stdout :err stderr})
          terminal (deref child reader-timeout-ms ::timeout)]
      (when (= terminal ::timeout)
        (process/destroy-tree child)
        (let [settled (deref child 5000 ::timeout)]
          (when-not (and (map? settled) (integer? (:exit settled)))
            (fail! ::reader-unsettled))
          (fail! ::reader-timeout)))
      (when-not (and (= 0 (:exit terminal)) (.isFile result))
        (fail! ::reader-failed))
      (let [receipt (edn/read-string (slurp result))]
        (when-not (and (= 1 (:schema receipt))
                       (= :complete (:status receipt))
                       (= expected-rows (:rows receipt))
                       (= digest (:full-row-digest receipt)))
          (fail! ::invalid-reader-receipt))
        (select-keys receipt [:rows :full-row-digest :provider :resources])))))

(defn run! []
  (let [run (require-confirmed-prefix!)
        context (#'throughput/aws-trial-context
                 {:trial 0 :target-wal-bytes nil :encode-included? false})
        object-id (str "confirmed-10000-" (random-uuid))
        provider (:provider-metrics context)
        root (File/createTempFile "confirmed-10000-" "")
        _ (when-not (and (.delete root) (.mkdir root))
            (fail! ::scratch-unavailable))
        configuration (merge
                       {:namespace-backend (:namespace-backend context)
                        :object-id object-id :scratch-parent (.getAbsolutePath root)
                        :owner "confirmed-10000-qualification"
                        :database "benchmark" :lease-ttl-ms 21600000
                        :heartbeat-interval-ms 7200000}
                       (:writer-options context))
        state (atom {:expected @#'throughput/empty-expected-aggregates
                     :cumulative-statement-bytes 0 :statements []
                     :measured-nanos [] :confirmations {:committed 0 :reconciled 0}})]
    (try
      (let [writer-fingerprint
            (with-open [connection (jdbc/connection
                                    (durable/writer-dbspec configuration))]
              (jdbc/execute! connection @#'throughput/logs-ddl)
              (when-not (#'throughput/successful-flush?
                         (durable/flush! connection))
                (fail! ::ddl-flush))
              (dotimes [ordinal total-calls]
                (let [{:keys [sql bytes sha256 expected]} (statement! ordinal)
                      cumulative (+ (:cumulative-statement-bytes @state) bytes)]
                  (when (> cumulative max-cumulative-statement-bytes)
                    (fail! ::statement-budget))
                  (metrics/set-phase! provider (if (< ordinal warmups)
                                                 :setup :flush))
                  (let [start (System/nanoTime)
                        outcome (durable/execute-and-flush! connection sql)
                        elapsed (- (System/nanoTime) start)
                        status (:status outcome)]
                    (when-not (contains? #{:committed :reconciled} status)
                      (fail! ::unconfirmed-call))
                    (swap! state
                           (fn [current]
                             (-> current
                                 (assoc :cumulative-statement-bytes cumulative)
                                 (update :expected merge-expected expected)
                                 (update :statements conj
                                         {:ordinal ordinal :bytes bytes :sha256 sha256})
                                 (update-in [:confirmations status] inc)
                                 (cond-> (>= ordinal warmups)
                                   (update :measured-nanos conj elapsed)))))
                    (check-resource-bounds! (metrics/report provider) cumulative))))
              (when-not (= expected-rows (get-in @state [:expected :n]))
                (fail! ::row-budget))
              (#'throughput/verify-counts! connection (:expected @state)
               :confirmed-10000-writer)
              (fingerprint! connection))
            readback (run-reader! root object-id (:expected @state)
                                  writer-fingerprint)
            provider-report (metrics/report provider)
            budget (check-resource-bounds! provider-report
                                           (:cumulative-statement-bytes @state))
            total-resources (merge-with + budget (:resources readback))
            samples (:measured-nanos @state)
            report {:schema 1 :status :complete
                    :scope :public-confirmed-s3-10000
                    :profile :confirmed-10000
                    :run run
                    :runtime (#'throughput/runtime-metadata)
                    :harness {:path "bench/jdbc/chdb_durable_confirmed_10000.clj"
                              :sha256 (digest/sha256-file
                                       (.toPath (File.
                                                 "bench/jdbc/chdb_durable_confirmed_10000.clj")))}
                    :workload {:batch-rows batch-rows :warmups warmups
                               :measured measured :expected-rows expected-rows
                               :statement-byte-cap max-statement-bytes
                               :cumulative-statement-byte-cap
                               max-cumulative-statement-bytes
                               :transport-request-cap max-transport-requests
                               :transport-body-byte-cap max-transport-body-bytes
                               :object-write-attempt-cap max-object-write-attempts
                               :resource-cap-semantics :post-call-audit
                               :statements (:statements @state)}
                    :confirmations (:confirmations @state)
                    :latency {:boundary :public-execute-and-flush-return
                              :raw-nanos samples
                              :p99-interpretation :rough-nearest-rank-tail-estimate
                              :summary (#'throughput/latency-summary samples)}
                    :reconciliation {:rows expected-rows
                                     :aggregates (:expected @state)
                                     :digest-method :sum-cityhash64-full-row-tuple
                                     :full-row-digest writer-fingerprint
                                     :fresh-reader readback}
                    :resources total-resources :provider provider-report
                    :limitations {:collector :not-measured
                                  :exporter :not-measured}}]
        (when-not (and (= measured (count samples))
                       (true? (get-in report [:latency :summary
                                              :p99-qualification?])))
          (fail! ::sample-count))
        (when-not (and (<= (:requests total-resources) max-transport-requests)
                       (<= (:transport-body-bytes total-resources)
                           max-transport-body-bytes)
                       (<= (:object-write-attempts total-resources)
                           max-object-write-attempts))
          (fail! ::resource-budget))
        (metrics/assert-redacted! report "" "" (#'throughput/evidence-canaries))
        report)
      (finally
        (#'throughput/delete-tree! root)))))

(defn -main [& [mode output result]]
  (if (= mode "--reader")
    (try
      (reader-main! output result)
      (catch Throwable _
        (println :confirmed-10000-reader-failed)
        (System/exit 1)))
    (try
      (when-not (and (= mode "confirmed-10000") output (nil? result))
        (fail! ::arguments))
      (let [report (run!)]
        (spit output (str (pr-str report) "\n"))
        (println :confirmed-10000-complete))
      (catch Throwable error
        (metrics/throw-redacted-failure! error)))))
