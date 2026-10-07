(ns jdbc.chdb-exporter-storage-qualification
  "The same exporter workload and committed ACK boundary on local storage/S3.
  Invoke writer and reader in separate processes. Credentials are environment
  inputs only; neither backend options nor exception details enter receipts."
  (:require [clojure.data.json :as json]
            [clojure.data.json.jolt-native :as native]
            [clojure.edn :as edn]
            [clojure.java.io :as io]
            [clojure.string :as str]
            [jdbc.chdb.durable :as durable]
            [jdbc.chdb.durable.backend :as storage]
            [jdbc.chdb.durable.control :as control]
            [jdbc.chdb.durable.digest :as digest]
            [jdbc.chdb.durable.local-posix :as local]
            [jdbc.chdb.durable.s3-curl :as s3]
            [jdbc.core :as jdbc]
            [otel.exporter.chdb-benchmark :as benchmark]))

(defn- required-env [name]
  (let [value (System/getenv name)]
    (when (str/blank? value)
      (throw (ex-info "Required benchmark environment input is absent" {})))
    value))

(defn- checked-source! [actual root relative]
  (assert (= (.getCanonicalPath (io/file actual))
             (.getCanonicalPath (io/file root relative)))
          "Benchmark resolved a different source checkout"))

(defn- provenance! [output]
  (native/load-writer!)
  (checked-source! (:file (meta #'native/load-writer!))
                   (required-env "BENCH_EXPECT_JSON_ROOT")
                   "src/main/clojure/clojure/data/json/jolt_native.clj")
  (checked-source! (:file (meta #'benchmark/run!))
                   (required-env "BENCH_EXPECT_EXPORTER_ROOT")
                   "bench/otel/exporter/chdb_benchmark.clj")
  (checked-source! (:file (meta #'durable/open-writer!))
                   (required-env "BENCH_EXPECT_CHDB_ROOT")
                   "src/jdbc/chdb/durable.clj")
  (let [hash (digest/sha256-bytes (.getBytes @#'native/source "UTF-8"))]
    (assert (= "85204463ae6a7d5fcd89634b4947e83cdc85a1e29279039e05a404380d5d403a" hash)
            "Benchmark resolved a different native JSON writer resource")
    (spit (str output ".provenance.edn")
          (pr-str {:scope :loaded-source-before-timing
                   :source-checkouts-confirmed true
                   :native-writer-sha256 hash}))))

(defn- namespace-backend [kind root]
  (case kind
    "local" (local/local-backend (str root "/objects"))
    "s3" (let [prefix (required-env "JOLT_CHDB_S3_PREFIX")]
           ;; No empty/global prefix, no fallback to a local store.
           (assert (and (str/starts-with? prefix "ci/jolt-chdb/")
                        (> (count prefix) (count "ci/jolt-chdb/")))
                   "S3 qualification requires an isolated CI prefix")
           (s3/s3-backend
             {:endpoint (required-env "JOLT_CHDB_S3_ENDPOINT")
              :bucket (required-env "JOLT_CHDB_S3_BUCKET")
              :prefix prefix :region (required-env "JOLT_CHDB_S3_REGION")
              :access-key (required-env "JOLT_CHDB_S3_ACCESS_KEY")
              :secret-key (required-env "JOLT_CHDB_S3_SECRET_KEY")
              :session-token (required-env "JOLT_CHDB_S3_SESSION_TOKEN")}))))

(defn- writer! [kind options output items batches]
  (let [reader (native/load-reader!)
        accepted (atom 0)
        tracked-reader (fn [source opts number]
                         (let [result (reader source opts number)]
                           (when (vector? result) (swap! accepted inc))
                           result))
        opens (atom 0)
        original durable/open-writer!
        report (binding [json/*experimental-native-reader* tracked-reader]
                 (with-redefs [durable/open-writer!
                               (fn [opts]
                                 (assert (= 128 (:checkpoint-wal-reference-threshold opts))
                                         "JDBC omitted the checkpoint policy")
                                 (swap! opens inc)
                                 (original opts))]
                   (benchmark/run!
                     {:db-spec (durable/writer-dbspec
                                 (merge options
                                        {:owner "collector-perf" :instance "writer-1"
                                         :database "otel" :lease-ttl-ms 900000
                                         :heartbeat-interval-ms 300000
                                         :checkpoint-wal-reference-threshold 128
                                         :operations {:json-reader tracked-reader}}))
                      :durable? true :json-backend :native-guarded-string-cache
                      :insert-format :json-compact-each-row
                      :batches batches :items items :query-iterations 3})))
        head (:head (control/read-head-read-only!
                      (storage/object-backend (:namespace-backend options) "telemetry")))
        references (count (get-in head ["manifest" "wal"]))]
    (assert (= 1 @opens))
    (assert (>= @accepted (* 10 batches))
            "Whole JSON reader did not reach the measured writer path")
    (assert (< references 128) "Published WAL exceeds configured reference bound")
    (assert (= :per-physical-insert-commit (get-in report [:configuration :ack-boundary])))
    (assert (= (get-in report [:workload :expected-counts])
               (get-in report [:workload :actual-counts])))
    ;; Benchmark/run! sanitizes db-spec to vendor only. Never append backend opts.
    (assert (= #{:vendor} (set (keys (get-in report [:configuration :db-spec])))))
    (spit output (pr-str (assoc report :storage-kind (keyword kind))))
    (spit (str output ".policy.edn")
          (pr-str {:scope :effective-jdbc-checkpoint-policy
                   :threshold 128 :writer-handoff-confirmed true
                   :remaining-wal-references references
                   :checkpoint-present? (some? (get-in head ["manifest" "base"]))
                   :whole-reader-accepted @accepted}))
    (prn {:phase :writer :storage-kind (keyword kind)
          :ingest (select-keys (:ingest report) [:wall-ms :stored-items-per-second])})))

(defn- reader! [kind options output]
  (let [report (edn/read-string (slurp output))
        _ (assert (= (keyword kind) (:storage-kind report)))
        expected (get-in report [:workload :expected-counts])
        actual (with-open [connection (jdbc/connection (durable/snapshot-dbspec options))]
                 (#'benchmark/stored-counts connection
                                           (get-in report [:workload :service-name])))]
    (assert (= expected actual) "Independent reader counts differ")
    (spit (str output ".recovery.edn")
          (pr-str {:scope :independent-count-readback :storage-kind (keyword kind)
                   :expected expected :actual actual}))
    (prn {:phase :reader :reader-counts-confirmed true
          :physical-rows (reduce + (vals actual))})))

(defn -main [& args]
  (try
    (let [[phase kind root output items-text batches-text] args
          items (parse-long items-text) batches (parse-long batches-text)]
      (assert (= 6 (count args)))
      (assert (#{"writer" "reader"} phase))
      (assert (#{"local" "s3"} kind))
      (assert (and (integer? items) (pos? items) (<= items 10000)
                   (integer? batches) (pos? batches) (<= batches 200)))
      (let [scratch (str root "/scratch-" phase)
            _ (.mkdirs (io/file scratch))
            options {:namespace-backend (namespace-backend kind root)
                     :object-id "telemetry" :scratch-parent scratch}]
        (if (= phase "writer")
          (do (provenance! output) (writer! kind options output items batches))
          (reader! kind options output))))
    (catch Throwable _
      ;; Provider errors/causes can contain sensitive material. Do not print them.
      (binding [*out* *err*] (prn {:qualification-failed true}))
      (System/exit 1))))
