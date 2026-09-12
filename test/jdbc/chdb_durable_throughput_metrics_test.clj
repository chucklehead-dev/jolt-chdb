(ns jdbc.chdb-durable-throughput-metrics-test
  (:require [clojure.string :as str]
            [jdbc.chdb-durable-throughput-metrics :as metrics]
            [jdbc.chdb.durable.backend :as backend]
            [jdbc.chdb.durable.s3 :as s3])
  (:import [java.io StringWriter]))

(def failures (atom 0))

(defn- check [label expected actual]
  (if (= expected actual)
    (println "  ok  " label)
    (do
      (swap! failures inc)
      (println "  FAIL" label))))

(defn- rejected [f]
  (try (f) nil (catch Throwable error error)))

(defn run-checks! []
  (println "Durable throughput metrics redaction contracts")
  (let [canaries ["AKIA-CANARY" "secret-key-canary" "session-canary"
                  "bucket-canary" "prefix-canary" "object-key-canary"
                  "signed-url-canary" "etag-canary" "sql-canary"
                  "payload-canary" "header-canary"]
        recorder (metrics/recorder)
        request! (metrics/instrument-transport
                  recorder
                  (fn [_]
                    {:status 200
                     :headers {"etag" "etag-canary"}
                     :body (.getBytes "payload-canary" "UTF-8")}))
        request {:operation :get-with-etag
                 :method :get
                 :url "https://signed-url-canary/bucket-canary/prefix-canary/object-key-canary"
                 :headers {"authorization" "header-canary"}
                 :auth {:access-key "AKIA-CANARY"
                        :secret-key "secret-key-canary"
                        :session-token "session-canary"}
                 :request-body {:bytes (.getBytes "sql-canary" "UTF-8")
                                :byte-count 10}}
        out (StringWriter.)
        err (StringWriter.)]
    (metrics/set-phase! recorder :flush)
    (binding [*out* out *err* err]
      (request! request))
    (let [report (metrics/report recorder)
          rendered (pr-str report)]
      (check "transport report retains bounded numeric HTTP status evidence"
             {:calls 1 :request-body-bytes 10 :response-body-bytes 14
              :results {200 1}}
             (select-keys (get-in report [:transport :flush :get-with-etag])
                          [:calls :request-body-bytes
                           :response-body-bytes :results]))
      (check "transport report does not retain any request or response canary"
             false
             (boolean (some #(str/includes? rendered %) canaries)))
      (check "quiet scan accepts bounded report and captured streams"
             nil
             (metrics/assert-redacted! report (str out) (str err) canaries)))

    (let [leaking (rejected
                   #(metrics/assert-redacted! {:safe true}
                                              "payload-canary" "" canaries))]
      (check "canary scan fails with one generic type"
             :jdbc.chdb-durable-throughput-metrics/redaction-contract
             (:type (ex-data leaking)))
      (check "canary failure does not echo the match"
             false
             (boolean
              (some #(str/includes?
                      (pr-str [(ex-message leaking) (ex-data leaking)]) %)
                    canaries))))

    (let [store (metrics/instrument-backend recorder (backend/memory-backend))
          key "object-key-canary"
          body (.getBytes "payload-canary" "UTF-8")]
      (metrics/set-phase! recorder :recovery)
      (backend/put-bytes-if-absent! store key body)
      (backend/get-with-etag store key)
      (let [report (metrics/report recorder)
            rendered (pr-str report)]
        (check "logical report retains closed keyword results and byte totals"
               [1 14 {:created 1} 1 14 {:ok 1}]
               [(get-in report [:logical :recovery :put-bytes-if-absent :calls])
                (get-in report [:logical :recovery :put-bytes-if-absent
                                :request-body-bytes])
                (get-in report [:logical :recovery :put-bytes-if-absent
                                :results])
                (get-in report [:logical :recovery :get-with-etag :calls])
                (get-in report [:logical :recovery :get-with-etag
                                :response-body-bytes])
                (get-in report [:logical :recovery :get-with-etag :results])])
        (check "logical backend wrapper retains neither key nor ETag nor body"
               false
               (boolean
                (some #(str/includes? rendered %)
                      [key "payload-canary" "opaque-etag-"]))))))

  (check "unknown report phases fail before observation"
         :jdbc.chdb-durable-throughput-metrics/invalid-phase
         (:type (ex-data (rejected #(metrics/set-phase! (metrics/recorder)
                                                        :tenant-canary)))))
  (check "S3 WAL curve uses all requested independent byte targets"
         [419430 3145728 12582912 50331648 67108864 100663296 133169152]
         metrics/wal-target-bytes)
  (let [payload (byte-array [97 10 98 98 98 98 10])]
    (check "row scan consumes the supplied byte array directly"
           4 (metrics/maximum-json-each-row-bytes payload))
    (aset-byte payload 1 (byte 120))
    (check "row scan observes mutation of that same retained array"
           6 (metrics/maximum-json-each-row-bytes payload)))
  (check "persisted targets account for admission time and flush latency"
         {20000 {:possible? true :rows 5000 :batches 10}
          25000 {:possible? true :rows 7500 :batches 15}}
         (metrics/aggregation-requirements 50000.0 150.0))
  (check "persisted targets are impossible when admission is not faster"
         {20000 {:possible? false
                 :reason :admission-rate-not-above-target}
          25000 {:possible? false
                 :reason :admission-rate-not-above-target}}
         (metrics/aggregation-requirements 20000.0 150.0))
  (check "negative flush latency fails without retaining its value"
         :jdbc.chdb-durable-throughput-metrics/invalid-flush-latency
         (:type (ex-data
                 (rejected #(metrics/aggregation-requirements 50000.0 -1)))))
  (check "nonpositive admission rate fails generically"
         :jdbc.chdb-durable-throughput-metrics/invalid-admission-rate
         (:type (ex-data
                 (rejected #(metrics/aggregation-requirements 0 1)))))
  (doseq [[label value] [["NaN" ##NaN]
                         ["positive infinity" ##Inf]
                         ["negative infinity" ##-Inf]]]
    (check (str "non-finite admission rate fails generically: " label)
           {:type :jdbc.chdb-durable-throughput-metrics/invalid-admission-rate}
           (ex-data
            (rejected #(metrics/aggregation-requirements value 1))))
    (check (str "non-finite flush latency fails generically: " label)
           {:type :jdbc.chdb-durable-throughput-metrics/invalid-flush-latency}
           (ex-data
            (rejected #(metrics/aggregation-requirements 50000.0 value)))))
  (check "committed flush requires the exact production logical shape"
         nil
         (metrics/assert-uncontended-flush-control!
          {:logical {:flush {:put-bytes-if-absent {:calls 1}
                             :get {:calls 1}
                             :get-with-etag {:calls 3}
                             :replace-if-match
                             {:calls 1 :results {:replaced 1}}}}}
          :committed))
  (doseq [head-reads [4 6]]
    (check (str "reconciled flush accepts bounded proof reads: " head-reads)
           nil
           (metrics/assert-uncontended-flush-control!
            {:logical {:flush {:put-bytes-if-absent {:calls 1}
                               :get {:calls 1}
                               :get-with-etag {:calls head-reads}
                               :replace-if-match
                               {:calls 1 :results {:ambiguous 1}}}}}
            :reconciled)))
  (doseq [head-reads [3 7]]
    (check (str "reconciled flush rejects malformed proof reads: " head-reads)
           :jdbc.chdb-durable-throughput-metrics/flush-control-mismatch
           (:type
            (ex-data
             (rejected
              #(metrics/assert-uncontended-flush-control!
                {:logical {:flush {:put-bytes-if-absent {:calls 1}
                                   :get {:calls 1}
                                   :get-with-etag {:calls head-reads}
                                   :replace-if-match
                                   {:calls 1 :results {:ambiguous 1}}}}}
                :reconciled))))))
  (check "reconciled flush rejects a non-ambiguous CAS result"
         :jdbc.chdb-durable-throughput-metrics/flush-control-mismatch
         (:type
          (ex-data
           (rejected
            #(metrics/assert-uncontended-flush-control!
              {:logical {:flush {:put-bytes-if-absent {:calls 1}
                                 :get {:calls 1}
                                 :get-with-etag {:calls 4}
                                 :replace-if-match
                                 {:calls 1 :results {:replaced 1}}}}}
              :reconciled)))))
  (check "flush request amplification mismatch fails generically"
         :jdbc.chdb-durable-throughput-metrics/flush-control-mismatch
         (:type (ex-data
                 (rejected
                  #(metrics/assert-uncontended-flush-control!
                    {:logical {:flush {:put-bytes-if-absent {:calls 2}}}}
                    :committed)))))
  (check "flush control rejects otherwise-correct calls plus any extra op"
         :jdbc.chdb-durable-throughput-metrics/flush-control-mismatch
         (:type
          (ex-data
           (rejected
            #(metrics/assert-uncontended-flush-control!
              {:logical
               {:flush {:put-bytes-if-absent {:calls 1}
                        :get {:calls 1}
                        :get-with-etag {:calls 3}
                        :replace-if-match
                        {:calls 1 :results {:replaced 1}}
                        :download-to-file {:calls 1}}}}
              :committed)))))
  (check "transport coverage accepts retries beyond logical calls"
         nil
         (metrics/assert-transport-coverage!
          {:retry-amplification
           {[:flush :get] {:logical-calls 1 :transport-attempts 2}}}))
  (check "missing transport attempts fail generically"
         :jdbc.chdb-durable-throughput-metrics/transport-coverage-mismatch
         (:type
          (ex-data
           (rejected
            #(metrics/assert-transport-coverage!
              {:retry-amplification
               {[:flush :get] {:logical-calls 1
                               :transport-attempts 0}}})))))
  (check "transport attempts without a logical call fail generically"
         :jdbc.chdb-durable-throughput-metrics/transport-coverage-mismatch
         (:type
          (ex-data
           (rejected
            #(metrics/assert-transport-coverage!
              {:retry-amplification
               {[:flush :get] {:logical-calls 0
                               :transport-attempts 1}}})))))

  (let [recorder (metrics/recorder)
        retry-injected? (atom false)
        request!
        (metrics/instrument-transport
         recorder
         (fn [{:keys [operation method]}]
           (if (and (= :get-with-etag operation)
                    (compare-and-set! retry-injected? false true))
             {:status 503}
             (if (= :get method)
               {:status 200
                :headers {"etag" "etag-canary"}
                :body (.getBytes "payload-canary" "UTF-8")}
               {:status 200 :headers {"etag" "etag-canary"}}))))
        namespace
        (s3/s3-backend
         {:endpoint "https://signed-url-canary"
          :bucket "bucket-canary" :prefix "prefix-canary"
          :region "us-west-2" :access-key "AKIA-CANARY"
          :secret-key "secret-key-canary" :session-token "session-canary"
          :max-attempts 2 :retry-deadline-ms 1000
          :retry-initial-backoff-ms 1 :retry-max-backoff-ms 1
          :monotonic-ms! (constantly 0) :await-backoff! (fn [_] true)
          :request! request!})
        store (metrics/instrument-backend recorder namespace)
        body (.getBytes "sql-canary" "UTF-8")]
    (metrics/set-phase! recorder :flush)
    (dotimes [_ 3] (backend/get-with-etag store "object-key-canary"))
    (backend/put-bytes-if-absent! store "object-key-canary" body)
    (backend/get-bytes store "object-key-canary")
    (backend/replace-if-match! store "object-key-canary" body "etag-canary")
    (let [report (metrics/report recorder)
          rendered (pr-str report)]
      (check "composed S3 fake satisfies exact six-call flush control"
             nil (metrics/assert-uncontended-flush-control! report :committed))
      (check "composed S3 fake preserves retry-loop transport attempts"
             {:logical-calls 3 :transport-attempts 4 :extra-attempts 1}
             (get-in report
                     [:retry-amplification [:flush :get-with-etag]]))
      (check "composed S3 fake has complete transport coverage"
             nil (metrics/assert-transport-coverage! report))
      (check "composed S3 fake retains no identity or content canary"
             false
             (boolean
              (some #(str/includes? rendered %)
                    ["signed-url-canary" "bucket-canary" "prefix-canary"
                     "AKIA-CANARY" "secret-key-canary" "session-canary"
                     "object-key-canary" "etag-canary" "payload-canary"
                     "sql-canary"])))))

  (let [failure
        (rejected
         #(metrics/throw-redacted-failure!
           (ex-info "signed-url-canary"
                    {:token "session-canary" :etag "etag-canary"})))]
    (check "top-level failure replaces arbitrary dynamic exception content"
           :jdbc.chdb-durable-throughput-metrics/throughput-failed
           (:type (ex-data failure)))
    (check "top-level failure retains no dynamic exception content"
           false
           (boolean
            (some #(str/includes?
                    (pr-str [(ex-message failure) (ex-data failure)]) %)
                  ["signed-url-canary" "session-canary" "etag-canary"]))))

  (let [workflow (slurp ".github/workflows/durable-aws.yml")
        action (slurp ".github/actions/install-jolt-aspects/action.yml")
        benchmark (slurp "bench/jdbc/chdb_durable_throughput.clj")
        metrics-source
        (slurp "bench/jdbc/chdb_durable_throughput_metrics.clj")
        workflow-files
        (filter #(and (.isFile %)
                      (re-matches #".*\.ya?ml" (.getName %)))
                (file-seq (java.io.File. ".github/workflows")))
        hosted-text (str action "\n" (str/join "\n" (map slurp workflow-files)))
        stale-pin (str "fd" "216943")]
    (check "throughput waits for successful provider qualification"
           true
           (and (str/includes? workflow "needs: s3-provider")
                (str/includes?
                 workflow
                 "inputs.run_throughput && needs.s3-provider.result == 'success'")))
    (check "workflow removes process-wide file-size limits"
           false (str/includes? workflow "ulimit -f"))
    (check "workflow bounds only the copied artifact log"
           true
           (and (str/includes? workflow
                               "raw_log=\"$RUNNER_TEMP/durable-s3-throughput.log\"")
                (str/includes? workflow
                               "head -c 8388608 \"$raw_log\" > \"$log\"")
                (str/includes? workflow
                               "test \"$raw_log_bytes\" -le 8388608")))
    (check "all hosted compiler provenance removes the superseded pin"
           false (str/includes? hosted-text stale-pin))
    (check "shared action asserts the exact strict-decoder compiler version"
           true
           (and (str/includes?
                 action "120643d6bc322800a700e870de5c8087ad6085fa")
                (str/includes? action "jolt v0.8.6-97-g120643d6")))
    (check "throughput provenance consumes shared action outputs"
           true
           (and (str/includes?
                 workflow
                 "BENCH_JOLT_SOURCE_SHA: ${{ steps.pinned-jolt.outputs.source-sha }}")
                (str/includes?
                 workflow
                 "BENCH_JOLT_VERSION: ${{ steps.pinned-jolt.outputs.version }}")
                (str/includes?
                 workflow
                 "steps.pinned-jolt.outputs.source-sha }}\" = \"$QUALIFIED_JOLT_SHA")))
    (check "runner reports measured flush cadence bytes"
           true (str/includes? benchmark ":flush-cadence-bytes"))
    (check "runner removes the second per-row JSON encoding pass"
           true
           (and (str/includes? benchmark
                               "maximum-serialized-row-payload-bytes encoded")
                (str/includes? benchmark
                               "::payload-byte-array payload-byte-array")
                (str/includes? metrics-source "(aget payload index)")
                (not (str/includes? benchmark "(str/split-lines payload)"))
                (not (str/includes? benchmark
                                    "maximum-row-payload-bytes rows")))))
  nil)

(defn -main [& _]
  (reset! failures 0)
  (run-checks!)
  (when-not (zero? @failures)
    (throw (ex-info "throughput metrics checks failed"
                    {:failures @failures}))))
