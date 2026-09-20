(ns jdbc.chdb-durable-throughput-test
  (:require [clojure.string :as str]
            [jdbc.chdb-durable-cross-binding-recovery :as cross-binding]
            [jdbc.chdb-durable-throughput :as throughput]))

(def failures (atom 0))

(defn- check [label expected actual]
  (if (= expected actual)
    (println "  ok  " label)
    (do
      (swap! failures inc)
      (println "  FAIL" label "- expected" (pr-str expected)
               "got" (pr-str actual)))))

(defn- rejected-type [f]
  (try
    (f)
    nil
    (catch Throwable error
      (:type (ex-data error)))))

(defn run-checks! []
  (println "Durable throughput profile contracts")
  (let [profile-configs #'throughput/profile-configs
        reduce-row-batches #'throughput/reduce-row-batches
        log-row #'throughput/log-row
        empty-expected @#'throughput/empty-expected-aggregates
        accumulate-expected-batch #'throughput/accumulate-expected-batch
        parse-profile! #'throughput/parse-profile!
        validate-profile-configs! #'throughput/validate-profile-configs!
        provenance! #'throughput/require-qualification-provenance!
        latency-summary #'throughput/latency-summary
        memory-limitations @#'throughput/recovery-memory-limitations
        report-contract @#'throughput/report-contract
        wal-size-observation #'throughput/wal-size-observation
        recovery-memory-observation #'throughput/recovery-memory-observation
        checked-trial-context! #'throughput/checked-trial-context!
        report-configuration #'throughput/report-configuration
        collect-measured-batches! #'throughput/collect-measured-batches!
        encode-batch-production #'throughput/encode-batch-production
        maximum-serialized-row-payload-bytes
        #'throughput/maximum-serialized-row-payload-bytes
        successful-flush? #'throughput/successful-flush?
        timed-operations #'throughput/timed-operations
        stage-metrics #'throughput/stage-metrics
        attribution-events #'throughput/attribution-events
        stage-values #'throughput/stage-values
        stage-report #'throughput/stage-report
        stage-attribution! #'throughput/stage-attribution!
        admission-attribution-stages @#'throughput/admission-attribution-stages
        flush-attribution-stages @#'throughput/flush-attribution-stages
        redact-batch-samples #'throughput/redact-batch-samples
        recovery-phase-recorder #'cross-binding/recovery-phase-recorder
        checked-phase-source-sha!
        #'cross-binding/checked-phase-source-sha!
        clean-runtime {:jolt-version "jolt v0.8.6-599-gbf8a5dde"
                       :jolt-source-sha
                       "bf8a5dde7bebb5658d218e9757ab1df0aa9c3b95"
                       :jolt-executable {:bytes 1 :sha256 "jolt-digest"}
                       :started-at "2026-09-11T00:00:00Z"
                       :native-library {:bytes 1 :sha256 "digest"}
                       :git {:head "head" :parent "parent" :tree "tree"
                             :status "clean"}}]
    (check "scale profile fixes the requested batch sweep"
           [[512 100 5] [1000 50 5] [5000 10 5] [10000 5 5]]
           (mapv (juxt :batch-size :batches :trials)
                 (profile-configs :scale)))
    (check "scale trials each measure about fifty thousand rows"
           [51200 50000 50000 50000]
           (mapv #(* (:batch-size %) (:batches %))
                 (profile-configs :scale)))
    (check "each scale selector resolves to one exact process configuration"
           [[:scale-512 512 100]
            [:scale-1000 1000 50]
            [:scale-5000 5000 10]
            [:scale-10000 10000 5]]
           (mapv (fn [selector]
                   (let [configs (validate-profile-configs!
                                  selector (profile-configs selector))
                         config (first configs)]
                     [(:selector config) (:batch-size config)
                      (:batches config)]))
                 [:scale-512 :scale-1000 :scale-5000 :scale-10000]))
    (check "stage selector is one diagnostic-only 512-row Durable-preencoded trial"
           [:stage-512 512 100 1 true [:durable-preencoded]]
           (let [config (first (profile-configs :stage-512))]
             [(:selector config) (:batch-size config) (:batches config)
              (:trials config) (:instrumented? config) (:modes config)]))
    (check "stage smoke keeps the real instrumented writer path bounded"
           [:stage-smoke 32 1 1 true [:durable-preencoded]]
           (let [config (first (profile-configs :stage-smoke))]
             [(:selector config) (:batch-size config) (:batches config)
              (:trials config) (:instrumented? config) (:modes config)]))
    (let [metrics (stage-metrics)]
      (check "stage timing keeps its private event ledger outside Jolt atom metadata"
             []
             @(attribution-events metrics))
      (check "stage timing omits its private event ledger from child handoff values"
             {}
             (stage-values metrics)))
    (check "staged recovery selectors are bounded 512-row fresh-process workloads"
           [[:recovery-512-10 512 10 0 1 [:durable-preencoded]]
            [:recovery-512-25 512 25 0 1 [:durable-preencoded]]
            [:recovery-512-50 512 50 0 1 [:durable-preencoded]]]
           (mapv (fn [selector]
                   (let [config (first (profile-configs selector))]
                     [(:selector config) (:batch-size config) (:batches config)
                      (:warmup-batches config) (:trials config) (:modes config)]))
                 [:recovery-512-10 :recovery-512-25 :recovery-512-50]))
    (check "S3 curve uses independent measured-WAL targets and both admission modes"
           (mapv (fn [target]
                   [target 512 0 1 :aws-s3
                    [:durable-encode-included :durable-preencoded]])
                 [419430 3145728 12582912 50331648 67108864
                  100663296 133169152])
           (mapv (fn [configuration]
                   [(:target-wal-bytes configuration)
                    (:batch-size configuration)
                    (:warmup-batches configuration)
                    (:trials configuration)
                    (:provider-kind configuration)
                    (:modes configuration)])
                 (profile-configs :s3-curve)))
    (let [bytes (atom 0)
          result (collect-measured-batches!
                  {:target-wal-bytes 9}
                  (fn [_] (swap! bytes + 4))
                  (fn [] {:pending-statements (quot @bytes 4)
                          :pending-wal-bytes @bytes}))]
      (check "target runner stops only after measured pending WAL reaches target"
             {:batches 3
              :pending {:pending-statements 3 :pending-wal-bytes 12}}
             result))
    (let [steps (atom 0)]
      (check "fixed local batch runner preserves its existing boundary"
             2
             (:batches
              (collect-measured-batches!
               {:batches 2}
               (fn [_] (swap! steps inc))
               (fn [] {:pending-statements @steps
                       :pending-wal-bytes @steps}))))
      (check "fixed local runner executes exactly its requested batches"
             2 @steps))
    (let [encoded (encode-batch-production [{"x" "é"} {"x" "abcd"}])
          retained
          (:jdbc.chdb-durable-throughput/payload-byte-array encoded)]
      (check "production encoding retains its one payload byte array"
             [true (:payload-bytes encoded)]
             [(bytes? retained) (alength retained)])
      (check "row maximum scans retained bytes without consulting SQL text"
             14
             (maximum-serialized-row-payload-bytes
              (assoc encoded :sql (Object.))))
      (check "bounded WAL evidence never retains the raw payload byte array"
             false
             (contains?
              (wal-size-observation
               {:pending-wal-bytes 10 :pending-statements 1}
               (assoc encoded
                      :maximum-batch-payload-bytes (:payload-bytes encoded)
                      :maximum-batch-statement-bytes
                      (:statement-bytes encoded)))
              :jdbc.chdb-durable-throughput/payload-byte-array)))
    (let [target (* 127 1024 1024)]
      (check "127 MiB target remains accepted"
             target
             (get-in
              (collect-measured-batches!
               {:target-wal-bytes target}
               (fn [_] nil)
               (fn [] {:pending-statements 1 :pending-wal-bytes target}))
              [:pending :pending-wal-bytes])))
    (check "requested 128 MiB target fails closed"
           :jdbc.chdb-durable-throughput/invalid-measurement-boundary
           (rejected-type
            #(collect-measured-batches!
              {:target-wal-bytes (* 128 1024 1024)}
              (fn [_] nil)
              (fn [] {:pending-statements 1
                      :pending-wal-bytes (* 128 1024 1024)}))))
    (check "observed WAL above frozen 128 MiB guard fails closed"
           :jdbc.chdb-durable-throughput/wal-limit-crossed
           (rejected-type
            #(collect-measured-batches!
              {:target-wal-bytes (* 127 1024 1024)}
              (fn [_] nil)
              (fn [] {:pending-statements 1
                      :pending-wal-bytes (inc (* 128 1024 1024))}))))
    (check "committed and ambiguity-reconciled flushes both persisted"
           [true true false]
           (mapv successful-flush?
                 [{:status :committed} {:status :reconciled}
                  {:status :ambiguous}]))
    (check "selector-resolution red control rejects a drifted selector"
           :jdbc.chdb-durable-throughput/invalid-selector-resolution
           (rejected-type
            #(validate-profile-configs!
              :scale-512 [(assoc (first (profile-configs :scale-512))
                                 :selector :scale-1000)])))
    (let [constructed (atom 0)
          observations
          (reduce-row-batches
           (fn [index _]
             (swap! constructed inc)
             index)
           10000 5 false 10000 []
           (fn [acc batch rows]
             (conj acc {:batch batch
                        :constructed @constructed
                        :retained-rows (count rows)
                        :first-index (first rows)
                        :last-index (peek rows)})))]
      (check "10k scale construction reaches its consumer one bounded batch at a time"
             [{:batch 0 :constructed 10000 :retained-rows 10000
               :first-index 10000 :last-index 19999}
              {:batch 1 :constructed 20000 :retained-rows 10000
               :first-index 20000 :last-index 29999}
              {:batch 2 :constructed 30000 :retained-rows 10000
               :first-index 30000 :last-index 39999}
              {:batch 3 :constructed 40000 :retained-rows 10000
               :first-index 40000 :last-index 49999}
              {:batch 4 :constructed 50000 :retained-rows 10000
               :first-index 50000 :last-index 59999}]
             observations))
    (let [rows (mapv #(log-row % true) [0 1 20])
          trace-ids (sort (map #(get % "TraceId") rows))
          span-ids (sort (map #(get % "SpanId") rows))
          reference {:n 3
                     :flags (reduce + (map #(get % "TraceFlags") rows))
                     :severity_sum (reduce + (map #(get % "SeverityNumber") rows))
                     :body_bytes (reduce + (map #(alength (.getBytes (get % "Body") "UTF-8")) rows))
                     :question_bodies 3
                     :min_trace (first trace-ids) :max_trace (last trace-ids)
                     :min_span (first span-ids) :max_span (last span-ids)}]
      (check "incremental recovery oracle preserves the prior aggregate contract"
             reference
             (accumulate-expected-batch empty-expected rows true)))
    (check "qualification remains an explicit 512/question/single-row plan"
           [:batched-512 :batched-512-question-mark :single-row]
           (mapv :label (profile-configs :qualification)))
    (check "fewer than one hundred samples cannot qualify p99"
           false
           (:p99-qualification? (latency-summary (range 99))))
    (check "admission percentile support is explicit at each sample boundary"
           [[false false] [true false] [true true]]
           (mapv (fn [samples]
                   (let [summary (latency-summary (range samples))]
                     [(:p50-supported? summary) (:p95-supported? summary)]))
                 [1 2 20]))
    (check "one hundred samples can qualify p99"
           true
           (:p99-qualification? (latency-summary (range 100))))
    (check "instrumented result samples remain available for summary before redaction"
           2
           (:count
            (latency-summary
             (:jdbc.chdb-durable-throughput/batch-latency-samples
              {:jdbc.chdb-durable-throughput/batch-latency-samples [10 20]}))))
    (check "retained worker values redact batch samples without changing aggregates"
           [{:result {:batch-latency {:count 2}}}
            {:batch-latency {:count 2}}]
           [(redact-batch-samples
             {:result {:batch-latency {:count 2}
                       :jdbc.chdb-durable-throughput/batch-latency-samples [10 20]}})
            (redact-batch-samples
             {:batch-latency {:count 2}
              :jdbc.chdb-durable-throughput/batch-latency-samples [10 20]})])
    (check "clean complete qualification provenance is accepted"
           nil
           (provenance! :qualification clean-runtime))
    (let [s3-runtime
          (-> clean-runtime
              (assoc-in [:benchmark-harness :metrics-sha256]
                        "metrics-digest")
              (assoc :hosted-run
                     {:workflow :durable-aws-qualification
                      :event :workflow-dispatch :runner-os :linux
                      :runner-arch :x64 :run-id "1" :run-attempt "2"})
              (assoc :provider {:kind :aws-s3 :region "us-west-2"}))]
      (check "complete sanitized S3 provenance is accepted"
             nil (provenance! :s3-curve s3-runtime))
      (check "S3 provenance fails closed without hosted identity"
             :jdbc.chdb-durable-throughput/missing-provenance
             (rejected-type #(provenance! :s3-curve
                                          (dissoc s3-runtime :hosted-run)))))
    (check "isolated recovery evidence also requires complete provenance"
           nil
           (provenance! :recovery-512-10 clean-runtime))
    (check "diagnostic profiles do not claim qualification provenance"
           [nil nil nil]
           (mapv #(provenance! % {}) [:smoke :probe :diagnostic]))
    (check "missing qualification provenance fails closed"
           :jdbc.chdb-durable-throughput/missing-provenance
           (rejected-type
            #(provenance! :scale (assoc-in clean-runtime [:git :head] nil))))
    (check "empty native qualification provenance fails closed"
           :jdbc.chdb-durable-throughput/missing-provenance
           (rejected-type
            #(provenance! :scale
                          (assoc-in clean-runtime [:native-library :bytes] 0))))
    (check "missing Jolt source provenance fails closed"
           :jdbc.chdb-durable-throughput/missing-provenance
           (rejected-type
            #(provenance! :scale (assoc clean-runtime :jolt-source-sha nil))))
    (check "abbreviated Jolt source provenance fails closed"
           :jdbc.chdb-durable-throughput/invalid-provenance
           (rejected-type
            #(provenance! :scale (assoc clean-runtime
                                        :jolt-source-sha "cf0b6928"))))
    (check "empty Jolt executable provenance fails closed"
           :jdbc.chdb-durable-throughput/missing-provenance
           (rejected-type
            #(provenance! :scale
                          (assoc-in clean-runtime [:jolt-executable :bytes] 0))))
    (check "dirty qualification provenance fails closed"
           :jdbc.chdb-durable-throughput/dirty-provenance
           (rejected-type
            #(provenance! :scale
                          (assoc-in clean-runtime [:git :status] "dirty"))))
    (check "unknown profile fails before benchmark work"
           :jdbc.chdb-durable-throughput/unknown-profile
           (rejected-type #(parse-profile! "unknown")))
    (check "near-miss selector fails instead of widening to a sweep"
           :jdbc.chdb-durable-throughput/unknown-profile
           (rejected-type #(parse-profile! "scale-1024")))
    (check "report contract retains provenance and both recovery memory endpoints"
           [[[:runtime :jolt-version]
             [:runtime :jolt-source-sha]
             [:runtime :jolt-executable :sha256]
             [:runtime :native-library :sha256]
             [:runtime :git :head]
             [:runtime :git :parent]
             [:runtime :git :tree]
             [:runtime :git :status]
             [:runtime :started-at]]
            [[:recovery :memory :immediately-before-reader-open]
             [:recovery :memory :after-open-and-reconciliation]]]
           [(:provenance-paths report-contract)
            (:recovery-memory-paths report-contract)])
    (check "report contract locates trial-relative paths"
           :configuration-result
           (:relative-trial-paths-to report-contract))
    (let [metrics (atom {})
          observe! (:recovery-phase! (timed-operations metrics))]
      (observe! {:phase :wal-json-parse :status :complete
                 :calls 1 :nanos 10 :bytes 20})
      (observe! {:phase :wal-json-parse :status :failed
                 :calls 1 :nanos 30 :bytes 40})
      (check "throughput reports aggregate bounded recovery phase events"
             {:calls 2 :total-ms 0.00004 :bytes 60 :mean-ms 0.00002
              :statuses {:complete 1 :failed 1}}
             (:wal-json-parse (stage-report metrics))))
    (let [metrics (atom {})
          ordinary-operations (timed-operations metrics)
          stage-operations (timed-operations metrics true)
          observe! (:writer-phase! stage-operations)]
      (observe! {:phase :wal-prepare :status :complete
                 :calls 1 :nanos 10 :bytes 0})
      (observe! {:phase :wal-head-cas :status :complete
                 :calls 1 :nanos 30 :bytes 40})
      (check "writer phase hook is stage-selector-only and remains scalar"
             [false true false
              {:calls 1 :total-ms 0.00001 :bytes 0 :mean-ms 0.00001
               :statuses {:complete 1}}
              {:calls 1 :total-ms 0.00003 :bytes 40 :mean-ms 0.00003
               :statuses {:complete 1}}]
             [(contains? ordinary-operations :writer-phase!)
              (fn? observe!)
              (contains? stage-operations :publish-wal!)
              (:wal-prepare (stage-report metrics))
              (:wal-head-cas (stage-report metrics))]))
    (let [stages {:data-json {:nanos 10}
                  :exporter-materialization {:nanos 20}
                  :prepare-query {:nanos 30}
                  :native-classify {:nanos 40}
                  :native-execute {:nanos 50}
                  :backend/put-bytes-if-absent {:nanos 60}
                  :backend/get-bytes {:nanos 70}
                  :backend/get-with-etag {:nanos 80}
                  :backend/replace-if-match {:nanos 90}}
          admission (stage-attribution! :admission 200 stages
                                        [[:start :data-json]
                                         [:finish :data-json]
                                         [:start :exporter-materialization]
                                         [:finish :exporter-materialization]
                                         [:start :prepare-query]
                                         [:finish :prepare-query]
                                         [:start :native-classify]
                                         [:finish :native-classify]
                                         [:start :native-execute]
                                         [:finish :native-execute]]
                                        admission-attribution-stages)
          flush (stage-attribution! :flush 400 stages
                                    [[:start :backend/put-bytes-if-absent]
                                     [:finish :backend/put-bytes-if-absent]
                                     [:start :backend/get-bytes]
                                     [:finish :backend/get-bytes]
                                     [:start :backend/get-with-etag]
                                     [:finish :backend/get-with-etag]
                                     [:start :backend/replace-if-match]
                                     [:finish :backend/replace-if-match]]
                                    flush-attribution-stages)]
      (check "admission timing categories form an exact non-overlapping partition"
             [200 150 50 true
              {:encoding/data-json 10
               :encoding/materialization 20
               :writer/prepare-query 30
               :writer/native-classify 40
               :writer/native-execute 50
               :writer/unattributed 50}]
             [(:total-nanos admission) (:accounted-nanos admission)
              (:unattributed-nanos admission) (:partition-verified? admission)
              (into {}
                    (map (fn [[category value]] [category (:total-nanos value)]))
                    (:categories admission))])
      (check "flush timing categories partition publication and retain residual"
             [400 300 100 true
              [:driver-and-queue-dispatch :writer-lease-checks
               :wal-join-and-clear :publication-and-commit-control
               :timer-bookkeeping]]
             [(:total-nanos flush) (:accounted-nanos flush)
              (:unattributed-nanos flush) (:partition-verified? flush)
              (:unattributed-semantics flush)])
      (check "overlapping or drifted stage timing fails instead of clamping residual"
             :jdbc.chdb-durable-throughput/invalid-stage-attribution
             (rejected-type
              #(stage-attribution!
                :admission 149 stages
                [[:start :data-json] [:finish :data-json]
                 [:start :exporter-materialization]
                 [:finish :exporter-materialization]
                 [:start :prepare-query] [:finish :prepare-query]
                 [:start :native-classify] [:finish :native-classify]
                 [:start :native-execute] [:finish :native-execute]]
                admission-attribution-stages)))
      (check "nested selected stage timing fails even with positive residual"
             :jdbc.chdb-durable-throughput/invalid-stage-attribution
             (rejected-type
              #(stage-attribution!
                :admission 300 stages
                [[:start :data-json] [:start :prepare-query]
                 [:finish :prepare-query] [:finish :data-json]
                 [:start :exporter-materialization]
                 [:finish :exporter-materialization]
                 [:start :native-classify] [:finish :native-classify]
                 [:start :native-execute] [:finish :native-execute]]
                admission-attribution-stages)))
      (check "missing causality evidence cannot qualify a timing partition"
             :jdbc.chdb-durable-throughput/invalid-stage-attribution
             (rejected-type
              #(stage-attribution! :admission 200 stages []
                                    admission-attribution-stages))))
    (let [{:keys [metrics observe!]} (recovery-phase-recorder)]
      (observe! {:phase :wal-lf-scan :status :complete
                 :calls 1 :nanos 11 :bytes 12})
      (observe! {:phase :wal-lf-scan :status :failed
                 :calls 1 :nanos 13 :bytes 14})
      (check "cross-binding phase sidecar aggregation stays scalar and bounded"
             {:calls 2 :nanos 24 :bytes 26
              :statuses {:complete 1 :failed 1}}
             (:wal-lf-scan @metrics)))
    (check "phase sidecar requires an exact lowercase source SHA"
           ["aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
            :jdbc.chdb-durable-cross-binding-recovery/invalid-oracle
            :jdbc.chdb-durable-cross-binding-recovery/invalid-oracle]
           [(checked-phase-source-sha!
             "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
            (rejected-type #(checked-phase-source-sha! "aaaaaaaa"))
            (rejected-type
             #(checked-phase-source-sha!
               "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"))])
    (check "WAL total and maximum input batch are distinct report fields"
           {:wal-growth-total [:wal-growth :total-bytes]
            :maximum-row-payload [:maximum-row-payload-bytes]
            :maximum-input-batch [:maximum-batch-statement-bytes]}
           (:wal-size-semantics report-contract))
    (check "WAL output builder does not conflate total growth with batch maxima"
           {:wal-growth {:total-bytes 900 :records 3}
            :maximum-batch-payload-bytes 250
            :maximum-row-payload-bytes 125
            :maximum-batch-statement-bytes 300}
           (wal-size-observation
            {:pending-wal-bytes 900 :pending-statements 3}
            {:maximum-batch-payload-bytes 250
             :maximum-row-payload-bytes 125
             :maximum-batch-statement-bytes 300}))
    (check "recovery output builder preserves both absolute endpoint samples"
           {:immediately-before-reader-open {:reserved-from-os-bytes 10}
            :after-open-and-reconciliation {:reserved-from-os-bytes 20}
            :limitations memory-limitations}
           (recovery-memory-observation
            {:reserved-from-os-bytes 10} {:reserved-from-os-bytes 20}))
    (check "memory report refuses an unsupported endpoint-only growth oracle"
           [:not-supported
            :two-endpoint-jolt-allocator-samples-do-not-establish-process-rss-plateau
            true]
           [(:plateau-or-growth-oracle memory-limitations)
            (:reason memory-limitations)
            (string? (:external-peak-rss-required memory-limitations))])
    (check "direct trial-runner misuse also fails before benchmark work"
           :jdbc.chdb-durable-throughput/unsupported-run-profile
           (rejected-type #(profile-configs :diagnostic)))
    (let [factory (fn [_] :provider-secret)]
      (check "backend factory is execution-only report configuration"
             {:batch-size 512 :provider-kind :aws-s3}
             (report-configuration
              {:batch-size 512 :provider-kind :aws-s3 :modes [:one]
               :backend-context! factory :endpoint "provider-secret"})))
    (check "invalid backend contexts fail with redacted diagnostics"
           :jdbc.chdb-durable-throughput/invalid-backend-context
           (rejected-type
            #(checked-trial-context!
              {:namespace-backend nil
               :object-id "object-identity-canary"
               :provider-kind :provider-identity-canary
               :cleanup! (fn [] nil)})))))

(defn- worker-contract-checks! []
  (let [configuration #'throughput/matched-configuration
        local (configuration :local-posix 512)
        aws (configuration :aws-s3 512)
        schedule [:batch-size :batches :warmup-batches :trials :question-mark? :modes]]
    (check "matched local and AWS use the same logical schedule"
           (select-keys local schedule) (select-keys aws schedule))
    (check "matched sweep reuses all four existing batch sizes"
           [512 1000 5000 10000]
           (mapv #(get (configuration :local-posix %) :batch-size) [512 1000 5000 10000]))
    (check "matched selector does not claim crash-safe admission ACK"
           :jdbc-return-not-crash-safe-ack
           (:admission @#'throughput/measurement-boundaries))
    (doseq [[provider size] [[:unknown 512] [:local-posix 7]]]
      (check "unknown matched provider or batch fails closed"
             :jdbc.chdb-durable-throughput/invalid-matched-configuration
             (rejected-type #(configuration provider size)))))
  (let [options {:batch-size 2 :batches 1 :warmup-batches 0 :trial 1
                 :question-mark? false :encode-included? false}
        descriptor {:root "/tmp/owned-benchmark-contract"
                    :provider-kind :local-posix
                    :object-id "bench-worker-00000000-0000-0000-0000-000000000001"}
        token "00000000-0000-0000-0000-000000000002"
        runtime {:scheme-version "10.4.1"}
        request {:kind :uninstrumented :role :writer :token token
                 :options options :descriptor descriptor :runtime-identity runtime}
        receipt {:schema-version 1 :role :writer :token token :runtime runtime
                 :value {} :inventory (select-keys request [:kind :options :descriptor])}
        require-receipt #'throughput/require-worker-receipt!
        require-scope #'throughput/require-worker-scope!
        require-completion #'throughput/require-worker-completion!
        require-terminal #'throughput/require-worker-terminal!
        child-failure #'throughput/child-failure-evidence
        require-marker #'throughput/require-worker-marker!
        line (str ":durable-bench-worker-complete :writer " token)]
    (check "owned worker positive receipt preserves its exact inventory"
           receipt (require-receipt receipt :writer token request))
    (doseq [[label altered]
            [["cross-role" (assoc receipt :role :reader)]
             ["wrong-trial" (assoc-in receipt [:inventory :options :trial] 2)]
             ["wrong-descriptor" (assoc-in receipt [:inventory :descriptor :root] "/tmp/other")]
             ["wrong-source" (assoc-in receipt [:runtime :scheme-version] "other")]]]
      (check (str "worker rejects " label " receipt")
             :jdbc.chdb-durable-throughput/invalid-worker-receipt
             (rejected-type #(require-receipt altered :writer token request))))
    (check "owned worker request path scope is exact" nil
           (require-scope request "/tmp/owned-benchmark-contract/writer-request.edn"
                          "/tmp/owned-benchmark-contract/writer-result.edn"))
    (check "worker rejects descriptor tampering before backend reconstruction"
           :jdbc.chdb-durable-throughput/invalid-worker-receipt
           (rejected-type #(require-scope (assoc-in request [:descriptor :root] "/tmp/other")
                                         "/tmp/owned-benchmark-contract/writer-request.edn"
                                         "/tmp/owned-benchmark-contract/writer-result.edn")))
    (check "exact worker completion marker is accepted" nil
           (require-marker [line] :writer token))
    (doseq [lines [[] [line line]]]
      (check "missing/duplicate completion is not qualified"
             :jdbc.chdb-durable-throughput/invalid-worker-receipt
             (rejected-type #(require-marker lines :writer token))))
    (check "ordinary settled exit zero permits the next worker" nil
           (require-completion {:exit 0} {:exit 0}))
    (doseq [[initial settled] [[nil nil] [nil {:exit 0}] [{:exit 7} {:exit 7}]
                              [{:exit 7} {:exit 0}]]]
      (check "timeout/unconfirmed/nonzero primary cannot qualify a later receipt"
             :jdbc.chdb-durable-throughput/worker-failed
             (rejected-type #(require-completion initial settled))))
    (check "late zero exit does not replace the failed initial-wait primary"
           {:type :jdbc.chdb-durable-throughput/worker-failed
            :terminal? true :primary :wait-failed}
           (try (require-completion nil {:exit 0}) nil
                (catch Throwable error (ex-data error))))
    (let [receipt-file (java.io.File/createTempFile "throughput-child-failure-" ".edn")
          safe {:schema-version 1 :role :writer :token token :status :failed
                :failure {:category :controlled-worker-failure
                          :stage :writer-trial}}
          secret "https://secret.example/endpoint?token=never-report"]
      (try
        (spit receipt-file (str (pr-str safe) "\n"))
        (let [data (try
                     (require-terminal {:exit 7} {:exit 7} receipt-file :writer token)
                     nil
                     (catch Throwable error (ex-data error)))]
          (spit receipt-file
                (str (pr-str (assoc safe :secret secret)) "\n"))
          (check "terminal worker failure retains only its closed writer stage"
                 {:type :jdbc.chdb-durable-throughput/worker-failed
                  :terminal? true :primary :nonzero-exit
                  :failure {:category :controlled-worker-failure
                            :stage :writer-trial}}
                 (select-keys data [:type :terminal? :primary :failure]))
          (check "malformed child evidence is reduced to an unclassified closed stage"
                 {:category :unclassified-child-failure :stage :writer-trial}
                 (child-failure receipt-file :writer token))
          (check "worker failure evidence does not retain a secret canary"
                 false (str/includes? (pr-str data) secret)))
        (finally (.delete receipt-file))))
    (with-redefs [clojure.core/slurp (fn [_] (str (pr-str receipt) "\n" (pr-str receipt)))]
      (check "duplicate result forms cannot hide behind one stdout marker"
             :jdbc.chdb-durable-throughput/invalid-worker-receipt
             (rejected-type #( #'throughput/read-owned-edn! :controlled-path))))
    (check "nonserializable custom provider remains explicitly unsupported"
           :jdbc.chdb-durable-throughput/nonserializable-provider
           (rejected-type #( #'throughput/provider-descriptor!
                             {:backend-context! (fn [_] nil)} "/tmp/owned")))))

(defn- worker-failure-receipt-checks! []
  ;; Mutation-level child contract: the writer branch may fail anywhere in the
  ;; actual Durable trial, but must rethrow that same throwable after placing
  ;; only the closed, role-derived stage in its receipt.
  (let [root (java.io.File/createTempFile "throughput-writer-failure-" "")
        _ (.delete root)
        _ (.mkdirs root)
        token "00000000-0000-0000-0000-000000000003"
        request-file (java.io.File. root "writer-request.edn")
        result-file (java.io.File. root "writer-result.edn")
        descriptor {:root (.getAbsolutePath root)
                    :provider-kind :local-posix
                    :object-id "bench-worker-00000000-0000-0000-0000-000000000004"}
        runtime {:scheme-version "10.4.1"}
        request {:kind :uninstrumented :role :writer :token token
                 :options {:batch-size 2 :batches 1 :warmup-batches 0 :trial 1
                           :question-mark? false :encode-included? false}
                 :descriptor descriptor :runtime-identity runtime}
        terminal (ex-info "writer secret http://private.invalid/never-report"
                          {:endpoint "private.invalid" :sql "INSERT secret"})]
    (try
      (spit request-file (str (pr-str request) "\n"))
      (let [caught
            (with-redefs-fn
              {#'throughput/runtime-metadata (fn [] runtime)
               #'throughput/reconstruct-context! (fn [_ _] {:test-context true})
               #'throughput/durable-uninstrumented-trial (fn [_] (throw terminal))}
              (fn []
                (try
                  (#'throughput/worker-main! (.getAbsolutePath request-file)
                                             (.getAbsolutePath result-file))
                  nil
                  (catch Throwable error error))))
            evidence (#'throughput/child-failure-evidence result-file :writer token)
            retained (slurp result-file)]
        (check "writer child rethrows its exact original throwable"
               true (identical? terminal caught))
        (check "writer child receipt reports only the closed writer trial stage"
               {:category :controlled-worker-failure :stage :writer-trial}
               evidence)
        (check "writer child receipt excludes throwable message and exception data"
               false
               (or (str/includes? retained "private.invalid")
                   (str/includes? retained "INSERT secret"))))
      (finally
        (.delete request-file)
        (.delete result-file)
        (.delete root)))))

(defn- orchestration-contract-checks! []
  ;; Real orchestration, mocked native subprocess only. Keep this unique owned
  ;; persistent fixture and its child directories; never broad-delete evidence.
  (let [parent (java.io.File. "target/profiles/worker-orchestration-contracts")
        _ (.mkdirs parent)
        owned (java.io.File/createTempFile "orchestration-" "" parent)
        _ (when-not (and (.delete owned) (.mkdirs owned))
            (throw (ex-info "cannot create orchestration fixture" {})))
        options {:batch-size 2 :batches 1 :warmup-batches 0 :trial 1
                 :question-mark? false :encode-included? false}
        calls (atom [])
        handoff {:result {:batches 1 :ingest-ms 1} :expected {:n 2}}
        runtime {:scheme-version "10.4.1"}
        shared {#'throughput/persistent-evidence-root! (fn [] owned)
                #'throughput/runtime-metadata (fn [] runtime)}]
    (with-redefs-fn
      (assoc shared #'throughput/run-worker!
             (fn [root role request]
               (swap! calls conj {:root (.getAbsolutePath root) :role role :request request})
               {:runtime runtime :value (case role :writer handoff
                                              :reader {:recovery {:result {:n 2}}})}))
      (fn []
        (#'throughput/owned-trial! :uninstrumented options)
        (check "actual owned-trial launches writer then independent reader"
               [:writer :reader] (mapv :role @calls))
        (let [[writer reader] @calls]
          (check "actual writer and reader retain one parent-owned store descriptor"
                 (get-in writer [:request :descriptor]) (get-in reader [:request :descriptor]))
          (check "actual writer and reader retain the same bounded options"
                 options (get-in reader [:request :options]))
          (check "actual reader receives the exact writer handoff"
                 handoff (get-in reader [:request :handoff]))
          (check "actual child roots are identical owned scope"
                 (:root writer) (:root reader)))))
    (doseq [failure [:nonzero :unknown]]
      (reset! calls [])
      (with-redefs-fn
        (assoc shared #'throughput/run-worker!
               (fn [_ role _]
                 (swap! calls conj role)
                 (#'throughput/require-worker-completion!
                  (when (= failure :nonzero) {:exit 7})
                  (when (= failure :nonzero) {:exit 7}))))
        (fn []
          (check "actual multi-trial orchestration preserves failed/unknown writer cause"
                 :jdbc.chdb-durable-throughput/worker-failed
                 (rejected-type #( #'throughput/run-config
                                   (assoc options :trials 2
                                          :modes [:durable-preencoded :durable-encode-included]))))
          (check "actual failure prevents reader and all later trial workers"
                 [:writer] @calls))))))

(defn -main [& _]
  (reset! failures 0)
  (run-checks!)
  (worker-contract-checks!)
  (worker-failure-receipt-checks!)
  (orchestration-contract-checks!)
  (when-not (zero? @failures)
    (throw (ex-info (str @failures " throughput checks failed")
                    {:failures @failures}))))
