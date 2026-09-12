(ns jdbc.chdb-durable-throughput-test
  (:require [jdbc.chdb-durable-throughput :as throughput]))

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
        clean-runtime {:jolt-version "jolt v0.8.6-8-gcf0b6928"
                       :jolt-source-sha
                       "cf0b69284b17d390449517c29fd418ef317d34cb"
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

(defn -main [& _]
  (reset! failures 0)
  (run-checks!)
  (when-not (zero? @failures)
    (throw (ex-info (str @failures " throughput checks failed")
                    {:failures @failures}))))
