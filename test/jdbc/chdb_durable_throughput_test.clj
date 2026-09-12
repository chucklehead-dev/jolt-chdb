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
    (check "one hundred samples can qualify p99"
           true
           (:p99-qualification? (latency-summary (range 100))))
    (check "clean complete qualification provenance is accepted"
           nil
           (provenance! :qualification clean-runtime))
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
            :maximum-input-batch [:maximum-batch-statement-bytes]}
           (:wal-size-semantics report-contract))
    (check "WAL output builder does not conflate total growth with batch maxima"
           {:wal-growth {:total-bytes 900 :records 3}
            :maximum-batch-payload-bytes 250
            :maximum-batch-statement-bytes 300}
           (wal-size-observation
            {:pending-wal-bytes 900 :pending-statements 3}
            {:maximum-batch-payload-bytes 250
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
           (rejected-type #(profile-configs :diagnostic)))))

(defn -main [& _]
  (reset! failures 0)
  (run-checks!)
  (when-not (zero? @failures)
    (throw (ex-info (str @failures " throughput checks failed")
                    {:failures @failures}))))
