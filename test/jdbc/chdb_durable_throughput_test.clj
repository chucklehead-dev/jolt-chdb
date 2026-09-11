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
        provenance! #'throughput/require-qualification-provenance!
        latency-summary #'throughput/latency-summary
        clean-runtime {:jolt-version "jolt v0.8.6"
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
    (check "dirty qualification provenance fails closed"
           :jdbc.chdb-durable-throughput/dirty-provenance
           (rejected-type
            #(provenance! :scale
                          (assoc-in clean-runtime [:git :status] "dirty"))))
    (check "unknown profile fails before benchmark work"
           :jdbc.chdb-durable-throughput/unknown-profile
           (rejected-type #(parse-profile! "unknown")))
    (check "direct trial-runner misuse also fails before benchmark work"
           :jdbc.chdb-durable-throughput/unsupported-run-profile
           (rejected-type #(profile-configs :diagnostic)))))

(defn -main [& _]
  (reset! failures 0)
  (run-checks!)
  (when-not (zero? @failures)
    (throw (ex-info (str @failures " throughput checks failed")
                    {:failures @failures}))))
