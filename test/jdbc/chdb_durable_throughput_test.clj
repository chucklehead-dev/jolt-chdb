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
