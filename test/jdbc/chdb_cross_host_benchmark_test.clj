(ns jdbc.chdb-cross-host-benchmark-test
  (:require [clojure.edn :as edn]
            [clojure.string :as str]
            [jdbc.chdb-cross-host-report :as report]
            [jdbc.chdb-cross-host-wal]))

(def failures (atom 0))

(defn- check [label expected actual]
  (if (= expected actual)
    (println "  ok  " label)
    (do (swap! failures inc)
        (println "  FAIL" label "expected" (pr-str expected)
                 "got" (pr-str actual)))))

(defn- private-fn [name]
  (deref (ns-resolve 'jdbc.chdb-cross-host-wal name)))

(defn- error-type [f]
  (try (f) nil (catch Throwable error (:type (ex-data error)))))

(defn -main [& _]
  (println "cross-host benchmark report contract")
  (check "nearest-rank p50" 2 (report/percentile [4 1 3 2] 0.5))
  (check "nearest-rank p95" 4 (report/percentile [4 1 3 2] 0.95))
  (check "distribution totals and support"
         {:count 4 :total-ns 10 :p50-ns 2 :p95-ns 4 :p99-ns 4 :max-ns 4
          :p50-supported? true :p95-supported? false :p99-supported? false
          :samples-ns [4 1 3 2]}
         (report/distribution [4 1 3 2]))
  (check "canonical rendering is insertion-order independent"
         (report/render {:z 1 :a {:d 4 :c 3}})
         (report/render (array-map :a (array-map :c 3 :d 4) :z 1)))
  (let [error (try (report/distribution []) nil
                   (catch Throwable value value))]
    (check "empty samples fail closed"
           :jdbc.chdb-cross-host-report/invalid-report
           (:type (ex-data error))))
  (let [bytes (.getBytes "{\"sql\":\"β\"}\n{\"sql\":\"two\"}\n" "UTF-8")
        scan-record-boundaries (private-fn 'scan-record-boundaries)
        strict-decode (private-fn 'strict-decode)
        sha256-bytes (private-fn 'sha256-bytes)
        require-digest! (private-fn 'require-digest!)
        measure-boundary-phase (private-fn 'measure-boundary-phase)
        copy-record (fn [{:keys [record-start record-end-exclusive]}]
                      (java.util.Arrays/copyOfRange
                       bytes record-start record-end-exclusive))
        first-boundaries (scan-record-boundaries bytes 1)
        second-boundaries (scan-record-boundaries bytes 2)
        first-record (copy-record first-boundaries)
        segment-sha (sha256-bytes bytes)
        record-sha (sha256-bytes first-record)]
    (check "boundary scanner counts LF-delimited records" 2
           (:record-count first-boundaries))
    (check "record selection preserves multibyte UTF-8"
           "{\"sql\":\"β\"}"
           (strict-decode (copy-record first-boundaries)))
    (check "record selection uses one-based ordinals"
           "{\"sql\":\"two\"}"
           (strict-decode (copy-record second-boundaries)))
    (check "full segment and selected record have distinct digest scopes"
           false (= segment-sha record-sha))
    (check "full segment digest validates only full segment bytes"
           segment-sha (require-digest! "full WAL segment"
                                        segment-sha (sha256-bytes bytes)))
    (check "selected-record digest excludes its terminating LF"
           record-sha
           (require-digest! "selected WAL record (excluding LF)"
                            record-sha (sha256-bytes first-record)))
    (check "swapping selected-record digest into segment scope fails closed"
           :jdbc.chdb-cross-host-wal/invalid-benchmark
           (error-type #(require-digest! "full WAL segment"
                                         record-sha segment-sha)))
    (check "swapping segment digest into selected-record scope fails closed"
           :jdbc.chdb-cross-host-wal/invalid-benchmark
           (error-type #(require-digest!
                         "selected WAL record (excluding LF)"
                         segment-sha record-sha)))
    (check "unterminated WAL fails closed"
           :jdbc.chdb-cross-host-wal/invalid-benchmark
           (error-type #(scan-record-boundaries (.getBytes "{}" "UTF-8") 1)))
    (let [calls (atom 0)
          {:keys [value result]}
          (measure-boundary-phase
                  bytes 2 2 {:snapshot nil}
                  (fn [input ordinal]
                    (swap! calls inc)
                    (scan-record-boundaries input ordinal)))]
      (check "combined boundary phase scans the segment exactly once" 1 @calls)
      (check "combined boundary phase returns target offsets"
             second-boundaries value)
      (check "single boundary scan does not publish unsupported percentiles"
             {:count 1 :p50-supported? false :p95-supported? false
              :p99-supported? false}
             (select-keys (:latency result)
                          [:count :p50-supported? :p95-supported?
                           :p99-supported?]))
      (check "single boundary scan result retains offsets but no payload"
             false
             (or (contains? result :value)
                 (str/includes? (pr-str result) "{\"sql\""))))
    (let [calls (atom 0)
          measure-once (private-fn 'measure-once)
          ;; Causal reconstruction of the rejected harness: two setup calls,
          ;; five warmups, and twenty measured calls all invoke the same
          ;; whole-segment scanner before returning the final measurement.
          legacy-measurer
          (fn [metrics f]
            (dotimes [_ 26] (f))
            (measure-once metrics f))]
      (measure-boundary-phase
       bytes 2 2 {:snapshot nil}
       (fn [input ordinal]
         (swap! calls inc)
         (scan-record-boundaries input ordinal))
       legacy-measurer)
      (check "legacy whole-segment mutant performs 27 scans at 5/20"
             27 @calls))
    (when (= :jvm (keyword (or (System/getenv "BENCH_RUNTIME") "jvm")))
      (let [primitive
            (requiring-resolve
             'jdbc.chdb-cross-host-jvm-scan/scan-record-boundaries)
            enable-if-supported!
            (requiring-resolve
             'jdbc.chdb-cross-host-jvm-metrics/enable-if-supported!)
            enable-calls (atom 0)]
        (check "JVM primitive control matches shared boundary scanner"
               second-boundaries
               (primitive bytes 2))
        (check "unsupported allocation counters remain unsupported"
               false
               (enable-if-supported!
                false
                #(throw (ex-info "unsupported capability was queried" {}))
                (fn [_] (swap! enable-calls inc))))
        (check "unsupported allocation counters are never enabled"
               0 @enable-calls))))
  (let [append-checkpoint! (private-fn 'append-checkpoint!)
        file (java.io.File/createTempFile "cross-host-checkpoint-" ".edn")]
    (try
      (spit file "")
      (append-checkpoint! (.getPath file)
                          {:event :phase :phase :record-boundary-scan
                           :result {:status :complete :samples 1}})
      (try
        (throw (ex-info "injected late failure" {:type ::injected}))
        (catch Throwable throwable
          (append-checkpoint! (.getPath file)
                              {:event :host :status :failed
                               :error {:type (:type (ex-data throwable))}})))
      (let [events (mapv edn/read-string
                         (remove str/blank?
                                 (str/split-lines (slurp file))))]
        (check "completed phase survives an injected late failure"
               [:complete :failed]
               [(get-in (first events) [:result :status])
               (:status (second events))]))
      (finally (.delete file))))
  (let [load-reused-boundaries (private-fn 'load-reused-boundaries)
        file (java.io.File/createTempFile "cross-host-boundaries-" ".edn")
        source {:schema-version report/schema-version
                :status :complete
                :host {:runtime :jvm}
                :libraries {:json-parser {:implementation :casselc-data-json}}
                :fixture {:segment-sha256 "fixture-sha" :bytes 12 :record-count 2
                          :record-ordinal 1 :record-start 0
                          :record-end-exclusive 5}}]
    (try
      (spit file (report/render source))
      (check "secondary JVM row reuses only bounded verified offsets"
             {:record-count 2 :record-start 0 :record-end-exclusive 5}
             (dissoc (load-reused-boundaries
                      (.getPath file) "fixture-sha" 12 2 1)
                     :source-report-sha256))
      (check "secondary JVM row rejects mismatched fixture provenance"
             :jdbc.chdb-cross-host-wal/invalid-benchmark
             (error-type #(load-reused-boundaries
                           (.getPath file) "wrong-sha" 12 2 1)))
      (spit file (report/render (assoc source :schema-version 3)))
      (check "secondary JVM row rejects an older report schema"
             :jdbc.chdb-cross-host-wal/invalid-benchmark
             (error-type #(load-reused-boundaries
                           (.getPath file) "fixture-sha" 12 2 1)))
      (finally (.delete file))))
  (check "missing JFR configuration is an optional not-run capability"
         nil
         ((private-fn 'start-jvm-profile)))
  (check "failed optional JFR state does not fail measured results"
         {:status :failed :stage :start}
         ((private-fn 'stop-jvm-profile)
          {:status :failed :stage :start}))
  (when (and (= :jvm (keyword (or (System/getenv "BENCH_RUNTIME") "jvm")))
             (System/getProperty "java.vm.name"))
    (let [start! (requiring-resolve 'jdbc.chdb-cross-host-jvm-profile/start!)
          stop! (requiring-resolve 'jdbc.chdb-cross-host-jvm-profile/stop!)
          file (java.io.File/createTempFile "cross-host-profile-" ".jfr")
          recording (start! (.getPath file))]
      (try
        (check "JFR disables ambient environment and process payload events"
               {"jdk.InitialEnvironmentVariable#enabled" "false"
                "jdk.InitialSystemProperty#enabled" "false"
                "jdk.SystemProcess#enabled" "false"}
               (select-keys
                (.getSettings recording)
                ["jdk.InitialEnvironmentVariable#enabled"
                 "jdk.InitialSystemProperty#enabled"
                 "jdk.SystemProcess#enabled"]))
        (finally
          (stop! recording)
          (.delete file)))))
  (let [runtime (keyword (or (System/getenv "BENCH_RUNTIME") "jvm"))
        profile (keyword (or (System/getenv "BENCH_JSON_PARSER")
                             (if (= runtime :babashka)
                               "babashka-bundled-cheshire"
                               "casselc-data-json")))
        parser ((private-fn 'json-parser)
                runtime
                (get-in (edn/read-string (slurp "deps.edn"))
                        [:deps 'org.clojure/data.json :git/sha]))]
    (if (= profile :babashka-bundled-cheshire)
      (do
        (check "Babashka uses its bundled Cheshire parser"
               :babashka-bundled-cheshire
               (get-in parser [:identity :implementation]))
        (check "Babashka natural parser preserves string keys and SQL"
               "β" (get ((:read-str parser) "{\"sql\":\"β\"}") "sql")))
      (do
        (check "selected parser implementation is exact"
               profile (get-in parser [:identity :implementation]))
        (check "selected parser preserves string keys and SQL"
               "β" (get ((:read-str parser) "{\"sql\":\"β\"}") "sql")))))
  (let [deps (edn/read-string (slurp "deps.edn"))
        pin (get-in deps [:deps 'org.clojure/data.json :git/sha])
        benchmark-pins
        (edn/read-string
         (slurp "resources/jdbc/chdb/cross-host-benchmark.edn"))]
    (check "project keeps one full data.json git pin" true
           (boolean (and (string? pin)
                         (re-matches #"[0-9a-f]{40}" pin))))
    (when (= :casselc-data-json
             (keyword (or (System/getenv "BENCH_JSON_PARSER")
                          "casselc-data-json")))
      (check "loaded data.json resource is the exact project git pin"
             pin
             (:git-sha
              (assoc ((private-fn 'pinned-resource-identity)
                      "clojure/data/json.clj" pin)
                     :git-sha pin))))
    (check "benchmark pins one exact Jolt source commit" true
           (boolean (re-matches #"[0-9a-f]{40}"
                                (get-in benchmark-pins [:jolt :source-sha]))))
    (check "upstream data.json alias matches the benchmark pin"
           (get-in benchmark-pins [:parsers :upstream-data-json :version])
           (get-in deps [:aliases :cross-host-wal-upstream-data-json
                         :override-deps 'org.clojure/data.json :mvn/version]))
    (check "JVM Cheshire alias matches the benchmark pin"
           (get-in benchmark-pins [:parsers :jvm-cheshire :version])
           (get-in deps [:aliases :cross-host-wal-cheshire
                         :extra-deps 'cheshire/cheshire :mvn/version])))
  (when (pos? @failures)
    (throw (ex-info "cross-host benchmark contract failed"
                    {:failures @failures})))
  (println "cross-host benchmark contract passed"))
