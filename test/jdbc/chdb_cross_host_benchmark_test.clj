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
        count-newlines (private-fn 'count-newlines)
        selected-record (private-fn 'selected-record)
        strict-decode (private-fn 'strict-decode)
        measure-raw-phase (private-fn 'measure-raw-phase)]
    (check "raw scanner counts LF-delimited records" 2
           (count-newlines bytes))
    (check "record selection preserves multibyte UTF-8"
           "{\"sql\":\"β\"}"
           (strict-decode (selected-record bytes 1)))
    (check "record selection uses one-based ordinals"
           "{\"sql\":\"two\"}"
           (strict-decode (selected-record bytes 2)))
    (check "unterminated WAL fails closed"
           :jdbc.chdb-cross-host-wal/invalid-benchmark
           (error-type #(selected-record (.getBytes "{}" "UTF-8") 1)))
    (let [calls (atom 0)
          result (measure-raw-phase
                  bytes 2 {:snapshot nil}
                  (fn [input]
                    (swap! calls inc)
                    (count-newlines input)))]
      (check "corrected raw phase scans the segment exactly once" 1 @calls)
      (check "single raw scan does not publish unsupported percentiles"
             {:count 1 :p50-supported? false :p95-supported? false
              :p99-supported? false}
             (select-keys (:latency result)
                          [:count :p50-supported? :p95-supported?
                           :p99-supported?]))
      (check "single raw scan does not retain its measured value or payload"
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
      (measure-raw-phase
       bytes 2 {:snapshot nil}
       (fn [input]
         (swap! calls inc)
         (count-newlines input))
       legacy-measurer)
      (check "legacy whole-segment mutant performs 27 scans at 5/20"
             27 @calls)))
  (let [append-checkpoint! (private-fn 'append-checkpoint!)
        file (java.io.File/createTempFile "cross-host-checkpoint-" ".edn")]
    (try
      (spit file "")
      (append-checkpoint! (.getPath file)
                          {:event :phase :phase :raw-lf-scan
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
  (check "missing JFR configuration is an optional not-run capability"
         nil
         ((private-fn 'start-jvm-profile)))
  (check "failed optional JFR state does not fail measured results"
         {:status :failed :stage :start}
         ((private-fn 'stop-jvm-profile)
          {:status :failed :stage :start}))
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
