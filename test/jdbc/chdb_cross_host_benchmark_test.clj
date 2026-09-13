(ns jdbc.chdb-cross-host-benchmark-test
  (:require [clojure.edn :as edn]
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
        strict-decode (private-fn 'strict-decode)]
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
           (error-type #(selected-record (.getBytes "{}" "UTF-8") 1))))
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
        pin (get-in deps [:deps 'org.clojure/data.json :git/sha])]
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
                     :git-sha pin)))))
  (when (pos? @failures)
    (throw (ex-info "cross-host benchmark contract failed"
                    {:failures @failures})))
  (println "cross-host benchmark contract passed"))
