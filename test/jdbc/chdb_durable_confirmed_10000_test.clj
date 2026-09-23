(ns jdbc.chdb-durable-confirmed-10000-test
  (:require [jdbc.chdb.durable :as durable]
            [jdbc.chdb.durable.local-posix :as local]
            [jdbc.chdb-durable-confirmed-10000 :as confirmed]
            [jdbc.chdb-durable-throughput :as throughput]
            [jdbc.core :as jdbc]
            [jolt.process :as process])
  (:import [java.io File]))

(defn- check [label expected actual]
  (when-not (= expected actual)
    (throw (ex-info (str "confirmed 10000 contract failed: " label)
                    {:expected expected :actual actual}))))

(defn- native-readback-check! []
  (let [root (File/createTempFile "tiny-confirmed-" "")
        _ (when-not (and (.delete root) (.mkdir root))
            (throw (ex-info "tiny test scratch unavailable" {})))
        store (local/local-backend (str (File. root "objects")))
        object-id (str "tiny-confirmed-" (random-uuid))
        rows (mapv #(#'throughput/log-row % false) (range 2))
        sql (:sql (#'throughput/encode-batch-production rows))
        expected (#'throughput/accumulate-expected-batch
                  @#'throughput/empty-expected-aggregates rows false)
        spec {:namespace-backend store :object-id object-id
              :scratch-parent (.getAbsolutePath root)}]
    (try
      (let [writer-digest
            (with-open [writer (jdbc/connection
                               (durable/writer-dbspec
                                (assoc spec :owner "tiny-confirmed-check"
                                       :database "benchmark")))]
              (jdbc/execute! writer @#'throughput/logs-ddl)
              (durable/flush! writer)
              (check "tiny confirmed mutation" :committed
                     (:status (durable/execute-and-flush! writer sql)))
              (#'throughput/verify-counts! writer expected :tiny-confirmed-writer)
              (#'confirmed/fingerprint! writer))
            wrapper (System/getenv "JOLT_WRAPPER")
            executable (System/getenv "BENCH_JOLT_BIN")
            stdout (File. root "reader.stdout")
            stderr (File. root "reader.stderr")
            _ (when-not (and wrapper executable)
                (throw (ex-info "tiny test child runtime missing" {})))
            child (process/process
                   [wrapper executable "-M:durable-confirmed-10000-test"
                    "--native-reader" (.getAbsolutePath root) object-id
                    writer-digest]
                   {:out stdout :err stderr})
            terminal (deref child 60000 ::timeout)]
        (when (= terminal ::timeout)
          (process/destroy-tree child))
        (check "fresh snapshot child exit" 0 (:exit terminal))
        (check "fresh snapshot child marker"
               "tiny confirmed fresh reader passed\n" (slurp stdout)))
      (finally
        (#'throughput/delete-tree! root)))))

(defn- native-reader-check! [root object-id expected-digest]
  (let [store (local/local-backend (str (File. root "objects")))
        rows (mapv #(#'throughput/log-row % false) (range 2))
        expected (#'throughput/accumulate-expected-batch
                  @#'throughput/empty-expected-aggregates rows false)]
    (with-open [reader (jdbc/connection
                       (durable/snapshot-dbspec
                        {:namespace-backend store :object-id object-id
                         :scratch-parent root}))]
      (#'throughput/verify-counts! reader expected :tiny-confirmed-reader)
      (check "fresh snapshot full-row fingerprint" expected-digest
             (#'confirmed/fingerprint! reader)))))

(defn -main [& args]
  (when (= "--native-reader" (first args))
    (apply native-reader-check! (rest args))
    (println "tiny confirmed fresh reader passed")
    (System/exit 0))
  (let [valid? #'confirmed/valid-confirmed-prefix?
        merge-expected #'confirmed/merge-expected
        resource-totals #'confirmed/resource-totals
        bound! #'confirmed/check-resource-bounds!
        left {:n 10000 :flags 100 :severity_sum 200
              :body_bytes 300 :question_bodies 0
              :min_trace "bbb" :max_trace "ddd"
              :min_span "222" :max_span "444"}
        right {:n 10000 :flags 101 :severity_sum 201
               :body_bytes 301 :question_bodies 0
               :min_trace "aaa" :max_trace "eee"
               :min_span "111" :max_span "555"}
        provider {:logical {:flush {:put-file-if-absent {:calls 1}
                                    :put-bytes-if-absent {:calls 1}
                                    :replace-if-match {:calls 1}}}
                  :transport {:flush {:get {:calls 2 :request-body-bytes 0}
                                      :put-file-if-absent
                                      {:calls 1 :request-body-bytes 300}}}}]
    (check "warmup calls" 3 @#'confirmed/warmups)
    (check "measured calls" 100 @#'confirmed/measured)
    (check "full readback rows" 1030000 @#'confirmed/expected-rows)
    (check "exact unique prefix" true
           (boolean (valid? "123" "2" "ci/jolt-chdb/123-2/confirmed-10000")))
    (doseq [prefix ["ci/jolt-chdb/123-2/throughput"
                    "ci/jolt-chdb/123-2/confirmed-10000/extra"
                    "ci/jolt-chdb/123-3/confirmed-10000"]]
      (check "reject another prefix" false (boolean (valid? "123" "2" prefix))))
    (check "reject malformed run identity" false
           (boolean (valid? "0" "2" "ci/jolt-chdb/0-2/confirmed-10000")))
    (check "aggregate extrema and sums"
           {:n 20000 :flags 201 :severity_sum 401 :body_bytes 601
            :question_bodies 0 :min_trace "aaa" :max_trace "eee"
            :min_span "111" :max_span "555"}
           (merge-expected left right))
    (check "provider request and object-write counts"
           {:requests 3 :transport-body-bytes 300 :object-write-attempts 3}
           (resource-totals provider))
    (check "in-budget provider" (resource-totals provider)
           (bound! provider 300))
    (check "oversized cumulative SQL fails closed"
           :jdbc.chdb-durable-confirmed-10000/resource-budget
           (try (bound! provider (inc (* 2 1024 1024 1024)))
                nil
                (catch Throwable error (:type (ex-data error)))))
    (when (= ["--native"] (vec args))
      (native-readback-check!))
    (println "confirmed 10000 source contracts passed")))
