(ns jdbc.chdb-encoded-threadstatus-probe
  (:require [db.jdbc]
            [db.export :as export]
            [jdbc.chdb :as chdb]
            [jdbc.core :as jdbc]))

(defn- expected-error! [f]
  (try
    (f)
    (throw (ex-info "encoded ThreadStatus probe expected a bounded query error" {}))
    (catch Throwable error
      (when (= "encoded ThreadStatus probe expected a bounded query error"
               (ex-message error))
        (throw error)))))

(defn -main [& _]
  ;; Exercise success, native bounded failure plus stale-format recovery, normal
  ;; JDBC reuse, result destruction, and connection close in one native process.
  (dotimes [iteration 24]
    (with-open [conn (jdbc/connection "chdb::memory:")]
      (doseq [format [:arrow :parquet]]
        (let [query-bytes (if (even? iteration)
                            chdb/query-bytes export/query-bytes)
              result (query-bytes
                      conn ["select ? as iteration, ? as label"
                            iteration (str "probe-" iteration)]
                      {:format format :max-rows 8 :max-bytes 1048576})]
          (when-not (pos? (:byte-count result))
            (throw (ex-info "encoded ThreadStatus probe returned empty bytes"
                            {:format format :iteration iteration})))
          (expected-error!
           #(query-bytes conn "select number from numbers(2)"
                         {:format format :max-rows 1 :max-bytes 1048576}))
          (when-not (= iteration
                       (:iteration
                        (jdbc/fetch-one conn ["select ? as iteration" iteration])))
            (throw (ex-info "ordinary JDBC failed after encoded query recovery"
                            {:format format :iteration iteration})))))))
  (println "PASS: bounded encoded query ThreadStatus probe"))
