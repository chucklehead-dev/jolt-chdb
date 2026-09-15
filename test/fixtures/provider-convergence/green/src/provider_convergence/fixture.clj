(ns provider-convergence.fixture
  (:require [jdbc.chdb.durable :as durable]
            [jdbc.chdb.durable.local-posix :as local-posix]
            [jdbc.core :as jdbc]
            [samizdat.store.db :as samizdat-db])
  (:import [java.nio.file Files Paths]
           [java.nio.file.attribute FileAttribute]))

(defn- assert= [label expected actual]
  (when-not (= expected actual)
    (throw (ex-info (str label " did not reconcile")
                    {:expected expected :actual actual}))))

(defn -main [& [mode]]
  (let [root-value (System/getenv "PROVIDER_CONVERGENCE_RUNTIME_ROOT")
        _ (when-not (seq root-value)
            (throw (ex-info "fixture requires its run-scoped root" {})))
        root (Paths/get root-value (make-array String 0))
        _ (Files/createDirectories root (make-array FileAttribute 0))
        sqlite-path (str (.resolve root "samizdat.sqlite"))
        object-root (str (.resolve root "durable-objects"))
        storage (local-posix/local-backend object-root)
        sqlite (samizdat-db/connect sqlite-path)]
    (try
      ;; Exercise Samizdat's authoritative-state adapter, including its WAL and
      ;; busy-timeout connection setup plus bound-value execution.
      (when (= mode "write")
        (samizdat-db/execute!
         sqlite "CREATE TABLE authoritative_state (id INTEGER PRIMARY KEY, value TEXT)")
        (samizdat-db/execute!
         sqlite ["INSERT INTO authoritative_state (id, value) VALUES (?, ?)"
                 7 "accepted"]))
      (assert= "Samizdat SQLite state"
               {:id 7 :value "accepted"}
               (samizdat-db/fetch-one
                sqlite "SELECT id, value FROM authoritative_state"))

      ;; Each Durable lifetime owns a fresh Jolt process. Both the writer and
      ;; snapshot process keep Samizdat's SQLite connection live, preserving
      ;; provider coexistence without reinitializing chDB.
      (case mode
        "write"
        (with-open [writer
                    (jdbc/connection
                     (durable/writer-dbspec
                      {:namespace-backend storage
                       :object-id "telemetry"
                       :owner "provider-convergence-fixture"
                       :instance "provider-convergence-writer"
                       :database "default"}))]
          (jdbc/execute!
           writer
           "CREATE TABLE telemetry (id UInt64, body String) ENGINE = MergeTree ORDER BY id")
          (jdbc/execute! writer "INSERT INTO telemetry VALUES (1, 'observed')")
          (assert= "Durable writer query"
                   {:n 1 :body "observed"}
                   (jdbc/fetch-one writer
                                   "SELECT count() AS n, any(body) AS body FROM telemetry"))
          (assert= "Durable flush" :committed (:status (durable/flush! writer))))

        "read"
        (with-open [snapshot
                    (jdbc/connection
                     (durable/snapshot-dbspec
                      {:namespace-backend storage :object-id "telemetry"}))]
          (assert= "Durable snapshot"
                   {:n 1 :body "observed"}
                   (jdbc/fetch-one snapshot
                                   "SELECT count() AS n, any(body) AS body FROM telemetry")))

        (throw (ex-info "fixture mode must be write or read" {})))

      (println "provider convergence fixture passed")
      (finally
        (samizdat-db/close sqlite)))))
