(ns provider-convergence.fixture
  (:require [jdbc.chdb.durable :as durable]
            [jdbc.chdb.durable.local-posix :as local-posix]
            [jdbc.core :as jdbc]
            [samizdat.store.db :as samizdat-db])
  (:import [java.io File]
           [java.nio.file Files]
           [java.nio.file.attribute FileAttribute]))

(defn- delete-tree! [^File file]
  (when (.isDirectory file)
    (doseq [child (.listFiles file)]
      (delete-tree! child)))
  (when (and (.exists file) (not (.delete file)))
    (throw (ex-info "fixture could not remove temporary path" {}))))

(defn- assert= [label expected actual]
  (when-not (= expected actual)
    (throw (ex-info (str label " did not reconcile")
                    {:expected expected :actual actual}))))

(defn -main [& _]
  (let [root (Files/createTempDirectory
              "jolt-chdb-provider-convergence-"
              (into-array FileAttribute []))
        root-file (.toFile root)
        sqlite-path (str (.resolve root "samizdat.sqlite"))
        object-root (str (.resolve root "durable-objects"))
        storage (local-posix/local-backend object-root)
        sqlite (samizdat-db/connect sqlite-path)]
    (try
      ;; Exercise Samizdat's authoritative-state adapter, including its WAL and
      ;; busy-timeout connection setup plus bound-value execution.
      (samizdat-db/execute!
       sqlite "CREATE TABLE authoritative_state (id INTEGER PRIMARY KEY, value TEXT)")
      (samizdat-db/execute!
       sqlite ["INSERT INTO authoritative_state (id, value) VALUES (?, ?)"
               7 "accepted"])
      (assert= "Samizdat SQLite state"
               {:id 7 :value "accepted"}
               (samizdat-db/fetch-one
                sqlite "SELECT id, value FROM authoritative_state"))

      ;; Exercise the production Durable JDBC writer, explicit publication,
      ;; logical close, immutable restore, and snapshot close in the same Jolt
      ;; process as Samizdat's live SQLite ownership domain.
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

      (with-open [snapshot
                  (jdbc/connection
                   (durable/snapshot-dbspec
                    {:namespace-backend storage :object-id "telemetry"}))]
        (assert= "Durable snapshot"
                 {:n 1 :body "observed"}
                 (jdbc/fetch-one snapshot
                                 "SELECT count() AS n, any(body) AS body FROM telemetry")))

      (println "provider convergence fixture passed")
      (finally
        (samizdat-db/close sqlite)
        (delete-tree! root-file)))))
