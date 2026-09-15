(ns native-process-typed-exit.fixture
  (:require [db.jdbc]
            [jdbc.chdb.durable :as durable]
            [jdbc.chdb.durable.local-posix :as local-posix]
            [jdbc.chdb.native :as native]
            [jdbc.core :as jdbc]
            [jdbc.proto :as jdbc-proto]
            [otel.exporter.chdb :as chdb-export]
            [otel.exporter.chdb.attribute-manifest :as manifest]
            [otel.exporter.chdb.attribute-registry-installer :as installer]
            [otel.exporter.chdb.explorer :as explorer]
            [otel.exporter.chdb.schema :as schema]
            [otel.sdk.export :as export]))

(def ^:private timestamp 1700000000000000000)
(def ^:private typed-key "exit.confirmed")

(defn- compiled-manifest []
  (manifest/compile-manifest
   {:dataset-id "jolt-chdb-issue111"
    :application-id "jolt-chdb-issue111"
    :lineage "typed-process-exit-v1"
    :version 1
    :fragments
    [{:schema manifest/reviewed-fragment-schema
      :authority :runtime-reviewed
      :source "jolt-chdb-native-process-exit-regression"
      :entries [{:signal :spans
                 :table "otel_traces"
                 :location :span-attributes
                 :key typed-key
                 :type :boolean}]}]}))

(defn- observed-columns [connection]
  [{:columns (into {} (map (juxt :name :type))
                   (jdbc/fetch connection "describe table otel_traces"))
    :signal :spans
    :table "otel_traces"}])

(defn- span []
  {:name "typed-process-exit"
   :kind :internal
   :start-time-unix-nano timestamp
   :end-time-unix-nano (inc timestamp)
   :span-context {:trace-id "11111111111111111111111111111111"
                  :span-id "2222222222222222"}
   :resource {:attributes {}}
   :scope {:name "jolt-chdb-issue111"}
   :attributes {typed-key true}
   :events []
   :links []
   :status {:code :unset}})

(defn- assert! [truth message data]
  (when-not truth (throw (ex-info message data))))

(defn- observe-anchor-exit! []
  ;; Exact runtime oracle: casselc/jolt 2d39e854, concurrency.ss lines
  ;; 2829-2856. Registration prepends; the once-only runner reverses and invokes
  ;; each body inline with for-each. The driver registers its anchor hook during
  ;; the first open, so this later observer proves that owner close returned
  ;; before host teardown rather than being left to native static finalization.
  (let [storage (native/active-storage)]
    (assert! (= {:phase :exit-closed
                 :references 0
                 :anchored? false}
                (select-keys storage [:phase :references :anchored?]))
             "native anchor was not closed before host teardown"
             {:storage storage})
    (println "PASS typed process-exit anchor closed before host teardown")))

(defn -main [& [root]]
  (assert! (and root (not= "" root)) "fixture root is required" {})
  (let [store (local-posix/local-backend (str root "/store"))
        registry (local-posix/local-backend (str root "/registry"))
        connection
        (jdbc/connection
         (durable/writer-dbspec
          {:backend store
           :owner "jolt-chdb-issue111"
           :instance "typed-process-exit-1"
           :database "default"
           :scratch-parent root
           :lease-ttl-ms 30000}))]
    (.addShutdownHook (Runtime/getRuntime) (Thread. observe-anchor-exit!))
    (schema/ensure-schema! connection)
    (let [installation
          (installer/install-approved!
           registry (compiled-manifest)
           {:target connection
            :observe-columns #(observed-columns connection)
            :execute-ddl! #(jdbc/execute! connection %)})
          descriptors (:descriptor-set installation)
          exporter (chdb-export/exporter
                    {:connection connection
                     :create-schema? false
                     :signals #{:spans}
                     :typed-span-descriptors descriptors})]
      (assert! (export/export-spans! exporter [(span)])
               "typed span export failed"
               {:error (chdb-export/last-error exporter)})
      (let [readback
            (explorer/typed-span-filtered-traces
             connection descriptors
             {:signal :spans
              :attribute-key typed-key
              :operator :eq
              :value true
              :start-unix-nano timestamp
              :end-unix-nano (+ timestamp 10)
              :limit 1
              :max-text-length 64})]
        (assert! (= [["typed-process-exit" true 3]]
                    (mapv (juxt :span-name :attribute-value :typed-status)
                          (:matches readback)))
                 "typed span readback failed"
                 {:readback readback}))
      (assert! (export/shutdown-exporter! exporter)
               "shared exporter shutdown failed" {}))
    (let [checkpoint (durable/checkpoint! connection)]
      (assert! (contains? #{:committed :reconciled} (:status checkpoint))
               "Durable checkpoint failed"
               {:status (:status checkpoint)}))
    (.close connection)
    (assert! (.isClosed (jdbc-proto/connection connection))
             "explicit application close failed" {})
    (println "PASS typed export readback checkpoint explicit close")))
