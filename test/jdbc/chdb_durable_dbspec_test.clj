(ns jdbc.chdb-durable-dbspec-test
  (:require [db.driver :as driver]
            [db.jdbc]
            [hegel.core :as h]
            [hegel.generator :as g]
            [jdbc.chdb.durable :as durable]
            [jdbc.chdb.durable.backend :as backend]
            [jdbc.chdb.durable.time-domain :as time-domain]
            [jdbc.chdb.durable.local-posix :as local]
            [jdbc.chdb.durable.s3 :as s3]
            [jdbc.chdb-durable-open-test-support :as support]
            [jdbc.core :as jdbc])
  (:import [java.nio.file Files Path]
           [java.nio.file.attribute FileAttribute]
           [java.util UUID]))

(def failures (atom 0))

(defn- check [label expected actual]
  (if (= expected actual)
    (println "  ok  " label)
    (do
      (swap! failures inc)
      (println "  FAIL" label "- expected" (pr-str expected)
               "got" (pr-str actual)))))

(defn- rejected [f]
  (try
    (f)
    nil
    (catch Throwable error error)))

(defn- error-type [error]
  (loop [current error]
    (when current
      (or (:type (ex-data current))
          (recur (.getCause current))))))

(defn- temporary-directory []
  (Files/createTempDirectory "jolt-chdb-dbspec-"
                             (make-array FileAttribute 0)))

(def writer-only-keys
  [:owner :instance :database :lease-ttl-ms :clock-skew-ms
   :heartbeat-interval-ms :force? :max-attempts :retry-deadline-ms
   :retry-initial-backoff-ms :retry-max-backoff-ms])

(defn- run-validation-checks! []
  (println "Durable dbspec validation")
  (let [namespace (backend/memory-backend)
        spec (durable/writer-dbspec
              {:namespace-backend namespace
               :object-id "primary"
               :owner "collector"
               :database "default"})
        instance (UUID/fromString (:instance spec))]
    (check "writer constructor selects the existing JDBC driver"
           "chdb-durable" (:vendor spec))
    (check "writer constructor defaults a canonical UUIDv4 instance"
           4 (.version instance))
    (check "writer constructor supplies explicit safe lifecycle defaults"
           [durable/default-lease-ttl-ms durable/default-clock-skew-ms false
            durable/default-max-attempts durable/default-retry-deadline-ms
            durable/default-retry-initial-backoff-ms
            durable/default-retry-max-backoff-ms]
           [(:lease-ttl-ms spec) (:clock-skew-ms spec) (:force? spec)
            (:max-attempts spec) (:retry-deadline-ms spec)
            (:retry-initial-backoff-ms spec)
            (:retry-max-backoff-ms spec)]))

  (let [namespace (backend/memory-backend)
        spec (durable/snapshot-dbspec
              {:namespace-backend namespace :object-id "primary"})]
    (check "snapshot constructor selects one read-only JDBC snapshot"
           ["chdb-durable" true namespace "primary"]
           [(:vendor spec) (:read-only? spec)
            (:namespace-backend spec) (:object-id spec)]))

  (let [scoped (backend/memory-backend)]
    (check "already object-scoped backends remain an explicit advanced shape"
           [scoped scoped]
           [(:backend
             (durable/writer-dbspec
              {:backend scoped :owner "collector" :database "default"}))
            (:backend (durable/snapshot-dbspec {:backend scoped}))]))

  (let [storage (backend/memory-backend)
        spec (durable/writer-dbspec
              {:backend storage :owner "owner" :database "default"
               :lease-ttl-ms time-domain/max-safe-epoch-milliseconds
               :clock-skew-ms time-domain/max-safe-epoch-milliseconds
               :heartbeat-interval-ms 1})]
    (check "public lease fields accept the exact cross-runtime boundary"
           [time-domain/max-safe-epoch-milliseconds
            time-domain/max-safe-epoch-milliseconds 1]
           [(:lease-ttl-ms spec) (:clock-skew-ms spec)
            (:heartbeat-interval-ms spec)]))

  (doseq [key writer-only-keys]
    (check (str "snapshot rejects writer-only " key)
           ::durable/invalid-options
           (error-type
            (rejected
             #(durable/snapshot-dbspec
               {:namespace-backend (backend/memory-backend)
                :object-id "primary" key
                (case key
                  :owner "owner"
                  :instance "instance"
                  :database "default"
                  :lease-ttl-ms 30000
                  :clock-skew-ms 0
                  :heartbeat-interval-ms 10000
                  :force? false
                  :max-attempts 4
                  :retry-deadline-ms 5000
                  :retry-initial-backoff-ms 10
                  :retry-max-backoff-ms 250)})))))

  (doseq [[label options]
          [["missing storage" {:owner "owner" :database "default"}]
           ["half namespace identity"
            {:namespace-backend (backend/memory-backend)
             :owner "owner" :database "default"}]
           ["mixed storage forms"
            {:backend (backend/memory-backend)
             :namespace-backend (backend/memory-backend) :object-id "primary"
             :owner "owner" :database "default"}]
           ["missing owner"
            {:backend (backend/memory-backend) :database "default"}]
           ["fractional lease"
            {:backend (backend/memory-backend) :owner "owner"
             :database "default" :lease-ttl-ms 1.5}]
           ["fractional clock skew"
            {:backend (backend/memory-backend) :owner "owner"
             :database "default" :clock-skew-ms 0.5}]
           ["fractional heartbeat"
            {:backend (backend/memory-backend) :owner "owner"
             :database "default" :lease-ttl-ms 30
             :heartbeat-interval-ms 9.5}]
           ["lease above the cross-runtime boundary"
            {:backend (backend/memory-backend) :owner "owner"
             :database "default"
             :lease-ttl-ms
             (inc time-domain/max-safe-epoch-milliseconds)}]
           ["skew above the cross-runtime boundary"
            {:backend (backend/memory-backend) :owner "owner"
             :database "default"
             :clock-skew-ms
             (inc time-domain/max-safe-epoch-milliseconds)}]
           ["overslow heartbeat"
            {:backend (backend/memory-backend) :owner "owner"
             :database "default" :lease-ttl-ms 30
             :heartbeat-interval-ms 11}]
           ["zero retry attempts"
            {:backend (backend/memory-backend) :owner "owner"
             :database "default" :max-attempts 0}]
           ["zero retry deadline"
            {:backend (backend/memory-backend) :owner "owner"
             :database "default" :retry-deadline-ms 0}]
           ["inverted retry backoff"
            {:backend (backend/memory-backend) :owner "owner"
             :database "default" :retry-initial-backoff-ms 20
             :retry-max-backoff-ms 10}]]]
    (check (str "writer rejects " label)
           ::durable/invalid-options
           (error-type (rejected #(durable/writer-dbspec options)))))

  (let [secret "credential-value-must-not-escape"
        secret-key (keyword (str "credential-" secret))
        error (rejected
               #(durable/writer-dbspec
                 {:backend (backend/memory-backend)
                  :owner "owner" :database "default"
                  secret-key secret}))
        public (pr-str [(ex-message error) (ex-data error)])]
    (check "unknown option errors retain their structured type"
           ::durable/invalid-options (error-type error))
    (check "unknown option diagnostics omit key names and values"
           false (.contains public secret)))

  (let [opens (atom 0)
        invalid-reader {:vendor "chdb-durable"
                        :backend (backend/memory-backend)
                        :read-only? true
                        :lease-ttl-ms 30000}
        invalid-writer {:vendor "chdb-durable"
                        :backend (backend/memory-backend)
                        :owner "owner" :database "default"
                        :unknown-option true}
        invalid-mode {:vendor "chdb-durable"
                      :backend (backend/memory-backend)
                      :read-only? :yes}]
    (with-redefs [durable/open-reader! (fn [_] (swap! opens inc))
                  durable/open-writer! (fn [_] (swap! opens inc))]
      (check "handwritten reader writer fields fail through JDBC"
             ::durable/invalid-options
             (error-type (rejected #(jdbc/connection invalid-reader))))
      (check "handwritten writer unknown fields fail through JDBC"
             ::durable/invalid-options
             (error-type (rejected #(jdbc/connection invalid-writer))))
      (check "handwritten non-boolean reader mode fails through JDBC"
             ::durable/invalid-options
             (error-type (rejected #(jdbc/connection invalid-mode)))))
    (check "invalid JDBC maps open no storage or native lifecycle"
           0 @opens))

  (let [storage (backend/memory-backend)
        input {:vendor "chdb-durable" :backend storage
               :owner "owner" :database "default"}
        normalized (with-redefs [durable/open-writer! identity]
                     (driver/open-handle durable/durable-driver input))]
    (check "handwritten writer uses the constructor defaults"
           [storage "owner" "default"
            durable/default-lease-ttl-ms durable/default-clock-skew-ms false
            durable/default-max-attempts durable/default-retry-deadline-ms
            durable/default-retry-initial-backoff-ms
            durable/default-retry-max-backoff-ms 4]
           [(:store normalized) (:owner normalized) (:database normalized)
            (:lease-ttl-ms normalized)
            (:clock-skew-ms normalized) (:force? normalized)
            (:max-attempts normalized) (:retry-deadline-ms normalized)
            (:retry-initial-backoff-ms normalized)
            (:retry-max-backoff-ms normalized)
            (.version (UUID/fromString (:instance normalized)))])))

(defn- run-provider-shape-checks! []
  (println "Durable dbspec provider shapes")
  (let [root (temporary-directory)]
    (try
      (let [namespace (local/local-backend (str (.resolve ^Path root "objects")))
            writer-calls (atom [])
            writer-clocks (atom [0M 1M])
            writer-close-count (atom 0)
            writer-cleanup-count (atom 0)
            writer-operations
            (assoc
             (support/fake-open-operations writer-calls writer-clocks
                                           writer-close-count
                                           writer-cleanup-count)
             :execute-native!
             (fn [_ sql params]
               (swap! writer-calls conj [:execute sql (vec params)])
               {:labels [] :rows [] :count 0}))
            writer-spec
            (durable/writer-dbspec
             {:namespace-backend namespace :object-id "local-primary"
              :owner "local-test" :database "default"
              :lease-ttl-ms 300M :operations writer-operations})]
        (with-open [connection (jdbc/connection writer-spec)]
          (jdbc/execute! connection "INSERT INTO t VALUES (7)")
          (durable/flush! connection))
        (let [reader-calls (atom [])
              reader-close-count (atom 0)
              reader-cleanup-count (atom 0)
              reader-spec
              (durable/snapshot-dbspec
               {:namespace-backend namespace :object-id "local-primary"
                :operations
                (support/fake-open-operations reader-calls (atom [0M])
                                              reader-close-count
                                              reader-cleanup-count)})]
          (with-open [_ (jdbc/connection reader-spec)])
          (check "local constructor writer publishes for a fresh snapshot reader"
                 ["INSERT INTO t VALUES (7)"]
                 (mapv second (filter #(= :execute (first %)) @reader-calls)))
          (check "local writer and fresh reader each own cleanup"
                 [1 1] [@writer-cleanup-count @reader-cleanup-count])))
      (finally
        (support/delete-tree! root))))

  (let [requests (atom 0)
        namespace
        (s3/s3-backend
         {:endpoint "https://s3.example.invalid"
          :bucket "durable-tests" :prefix "ci/jolt-chdb"
          :region "us-east-2" :access-key "test-access"
          :secret-key "test-secret"
          :request! (fn [_] (swap! requests inc))})
        writer (durable/writer-dbspec
                {:namespace-backend namespace :object-id "s3-primary"
                 :owner "s3-test" :database "default"})
        reader (durable/snapshot-dbspec
                {:namespace-backend namespace :object-id "s3-primary"})]
    (check "S3 namespace produces matching writer and snapshot identities"
           [namespace "s3-primary" namespace "s3-primary"]
           [(:namespace-backend writer) (:object-id writer)
            (:namespace-backend reader) (:object-id reader)])
    (check "S3-shaped construction performs no provider request" 0 @requests)))

(defn- run-writer-property! []
  (println "Durable dbspec Hegel role property")
  (let [namespace (backend/memory-backend)
        result
        (h/run-test!
         {:name "chdb/durable-writer-dbspec-role"
          :database ""
          :derandomize? true
          :verbosity :quiet
          :test-cases 100}
         (fn [_]
           (g/let [owner-suffix (g/string {:max-size 20 :alphabet "abcXYZ09_-"})
                   object-suffix (g/string {:max-size 20 :alphabet "abcXYZ09_-"})
                   database-suffix (g/string {:max-size 20 :alphabet "abcXYZ09_"})
                   explicit-instance? (g/boolean)
                   instance (g/uuid 4)]
             (let [owner (str "owner-" owner-suffix)
                   object-id (str "object-" object-suffix)
                   database (str "db_" database-suffix)
                   options (cond->
                            {:namespace-backend namespace
                             :object-id object-id
                             :owner owner :database database}
                             explicit-instance? (assoc :instance instance))
                   spec (durable/writer-dbspec options)
                   actual-instance (UUID/fromString (:instance spec))]
               (when-not (and (= "chdb-durable" (:vendor spec))
                              (= namespace (:namespace-backend spec))
                              (= object-id (:object-id spec))
                              (= owner (:owner spec))
                              (= database (:database spec))
                              (= 4 (.version actual-instance))
                              (or (not explicit-instance?)
                                  (= instance (:instance spec))))
                 (throw
                  (ex-info "writer dbspec did not preserve its role/configuration"
                           {:hegel/origin
                            "chdb/durable-dbspec/writer-role"})))))))]
    (println "  hegel writer-role seed" (:seed result)
             "valid" (:valid-test-cases result))
    (when-not (and (:passed? result) (not (:flaky? result)))
      (swap! failures inc)
      (println "  FAIL writer-role" (pr-str result)))))

(defn run-checks! []
  (reset! failures 0)
  (run-validation-checks!)
  (run-provider-shape-checks!)
  (run-writer-property!)
  (when-not (zero? @failures)
    (throw (ex-info (str @failures " Durable dbspec checks failed")
                    {:failures @failures})))
  (println "all Durable dbspec checks passed")
  true)

(defn -main [& _]
  (run-checks!))
