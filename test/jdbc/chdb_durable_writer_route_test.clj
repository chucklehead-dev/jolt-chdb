(ns jdbc.chdb-durable-writer-route-test
  "Deterministic, redacted preflight traces for the two public writer routes."
  (:require [db.jdbc]
            [jdbc.chdb :as chdb]
            [jdbc.chdb.durable :as durable]
            [jdbc.chdb.durable.backend :as backend]
            [jdbc.chdb.durable.control :as control]
            [jdbc.chdb.durable.writer :as writer]
            [jdbc.chdb-durable-open-test-support :as open-support]
            [jdbc.chdb-durable-writer-test-support :as writer-support]
            [jdbc.core :as jdbc]))

(def failures (atom 0))

(defn- check [label expected actual]
  (writer-support/check failures label expected actual))

(defn- error-type [f]
  ;; Public JDBC wraps the driver exception in SQLException; the raw writer
  ;; returns the original exception. Preserve the causal type in either case.
  (try
    (f)
    nil
    (catch Throwable error
      (loop [current error]
        (when current
          (or (:type (ex-data current))
              (recur (.getCause current))))))))

(defn- mutating-analysis []
  {:query-class :mutating :statement-count 1 :has-secrets false
   :writes-only-target-database true :changes-database-lifecycle false})

(defn- observe-wal-phase! [events event]
  ;; Keep the production event intact in memory until its shape has been
  ;; checked. Neither the event nor its fields are printed on failure.
  (swap! events conj event))

(defn- phase-names [events]
  (mapv #(if (map? %) (:phase %) %) events))

(def ^:private wal-event-keys #{:phase :status :calls :nanos :bytes})

(defn- closed-wal-event? [event]
  (and (map? event)
       (= wal-event-keys (set (keys event)))
       (#{:wal-prepare :wal-append} (:phase event))
       (#{:complete :failed} (:status event))
       (= 1 (:calls event))
       (integer? (:nanos event))
       (<= 0 (:nanos event))
       (integer? (:bytes event))
       (<= 0 (:bytes event))))

(defn- closed-wal-events? [events expected-count]
  (let [observed (filter map? events)]
    (and (= expected-count (count observed))
         (every? closed-wal-event? observed))))

(defn- jdbc-operations [events reject?]
  (let [lifecycle-calls (atom [])
        clocks (atom [0M 1M])
        close-count (atom 0)
        cleanup-count (atom 0)]
    (assoc (open-support/fake-open-operations
            lifecycle-calls clocks close-count cleanup-count)
           :prepare-query!
           (fn [sql params]
             (swap! events conj :prepare-query)
             (chdb/prepare-query sql params))
           :classify!
           (fn [_ _ _]
             (swap! events conj :classify)
             (if reject?
               (throw (ex-info "rejected by test classifier"
                               {:type ::classifier-rejected}))
               (mutating-analysis)))
           :execute-prepared-native!
           (fn [_ _]
             (swap! events conj :native)
             {:labels [] :rows [] :count 0})
           :writer-phase! #(observe-wal-phase! events %))))

(defn- with-jdbc-writer [events reject? f]
  (let [namespace (backend/memory-backend)
        store (backend/object-backend namespace "route-jdbc")]
    (with-open [connection
                (jdbc/connection
                 {:vendor "chdb-durable"
                  :namespace-backend namespace :object-id "route-jdbc"
                  :owner "route-writer" :instance "route-instance"
                  :database "default" :lease-ttl-ms 300M
                  :operations (jdbc-operations events reject?)})]
      (f connection store))))

(defn- raw-operations [events reject?]
  (assoc (writer-support/fake-operations (atom []) (atom 0))
         :analyze-execute!
         (fn [_ _ _]
           (swap! events conj :classify)
           (when reject?
             (throw (ex-info "rejected by test classifier"
                             {:type ::classifier-rejected}))))
         :execute-native!
         (fn [_ _ _]
           (swap! events conj :native)
           {:labels [] :rows [] :count 0})
         :writer-phase! #(observe-wal-phase! events %)))

(defn- with-raw-writer [events reject? f]
  (let [store (backend/memory-backend)
        acquired (control/acquire! store writer-support/base-options)
        opened (writer/start!
                {:store store :token (:token acquired) :handle :fake-handle
                 :database "default"
                 :engine-metadata
                 {:version "26.7.3" :backup-format 1 :min-reader "26.7.3"}
                 :operations (raw-operations events reject?)})]
    (try (f opened)
         (finally (writer/close! opened)))))

(defn- run-jdbc-materialized! []
  (let [events (atom [])]
    (with-jdbc-writer
      events false
      (fn [connection _]
        (check "JDBC materialized mutation returns through public execute!"
               0 (jdbc/execute! connection "INSERT INTO t VALUES (1)"))
        (check "JDBC preflight and replay staging are totally ordered"
               [:prepare-query :classify :wal-prepare :native :wal-append]
               (phase-names @events))
        (check "JDBC WAL observer has only closed scalar fields"
               true (closed-wal-events? @events 2))))))

(defn- run-raw-materialized! []
  (let [events (atom [])]
    (with-raw-writer
      events false
      (fn [opened]
        (writer/execute! opened "INSERT INTO t VALUES (2)")
        (check "raw execute prepares its WAL line before classifier"
               [:wal-prepare :classify :native :wal-append]
               (phase-names @events))
        (check "raw WAL observer has only closed scalar fields"
               true (closed-wal-events? @events 2))))))

(defn- run-classifier-rejections! []
  (let [jdbc-events (atom [])]
    (with-jdbc-writer
      jdbc-events true
      (fn [connection _]
        (check "JDBC classifier rejection reaches the caller"
               ::classifier-rejected
               (error-type #(jdbc/execute! connection
                                           "INSERT INTO t VALUES (3)")))
        (check "JDBC rejection has neither WAL preparation nor native effect"
               [:prepare-query :classify] (phase-names @jdbc-events))
        (check "JDBC rejected request has no WAL observer event"
               true (closed-wal-events? @jdbc-events 0)))))
  (let [raw-events (atom [])]
    (with-raw-writer
      raw-events true
      (fn [opened]
        (check "raw classifier rejection reaches the caller"
               ::classifier-rejected
               (error-type #(writer/execute! opened
                                             "INSERT INTO t VALUES (4)")))
        (check "raw rejection may prepare but never appends or mutates"
               [:wal-prepare :classify] (phase-names @raw-events))
        (check "raw rejected request's preparation event is redacted"
               true (closed-wal-events? @raw-events 1))
        (check "raw rejection creates no pending statement"
               0 (:pending-statements (writer/status opened)))))))

(defn- run-bound-parameter-checkpoint! []
  (let [events (atom [])]
    (with-jdbc-writer
      events false
      (fn [connection store]
        (check "bound JDBC mutation returns through public execute!"
               0 (jdbc/execute! connection
                                ["INSERT INTO t VALUES (?)" 5]))
        (check "bound JDBC mutation skips V1 SQL WAL line"
               [:prepare-query :classify :native] (phase-names @events))
        (check "bound JDBC mutation has no WAL observer event"
               true (closed-wal-events? @events 0))
        (check "bound JDBC flush confirms a full checkpoint"
               :committed (:status (durable/flush! connection)))
        (let [head (:head (control/read-head! store))]
          (check "bound value is covered by a base, never statement WAL"
                 [true []]
                 [(boolean (get-in head ["manifest" "base"]))
                  (get-in head ["manifest" "wal"])]))))))

(defn run-checks! []
  (reset! failures 0)
  (println "Durable JDBC/raw writer route traces")
  (run-jdbc-materialized!)
  (run-raw-materialized!)
  (run-classifier-rejections!)
  (run-bound-parameter-checkpoint!)
  (when-not (zero? @failures)
    (throw (ex-info (str @failures " route-trace checks failed")
                    {:failures @failures})))
  (println "all Durable route-trace checks passed")
  true)

(defn -main [& _]
  (run-checks!))
