(ns jdbc.chdb-confirmed-request-test
  (:require [clojure.test :refer [deftest is run-tests]]
            [jdbc.chdb.durable.writer :as writer]
            [jdbc.chdb-owned-writer-test :as owned-test]))

(deftest submission-is-not-completion-and-waits-never-resubmit
  (let [entered (promise) release (promise)]
    (owned-test/fixture
     (fn [base _]
       (let [execute (:execute-native! base)]
         (assoc base :execute-native!
                (fn [& args]
                  (deliver entered true)
                  @release
                  (apply execute args)))))
     (fn [instance store calls _]
       (let [sql "INSERT INTO t VALUES ('confirmed-ticket')"
             ticket (writer/submit-owned-and-flush! instance (owned-test/snapshot sql))]
         (try
           (is (= true (deref entered 5000 ::timeout)))
           (is (= [] (owned-test/published-sql store)))
           (is (= "#<Durable confirmed request>" (str ticket)))
           (is (= "#<Durable confirmed request>" (pr-str ticket)))
           (deliver release true)
           (let [receipt (writer/await-confirmed! ticket)]
             (is (contains? #{:committed :reconciled} (:status receipt)))
             (is (identical? receipt (writer/await-confirmed! ticket))))
           (is (= [sql] (owned-test/published-sql store)))
           (is (= 1 (count (filter #(= :execute (first %)) @calls))))
           (finally (deliver release true))))))))

(deftest exact-worker-failure-is-retained-on-every-wait
  (let [failure (ex-info "controlled execution failure" {:fixture true})]
    (owned-test/fixture
     (fn [base _] (assoc base :execute-native! (fn [& _] (throw failure))))
     (fn [instance _ _ _]
       (let [ticket (writer/submit-owned-and-flush!
                     instance (owned-test/snapshot "INSERT INTO t VALUES (1)"))]
         (dotimes [_ 2]
           (is (identical? failure
                          (try (writer/await-confirmed! ticket) nil
                               (catch Throwable actual actual))))))))))

(deftest local-tickets-and-arbitrary-values-are-not-confirmation
  (doseq [value [nil {} (writer/->BufferedTicket (promise) 1)]]
    (is (= ::writer/invalid-confirmed-request
           (try (writer/await-confirmed! value) nil
                (catch clojure.lang.ExceptionInfo error (:type (ex-data error))))))))

(deftest interruption-after-admission-settles-before-return-on-success-or-failure
  (doseq [fail? [false true]]
    (let [entered (promise) release (promise) awaiting (promise) outcome (promise)
          failure (ex-info "controlled interrupted failure" {:fixture true})]
      (owned-test/fixture
       (fn [base _]
         (let [execute (:execute-native! base)]
           (assoc base :execute-native!
                  (fn [& args]
                    (deliver entered true)
                    @release
                    (if fail? (throw failure) (apply execute args))))))
       (fn [instance _ _ _]
         (let [ticket (writer/submit-owned-and-flush!
                       instance (owned-test/snapshot "INSERT INTO t VALUES (2)"))
               waiter (Thread.
                       (fn []
                         (deliver awaiting true)
                         (let [result (try {:value (writer/await-confirmed! ticket)}
                                           (catch Throwable error {:error error}))]
                           (deliver outcome
                                    (assoc result :interrupted?
                                           (.isInterrupted (Thread/currentThread)))))))]
           (try
             (is (= true (deref entered 5000 ::timeout)))
             (.start waiter)
             (is (= true (deref awaiting 5000 ::timeout)))
             (.interrupt waiter)
             (is (= ::pending (deref outcome 20 ::pending)))
             (deliver release true)
             (.join waiter 5000)
             (is (not (.isAlive waiter)))
             (let [result (deref outcome 100 ::timeout)]
               (is (= true (:interrupted? result)))
               (if fail?
                 (is (identical? failure (:error result)))
                 (is (contains? #{:committed :reconciled} (get-in result [:value :status])))))
             (finally
               (deliver release true)
               (.join waiter 5000)))))))))

(deftest close-drains-admitted-tickets-and-rejects-new-submissions
  (owned-test/fixture
   (fn [base _] base)
   (fn [instance store _ closes]
     (let [a "INSERT INTO t VALUES ('A')" b "INSERT INTO t VALUES ('B')"
           ticket-a (writer/submit-owned-and-flush! instance (owned-test/snapshot a))
           ticket-b (writer/submit-owned-and-flush! instance (owned-test/snapshot b))]
       (writer/close! instance)
       (is (= :committed (:status (writer/await-confirmed! ticket-a))))
       (is (= :committed (:status (writer/await-confirmed! ticket-b))))
       (is (= [a b] (owned-test/published-sql store)))
       (is (= 1 @closes))
       (is (= ::writer/closed
              (try (writer/submit-owned-and-flush! instance (owned-test/snapshot a)) nil
                   (catch clojure.lang.ExceptionInfo error (:type (ex-data error))))))))))

(defn -main [& _]
  (let [result (run-tests 'jdbc.chdb-confirmed-request-test)]
    (System/exit (if (zero? (+ (:fail result) (:error result))) 0 1))))
