(ns jdbc.chdb-durable-retry-test
  (:require [clojure.test :refer [deftest is run-tests testing]]
            [jdbc.chdb.durable.retry :as retry]))

(defn- fake-budget [options]
  (let [now (atom 0)
        waits (atom [])]
    {:now now
     :waits waits
     :budget
     (retry/start
      (merge
       {:monotonic-ms! #(deref now)
        :await-backoff! (fn [milliseconds]
                          (swap! waits conj milliseconds)
                          (swap! now + milliseconds))}
       options))}))

(deftest retry-budget-backs-off-and-bounds-attempts
  (let [{:keys [budget waits]} (fake-budget {:max-attempts 3})]
    (is (= :retry (retry/await-next! budget 1)))
    (is (= :retry (retry/await-next! budget 2)))
    (is (= :attempt-limit (retry/await-next! budget 3)))
    (is (= [10 20] @waits)
        "there is no sleep after the final allowed attempt")))

(deftest retry-budget-clips-the-final-wait-to-its-deadline
  (let [{:keys [budget waits now]}
        (fake-budget {:max-attempts 8 :retry-deadline-ms 25})]
    (is (= :retry (retry/await-next! budget 1)))
    (is (= :deadline (retry/await-next! budget 2)))
    (is (= [10 15] @waits))
    (is (= 25 @now))
    (is (zero? (retry/remaining-ms budget)))))

(deftest retry-budget-observes-stopping-before-and-after-wait
  (testing "already stopped"
    (let [waits (atom 0)
          budget (retry/start {:stopped? (constantly true)
                               :await-backoff! #(swap! waits + %)})]
      (is (= :stopped (retry/await-next! budget 1)))
      (is (zero? @waits))))
  (testing "stopped while backing off"
    (let [stopped? (atom false)
          budget (retry/start
                  {:stopped? #(deref stopped?)
                   :await-backoff! (fn [_] (reset! stopped? true))})]
      (is (= :stopped (retry/await-next! budget 1))))))

(deftest retry-budget-validates-public-bounds
  (doseq [options [{:max-attempts 0}
                   {:retry-deadline-ms 0}
                   {:retry-initial-backoff-ms 0}
                   {:retry-max-backoff-ms 0}
                   {:retry-initial-backoff-ms 2
                    :retry-max-backoff-ms 1}]]
    (is (= ::retry/invalid-options
           (:type (ex-data (try (retry/start options) nil
                                (catch Throwable error error))))))))

(defn run-checks! []
  (let [{:keys [fail error]} (run-tests 'jdbc.chdb-durable-retry-test)]
    (when-not (zero? (+ fail error))
      (throw (ex-info "Durable retry checks failed"
                      {:failures fail :errors error})))
    true))

(defn -main [& _]
  (run-checks!))
