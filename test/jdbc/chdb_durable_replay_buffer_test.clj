(ns jdbc.chdb-durable-replay-buffer-test
  (:require [clojure.test :refer [deftest is run-tests]]
            [jdbc.chdb :as chdb]
            [jdbc.chdb.durable :as durable]
            [jdbc.chdb.native :as native]))

(def accepted
  {:query-class :mutating :statement-count 1 :has-secrets false
   :writes-only-target-database true :changes-database-lifecycle false})

(defn exercise [analysis execution-error observer?]
  (let [events (atom []) phase-events (atom [])
        query-buffer {:pointer :owned-pointer :length 123}
        error (try
                (with-redefs
                  [native/with-query-buffer
                   (fn [sql f]
                     (swap! events conj [:allocate sql])
                     (try (f query-buffer)
                          (finally (swap! events conj [:release query-buffer]))))
                   native/classify-query-buffer!
                   (fn [handle buffer database]
                     (swap! events conj [:classify handle buffer database])
                     (if (instance? Throwable analysis) (throw analysis) analysis))
                   chdb/execute-any-with-query-buffer
                   (fn [handle sql buffer]
                     (swap! events conj [:execute handle sql buffer])
                     (when execution-error (throw execution-error))
                     :consumed-result)]
                  (#'durable/replay-statement!
                   "INSERT INTO mem.t VALUES (1)"
                   (#'durable/configured-recovery-operations {})
                   :handle "mem" 123
                   (when observer? #(swap! phase-events conj %))))
                nil
                (catch Throwable e e))]
    {:events @events :phases @phase-events :error error :buffer query-buffer}))

(deftest shared-buffer-lifetime-and-observer-parity
  (let [plain (exercise accepted nil false)
        observed (exercise accepted nil true)]
    (is (nil? (:error plain)))
    (is (nil? (:error observed)))
    (is (= (:events plain) (:events observed)))
    (is (= [:allocate :classify :execute :release] (mapv first (:events plain))))
    (is (= (:buffer plain) (get-in plain [:events 1 2]) (get-in plain [:events 2 3])
           (get-in plain [:events 3 1])))
    (is (= [:wal-replay-classification :wal-replay-native] (mapv :phase (:phases observed))))
    (is (= [:complete :complete] (mapv :status (:phases observed))))
    (is (= [123 123] (mapv :bytes (:phases observed))))))

(deftest denied-classification-never-executes
  (doseq [analysis [(assoc accepted :query-class :read-only)
                   (assoc accepted :has-secrets true)
                   (assoc accepted :writes-only-target-database false)
                   (assoc accepted :changes-database-lifecycle true)
                   (assoc accepted :statement-count 2)
                   {}]]
    (let [r (exercise analysis nil true)]
      (is (= :jdbc.chdb.durable.policy/rejected (:type (ex-data (:error r)))))
      (is (= [:allocate :classify :release] (mapv first (:events r))))
      (is (= [:failed] (mapv :status (:phases r)))))))

(deftest exceptions-release-and-retain-phase-boundaries
  (let [failure (ex-info "synthetic native fault" {:test true})
        classification (exercise failure nil true)
        execution (exercise accepted failure true)]
    (is (identical? failure (:error classification)))
    (is (identical? failure (:error execution)))
    (is (= [:allocate :classify :release] (mapv first (:events classification))))
    (is (= [:allocate :classify :execute :release] (mapv first (:events execution))))
    (is (= [:failed] (mapv :status (:phases classification))))
    (is (= [:complete :failed] (mapv :status (:phases execution))))))

(deftest custom-operation-seams-and-reserved-helper
  (doseq [key [:analyze-execute! :execute-native! :open-native!]]
    (is (not (contains? (#'durable/configured-recovery-operations {key (fn [& _])})
                       :with-native-replay-buffer!))))
  (is (contains? (#'durable/configured-recovery-operations {:recovery-phase! (fn [_])})
                 :with-native-replay-buffer!))
  (is (= :jdbc.chdb.durable/invalid-options
         (try (#'durable/configured-recovery-operations {:with-native-replay-buffer! (fn [& _])})
              nil (catch Throwable e (:type (ex-data e))))))
  (let [events (atom [])]
    (#'durable/replay-statement!
     "sql" {:analyze-execute! (fn [h s d] (swap! events conj [:admit h s d]))
             :execute-native! (fn [h s p] (swap! events conj [:execute h s p]))}
     :handle "mem" 7 nil)
    (is (= [[:admit :handle "sql" "mem"] [:execute :handle "sql" []]] @events))))

(defn -main [& _]
  (let [r (run-tests 'jdbc.chdb-durable-replay-buffer-test)]
    (System/exit (if (zero? (+ (:fail r) (:error r))) 0 1))))
