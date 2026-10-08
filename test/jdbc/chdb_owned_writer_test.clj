(ns jdbc.chdb-owned-writer-test
  (:require [clojure.test :refer [deftest is run-tests]]
            [clojure.data.json :as json]
            [jdbc.chdb.owned-statement :as owned]
            [jdbc.chdb.durable.writer :as writer]
            [jdbc.chdb.durable.backend :as backend]
            [jdbc.chdb.durable.control :as control]
            [jdbc.chdb.durable.policy :as policy]
            [jdbc.chdb-durable-writer-test-support :as support]))

(def accepted {:query-class :mutating :statement-count 1 :has-secrets false
               :writes-only-target-database true :changes-database-lifecycle false})

(defn fixture [make-operations f]
  (let [store (backend/memory-backend) calls (atom []) closes (atom 0)
        acquired (control/acquire! store support/base-options)
        operations (make-operations (support/model-checkpoint-operations calls closes) calls)
        instance (writer/start! {:store store :token (:token acquired)
                                :handle :fake-handle :database "default"
                                :engine-metadata {:version "26.7.3" :backup-format 1
                                                  :min-reader "26.7.3"}
                                :operations operations})]
    (try (f instance store calls closes)
         (finally (writer/close! instance)))))

(defn published-sql [store]
  (mapv (fn [reference]
          (get (json/read-str (String. (backend/get-bytes store (get reference "key")) "UTF-8")) "sql"))
        (get-in (:head (control/read-head! store)) ["manifest" "wal"])))

(defn snapshot [sql] (owned/try-snapshot (.getBytes sql "UTF-8")))

(deftest owned-worker-prepares-before-classification-and-publishes-exact-snapshot
  (let [sql "INSERT INTO t VALUES ('ready?')" bytes (.getBytes sql "UTF-8")
        statement (owned/try-snapshot bytes) events (atom [])]
    (aset-byte bytes 0 (byte 88))
    (fixture
     (fn [base _]
       (assoc base
              :writer-phase! #(swap! events conj (:phase %))
              :with-native-owned-admitted-buffer!
              (fn [_ value _ admitted!]
                (is (identical? statement value))
                (swap! events conj :classify)
                (admitted! accepted #(do (swap! events conj :native) {:count 1})))))
     (fn [instance store _ _]
       (with-redefs [owned/text (fn [_] (throw (ex-info "unexpected text materialization" {})))]
         (writer/execute-owned-and-flush! instance statement))
       (is (= [:wal-prepare :classify :native :wal-append]
              (vec (filter #{:wal-prepare :classify :native :wal-append} @events))))
       (is (= [sql] (published-sql store)))
       (is (= 0 (:pending-statements (writer/status instance))))
       (is (false? (:checkpoint-required? (writer/status instance))))))))

(deftest custom-operations-get-exact-text-fallback-and-existing-apis-stay-string-only
  (fixture (fn [base _] base)
    (fn [instance store calls _]
      (let [sql "INSERT INTO t VALUES ('ready?')" statement (snapshot sql)]
        (writer/execute-owned-and-flush! instance statement)
        (is (= [[:analyze-execute sql "default"] [:execute sql]]
               (vec (filter #(contains? #{:analyze-execute :execute} (first %)) @calls))))
        (is (= [sql] (published-sql store)))
        (is (thrown? clojure.lang.ExceptionInfo (writer/execute-and-flush! instance statement)))
        (is (thrown? clojure.lang.ExceptionInfo
                     (writer/execute-owned-and-flush! instance (.getBytes sql "UTF-8"))))
        (is (= [sql] (published-sql store)))))))

(deftest owned-policy-and-budget-rejections-do-not-mutate-or-publish
  (doseq [mode [:policy :statement-budget :wal-budget]]
    (let [events (atom [])]
      (fixture
       (fn [base _]
         (assoc base :writer-phase! #(swap! events conj (:phase %))
                :with-native-owned-admitted-buffer!
                (fn [_ _ _ admitted!]
                  (swap! events conj :classify)
                  (admitted! (assoc accepted :statement-count 2)
                             #(swap! events conj :native)))))
       (fn [instance store _ _]
         (let [run #(writer/execute-owned-and-flush! instance (snapshot "INSERT INTO t VALUES (1)"))]
           (case mode
             :policy (is (thrown? clojure.lang.ExceptionInfo (run)))
             :statement-budget (with-redefs [writer/max-statement-bytes 1]
                                 (is (thrown? clojure.lang.ExceptionInfo (run))))
             :wal-budget (with-redefs [writer/max-wal-segment-bytes 1]
                           (is (thrown? clojure.lang.ExceptionInfo (run)))))
           (is (not (some #{:native} @events)))
           (is (= (if (= mode :policy) [:wal-prepare :classify] [:wal-prepare]) @events))
           (is (= [] (published-sql store)))
           (is (= 0 (:pending-statements (writer/status instance))))))))))

(deftest owned-native-and-wal-append-failures-require-checkpoint-not-wal-publication
  (doseq [mode [:native :append]]
    (fixture
     (fn [base _]
       (assoc base :with-native-owned-admitted-buffer!
              (fn [_ _ _ admitted!]
                (admitted! accepted
                           #(if (= mode :native)
                              (throw (ex-info "controlled native failure" {:fixture true}))
                              {:count 1})))))
     (fn [instance store _ _]
       (let [run #(writer/execute-owned-and-flush! instance (snapshot "INSERT INTO t VALUES (1)"))]
         (if (= mode :append)
           (with-redefs [writer/append-wal! (fn [& _] (throw (ex-info "controlled append failure" {:fixture true})))]
             (is (thrown? clojure.lang.ExceptionInfo (run))))
           (is (thrown? clojure.lang.ExceptionInfo (run))))
         (is (:checkpoint-required? (writer/status instance)))
         (is (= [] (published-sql store))))))))

(deftest retained-sealed-wal-rejects-next-owned-request-before-preparation
  (let [reject-commit (atom true) events (atom [])]
    (fixture
     (fn [base _]
       (assoc base :writer-phase! #(swap! events conj (:phase %))
              :with-native-owned-admitted-buffer!
              (fn [_ _ _ admitted!] (swap! events conj :classify)
                (admitted! accepted #(do (swap! events conj :native) {:count 1})))
              :commit-reference!
              (fn [store token options]
                (if @reject-commit
                  (throw (ex-info "controlled publication failure" {:fixture true}))
                  (control/commit-reference! store token options)))))
     (fn [instance store _ _]
       (try
         (is (thrown? clojure.lang.ExceptionInfo
                      (writer/execute-owned-and-flush! instance (snapshot "INSERT INTO t VALUES (1)"))))
         (is (get-in @(:wal-state instance) [:spool :sealed?]))
         (let [before @events]
           (is (thrown? clojure.lang.ExceptionInfo
                        (writer/execute-owned-and-flush! instance (snapshot "INSERT INTO t VALUES (2)"))))
           (is (= before @events)))
         (is (= [] (published-sql store)))
         (finally (reset! reject-commit false)))))))

(defn -main [& _]
  (let [{:keys [fail error]} (run-tests 'jdbc.chdb-owned-writer-test)]
    (System/exit (if (zero? (+ fail error)) 0 1))))
