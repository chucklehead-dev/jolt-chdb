(ns jdbc.chdb-durable-checkpoint-threshold-jdbc-test
  (:require [clojure.test :refer [deftest is]]
            [db.driver :as driver] [db.jdbc]
            [jdbc.core :as jdbc]
            [jdbc.chdb.durable :as durable]
            [jdbc.chdb.durable.backend :as backend]
            [jdbc.chdb.durable.control :as control]
            [jdbc.chdb-durable-open-test-support :as support]))

(deftest threshold-survives-the-jdbc-open-boundary
  (let [storage (backend/memory-backend)
        options {:backend storage :owner "threshold-test" :database "default"}]
    (doseq [spec [(durable/writer-dbspec (assoc options :checkpoint-wal-reference-threshold 2))
                 (assoc options :vendor "chdb-durable" :checkpoint-wal-reference-threshold 3)]]
      (let [opened (with-redefs [durable/open-writer! identity]
                     (driver/open-handle durable/durable-driver spec))]
        (is (= (:checkpoint-wal-reference-threshold spec)
               (:checkpoint-wal-reference-threshold opened)))))
    (let [opened (with-redefs [durable/open-writer! identity]
                   (driver/open-handle durable/durable-driver (durable/writer-dbspec options)))]
      (is (not (contains? opened :checkpoint-wal-reference-threshold))))))

(deftest public-jdbc-threshold-causes-a-confirmed-checkpoint
  (let [storage (backend/memory-backend) calls (atom [])
        closes (atom 0) cleanups (atom 0)
        spec (durable/writer-dbspec
               {:backend storage :owner "threshold-test" :instance "threshold-instance"
                :database "default" :checkpoint-wal-reference-threshold 2
                :operations (assoc (support/fake-open-operations calls (atom [0M]) closes cleanups)
                                   :execute-native!
                                   (fn [_ sql _]
                                     (swap! calls conj [:execute sql])
                                     {:labels [] :rows [] :count 0}))})]
    (with-open [connection (jdbc/connection spec)]
      (jdbc/execute! connection "INSERT INTO t VALUES (1)")
      (is (= :committed (:status (durable/flush! connection))))
      (let [first-head (:head (control/read-head! storage))]
        (is (= 1 (count (get-in first-head ["manifest" "wal"]))))
        (jdbc/execute! connection "INSERT INTO t VALUES (2)")
        (is (= :committed (:status (durable/flush! connection))))
        (let [head (:head (control/read-head! storage))]
          (is (= [] (get-in head ["manifest" "wal"])))
          (is (= (inc (get-in first-head ["manifest" "seq"]))
                 (get-in head ["manifest" "seq"])))
          (is (not= (get-in first-head ["manifest" "base" "key"])
                    (get-in head ["manifest" "base" "key"])))
          (is (= 1 (count (filter #(= :backup (first %)) @calls)))))))
    (is (= 1 @closes)) (is (= 1 @cleanups))))
