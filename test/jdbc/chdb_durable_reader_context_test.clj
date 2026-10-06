(ns jdbc.chdb-durable-reader-context-test
  (:require [clojure.data.json :as json]
            [jdbc.chdb.durable :as durable]
            [jdbc.chdb.durable.writer :as writer]
            [clojure.test :refer [deftest is run-tests]]))

(deftest explicit-reader-is-scoped-and-restored
  (let [selected (fn [& _] [42 2])
        context {:operations {:json-reader selected} :backend-context {}}]
    (is (nil? json/*experimental-native-reader*))
    (is (= 42 (#'writer/call-with-backend-context context #(json/read-str "42"))))
    (is (nil? json/*experimental-native-reader*))
    (is (= {"a" 1} (json/read-str "{\"a\":1}")))))

(deftest reader-context-does-not-replace-operation-result-or-error
  (let [context {:operations {:json-reader (fn [& _] [42 2])} :backend-context {}}
        failure (Exception. "fixed test failure")]
    (is (= :operation-result (#'writer/call-with-backend-context context (constantly :operation-result))))
    (is (identical? failure
                    (try (#'writer/call-with-backend-context context #(throw failure))
                         (catch Exception error error))))
    (is (nil? json/*experimental-native-reader*))))

(deftest default-context-retains-normal-reader
  (is (= {"a" 1} (#'writer/call-with-backend-context
                   {:operations {} :backend-context {}} #(json/read-str "{\"a\":1}")))))

(deftest invalid-reader-is-rejected-before-startup-operations
  (is (= :jdbc.chdb.durable/invalid-options
         (try (#'durable/configured-recovery-operations {:json-reader :invalid})
              (catch Exception error (:type (ex-data error)))))))

(defn -main [& _]
  (let [result (run-tests 'jdbc.chdb-durable-reader-context-test)]
    (System/exit (if (zero? (+ (:fail result) (:error result))) 0 1))))
