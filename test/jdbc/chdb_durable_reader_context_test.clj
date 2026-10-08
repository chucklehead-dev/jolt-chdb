(ns jdbc.chdb-durable-reader-context-test
  (:require [clojure.data.json :as json]
            [clojure.test :refer [deftest is run-tests]]
            [jdbc.chdb.durable :as durable]
            [jdbc.chdb.durable.backend :as backend]
            [jdbc.chdb.durable.control :as control]
            [jdbc.chdb.durable.writer :as writer]
            [jdbc.chdb-durable-writer-test-support :as support]))

(deftest explicit-reader-is-scoped-and-restored
  (let [reader (fn [& _] [42 2])
        context {:operations {:json-reader reader} :backend-context {}}
        failure (Exception. "fixed test failure")]
    (is (nil? json/*experimental-native-reader*))
    (is (= 42 (#'writer/call-with-backend-context context #(json/read-str "42"))))
    (is (nil? json/*experimental-native-reader*))
    (is (identical? failure
                    (try (#'writer/call-with-backend-context context #(throw failure))
                         (catch Exception error error))))
    (is (nil? json/*experimental-native-reader*))
    (is (= {"a" 1} (json/read-str "{\"a\":1}")))))

(deftest default-context-and-invalid-reader
  (is (= {"a" 1} (#'writer/call-with-backend-context
                   {:operations {} :backend-context {}} #(json/read-str "{\"a\":1}"))))
  (is (= :jdbc.chdb.durable/invalid-options
         (try (#'durable/validate-recovery-phase-observer! {:json-reader :invalid})
              (catch Exception error (:type (ex-data error)))))))

(deftest owned-worker-uses-selected-reader-not-caller-binding
  ;; Real serialized worker and real head publication; only engine execution is
  ;; substituted. The worker must select its own reader, not inherit the caller.
  (let [calls (atom []) closes (atom 0) selected-calls (atom 0)
        worker-read (atom nil) store (backend/memory-backend)
        acquired (control/acquire! store support/base-options)
        selected (fn [text & _]
                   (swap! selected-calls inc)
                   [(binding [json/*experimental-native-reader* nil]
                      (json/read-str text)) (count text)])
        operations (assoc (support/fake-operations calls closes)
                          :json-reader selected
                          :execute-native!
                          (fn [_ sql _]
                            (reset! worker-read
                                    {:selected? (identical? selected json/*experimental-native-reader*)
                                     :value (json/read-str "42")})
                            {:sql sql}))
        handle (writer/start! {:store store :token (:token acquired)
                               :handle :fake-handle :database "default"
                               :operations operations})]
    (try
      (binding [json/*experimental-native-reader* (fn [& _] [99 2])]
        (is (= 99 (json/read-str "42")))
        (writer/execute-and-flush! handle "INSERT INTO t VALUES (42)")
        (is (= 99 (json/read-str "42"))))
      (is (= {:selected? true :value 42} @worker-read))
      (is (pos? @selected-calls))
      (is (nil? json/*experimental-native-reader*))
      (finally (writer/close! handle)))
    (is (= 1 @closes))
    ;; Fresh normal-decoder readback after owned worker close.
    (is (nil? (get-in (:head (control/read-head! store)) ["lease" "owner"])))
    (is (= 1 (count (get-in (:head (control/read-head! store)) ["manifest" "wal"]))))))

(deftest owned-heartbeat-uses-selected-reader
  (let [calls (atom []) closes (atom 0) ticked? (atom false)
        renewed (promise) selected (fn [& _] [42 2])
        store (backend/memory-backend)
        acquired (control/acquire! store support/base-options)
        operations
        (assoc (support/fake-operations calls closes)
               :json-reader selected :now-ms (constantly 0)
               :await-heartbeat!
               (fn [stop _]
                 (if (compare-and-set! ticked? false true)
                   :tick
                   (deref stop 5000 :stop)))
               :renew!
               (fn [_ _ expiry _]
                 (deliver renewed
                          {:selected? (identical? selected json/*experimental-native-reader*)
                           :value (json/read-str "42")})
                 {:head {"lease" {"expires_at" expiry}}}))
        handle (writer/start! {:store store :token (:token acquired)
                               :handle :fake-handle :database "default"
                               :lease-expiry 1000000 :lease-ttl-ms 1000000
                               :heartbeat-interval-ms 1 :operations operations})]
    (try
      (is (= {:selected? true :value 42} (deref renewed 2000 :timeout)))
      (is (nil? json/*experimental-native-reader*))
      (finally (writer/close! handle)))
    (is (= 1 @closes))))

(defn -main [& _]
  (let [result (run-tests 'jdbc.chdb-durable-reader-context-test)]
    (System/exit (if (zero? (+ (:fail result) (:error result))) 0 1))))
