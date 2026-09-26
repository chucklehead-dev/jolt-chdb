(ns jdbc.chdb-durable-json-rows-test
  (:require [clojure.data.json :as json]
            [clojure.test :refer [deftest is run-tests]]
            [db.jdbc]
            [jdbc.chdb :as chdb]
            [jdbc.chdb.durable :as durable]
            [jdbc.chdb.durable.backend :as backend]
            [jdbc.chdb.durable.control :as control]
            [jdbc.chdb.durable.json-rows :as rows]
            [jdbc.chdb.durable.wal :as wal]
            [jdbc.chdb.durable.writer :as writer]
            [jdbc.chdb.json-each-row :as encoder]
            [jdbc.chdb-durable-open-test-support :as support]
            [jdbc.chdb-durable-writer-test-support :as writer-support]
            [jdbc.core :as jdbc]
            [jolt.fibers :as fibers]))

(defn- error-type [f]
  (try
    (f)
    nil
    (catch Throwable error
      (loop [cause error]
        (when cause
          (or (:type (ex-data cause))
              (recur (.getCause cause))))))))

(defn- with-connection [id operations f]
  (let [storage (backend/memory-backend)
        store (backend/object-backend storage id)
        clocks (atom [0M 1M])
        calls (atom [])
        closes (atom 0)
        cleanups (atom 0)
        configured (merge (support/fake-open-operations
                           calls clocks closes cleanups)
                          operations)]
    (with-open [connection
                (jdbc/connection
                 {:vendor "chdb-durable" :namespace-backend storage
                  :object-id id :owner "row-test" :instance "row-test"
                  :database "default" :lease-ttl-ms 300M
                  :operations configured})]
      (f connection store calls))))

(deftest admission-only-uses-raw-writer-and-exact-statement-wal
  (let [seen (atom [])
        input [{"message" "URL?x=1; 'quote' \\ path λ😀"}]
        payload (str (json/write-str (first input)) "\n")
        expected-sql (str (chdb/json-rows-insert-prefix "events" ["message"])
                          payload)]
    (with-connection
      "row-admission" {:analyze-execute!
                       (fn [_ sql _] (swap! seen conj [:classify sql]))
                       :execute-native!
                       (fn [_ sql _] (swap! seen conj [:execute sql])
                         {:count 1})}
      (fn [connection store _]
        (let [context (rows/open-writer connection {:parallelism 1})]
          (try
            (is (= {:count 1}
                   (rows/admit-rows! context "events" ["message"] input)))
            (is (= [[:classify expected-sql] [:execute expected-sql]] @seen))
            (is (empty? (get-in (:head (control/read-head! store))
                                ["manifest" "wal"])))
            (is (= :committed (:status (durable/flush! connection))))
            (let [reference (first (get-in (:head (control/read-head! store))
                                           ["manifest" "wal"]))]
              (is (= (vec (wal/line expected-sql))
                     (vec (backend/get-bytes store (get reference "key"))))))
            (finally (rows/close! context))))))))

(deftest confirmed-variant-returns-a-publication-receipt
  (with-connection
    "row-confirmed" {}
    (fn [connection store _]
      (let [context (rows/open-writer connection {:parallelism 4})]
        (try
          (is (= :committed
                 (:status
                  (rows/insert-rows-and-flush!
                   context "events" ["id"] [{"id" 1} {"id" 2}]))))
          (is (= 1 (count (get-in (:head (control/read-head! store))
                                  ["manifest" "wal"]))))
          (finally (rows/close! context)))))))

(deftest durable-submission-skips-payload-utf8-materialization
  (with-connection
    "row-text-only" {:execute-native! (fn [& _] {:count 1})}
    (fn [connection _ _]
      (let [context (rows/open-writer connection {:parallelism 4})
            materialize-var (ns-resolve 'jdbc.chdb.json-each-row 'materialize-utf8)
            error (ex-info "unused payload bytes requested" {:canary :utf8})
            calls (atom 0)]
        (try
          (with-redefs-fn
            {materialize-var (fn [_] (swap! calls inc) (throw error))}
            (fn []
              ;; Nonvacuity: the canary still intercepts the byte-producing API.
              (is (identical? error
                             (try (encoder/encode-rows! (:encoder context) []) nil
                                  (catch Throwable caught caught))))
              (is (= {:count 1}
                     (rows/admit-rows! context "events" ["id"] [{"id" 1}])))
              (is (= :committed
                     (:status (rows/insert-rows-and-flush!
                               context "events" ["id"] [{"id" 2}]))))
              (is (= 1 @calls) "neither Durable route requests payload bytes")))
          (finally (rows/close! context)))))))

(deftest invalid-input-and-classifier-failure-do-not-execute
  (let [native-count (atom 0)
        classify-count (atom 0)]
    (with-connection
      "row-rejections"
      {:analyze-execute!
       (fn [_ _ _]
         (swap! classify-count inc)
         (throw (ex-info "test rejection" {:type ::classification-rejected})))
       :execute-native! (fn [& _] (swap! native-count inc))}
      (fn [connection store _]
        (let [context (rows/open-writer connection)]
          (try
            (is (= :jdbc.chdb/invalid-json-rows
                   (error-type #(rows/admit-rows!
                                 context "events; DROP TABLE x" ["id"]
                                 [{"id" 1}]))))
            (is (= :jdbc.chdb.json-each-row/invalid-rows
                   (error-type #(rows/admit-rows!
                                 context "events" ["id"] '({"id" 1})))))
            (is (= 0 @classify-count))
            (is (= ::classification-rejected
                   (error-type #(rows/admit-rows!
                                 context "events" ["id"] [{"id" 1}]))))
            (is (= [1 0] [@classify-count @native-count]))
            (is (empty? (get-in (:head (control/read-head! store))
                                ["manifest" "wal"])))
            (finally (rows/close! context))))))))

(deftest close-waits-for-active-native-request-and-leaves-connection-open
  (let [entered (promise)
        release-native (promise)
        native-count (atom 0)]
    (with-connection
      "row-close"
      {:execute-native!
       (fn [_ _ _]
         (swap! native-count inc)
         (deliver entered true)
         @release-native
         {:count 1})}
      (fn [connection _ _]
        (let [context (rows/open-writer connection)
              active (fibers/spawn #(rows/admit-rows!
                                     context "events" ["id"] [{"id" 1}]))]
          (try
            @entered
            (is (= :jdbc.chdb.durable.json-rows/busy
                   (error-type #(rows/admit-rows!
                                 context "events" ["id"] [{"id" 2}]))))
            (let [close-outcome (promise)
                  closer (Thread.
                          (fn []
                            (let [value (rows/close! context)]
                              (deliver close-outcome
                                       [value (.isInterrupted
                                               (Thread/currentThread))]))))]
              (.start closer)
              (loop [remaining 1000]
                (when (and (pos? remaining)
                           (= :open (:phase @(:state context))))
                  (Thread/yield)
                  (recur (dec remaining))))
              (is (= :closing (:phase @(:state context))))
              (is (not (realized? (:done (:active @(:state context))))))
              (is (= :jdbc.chdb.durable.json-rows/closed
                     (error-type #(rows/admit-rows!
                                   context "events" ["id"] [{"id" 3}]))))
              (.interrupt closer)
              (.join closer 1000)
              (is (not (realized? close-outcome)))
              (deliver release-native true)
              (is (= {:count 1} (fibers/join active)))
              (.join closer 5000)
              (is (= [:closed true] @close-outcome))
              (is (= :writer (durable/connection-role connection)))
              (is (= 1 @native-count)))
            (finally
              (deliver release-native true)
              (rows/close! context))))))))

(deftest interrupted-caller-does-not-release-active-request-before-worker-settles
  (let [entered (promise)
        release-native (promise)]
    (with-connection
      "row-interrupted"
      {:execute-native!
       (fn [_ _ _]
         (deliver entered true)
         @release-native
         {:count 1})}
      (fn [connection _ _]
        (let [context (rows/open-writer connection)
              outcome (promise)
              caller (Thread.
                      (fn []
                        (deliver outcome
                                 (let [result
                                       (try {:value (rows/admit-rows!
                                                     context "events" ["id"]
                                                     [{"id" 1}])}
                                            (catch Throwable error {:error error}))]
                                   (assoc result :interrupted?
                                          (.isInterrupted (Thread/currentThread)))))))]
          (try
            (.start caller)
            @entered
            (.interrupt caller)
            (.join caller 1000)
            ;; A severed caller cannot let close or another submission pass
            ;; an admitted native request that still owns the writer worker.
            (is (some? (:active @(:state context))))
            (is (= :jdbc.chdb.durable.json-rows/busy
                   (error-type #(rows/admit-rows!
                                 context "events" ["id"] [{"id" 2}]))))
            (finally
              (deliver release-native true)
              (.join caller 5000)
              (is (= {:value {:count 1} :interrupted? true} @outcome))
              (rows/close! context))))))))

(deftest interrupted-confirmed-caller-retains-gate-through-publication
  (let [entered (promise)
        release-publication (promise)]
    (with-connection
      "row-interrupted-confirmed"
      {:publish-wal-file!
       (fn [store token path]
         (deliver entered true)
         @release-publication
         (control/publish-wal-file! store token path))}
      (fn [connection store _]
        (let [context (rows/open-writer connection)
              outcome (promise)
              caller (Thread.
                      (fn []
                        (deliver outcome
                                 (let [result
                                       (try {:value (rows/insert-rows-and-flush!
                                                     context "events" ["id"]
                                                     [{"id" 1}])}
                                            (catch Throwable error {:error error}))]
                                   (assoc result :interrupted?
                                          (.isInterrupted (Thread/currentThread)))))))]
          (try
            (.start caller)
            @entered
            (.interrupt caller)
            (.join caller 1000)
            (is (some? (:active @(:state context))))
            (is (empty? (get-in (:head (control/read-head! store))
                                ["manifest" "wal"])))
            (finally
              (deliver release-publication true)
              (.join caller 5000)
              (is (= :committed (get-in @outcome [:value :status])))
              (is (true? (:interrupted? @outcome)))
              (rows/close! context))))))))

(deftest interrupted-full-queue-does-not-enqueue-a-third-request
  (let [store (backend/memory-backend)
        acquired (control/acquire! store writer-support/base-options)
        entered (promise)
        release-native (promise)
        executed (atom [])
        operations (assoc (writer-support/fake-operations (atom []) (atom 0))
                          :execute-native!
                          (fn [_ sql _]
                            (swap! executed conj sql)
                            (when (= sql "INSERT INTO t VALUES (1)")
                              (deliver entered true)
                              @release-native)
                            {:count 1}))
        opened (writer/start!
                {:store store :token (:token acquired) :handle :fake-handle
                 :database "default" :queue-capacity 1
                 :operations operations})]
    (try
      (let [first-call (fibers/spawn
                        #(writer/execute! opened "INSERT INTO t VALUES (1)"))]
        @entered
        (let [second-call (fibers/spawn
                           #(writer/execute! opened "INSERT INTO t VALUES (2)"))]
          (loop [remaining 1000]
            (when (and (pos? remaining) (zero? (.size (:queue opened))))
              (Thread/yield)
              (recur (dec remaining))))
          (is (= 1 (.size (:queue opened))))
          (let [third-ready (promise)
                third-outcome (promise)
                third (Thread.
                       (fn []
                         (deliver third-ready true)
                         (deliver third-outcome
                                  (try
                                    (writer/execute-settled!
                                     opened "INSERT INTO t VALUES (3)")
                                    :unexpected-success
                                    (catch InterruptedException _
                                      :interrupted-before-enqueue)))))]
            (try
              (.start third)
              @third-ready
              (.interrupt third)
              (.join third 5000)
              (is (not (.isAlive third)))
              (is (= :interrupted-before-enqueue @third-outcome))
              (finally
                (deliver release-native true)
                (fibers/join first-call)
                (fibers/join second-call)))
            (is (= ["INSERT INTO t VALUES (1)" "INSERT INTO t VALUES (2)"]
                   @executed)))))
      (finally
        (deliver release-native true)
        (writer/close! opened)))))

(defn -main [& _]
  (let [result (run-tests 'jdbc.chdb-durable-json-rows-test)]
    (System/exit (if (zero? (+ (:fail result) (:error result))) 0 1))))
