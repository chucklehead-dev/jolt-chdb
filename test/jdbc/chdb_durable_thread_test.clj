(ns jdbc.chdb-durable-thread-test
  "Isolated single-carrier checks for Durable owned OS threads.

  This namespace must run in its own Jolt process because the carrier count is
  fixed when the first fiber is spawned."
  (:require [jdbc.chdb.durable.backend :as backend]
            [jdbc.chdb.durable.control :as control]
            [jdbc.chdb.durable.reader :as reader]
            [jdbc.chdb.durable.writer :as writer]
            [jolt.fibers :as fibers]))

(def failures (atom 0))

(defn- check [label expected actual]
  (if (= expected actual)
    (println "  ok  " label)
    (do
      (swap! failures inc)
      (println "  FAIL" label "- expected" (pr-str expected)
               "got" (pr-str actual)))))

(defn- writer-operations [worker-in-fiber? heartbeat-in-fiber? renewed]
  {:classification-sql! (fn [sql _] sql)
   :analyze-query! (fn [_ _ _] nil)
   :query-native!
   (fn [_ _ _]
     (reset! worker-in-fiber? (fibers/in-fiber?))
     ;; Longer than the initial lease: a fiber pins the sole carrier and keeps
     ;; its sibling heartbeat from renewing. An owned OS thread does not.
     (Thread/sleep 1300)
     :query-result)
   :renew!
   (fn [store token expiry]
     (reset! heartbeat-in-fiber? (fibers/in-fiber?))
     (let [result (control/renew! store token expiry)]
       (deliver renewed result)
       result))
   :close-native! (fn [_] nil)
   :cleanup-scratch! (fn [] nil)})

(defn- run-writer-checks! []
  (let [store (backend/memory-backend)
        now (System/currentTimeMillis)
        acquired
        (control/acquire!
         store
         {:owner "owned-thread-writer" :instance "instance-1"
          :expires-at (+ now 1000) :now now :clock-skew 0
          :database "default" :engine-version "26.7.2-rc.2"
          :backup-format 1 :min-reader "26.7.2-rc.2"})
        worker-in-fiber? (atom nil)
        heartbeat-in-fiber? (atom nil)
        renewed (promise)
        durable-writer
        (writer/start!
         {:store store :token (:token acquired) :handle :fake-handle
          :database "default" :lease-expiry (+ now 1000)
          :lease-ttl-ms 1200 :heartbeat-interval-ms 200
          :operations (writer-operations worker-in-fiber?
                                         heartbeat-in-fiber? renewed)})]
    (try
      (check "blocking writer query completes" :query-result
             (writer/query! durable-writer "SELECT 1"))
      (check "heartbeat renews before a blocking native call outlives the lease"
             true (not= ::timeout (deref renewed 300 ::timeout)))
      (check "writer operation runs outside a Jolt fiber"
             false @worker-in-fiber?)
      (check "writer heartbeat runs outside a Jolt fiber"
             false @heartbeat-in-fiber?)
      (check "renewal keeps the writer locally writable"
             true (:writable? (writer/status durable-writer)))
      (finally
        (try (writer/close! durable-writer) (catch Throwable _))))))

(defn- run-reader-checks! []
  (let [worker-in-fiber? (atom nil)
        durable-reader
        (reader/start!
         {:handle :fake-handle :database "default"
          :operations
          {:classification-sql! (fn [sql _] sql)
           :analyze-query! (fn [_ _ _] nil)
           :query-native!
           (fn [_ _ _]
             (reset! worker-in-fiber? (fibers/in-fiber?))
             :reader-result)
           :close-native! (fn [_] nil)
           :cleanup-scratch! (fn [] nil)}})]
    (try
      (check "reader query completes" :reader-result
             (reader/query! durable-reader "SELECT 1" []))
      (check "reader operation runs outside a Jolt fiber"
             false @worker-in-fiber?)
      (finally (reader/close! durable-reader)))))

(defn -main [& _]
  (reset! failures 0)
  (fibers/set-carrier-count! 1)
  (println "Durable owned OS-thread isolation")
  (check "the regression process has exactly one Jolt carrier"
         1 (fibers/carrier-count))
  (run-writer-checks!)
  (run-reader-checks!)
  (when-not (zero? @failures)
    (throw (ex-info (str @failures " Durable thread checks failed")
                    {:failures @failures})))
  (println "all Durable thread checks passed"))
