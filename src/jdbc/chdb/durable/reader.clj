(ns jdbc.chdb.durable.reader
  "Serialized query-only operations for one recovered Durable V1 snapshot."
  (:require [jdbc.chdb :as chdb]
            [jdbc.chdb.durable.owned-thread :as owned-thread]
            [jdbc.chdb.durable.policy :as policy]
            [jdbc.chdb.native :as native])
  (:import [java.util.concurrent ArrayBlockingQueue]))

(def default-queue-capacity 64)

(defrecord DurableReader
    [handle database queue admission-lock lifecycle closed-result operations worker])

(defn reader? [value]
  (instance? DurableReader value))

(defn- fail! [type message]
  (throw (ex-info message {:type type})))

(defn- require-query! [reader sql params]
  (when-not (string? sql)
    (fail! ::invalid-options "sql must be a string"))
  (when-not (sequential? params)
    (fail! ::invalid-options "params must be sequential"))
  ((:analyze-query! (:operations reader))
   (:handle reader)
   ((:classification-sql! (:operations reader)) sql params)
   (:database reader)))

(defn- execute-request! [reader request]
  (case (:op request)
    :query
    (do
      (require-query! reader (:sql request) (:params request))
      ((:query-native! (:operations reader))
       (:handle reader) (:sql request) (:params request)))

    :query-bytes
    (do
      (require-query! reader (:sql request) (:params request))
      ((:query-bytes-native! (:operations reader))
       (:handle reader) (:sql request) (:params request) (:options request)))

    :close
    (let [primary (atom nil)]
      (doseq [attempt [#((:close-native! (:operations reader)) (:handle reader))
                        #((:cleanup-scratch! (:operations reader)))]]
        (try (attempt)
             (catch Throwable error
               (when-not @primary (reset! primary error)))))
      (when @primary (throw @primary))
      nil)

    (fail! ::invalid-operation "Unknown Durable reader operation")))

(defn- complete! [result value]
  (deliver result {:value value}))

(defn- fail-result! [result error]
  (deliver result {:error error}))

(defn- terminate-worker! [reader terminal]
  (locking (:admission-lock reader)
    (when (= :open @(:lifecycle reader))
      (reset! (:lifecycle reader) :closing)))
  (try
    (execute-request! reader {:op :close})
    (catch Throwable _))
  (locking (:admission-lock reader)
    (loop []
      (when-let [request (.poll ^ArrayBlockingQueue (:queue reader))]
        (fail-result! (:result request) terminal)
        (recur)))
    (reset! (:lifecycle reader) :closed)
    (deliver (:closed-result reader) {:error terminal})))

(defn- worker-loop [reader]
  (try
    (loop []
      (let [request ((:take-request! (:operations reader)) (:queue reader))
            closing? (= :close (:op request))]
        (try
          (complete! (:result request) (execute-request! reader request))
          (catch Throwable error
            (fail-result! (:result request) error))
          (finally
            (when closing?
              (reset! (:lifecycle reader) :closed)
              (deliver (:closed-result reader) @(:result request)))))
        (when-not closing? (recur))))
    (catch Throwable terminal
      (terminate-worker! reader terminal))))

(defn- await-result [result]
  (let [{:keys [value error]} @result]
    (if error (throw error) value)))

(defn- enqueue-open! [reader request]
  (locking (:admission-lock reader)
    (when-not (= :open @(:lifecycle reader))
      (fail! ::closed "Durable reader is closed"))
    (.put ^ArrayBlockingQueue (:queue reader) request))
  (await-result (:result request)))

(defn start!
  "Start a serialized reader over an already recovered immutable snapshot."
  [{:keys [handle database queue-capacity operations]
    :or {queue-capacity default-queue-capacity}}]
  (when-not handle (fail! ::invalid-options "handle is required"))
  (when-not (string? database)
    (fail! ::invalid-options "database must be a string"))
  (when-not (and (integer? queue-capacity) (pos? queue-capacity))
    (fail! ::invalid-options "queue-capacity must be a positive integer"))
  (let [operations
        (merge {:analyze-query! policy/analyze-query!
                :classification-sql! chdb/classification-sql
                :query-native! (fn [handle sql params]
                                 (chdb/execute-any handle sql params))
                :query-bytes-native! chdb/execute-query-bytes-handle
                :take-request! (fn [queue]
                                 (.take ^ArrayBlockingQueue queue))
                :close-native! native/close!
                :cleanup-scratch! (fn [] nil)}
               operations)
        required #{:analyze-query! :classification-sql! :query-native!
                   :query-bytes-native!
                   :take-request!
                   :close-native! :cleanup-scratch!}]
    (when-not (every? #(fn? (get operations %)) required)
      (fail! ::invalid-options "reader operations must be functions"))
    (let [worker (owned-thread/completion)
          reader (->DurableReader
                  handle database (ArrayBlockingQueue. queue-capacity)
                  (Object.) (atom :open) (promise) operations worker)]
      (owned-thread/start! worker #(worker-loop reader))
      reader)))

(defn query! [reader sql params]
  (enqueue-open! reader {:op :query :sql sql :params params :result (promise)}))

(defn query-bytes! [reader sql params options]
  (enqueue-open! reader {:op :query-bytes :sql sql :params params
                         :options options :result (promise)}))

(defn close! [reader]
  (let [request {:op :close :result (promise)}
        disposition
        (locking (:admission-lock reader)
          (case @(:lifecycle reader)
            :open
            (do
              (reset! (:lifecycle reader) :closing)
              (try
                (.put ^ArrayBlockingQueue (:queue reader) request)
                :owner
                (catch Throwable error
                  (reset! (:lifecycle reader) :open)
                  (throw error))))
            :closing :wait
            :closed :wait))]
    (owned-thread/join-after!
     (:worker reader)
     #(if (= :owner disposition)
        (await-result (:result request))
        (await-result (:closed-result reader))))))

(defn status [reader]
  {:lifecycle @(:lifecycle reader) :read-only? true})
