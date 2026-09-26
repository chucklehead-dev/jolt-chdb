(ns jdbc.chdb.durable.json-rows
  "Opt-in, caller-owned JSONEachRow submission through the Durable writer."
  (:require [jdbc.chdb :as chdb]
            [jdbc.chdb.durable :as durable]
            [jdbc.chdb.json-each-row :as encoder]))

(defn- fail! [kind message]
  (throw (ex-info message {:type kind})))

(defn open-writer
  "Bind a caller-owned Durable writer connection to one ordered row encoder.
  `:parallelism` is 1 by default; 4 is an explicit Jolt-only opt-in. The
  connection remains owned by its caller and is never closed by this context."
  ([connection] (open-writer connection {}))
  ([connection options]
   (when-not (= :writer (durable/connection-role connection))
     (fail! ::writer-required "JSONEachRow submission requires a Durable writer"))
   {:connection connection
    :encoder (encoder/open-encoder options)
    :state (atom {:phase :open :active nil})}))

(defn- admit! [context]
  (let [operation {:done (promise)}]
    (loop []
      (let [{:keys [phase active] :as before} @(:state context)]
        (when-not (= :open phase)
          (fail! ::closed "Durable JSONEachRow context is closing or closed"))
        (when active
          (fail! ::busy "Durable JSONEachRow context already has an active operation"))
        (if (compare-and-set! (:state context) before
                              (assoc before :active operation))
          operation
          (recur))))))

(defn- release! [context operation]
  (loop []
    (let [before @(:state context)]
      (when (identical? operation (:active before))
        (if (compare-and-set!
             (:state context) before
             (assoc before :active nil
                    :phase (if (= :closing (:phase before)) :closed :open)))
          (deliver (:done operation) :closed)
          (recur))))))

(defn- with-batch! [context table columns rows submit!]
  (let [operation (admit! context)]
    (try
      ;; Validate identifiers before encoding or touching the Durable writer.
      (let [prefix (chdb/json-rows-insert-prefix table columns)
            payload (encoder/encode-text! (:encoder context) rows)]
        ;; The raw writer classifies, prepares its exact SQL WAL line, executes,
        ;; and stages that line. Only text is needed here; separate payload
        ;; bytes would not replace the classified, replayable SQL statement.
        (submit! (:connection context) (str prefix payload)))
      (finally (release! context operation)))))

(defn admit-rows!
  "Encode and locally execute one ordered JSONEachRow batch. The returned
  native result is NOT persistent; call `jdbc.chdb.durable/flush!` later to
  obtain a confirmed/reconciled publication receipt. This operation does not
  automatically flush, so multiple admitted batches may share one barrier."
  [context table columns rows]
  (with-batch! context table columns rows durable/execute-settled!))

(defn insert-rows-and-flush!
  "Encode one ordered batch, then execute and flush it as one Durable FIFO
  request. Returns its confirmed/reconciled publication receipt; unlike
  `admit-rows!`, this includes the persistence barrier and must be measured
  separately."
  [context table columns rows]
  (with-batch! context table columns rows durable/execute-and-flush-settled!))

(defn close!
  "Stop row admission and wait for an active synchronous submission to return.
  An interrupted closer still drains the active operation before restoring its
  interrupt flag. Close only the owned encoder, never the caller-owned Durable
  connection."
  [context]
  (let [active
        (loop []
          (let [before @(:state context)
                after (assoc before :phase (if (:active before)
                                             :closing :closed))]
            (if (compare-and-set! (:state context) before after)
              (:active after)
              (recur))))]
    (let [interrupted? (atom false)]
      (try
        (when active
          (loop []
            (let [attempt (try {:value @(:done active)}
                               (catch InterruptedException _
                                 {:interrupted true}))]
              (when (:interrupted attempt)
                (reset! interrupted? true)
                (recur)))))
        (encoder/close! (:encoder context))
        :closed
        (finally
          (when @interrupted?
            (.interrupt (Thread/currentThread))))))))
