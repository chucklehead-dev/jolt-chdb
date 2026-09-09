(ns jdbc.chdb.durable.writer
  "Serialized Durable V1 writer operations after lease acquisition and recovery.

  `start!` deliberately does not open or recover an object. It accepts an
  already-acquired fencing token and an already-recovered engine handle, then
  owns the public query/execute/flush/close queue for that live writer."
  (:require [clojure.data.json :as json]
            [jdbc.chdb :as chdb]
            [jdbc.chdb.durable.control :as control]
            [jdbc.chdb.durable.owned-thread :as owned-thread]
            [jdbc.chdb.durable.policy :as policy]
            [jdbc.chdb.native :as native])
  (:import [java.util.concurrent ArrayBlockingQueue]))

(def max-statement-bytes (* 64 1024 1024))
(def max-wal-segment-bytes (* 128 1024 1024))
(def default-queue-capacity 64)

(defrecord DurableWriter
    [store token handle database queue admission-lock lifecycle closed-result
     wal-state lease-state heartbeat-stop operations worker heartbeat])

(defn- fail! [type message]
  (throw (ex-info message {:type type})))

(defn- require-string! [value label]
  (when-not (string? value)
    (fail! ::invalid-options (str label " must be a string")))
  value)

(defn- require-positive-int! [value label]
  (when-not (and (integer? value) (pos? value))
    (fail! ::invalid-options (str label " must be a positive integer")))
  value)

(defn- wal-line [sql]
  (.getBytes (str (json/write-str {"sql" sql}) "\n") "UTF-8"))

(defn- append-wal! [writer line]
  (swap! (:wal-state writer)
         (fn [state]
           (-> state
               (update :lines conj line)
               (update :byte-count + (alength line))))))

(defn- joined-wal [writer]
  (let [{:keys [lines byte-count]} @(:wal-state writer)
        total byte-count
        output (byte-array total)]
    (loop [remaining lines offset 0]
      (when-let [line (first remaining)]
        (let [length (alength line)]
          (System/arraycopy line 0 output offset length)
          (recur (next remaining) (+ offset length)))))
    output))

(defn- clear-wal! [writer]
  (reset! (:wal-state writer)
          {:lines [] :byte-count 0 :checkpoint-required? false}))

(defn- require-checkpoint! [writer]
  (swap! (:wal-state writer) assoc :checkpoint-required? true))

(defn- assert-writable! [writer]
  (when-let [lease-state (:lease-state writer)]
    (let [{:keys [expires-at fenced?]} @lease-state
          now ((:now-ms (:operations writer)))]
      (when (or fenced? (>= now expires-at))
        (swap! lease-state assoc :fenced? true)
        (fail! ::control/lease-fenced
               "The Durable writer cannot prove a live lease")))))

(defn- retry-stopped? [lease-state now-ms]
  (when lease-state
    (let [{:keys [expires-at fenced?]} @lease-state
          stopped? (or fenced? (>= (now-ms) expires-at))]
      (when stopped?
        (swap! lease-state assoc :fenced? true))
      stopped?)))

(defn- do-query! [writer sql params]
  (require-string! sql "sql")
  ((:analyze-query! (:operations writer))
   (:handle writer)
   ((:classification-sql! (:operations writer)) sql params)
   (:database writer))
  ((:query-native! (:operations writer)) (:handle writer) sql params))

(defn- do-query-bytes! [writer sql params options]
  (require-string! sql "sql")
  ((:analyze-query! (:operations writer))
   (:handle writer)
   ((:classification-sql! (:operations writer)) sql params)
   (:database writer))
  ((:query-bytes-native! (:operations writer))
   (:handle writer) sql params options))

(defn- validate-statement-size! [sql]
  (let [statement-bytes (alength (.getBytes sql "UTF-8"))]
    (when (> statement-bytes max-statement-bytes)
      (fail! ::limit-exceeded "Durable SQL statement exceeds 64 MiB"))
    statement-bytes))

(defn- prepare-wal-line! [writer sql]
  (validate-statement-size! sql)
  (let [line (wal-line sql)
          next-segment-bytes (+ (:byte-count @(:wal-state writer))
                                (alength line))]
    (when (> next-segment-bytes max-wal-segment-bytes)
      (fail! ::limit-exceeded "Durable WAL segment would exceed 128 MiB"))
    line))

(defn- execute-admitted! [writer sql params line]
  (let [result ((:execute-native! (:operations writer))
                (:handle writer) sql params)]
    ;; Local failure must not create recovery state. An exact materialized
    ;; statement enters V1 WAL; a bound mutation instead requires a full
    ;; checkpoint because V1 has no typed-parameter WAL record.
    (if line
      (append-wal! writer line)
      (require-checkpoint! writer))
    result))

(defn- do-execute! [writer sql]
  (assert-writable! writer)
  (require-string! sql "sql")
  (let [line (prepare-wal-line! writer sql)]
    ((:analyze-execute! (:operations writer))
     (:handle writer) sql (:database writer))
    (execute-admitted! writer sql [] line)))

(defn- do-sql! [writer sql params]
  (require-string! sql "sql")
  (when-not (sequential? params)
    (fail! ::invalid-options "params must be sequential"))
  (let [classification-sql
        ((:classification-sql! (:operations writer)) sql params)
        analysis ((:classify! (:operations writer))
                  (:handle writer) classification-sql (:database writer))]
    (case (:query-class analysis)
      :read-only
      (do
        (policy/authorize-query! analysis)
        ((:query-native! (:operations writer)) (:handle writer) sql params))

      :mutating
      (do
        (assert-writable! writer)
        (policy/authorize-execute! analysis)
        (let [line (if (seq params)
                     (do (validate-statement-size! sql) nil)
                     (prepare-wal-line! writer sql))]
          (execute-admitted! writer sql params line)))

      (policy/authorize-query! analysis))))

(declare do-checkpoint!)

(defn- do-flush! [writer]
  (assert-writable! writer)
  (let [{:keys [byte-count checkpoint-required?]} @(:wal-state writer)]
    (cond
      checkpoint-required?
      (do-checkpoint! writer)

      (zero? byte-count)
      {:status :empty}

      :else
      (let [payload (joined-wal writer)
            committed
            (let [publication ((:publish-wal! (:operations writer))
                               (:store writer) (:token writer) payload)]
              ((:commit-reference! (:operations writer))
               (:store writer) (:token writer)
               {:kind :wal
                :reference (:reference publication)
                :verify-reference! control/verify-byte-reference!}))]
        ;; Retain the complete pending buffer on every failure. Only a confirmed
        ;; or reconciled head commit proves that replay can recover these writes.
        (clear-wal! writer)
        committed))))

(defn- do-checkpoint! [writer]
  (assert-writable! writer)
  (let [path ((:create-checkpoint! (:operations writer))
              (:handle writer) (:database writer))]
    (try
      (let [committed
            (let [publication ((:publish-checkpoint! (:operations writer))
                               (:store writer) (:token writer) path)]
              ((:commit-reference! (:operations writer))
               (:store writer) (:token writer)
               {:kind :checkpoint
                :reference (:reference publication)
                :verify-reference!
                (:verify-checkpoint-reference! (:operations writer))}))]
        ;; The full backup contains every local mutation. Pending statement WAL
        ;; becomes redundant only after the checkpoint head CAS is proved.
        (clear-wal! writer)
        committed)
      (finally
        ((:delete-checkpoint! (:operations writer)) path)))))

(defn- first-error [attempts]
  (reduce
   (fn [first-error attempt]
     (try
       (attempt)
       first-error
       (catch Throwable error
         (or first-error error))))
   nil
   attempts))

(defn- do-close! [writer]
  (let [error
        (first-error
         [#(do-flush! writer)
          #(deliver (:heartbeat-stop writer) :stop)
          #(when-let [heartbeat (:heartbeat writer)]
             (owned-thread/join! heartbeat))
          #((:release! (:operations writer)) (:store writer) (:token writer))
          #((:close-native! (:operations writer)) (:handle writer))
          #((:cleanup-scratch! (:operations writer)))])]
    (when error (throw error))
    nil))

(defn- execute-request! [writer request]
  (case (:op request)
    :query (do-query! writer (:sql request) (:params request))
    :query-bytes (do-query-bytes! writer (:sql request) (:params request)
                                  (:options request))
    :execute (do-execute! writer (:sql request))
    :sql (do-sql! writer (:sql request) (:params request))
    :flush (do-flush! writer)
    :checkpoint (do-checkpoint! writer)
    :close (do-close! writer)
    (fail! ::invalid-operation "Unknown Durable writer operation")))

(defn- complete! [result value]
  (deliver result {:value value})
  nil)

(defn- fail-result! [result error]
  (deliver result {:error error})
  nil)

(defn- terminate-worker! [writer terminal]
  ;; Stop admission before cleanup, then resolve every request that may have
  ;; raced with the terminal queue failure. The worker error remains the
  ;; stable caller-visible cause; cleanup is still attempted in full.
  (locking (:admission-lock writer)
    (when (= :open @(:lifecycle writer))
      (reset! (:lifecycle writer) :closing)))
  (try (do-close! writer) (catch Throwable _))
  (locking (:admission-lock writer)
    (loop []
      (when-let [request (.poll ^ArrayBlockingQueue (:queue writer))]
        (fail-result! (:result request) terminal)
        (recur)))
    (reset! (:lifecycle writer) :closed)
    (deliver (:closed-result writer) {:error terminal}))
  nil)

(defn- worker-loop [writer]
  (try
    (loop []
      (let [request ((:take-request! (:operations writer)) (:queue writer))
            closing? (= :close (:op request))]
        (try
          (complete! (:result request) (execute-request! writer request))
          (catch Throwable error
            (fail-result! (:result request) error))
          (finally
            (when closing?
              (reset! (:lifecycle writer) :closed)
              (deliver (:closed-result writer) @(:result request)))))
        (when-not closing? (recur))))
    (catch Throwable terminal
      (terminate-worker! writer terminal))))

(defn- await-result [result]
  (let [{:keys [value error]} @result]
    (if error (throw error) value)))

(defn- enqueue-open! [writer request]
  (locking (:admission-lock writer)
    (when-not (= :open @(:lifecycle writer))
      (fail! ::closed "Durable writer is closed"))
    (.put ^ArrayBlockingQueue (:queue writer) request))
  (await-result (:result request)))

(defn- heartbeat-loop [writer interval-ms ttl-ms]
  (loop []
    (let [{:keys [expires-at fenced?]} @(:lease-state writer)
          now ((:now-ms (:operations writer)))
          wait-ms (long (max 1 (min interval-ms (- expires-at now))))
          signal ((:await-heartbeat! (:operations writer))
                  (:heartbeat-stop writer) wait-ms)]
      (when (and (= :tick signal)
                 (contains? #{:open :closing} @(:lifecycle writer))
                 (not (:fenced? @(:lease-state writer))))
        (let [renew-now ((:now-ms (:operations writer)))]
          (if (>= renew-now expires-at)
            (swap! (:lease-state writer) assoc :fenced? true)
            (try
              (when (and (not (realized? (:heartbeat-stop writer)))
                         (contains? #{:open :closing}
                                    @(:lifecycle writer))
                         (not (:fenced? @(:lease-state writer))))
                (let [renewed-expiry (max (inc expires-at)
                                          (+ renew-now ttl-ms))
                      result ((:renew! (:operations writer))
                              (:store writer) (:token writer) renewed-expiry)]
                  (reset! (:lease-state writer)
                          {:expires-at (get-in (:head result)
                                               ["lease" "expires_at"])
                           :fenced? false})))
              (catch Throwable error
                (when (or (= ::control/lease-fenced (:type (ex-data error)))
                          (>= ((:now-ms (:operations writer))) expires-at))
                  (swap! (:lease-state writer) assoc :fenced? true))))))
        (when-not (:fenced? @(:lease-state writer))
          (recur))))))

(defn start!
  "Start the serialized operation worker for an acquired, recovered writer.

  This is the composition seam for the later public Durable `open!`. The
  caller retains responsibility for recovery and must not use `handle` outside
  this writer after start. Optional operation functions exist for deterministic
  conformance tests; production callers should use the defaults."
  [{:keys [store token handle database queue-capacity operations
           lease-expiry lease-ttl-ms heartbeat-interval-ms retry-options]
    :or {queue-capacity default-queue-capacity}}]
  (when-not store (fail! ::invalid-options "store is required"))
  (when-not (map? token) (fail! ::invalid-options "token is required"))
  (when-not handle (fail! ::invalid-options "handle is required"))
  (require-string! database "database")
  (require-positive-int! queue-capacity "queue-capacity")
  (when-not (= (nil? lease-expiry) (nil? lease-ttl-ms))
    (fail! ::invalid-options
           "lease-expiry and lease-ttl-ms must be supplied together"))
  (when lease-ttl-ms
    (require-positive-int! lease-ttl-ms "lease-ttl-ms")
    (require-positive-int! heartbeat-interval-ms "heartbeat-interval-ms")
    (when (> heartbeat-interval-ms (quot lease-ttl-ms 3))
      (fail! ::invalid-options
             "heartbeat-interval-ms must not exceed one third of lease-ttl-ms")))
  (let [configured-operations operations
        now-ms (or (:now-ms configured-operations)
                   #(System/currentTimeMillis))
        lease-state (when lease-expiry
                      (atom {:expires-at lease-expiry :fenced? false}))
        retry-options (assoc (or retry-options {})
                             :stopped?
                             #(boolean (retry-stopped? lease-state now-ms)))
        operations
        (merge
         {:analyze-query! policy/analyze-query!
          :analyze-execute! policy/analyze-execute!
          :classification-sql! chdb/classification-sql
          :classify! native/classify-query!
          :query-native! (fn [handle sql params]
                           (chdb/execute-any handle sql params))
          :query-bytes-native! chdb/execute-query-bytes-handle
          :execute-native! (fn [handle sql params]
                             (chdb/execute-any handle sql params))
          :publish-wal!
          (fn [store token payload]
            (control/publish-wal-bytes! store token payload retry-options))
          :publish-checkpoint!
          (fn [store token path]
            (control/publish-checkpoint-file!
             store token path retry-options))
          :commit-reference!
          (fn [store token options]
            (control/commit-reference!
             store token (merge options retry-options)))
          :verify-checkpoint-reference! control/verify-file-reference!
          :create-checkpoint!
          (fn [_ _]
            (fail! ::checkpoint-unavailable
                   "This writer has no checkpoint archive provider"))
          :delete-checkpoint! (fn [_] nil)
          :renew! (fn [store token expires-at]
                    (control/renew! store token expires-at retry-options))
          :release! (fn [store token]
                      (control/release! store token retry-options))
          :now-ms now-ms
          :await-heartbeat! (fn [stop timeout-ms]
                              (if (= ::tick (deref stop timeout-ms ::tick))
                                :tick
                                :stop))
          :take-request! (fn [queue]
                           (.take ^ArrayBlockingQueue queue))
          :close-native! native/close!
          :cleanup-scratch! (fn [] nil)}
         configured-operations)
        required #{:analyze-query! :analyze-execute! :classification-sql!
                   :classify! :query-native!
                   :query-bytes-native!
                   :execute-native! :publish-wal! :publish-checkpoint!
                   :commit-reference! :verify-checkpoint-reference!
                   :create-checkpoint! :delete-checkpoint! :renew!
                   :release! :now-ms :await-heartbeat!
                   :take-request!
                   :close-native! :cleanup-scratch!}]
    (when-not (every? #(fn? (get operations %)) required)
      (fail! ::invalid-options "writer operations must be functions"))
    (let [worker (owned-thread/completion)
          heartbeat (when lease-expiry (owned-thread/completion))
          writer (->DurableWriter
                  store token handle database
                  (ArrayBlockingQueue. queue-capacity) (Object.)
                  (atom :open) (promise)
                  (atom {:lines [] :byte-count 0
                         :checkpoint-required? false})
                  lease-state
                  (promise) operations worker heartbeat)]
      (owned-thread/start! worker #(worker-loop writer))
      (when heartbeat
        (owned-thread/start!
         heartbeat
         #(heartbeat-loop writer heartbeat-interval-ms lease-ttl-ms)))
      writer)))

(defn query!
  ([writer sql] (query! writer sql []))
  ([writer sql params]
   (enqueue-open! writer {:op :query :sql sql :params params
                          :result (promise)})))

(defn query-bytes! [writer sql params options]
  (enqueue-open! writer {:op :query-bytes :sql sql :params params
                         :options options :result (promise)}))

(defn execute! [writer sql]
  (enqueue-open! writer {:op :execute :sql sql :result (promise)}))

(defn sql! [writer sql params]
  (enqueue-open! writer {:op :sql :sql sql :params params :result (promise)}))

(defn flush! [writer]
  (enqueue-open! writer {:op :flush :result (promise)}))

(defn checkpoint! [writer]
  (enqueue-open! writer {:op :checkpoint :result (promise)}))

(defn close!
  "Stop admission, drain prior requests, flush, release, and close exactly once."
  [writer]
  (let [request {:op :close :result (promise)}
        disposition
        (locking (:admission-lock writer)
          (case @(:lifecycle writer)
            :open (do
                    (reset! (:lifecycle writer) :closing)
                    (try
                      (.put ^ArrayBlockingQueue (:queue writer) request)
                      :owner
                      (catch Throwable error
                        ;; A failed or interrupted admission did not enqueue
                        ;; the close request. Restore the only state from which
                        ;; another caller can safely retry cleanup.
                        (reset! (:lifecycle writer) :open)
                        (throw error))))
            :closing :wait
            :closed :wait))]
    (if (= :owner disposition)
      (await-result (:result request))
      (let [{:keys [value error]} @(:closed-result writer)]
        (if error (throw error) value)))))

(defn status [writer]
  (let [{:keys [lines byte-count checkpoint-required?]}
        @(:wal-state writer)]
    {:lifecycle @(:lifecycle writer)
     :writable? (not (true? (:fenced? (some-> (:lease-state writer) deref))))
     :pending-wal-bytes byte-count
     :pending-statements (count lines)
     :checkpoint-required? (boolean checkpoint-required?)}))
