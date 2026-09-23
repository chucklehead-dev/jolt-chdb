(ns jdbc.chdb.durable.writer
  "Serialized Durable V1 writer operations after lease acquisition and recovery.

  `start!` deliberately does not open or recover an object. It accepts an
  already-acquired fencing token and an already-recovered engine handle, then
  owns the public query/execute/flush/close queue for that live writer."
  (:require [jdbc.chdb :as chdb]
            [jdbc.chdb.durable.backend :as backend]
            [jdbc.chdb.durable.control :as control]
            [jdbc.chdb.durable.observation :as observation]
            [jdbc.chdb.durable.owned-thread :as owned-thread]
            [jdbc.chdb.durable.policy :as policy]
            [jdbc.chdb.durable.time-domain :as time-domain]
            [jdbc.chdb.durable.wal :as wal]
            [jdbc.chdb.native :as native])
  (:import [java.io BufferedOutputStream]
           [java.nio.file Files OpenOption Path]
           [java.nio.file.attribute FileAttribute PosixFilePermissions]
           [java.util.concurrent ArrayBlockingQueue]))

(def max-statement-bytes (* 64 1024 1024))
(def max-wal-segment-bytes (* 128 1024 1024))
(def default-queue-capacity 64)

(defrecord DurableWriter
    [store token handle database queue admission-lock lifecycle closed-result
     wal-state lease-state heartbeat-stop backend-context retry-options
     operations worker heartbeat persistence-observation wal-spool-parent
     checkpoint-wal-reference-threshold])

(defn- fail! [type message]
  (throw (ex-info message {:type type})))

(defn- call-with-backend-context [writer f]
  (try
    (backend/call-with-operation-context (:backend-context writer) f)
    (catch Throwable error
      (if (= ::backend/operation-stopped (:type (ex-data error)))
        (do
          (observation/unavailable! (:persistence-observation writer))
          (fail! ::control/lease-fenced
                 "The Durable writer cannot prove a live lease"))
        (throw error)))))

(defn- require-string! [value label]
  (when-not (string? value)
    (fail! ::invalid-options (str label " must be a string")))
  value)

(defn- require-positive-int! [value label]
  (when-not (and (integer? value) (pos? value))
    (fail! ::invalid-options (str label " must be a positive integer")))
  value)

(defn- fence-invalid-time! [lease-state]
  (when lease-state
    (swap! lease-state assoc :fenced? true)
    (when-let [persistence-observation (:persistence-observation @lease-state)]
      (observation/unavailable! persistence-observation)))
  (fail! ::control/lease-fenced
         "The Durable writer cannot prove a live lease"))

(defn- sample-now-ms! [lease-state now-ms]
  (let [value (try
                (now-ms)
                (catch Throwable _
                  (fence-invalid-time! lease-state)))]
    (when-not (time-domain/supported-nonnegative-milliseconds? value)
      (fence-invalid-time! lease-state))
    value))

(defn- renewal-expiry! [lease-state expires-at renew-now ttl-ms]
  (let [value (max (inc expires-at) (+ renew-now ttl-ms))]
    (when-not (time-domain/supported-millisecond-magnitude? value)
      (fence-invalid-time! lease-state))
    value))

(def ^:private writer-phase-labels
  #{:wal-prepare :wal-append :wal-join})

(defn- observed-writer-phase
  "Run `f` while optionally emitting a closed-vocabulary scalar timing event.

  The observer is deliberately best-effort: it cannot change writer control
  flow, and it receives neither SQL/WAL payloads nor backend identities."
  [observe! phase bytes f]
  (if (and observe! (contains? writer-phase-labels phase))
    (let [started (System/nanoTime)]
      (try
        (let [value (f)]
          (try (observe! {:phase phase :status :complete :calls 1
                          :nanos (max 0 (- (System/nanoTime) started))
                          :bytes (max 0 (or bytes 0))})
               (catch Throwable _))
          value)
        (catch Throwable primary
          (try (observe! {:phase phase :status :failed :calls 1
                          :nanos (max 0 (- (System/nanoTime) started))
                          :bytes (max 0 (or bytes 0))})
               (catch Throwable _))
          (throw primary))))
    ;; Keep the ordinary production path free of clock reads.
    (f)))

(def ^:private private-file-attributes
  (into-array
   FileAttribute
   [(PosixFilePermissions/asFileAttribute
     (PosixFilePermissions/fromString "rw-------"))]))

(defn- allocate-wal-spool!
  "Allocate an empty, private staged WAL file below `scratch`.

   The descriptor deliberately contains only a path and scalar counters.  Its
   caller owns the buffered output stream and every close/delete transition.
   This is staging for publication, not a crash-local durability claim."
  [scratch]
  (let [parent (if (instance? Path scratch)
                 scratch
                 (java.nio.file.Paths/get (str scratch) (make-array String 0)))]
    (when-not (Files/isDirectory parent (make-array java.nio.file.LinkOption 0))
      (fail! ::invalid-options "Durable WAL spool parent must be a directory"))
    {:path (Files/createTempFile parent "wal-" ".jsonl" private-file-attributes)
     :byte-count 0
     :statement-count 0
     :sealed? false}))

(defn- open-wal-spool!
  [scratch]
  (let [spool (allocate-wal-spool! scratch)
        output (BufferedOutputStream.
                (Files/newOutputStream ^Path (:path spool)
                                       (make-array OpenOption 0)))]
    (assoc spool :output output)))

(defn- wal-line [sql]
  ;; Retain this narrow seam: writer tests inject an encoding failure here so
  ;; they cover the native and portable selectors without coupling to data.json.
  (wal/line sql))

(defn- ensure-open-wal-spool!
  "Allocate the current append target before native execution.

   The worker is serialized, so the state transition and subsequent native
   call have one owner.  Allocation therefore cannot turn a successful native
   mutation into an untracked record."
  [writer]
  (when-not (:spool @(:wal-state writer))
    (swap! (:wal-state writer)
           (fn [state]
             (if (:spool state)
               state
               (assoc state :spool (open-wal-spool! (:wal-spool-parent writer)))))))
  (:spool @(:wal-state writer)))

(defn- append-wal! [writer line]
  (let [spool (ensure-open-wal-spool! writer)]
    (when (:sealed? spool)
      (fail! ::wal-sealed
             "A sealed Durable WAL spool cannot accept another statement"))
    ;; Write first, then advance scalar counters. A write exception leaves the
    ;; record uncounted and lets the caller require a checkpoint for the
    ;; already-successful native mutation.
    (.write ^BufferedOutputStream (:output spool) line)
    (swap! (:wal-state writer)
           (fn [state]
             (-> state
                 (update :byte-count + (alength line))
                 (update :statement-count inc)
                 (update :spool
                         #(-> %
                              (update :byte-count + (alength line))
                              (update :statement-count inc))))))))

(defn- seal-wal! [writer]
  (let [spool (:spool @(:wal-state writer))]
    (when (and spool (not (:sealed? spool)))
      ;; A spool is only eligible for immutable publication after both buffered
      ;; bytes and the stream close have succeeded. No fsync/crash-local
      ;; durability property is claimed here.
      (let [flush-error (try (.flush ^BufferedOutputStream (:output spool))
                             nil
                             (catch Throwable error error))
            close-error (try (.close ^BufferedOutputStream (:output spool))
                             nil
                             (catch Throwable error error))]
        (swap! (:wal-state writer) update :spool assoc :sealed? true :output nil)
        (when-let [error (or flush-error close-error)]
          (throw error))))
    (:spool @(:wal-state writer))))

(defn- delete-wal-spool! [spool]
  (when-let [output (:output spool)]
    (try (.close ^BufferedOutputStream output) (catch Throwable _)))
  (when-let [path (:path spool)]
    (try (Files/deleteIfExists ^Path path) (catch Throwable _)))
  nil)

(defn- clear-wal! [writer]
  ;; The confirmed/reconciled control transition is authoritative. Forget the
  ;; staged state before best-effort local deletion so cleanup failure cannot
  ;; make a committed WAL appear retryable.
  (let [spool (:spool @(:wal-state writer))]
    (reset! (:wal-state writer)
            {:spool nil :byte-count 0 :statement-count 0
             :checkpoint-required? false})
    (delete-wal-spool! spool)))

(defn- require-checkpoint! [writer]
  (swap! (:wal-state writer) assoc :checkpoint-required? true))

(defn- assert-writable! [writer]
  (when-let [lease-state (:lease-state writer)]
    (let [{:keys [expires-at fenced?]} @lease-state
          now (sample-now-ms! lease-state (:now-ms (:operations writer)))]
      (when (or fenced? (>= now expires-at))
        (swap! lease-state assoc :fenced? true)
        (observation/unavailable! (:persistence-observation writer))
        (fail! ::control/lease-fenced
               "The Durable writer cannot prove a live lease")))))

(defn- retry-stopped? [lease-state now-ms persistence-observation]
  (when lease-state
    (let [{:keys [expires-at fenced?]} @lease-state
          stopped? (or fenced?
                       (>= (sample-now-ms! lease-state now-ms) expires-at))]
      (when stopped?
        (swap! lease-state assoc :fenced? true)
        (observation/unavailable! persistence-observation))
      stopped?)))

(defn- do-query! [writer sql params]
  (require-string! sql "sql")
  (let [analysis
        ((:analyze-query! (:operations writer))
         (:handle writer)
         ((:classification-sql! (:operations writer)) sql params)
         (:database writer))]
    (policy/call-with-query-error-redaction
     analysis
     #((:query-native! (:operations writer)) (:handle writer) sql params))))

(defn- do-query-bytes! [writer sql params options]
  (require-string! sql "sql")
  (let [analysis
        ((:analyze-query! (:operations writer))
         (:handle writer)
         ((:classification-sql! (:operations writer)) sql params)
         (:database writer))]
    (policy/call-with-query-error-redaction
     analysis
     #((:query-bytes-native! (:operations writer))
       (:handle writer) sql params options))))

(defn- exact-statement-bytes-exceed? [^String sql ^long limit]
  (> (alength (.getBytes sql "UTF-8")) limit))

(defn- statement-bytes-exceed?
  "Whether `sql` encodes to more than `limit` UTF-8 bytes.

  Jolt indexes Unicode scalar values, each of which occupies one to four UTF-8
  bytes. The JVM indexes UTF-16 code units; valid pairs need four bytes for two
  units and an isolated surrogate is replaced within the same conservative
  bound. Character count therefore proves the common far-from-limit cases. An
  exact encoding remains authoritative inside the narrow uncertain band."
  [^String sql ^long limit]
  (let [characters (.length sql)]
    (cond
      (> characters limit) true
      (<= characters (quot limit 4)) false
      :else (exact-statement-bytes-exceed? sql limit))))

(defn- validate-statement-size! [sql]
  (when (statement-bytes-exceed? sql max-statement-bytes)
    (fail! ::limit-exceeded "Durable SQL statement exceeds 64 MiB")))

(defn- prepare-wal-line! [writer sql]
  (observed-writer-phase
   (:writer-phase! (:operations writer)) :wal-prepare 0
   #(do
      (validate-statement-size! sql)
      (let [spool (ensure-open-wal-spool! writer)
            ;; A failed immutable publication deliberately retains a sealed
            ;; spool so a later flush can retry the same exact file.  It is
            ;; not an append target: reject the request at admission, before
            ;; it can reach the native mutation below.
            _ (when (:sealed? spool)
                (fail! ::wal-sealed
                       "A sealed Durable WAL spool cannot accept another statement"))
            line (wal-line sql)
            next-segment-bytes (+ (:byte-count @(:wal-state writer))
                                  (alength line))]
        (when (> next-segment-bytes max-wal-segment-bytes)
          (fail! ::limit-exceeded "Durable WAL segment would exceed 128 MiB"))
        line))))

(defn- clearly-read-only-sql?
  "Whether a request can remain on the read-only `sql!` path after WAL retry.

   A sealed spool is retained only after a failed immutable publication.  The
   writer must not execute any new mutation until that exact spool has been
   retried or checkpointed.  This deliberately recognizes only the ordinary
   single-statement read prefixes; ambiguous forms are conservatively refused
   without invoking the classifier.  `query!` retains its existing behavior."
  [^String sql]
  (boolean (re-find #"(?is)^\s*(?:select|show|describe|desc|explain)\b" sql)))

(defn- reject-sealed-wal! [writer]
  (when (get-in @(:wal-state writer) [:spool :sealed?])
    (fail! ::wal-sealed
           "A sealed Durable WAL spool cannot accept another statement")))

(defn- reject-sealed-wal-mutation! [writer sql]
  (when-not (clearly-read-only-sql? sql)
    (reject-sealed-wal! writer)))

(defn- prepare-execution! [writer sql params]
  (let [operations (:operations writer)
        prepare-query! (:prepare-query! operations)
        execute-prepared-native! (:execute-prepared-native! operations)]
    (if (and prepare-query! execute-prepared-native!)
      (let [prepared (prepare-query! sql params)]
        {:prepared prepared
         :classification-sql (chdb/prepared-sql prepared)
         :execute! #(execute-prepared-native! (:handle writer) prepared)})
      ;; Preserve the raw start! test seam and third-party operation overrides.
      ;; Public Durable open supplies the paired prepared-query operations.
      {:classification-sql
       ((:classification-sql! operations) sql params)
       :execute!
       #((:execute-native! operations) (:handle writer) sql params)})))

(defn- execute-admitted! [writer execute! line]
  ;; Make the public projection conservative before the native mutation. A
  ;; native exception does not in general prove that no mutation happened.
  (observation/pending! (:persistence-observation writer))
  (let [result (execute!)]
    ;; Local failure must not create recovery state. An exact materialized
    ;; statement enters V1 WAL; a bound mutation instead requires a full
    ;; checkpoint because V1 has no typed-parameter WAL record.
    (if line
      (try
        (observed-writer-phase
         (:writer-phase! (:operations writer)) :wal-append (alength line)
         #(append-wal! writer line))
        (catch Throwable error
          ;; Native execution has returned, but the exact replay line is not
          ;; proven staged. A later checkpoint is the only permitted recovery
          ;; path; do not publish this spool as a WAL segment.
          (require-checkpoint! writer)
          (throw error)))
      (require-checkpoint! writer))
    result))

(defn- do-execute! [writer sql]
  (assert-writable! writer)
  (require-string! sql "sql")
  ;; Raw execute is always a mutating API. Reject retained publication work
  ;; before even emitting a WAL-prepare timing event or validating/encoding
  ;; a potential next record.
  (reject-sealed-wal! writer)
  (let [line (prepare-wal-line! writer sql)]
    (if-let [with-buffer! (:with-native-admitted-buffer! (:operations writer))]
      (with-buffer!
       (:handle writer) sql (:database writer)
       (fn [analysis execute!]
         (policy/authorize-execute! analysis)
         (execute-admitted! writer execute! line)))
      (do
        ((:analyze-execute! (:operations writer))
         (:handle writer) sql (:database writer))
        (execute-admitted!
         writer
         #((:execute-native! (:operations writer)) (:handle writer) sql [])
         line)))))

(declare do-flush!)

(defn- do-execute-and-flush! [writer sql]
  ;; This is deliberately one worker request, rather than composition of the
  ;; public execute! and flush! calls.  That makes the caller's local mutation
  ;; adjacent to the confirmed/reconciled publication which settles it; another
  ;; caller cannot contribute work between those two halves.
  (do-execute! writer sql)
  (do-flush! writer))

(defn- apply-sql-analysis! [writer sql params analysis execute!]
  (case (:query-class analysis)
    :read-only
    (do
      (policy/authorize-query! analysis)
      (policy/call-with-query-error-redaction
       analysis
       #((:query-native! (:operations writer))
         (:handle writer) sql params)))

    :mutating
    (do
      (assert-writable! writer)
      (policy/authorize-execute! analysis)
      (let [line (if (seq params)
                   (do (validate-statement-size! sql) nil)
                   (prepare-wal-line! writer sql))]
        (execute-admitted! writer execute! line)))

    (policy/authorize-query! analysis)))

(defn- do-sql! [writer sql params]
  (require-string! sql "sql")
  (when-not (sequential? params)
    (fail! ::invalid-options "params must be sequential"))
  ;; This occurs before placeholder preparation and native classification for
  ;; every mutation-shaped sql! request.  It keeps the retained exact file as
  ;; the only recovery work and does not disturb ordinary read-only sql! or
  ;; query! requests.
  (reject-sealed-wal-mutation! writer sql)
  (let [{:keys [prepared classification-sql execute!]}
        (prepare-execution! writer sql params)]
    (if-let [with-buffer! (and prepared
                              (:with-native-prepared-buffer! (:operations writer)))]
      (with-buffer!
       (:handle writer) prepared (:database writer)
       (fn [analysis shared-execute!]
         (apply-sql-analysis! writer sql params analysis shared-execute!)))
      (apply-sql-analysis!
       writer sql params
       ((:classify! (:operations writer))
        (:handle writer) classification-sql (:database writer))
       execute!))))

(declare do-checkpoint!)

(defn- commit-ambiguous? [error]
  (loop [current error]
    (when current
      (or (= ::control/commit-ambiguous (:type (ex-data current)))
          (recur (.getCause current))))))

(defn- record-publication! [writer kind reference result]
  (observation/confirmed! (:persistence-observation writer)
                          kind reference result)
  result)

(defn- checkpoint-threshold-reached?
  "Whether committing the staged WAL as another reference would reach the
  configured manifest-WAL limit.

  The threshold is an owner-writer policy, not a new Durable V1 manifest
  transition. The following checkpoint uses the existing full-backup
  transition, which atomically replaces the base and clears WAL only after its
  head CAS is proved."
  [writer]
  (when-let [threshold (:checkpoint-wal-reference-threshold writer)]
    (let [snapshot (or (control/read-head! (:store writer))
                       (fail! ::control/lease-fenced
                              "The Durable head no longer exists"))]
      (>= (inc (count (get-in (:head snapshot) ["manifest" "wal"])))
          threshold))))

(defn- do-flush! [writer]
  (assert-writable! writer)
  (let [{:keys [byte-count checkpoint-required?]} @(:wal-state writer)]
    (cond
      checkpoint-required?
      (do-checkpoint! writer)

      (zero? byte-count)
      {:status :empty}

      (checkpoint-threshold-reached? writer)
      ;; The pending mutations are already present in the serialized owner
      ;; handle. Checkpoint before returning this flush result, so its caller
      ;; still receives one confirmed/reconciled V1 head witness.
      (do-checkpoint! writer)

      :else
      (let [spool
            (try
              (observed-writer-phase
               (:writer-phase! (:operations writer)) :wal-join byte-count
               #(seal-wal! writer))
              (catch Throwable error
                ;; A native mutation is staged only if the spool has a closed,
                ;; exact file image. A failed seal must force checkpoint and
                ;; cannot fall through to immutable WAL publication.
                (require-checkpoint! writer)
                (throw error)))
            [publication committed]
            (try
              (let [publication ((:publish-wal-file! (:operations writer))
                                 (:store writer) (:token writer) (:path spool))
                    committed ((:commit-reference! (:operations writer))
                               (:store writer) (:token writer)
                               {:kind :wal
                                :reference (:reference publication)
                                :verified-publication publication
                                :verify-reference! control/verify-file-reference!})]
                [publication committed])
              (catch Throwable error
                ;; Definite errors leave recovery work pending. Only an
                ;; explicit unprovable control outcome poisons it, and a later
                ;; WAL is intentionally unable to erase that uncertainty.
                (when (commit-ambiguous? error)
                  (observation/unconfirmed! (:persistence-observation writer)))
                (throw error)))]
        ;; Retain the complete pending buffer on every failure. Only a confirmed
        ;; or reconciled head commit proves that replay can recover these writes.
        (clear-wal! writer)
        (record-publication! writer :wal (:reference publication) committed)))))

(defn- do-checkpoint! [writer]
  (assert-writable! writer)
  ((:validate-checkpoint! (:operations writer))
   (:store writer) (:token writer))
  (let [path ((:create-checkpoint! (:operations writer))
              (:handle writer) (:database writer))
        outcome
        (try
          (let [[publication committed]
                (try
                  (let [publication ((:publish-checkpoint! (:operations writer))
                                     (:store writer) (:token writer) path)
                        committed ((:commit-reference! (:operations writer))
                                   (:store writer) (:token writer)
                                   {:kind :checkpoint
                                    :reference (:reference publication)
                                    :verify-reference!
                                    (:verify-checkpoint-reference! (:operations writer))})]
                    [publication committed])
                  (catch Throwable error
                    (when (commit-ambiguous? error)
                      (observation/unconfirmed! (:persistence-observation writer)))
                    (throw error)))]
            ;; The full backup contains every local mutation. Pending statement WAL
            ;; becomes redundant only after the checkpoint head CAS is proved.
            (clear-wal! writer)
            {:result (record-publication! writer :checkpoint
                                          (:reference publication) committed)})
          (catch Throwable error {:primary error}))
        cleanup-error
        (try
          ((:delete-checkpoint! (:operations writer)) path)
          nil
          (catch Throwable error error))]
    (cond
      (:primary outcome) (throw (:primary outcome))
      cleanup-error (throw cleanup-error)
      :else (:result outcome))))

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
  (try
    (let [error
          (first-error
           [#(do-flush! writer)
            #(deliver (:heartbeat-stop writer) :stop)
            #(when-let [heartbeat (:heartbeat writer)]
               (owned-thread/join! heartbeat))
            #((:release! (:operations writer)) (:store writer) (:token writer))
            #((:close-native! (:operations writer)) (:handle writer))
            #(delete-wal-spool! (:spool @(:wal-state writer)))
            #((:cleanup-scratch! (:operations writer)))])]
      (when error (throw error))
      nil)
    (finally
      ;; A close / forced-cleanup path never advertises current persistence,
      ;; even if its best-effort flush established a control witness.
      (observation/unavailable! (:persistence-observation writer)))))

(defn- execute-request! [writer request]
  (call-with-backend-context
   writer
   #(case (:op request)
      :query (do-query! writer (:sql request) (:params request))
      :query-bytes (do-query-bytes! writer (:sql request) (:params request)
                                    (:options request))
      :execute (do-execute! writer (:sql request))
      :execute-and-flush (do-execute-and-flush! writer (:sql request))
      :sql (do-sql! writer (:sql request) (:params request))
      :flush (do-flush! writer)
      :checkpoint (do-checkpoint! writer)
      :close (do-close! writer)
      (fail! ::invalid-operation "Unknown Durable writer operation"))))

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
      (reset! (:lifecycle writer) :closing))
    ;; Forced terminal cleanup may block in flush, heartbeat join, release, or
    ;; native close. Its in-memory evidence becomes unavailable at teardown
    ;; entry, not after those best-effort operations complete.
    (observation/unavailable! (:persistence-observation writer)))
  (try
    (call-with-backend-context writer #(do-close! writer))
    (catch Throwable _))
  (locking (:admission-lock writer)
    (loop []
      (when-let [request (.poll ^ArrayBlockingQueue (:queue writer))]
        (fail-result! (:result request) terminal)
        (recur)))
    (reset! (:lifecycle writer) :closed)
    (observation/unavailable! (:persistence-observation writer))
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
              (observation/unavailable! (:persistence-observation writer))
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
          now (sample-now-ms! (:lease-state writer)
                              (:now-ms (:operations writer)))
          wait-ms (long (max 1 (min interval-ms (- expires-at now))))
          signal ((:await-heartbeat! (:operations writer))
                  (:heartbeat-stop writer) wait-ms)]
      (when (and (= :tick signal)
                 (contains? #{:open :closing} @(:lifecycle writer))
                 (not (:fenced? @(:lease-state writer))))
        (let [renew-now (sample-now-ms! (:lease-state writer)
                                        (:now-ms (:operations writer)))]
          (if (>= renew-now expires-at)
            (do
              (swap! (:lease-state writer) assoc :fenced? true)
              (observation/unavailable! (:persistence-observation writer)))
            (try
              (when (and (not (realized? (:heartbeat-stop writer)))
                         (contains? #{:open :closing}
                                    @(:lifecycle writer))
                         (not (:fenced? @(:lease-state writer))))
                (let [renewed-expiry (renewal-expiry!
                                      (:lease-state writer)
                                      expires-at renew-now ttl-ms)
                      result
                      (call-with-backend-context
                       writer
                       #((:renew! (:operations writer))
                         (:store writer) (:token writer) renewed-expiry
                         (:retry-options writer)))]
                  (if (>= (sample-now-ms! (:lease-state writer)
                                          (:now-ms (:operations writer)))
                          expires-at)
                    (do
                      (swap! (:lease-state writer) assoc :fenced? true)
                      (observation/unavailable! (:persistence-observation writer)))
                    (reset! (:lease-state writer)
                            {:expires-at (get-in (:head result)
                                                 ["lease" "expires_at"])
                             :fenced? false
                             :persistence-observation
                             (:persistence-observation writer)}))))
              (catch Throwable error
                (when (or (= ::control/lease-fenced (:type (ex-data error)))
                          (>= (sample-now-ms! (:lease-state writer)
                                              (:now-ms (:operations writer)))
                              expires-at))
                  (swap! (:lease-state writer) assoc :fenced? true)
                  (observation/unavailable! (:persistence-observation writer)))))))
        (when-not (:fenced? @(:lease-state writer))
          (recur))))))

(defn start!
  "Start the serialized operation worker for an acquired, recovered writer.

  This is the composition seam for the later public Durable `open!`. The
  caller retains responsibility for recovery and must not use `handle` outside
  this writer after start. Optional operation functions exist for deterministic
  conformance tests; production callers should use the defaults. A caller that
  can publish checkpoints through the default control operation must supply the
  running producer's `engine-metadata`. At this raw seam, `lease-expiry`,
  `lease-ttl-ms`, `heartbeat-interval-ms`, and `:now-ms` are all milliseconds.
  The public open layer owns conversion to and from the Protocol V1
  epoch-seconds control seam."
  [{:keys [store token handle database queue-capacity operations
           lease-expiry lease-ttl-ms heartbeat-interval-ms retry-options
           engine-metadata recovered-document wal-spool-parent
           checkpoint-wal-reference-threshold]
    :or {queue-capacity default-queue-capacity}}]
  (when-not store (fail! ::invalid-options "store is required"))
  (when-not (map? token) (fail! ::invalid-options "token is required"))
  (when-not handle (fail! ::invalid-options "handle is required"))
  (require-string! database "database")
  (require-positive-int! queue-capacity "queue-capacity")
  (when (some? checkpoint-wal-reference-threshold)
    (require-positive-int! checkpoint-wal-reference-threshold
                          "checkpoint-wal-reference-threshold"))
  (when-not (= (nil? lease-expiry) (nil? lease-ttl-ms))
    (fail! ::invalid-options
           "lease-expiry and lease-ttl-ms must be supplied together"))
  (when lease-ttl-ms
    (require-positive-int! lease-ttl-ms "lease-ttl-ms")
    (require-positive-int! heartbeat-interval-ms "heartbeat-interval-ms")
    (when (> heartbeat-interval-ms (quot lease-ttl-ms 3))
      (fail! ::invalid-options
             "heartbeat-interval-ms must not exceed one third of lease-ttl-ms")))
  (let [wal-spool-parent (or wal-spool-parent
                             (System/getProperty "java.io.tmpdir"))
        _ (when-not (Files/isDirectory
                      (if (instance? Path wal-spool-parent)
                        wal-spool-parent
                        (java.nio.file.Paths/get (str wal-spool-parent)
                                                 (make-array String 0)))
                      (make-array java.nio.file.LinkOption 0))
            (fail! ::invalid-options "Durable WAL spool parent must be a directory"))
        configured-operations operations
        now-ms (or (:now-ms configured-operations)
                   #(System/currentTimeMillis))
        persistence-observation (observation/start :writer recovered-document)
        lease-state (when lease-expiry
                      (atom {:expires-at lease-expiry :fenced? false
                             :persistence-observation persistence-observation}))
        retry-options (assoc (or retry-options {})
                             :stopped?
                             #(boolean (retry-stopped?
                                        lease-state now-ms persistence-observation)))
        backend-context {:stopped? (:stopped? retry-options)}
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
          :publish-wal-file!
          (fn [store token path]
            (control/publish-wal-file!
             store token path
             (assoc retry-options :phase-observe! (:writer-phase! configured-operations))))
          :publish-checkpoint!
          (fn [store token path]
            (control/publish-checkpoint-file!
             store token path retry-options))
          :validate-checkpoint!
          (fn [store token]
            (when engine-metadata
              (control/validate-checkpoint-metadata!
               store token engine-metadata)))
          :commit-reference!
          (fn [store token options]
            (control/commit-reference!
             store token
             (cond-> (assoc (merge options retry-options)
                            :phase-observe! (:writer-phase! configured-operations))
               (= :checkpoint (:kind options))
               (assoc :engine-metadata engine-metadata))))
          :verify-checkpoint-reference! control/verify-file-reference!
          :create-checkpoint!
          (fn [_ _]
            (fail! ::checkpoint-unavailable
                   "This writer has no checkpoint archive provider"))
          :delete-checkpoint! (fn [_] nil)
          :renew! (fn [store token expires-at operation-retry-options]
                    (control/renew! store token expires-at
                                    operation-retry-options))
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
                   :execute-native! :publish-wal-file! :publish-checkpoint!
                   :commit-reference! :verify-checkpoint-reference!
                   :create-checkpoint! :delete-checkpoint! :renew!
                   :release! :now-ms :await-heartbeat!
                   :take-request!
                   :close-native! :cleanup-scratch!}]
    (when-not (every? #(fn? (get operations %)) required)
      (fail! ::invalid-options "writer operations must be functions"))
    (when (and (some? (:writer-phase! operations))
               (not (fn? (:writer-phase! operations))))
      (fail! ::invalid-options "writer-phase! must be a function"))
    (when (and (some? (:with-native-admitted-buffer! operations))
               (not (fn? (:with-native-admitted-buffer! operations))))
      (fail! ::invalid-options "with-native-admitted-buffer! must be a function"))
    (let [worker (owned-thread/completion)
          heartbeat (when lease-expiry (owned-thread/completion))
          writer (->DurableWriter
                  store token handle database
                  (ArrayBlockingQueue. queue-capacity) (Object.)
                  (atom :open) (promise)
                  (atom {:spool nil :byte-count 0 :statement-count 0
                         :checkpoint-required? false})
                  lease-state
                  (promise) backend-context retry-options operations
                  worker heartbeat persistence-observation wal-spool-parent
                  checkpoint-wal-reference-threshold)]
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

(defn execute-and-flush!
  "Execute one fully materialized Durable mutation and publish its recovery
  state as one serialized writer request.

  The returned value is the confirmed or reconciled publication receipt. A
  failure retains the pending recovery work exactly as `flush!` does. This is
  the caller-atomic alternative to separately calling `execute!` then `flush!`
  on a shared writer."
  [writer sql]
  (enqueue-open! writer {:op :execute-and-flush :sql sql :result (promise)}))

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
                    (observation/unavailable! (:persistence-observation writer))
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
    (owned-thread/join-after!
     (:worker writer)
     #(if (= :owner disposition)
        (await-result (:result request))
        (await-result (:closed-result writer))))))

(defn status [writer]
  (let [{:keys [statement-count byte-count checkpoint-required?]}
        @(:wal-state writer)]
    {:lifecycle @(:lifecycle writer)
     :writable? (not (true? (:fenced? (some-> (:lease-state writer) deref))))
     :pending-wal-bytes byte-count
     :pending-statements statement-count
     :checkpoint-required? (boolean checkpoint-required?)}))

(defn persistence-observation
  "Return the closed redacted persistence projection for this writer.

  The projection is intentionally not the existing operational `status`: it
  has no queue, lease expiry, backend, head, SQL, payload, error, or handle
  information and performs no I/O."
  [writer]
  (observation/projection (:persistence-observation writer)))
