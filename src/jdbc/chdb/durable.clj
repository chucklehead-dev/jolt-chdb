(ns jdbc.chdb.durable
  "Public Durable V1 reader/writer open and recovery orchestration."
  (:require [clojure.data.json :as json]
            [clojure.string :as str]
            [db.driver :as driver]
            [db.export :as export]
            [db.jdbc-shim :as shim]
            [jdbc.chdb :as chdb]
            [jdbc.chdb.durable.backend :as backend]
            [jdbc.chdb.durable.compatibility :as compatibility]
            [jdbc.chdb.durable.control :as control]
            [jdbc.chdb.durable.digest :as digest]
            [jdbc.chdb.durable.policy :as policy]
            [jdbc.chdb.durable.reader :as reader]
            [jdbc.chdb.durable.retry :as retry]
            [jdbc.chdb.durable.time-domain :as time-domain]
            [jdbc.chdb.durable.writer :as writer]
            [jdbc.chdb.native :as native]
            [jdbc.proto :as proto])
  (:import [java.io ByteArrayOutputStream File]
           [java.nio.file CopyOption Files OpenOption Path Paths StandardCopyOption]
           [java.nio.file.attribute FileAttribute PosixFilePermissions]
           [java.util Arrays UUID]))

(def reader-backup-format 1)
(def default-lease-ttl-ms 30000)
(def default-clock-skew-ms 0)
(def ^:private milliseconds-per-second 1000M)
(def default-max-attempts 4)
(def default-retry-deadline-ms 5000)
(def default-retry-initial-backoff-ms 10)
(def default-retry-max-backoff-ms 250)

(def ^:private common-dbspec-keys
  #{:vendor :backend :namespace-backend :object-id :scratch-parent
    :operations :read-only?})

(def ^:private writer-dbspec-keys
  (into common-dbspec-keys
        [:owner :instance :database :lease-ttl-ms :clock-skew-ms
         :heartbeat-interval-ms :force? :max-attempts :retry-deadline-ms
         :retry-initial-backoff-ms :retry-max-backoff-ms]))

(def ^:private snapshot-dbspec-keys common-dbspec-keys)

(def ^:private private-directory-attributes
  (into-array
   FileAttribute
   [(PosixFilePermissions/asFileAttribute
     (PosixFilePermissions/fromString "rwx------"))]))

(def ^:private atomic-move-options
  (into-array CopyOption [StandardCopyOption/ATOMIC_MOVE]))

(defn- fail! [type message]
  (throw (ex-info message {:type type})))

(defn- nonblank-string! [value label]
  (when-not (and (string? value) (not (str/blank? value)))
    (fail! ::invalid-options (str label " must be a nonblank string")))
  value)

(defn- whole-milliseconds? [value]
  (and (time-domain/finite-number? value)
       (<= Long/MIN_VALUE value Long/MAX_VALUE)
       (zero? (rem value 1))))

(defn- positive-integer! [value label]
  (when-not (and (whole-milliseconds? value) (pos? value))
    (fail! ::invalid-options (str label " must be a positive integer")))
  value)

(defn- nonnegative-integer! [value label]
  (when-not (and (whole-milliseconds? value) (not (neg? value)))
    (fail! ::invalid-options (str label " must be a nonnegative integer")))
  value)

(defn- positive-lease-milliseconds! [value label]
  (when-not (time-domain/supported-positive-milliseconds? value)
    (fail! ::invalid-options
           (str label " must be a positive whole number of milliseconds "
                "in the supported cross-runtime range")))
  value)

(defn- nonnegative-lease-milliseconds! [value label]
  (when-not (time-domain/supported-nonnegative-milliseconds? value)
    (fail! ::invalid-options
           (str label " must be a nonnegative whole number of milliseconds "
                "in the supported cross-runtime range")))
  value)

(defn- validate-lease-timing!
  [lease-ttl-ms clock-skew-ms heartbeat-interval-ms]
  (positive-lease-milliseconds! lease-ttl-ms "lease-ttl-ms")
  (nonnegative-lease-milliseconds! clock-skew-ms "clock-skew-ms")
  (when heartbeat-interval-ms
    (positive-lease-milliseconds! heartbeat-interval-ms
                                  "heartbeat-interval-ms")
    (when (> heartbeat-interval-ms (quot lease-ttl-ms 3))
      (fail! ::invalid-options
             "heartbeat-interval-ms must not exceed one third of lease-ttl-ms"))))

(defn- supported-derived-milliseconds! [value message]
  (when-not (time-domain/supported-millisecond-magnitude? value)
    (fail! ::invalid-options message))
  value)

(defn- sample-epoch-milliseconds! [operations]
  (nonnegative-lease-milliseconds! ((:now-ms operations)) "now-ms"))

(defn- reject-unknown-dbspec-keys! [spec allowed]
  (when (some #(not (contains? allowed %)) (keys spec))
    ;; Configuration keys can be derived from secret-bearing external input.
    ;; Report the shape failure without copying unknown names into public data.
    (fail! ::invalid-options "Durable dbspec contains an unknown option")))

(defn- validate-storage-selection!
  [{:keys [backend namespace-backend object-id]}]
  (cond
    (and backend (or namespace-backend object-id))
    (fail! ::invalid-options
           "Choose either backend or namespace-backend with object-id")

    backend
    (when-not (satisfies? backend/ObjectBackend backend)
      (fail! ::invalid-options "backend must implement the Durable backend contract"))

    (and namespace-backend object-id)
    (do
      (when-not (satisfies? backend/ObjectBackend namespace-backend)
        (fail! ::invalid-options
               "namespace-backend must implement the Durable backend contract"))
      ;; Validate the public object identity without touching provider storage.
      (backend/object-backend namespace-backend object-id)
      nil)

    (or namespace-backend object-id)
    (fail! ::invalid-options
           "namespace-backend and object-id must be supplied together")

    :else
    (fail! ::invalid-options
           "backend or namespace-backend with object-id is required")))

(defn- validate-common-dbspec! [spec]
  (when-not (map? spec)
    (fail! ::invalid-options "Durable chDB requires a map dbspec"))
  (when-not (= "chdb-durable" (:vendor spec))
    (fail! ::invalid-options "Durable dbspec vendor must be chdb-durable"))
  (validate-storage-selection! spec)
  (when-let [scratch-parent (:scratch-parent spec)]
    (nonblank-string! scratch-parent "scratch-parent"))
  (when-let [operations (:operations spec)]
    (when-not (map? operations)
      (fail! ::invalid-options "operations must be a map")))
  spec)

(defn- validate-writer-dbspec! [spec]
  (reject-unknown-dbspec-keys! spec writer-dbspec-keys)
  (when (:read-only? spec)
    (fail! ::invalid-options "A writer dbspec cannot be read-only"))
  (validate-common-dbspec! spec)
  (nonblank-string! (:owner spec) "owner")
  (nonblank-string! (:instance spec) "instance")
  (nonblank-string! (:database spec) "database")
  (validate-lease-timing! (:lease-ttl-ms spec) (:clock-skew-ms spec)
                          (:heartbeat-interval-ms spec))
  (positive-integer! (:max-attempts spec) "max-attempts")
  (positive-integer! (:retry-deadline-ms spec) "retry-deadline-ms")
  (positive-integer! (:retry-initial-backoff-ms spec)
                     "retry-initial-backoff-ms")
  (positive-integer! (:retry-max-backoff-ms spec) "retry-max-backoff-ms")
  (when (> (:retry-initial-backoff-ms spec)
           (:retry-max-backoff-ms spec))
    (fail! ::invalid-options
           "retry-initial-backoff-ms must not exceed retry-max-backoff-ms"))
  (when-not (instance? Boolean (:force? spec))
    (fail! ::invalid-options "force? must be boolean"))
  spec)

(defn- validate-snapshot-dbspec! [spec]
  (reject-unknown-dbspec-keys! spec snapshot-dbspec-keys)
  (when-not (true? (:read-only? spec))
    (fail! ::invalid-options "A snapshot dbspec must be read-only"))
  (validate-common-dbspec! spec))

(defn writer-dbspec
  "Return a validated ordinary JDBC dbspec for one Durable writer.

  Storage is either an already object-scoped `:backend`, or a
  `:namespace-backend` plus `:object-id`. `:owner` and `:database` are required.
  When omitted, `:instance` is a fresh UUIDv4 identity; lease generation remains
  the protocol's ordering and fencing authority."
  [options]
  (when-not (map? options)
    (fail! ::invalid-options "Durable writer options must be a map"))
  (when (or (contains? options :vendor) (contains? options :read-only?))
    (fail! ::invalid-options
           "writer-dbspec owns the vendor and read-only options"))
  (validate-writer-dbspec!
   (merge {:vendor "chdb-durable"
           :instance (str (UUID/randomUUID))
           :lease-ttl-ms default-lease-ttl-ms
           :clock-skew-ms default-clock-skew-ms
           :max-attempts default-max-attempts
           :retry-deadline-ms default-retry-deadline-ms
           :retry-initial-backoff-ms default-retry-initial-backoff-ms
           :retry-max-backoff-ms default-retry-max-backoff-ms
           :force? false}
          options)))

(defn snapshot-dbspec
  "Return a validated ordinary JDBC dbspec for one immutable Durable snapshot.

  The result contains no writer ownership, lease, heartbeat, force, or database
  configuration. It reads exactly the head snapshot observed during open."
  [options]
  (when-not (map? options)
    (fail! ::invalid-options "Durable snapshot options must be a map"))
  (when (or (contains? options :vendor) (contains? options :read-only?))
    (fail! ::invalid-options
           "snapshot-dbspec owns the vendor and read-only options"))
  (validate-snapshot-dbspec!
   (merge {:vendor "chdb-durable" :read-only? true} options)))

(defn- normalize-jdbc-dbspec! [spec]
  (when-not (map? spec)
    (fail! ::invalid-options "Durable chDB requires a map dbspec"))
  (when-not (= "chdb-durable" (:vendor spec))
    (fail! ::invalid-options "Durable dbspec vendor must be chdb-durable"))
  (when-not (contains? #{nil false true} (:read-only? spec))
    (fail! ::invalid-options "read-only? must be boolean"))
  (let [options (dissoc spec :vendor :read-only?)]
    (if (true? (:read-only? spec))
      (snapshot-dbspec options)
      (writer-dbspec options))))

(defn compare-release-versions
  "Compare two chDB releases by numeric release/prerelease precedence."
  [left right]
  (when-not (compatibility/release-version? left)
    (fail! ::engine-incompatible
           "The running chDB release cannot be compared"))
  (when-not (compatibility/release-version? right)
    (fail! ::engine-incompatible
           "The Durable minimum reader release cannot be compared"))
  (compatibility/compare-release-versions left right))

(defn check-engine-compatibility!
  "Fail closed unless this reader can restore the manifest's engine state."
  [document running-version]
  (let [engine (get document "engine")]
    (when (> (get engine "backup_format") reader-backup-format)
      (fail! ::engine-incompatible
             "The Durable backup format requires a newer reader"))
    (when (neg? (compare-release-versions running-version
                                          (get engine "min_reader")))
      (fail! ::engine-incompatible
             "The Durable object requires a newer chDB release"))
    document))

(defn- quote-identifier [database]
  (str "`" (str/replace database "`" "``") "`"))

(defn- default-create-database! [handle database]
  (chdb/execute-any handle
                    (str "CREATE DATABASE IF NOT EXISTS "
                         (quote-identifier database)) []))

(defn- default-use-database! [handle database]
  (chdb/execute-any handle (str "USE " (quote-identifier database)) []))

(defn- default-scratch! [scratch-parent]
  (let [parent (Paths/get scratch-parent (make-array String 0))]
    (when-not (Files/isDirectory parent (make-array java.nio.file.LinkOption 0))
      (fail! ::invalid-options "Durable scratch parent must be a directory"))
    (Files/createTempDirectory parent "jolt-chdb-" private-directory-attributes)))

(defn- delete-tree-pass! [path]
  (let [file (.toFile ^Path path)]
    (when (.exists file)
      (doseq [child (or (.listFiles file) (make-array File 0))]
        (delete-tree-pass! (.toPath child)))
      (Files/deleteIfExists ^Path path))))

(defn- delete-tree! [path]
  (when path
    (loop [attempt 1]
      (let [error (try (delete-tree-pass! path) nil
                       (catch Throwable error error))]
        (cond
          (not (Files/exists ^Path path (make-array java.nio.file.LinkOption 0)))
          nil

          (< attempt 4)
          (do (Thread/yield) (recur (inc attempt)))

          :else
          (throw (ex-info "Durable scratch cleanup failed"
                          {:type ::cleanup-failed
                           :cause-class (some-> error class str)}))))))
  nil)

(defn- download-reference! [store scratch label reference max-bytes]
  (let [size (get reference "size")]
    (when (and max-bytes (> size max-bytes))
      (fail! ::limit-exceeded "A Durable recovery object exceeds its limit"))
    (let [attempt (.resolve ^Path scratch
                            (str "." label "-" (UUID/randomUUID) ".part"))
          final (.resolve ^Path scratch label)]
      (try
        (let [result (backend/download-to-file! store (get reference "key") attempt)]
          (when-not (= :downloaded (:status result))
            (fail! ::corrupt "A referenced Durable object is missing"))
          (when-not (and (= size (:byte-count result))
                         (= size (Files/size attempt)))
            (fail! ::corrupt "A Durable recovery object has the wrong size"))
          (when-not (= (get reference "sha256") (digest/sha256-file attempt))
            (fail! ::corrupt "A Durable recovery object has the wrong digest"))
          (Files/move attempt final atomic-move-options)
          final)
        (catch Throwable error
          (Files/deleteIfExists attempt)
          (throw error))))))

(def ^:private wal-read-buffer-bytes (* 64 1024))

(defn- decode-wal-text! [bytes]
  (let [text (String. bytes "UTF-8")]
    (when-not (Arrays/equals bytes (.getBytes text "UTF-8"))
      (fail! ::corrupt "A Durable WAL is not canonical UTF-8"))
    text))

(defn- decode-wal-record! [text]
  (let [record (try
                 (json/read-str text)
                 (catch Throwable _
                   (fail! ::corrupt
                          "A Durable WAL record cannot be decoded")))
        sql (get record "sql")]
    (when-not (and (map? record) (= #{"sql"} (set (keys record)))
                   (string? sql))
      (fail! ::corrupt "A Durable WAL record is invalid"))
    (when (> (alength (.getBytes sql "UTF-8")) writer/max-statement-bytes)
      (fail! ::limit-exceeded "A Durable WAL statement exceeds 64 MiB"))
    sql))

(defn- visit-wal!
  "Stream, validate, and visit each record without retaining another record.

  The caller supplies the already size/digest-verified private scratch file.
  A raw LF cannot occur inside a valid JSON string, so byte scanning preserves
  the same JSONL record boundary as the wire format while allowing strict UTF-8
  validation before decoding."
  [path visit! delay-failure-until-termination?]
  (with-open [input (Files/newInputStream
                     ^Path path (make-array OpenOption 0))]
    (let [chunk (byte-array wal-read-buffer-bytes)
          line (ByteArrayOutputStream.)
          utf8-failure (atom nil)
          first-failure (atom nil)]
      (loop [record-count 0]
        (let [read-count (.read input chunk)]
          (cond
            (= -1 read-count)
            (do
              (when (or (zero? record-count) (pos? (.size line)))
                (fail! ::corrupt "A Durable WAL is not newline terminated"))
              (when-let [failure @utf8-failure]
                (throw failure))
              (when-let [failure @first-failure]
                (throw failure))
              record-count)

            (zero? read-count)
            ;; A regular-file stream with a non-empty destination should make
            ;; progress or report EOF. Fail closed if a provider violates that
            ;; contract instead of allowing recovery to spin indefinitely.
            (fail! ::corrupt "A Durable WAL could not be read")

            :else
            (let [next-record-count
                  (loop [index 0 start 0 count record-count]
                    (if (= index read-count)
                      (do
                        (when (< start read-count)
                          (.write line chunk start (- read-count start)))
                        count)
                      (if (= 10 (bit-and 255 (aget chunk index)))
                        (do
                          (when (< start index)
                            (.write line chunk start (- index start)))
                          (try
                            ;; UTF-8 has whole-segment precedence over record
                            ;; parsing and limits in the legacy decoder. Even
                            ;; after remembering a record failure, validate the
                            ;; encoding of every later record before EOF.
                            (let [text (decode-wal-text! (.toByteArray line))]
                              (when-not @first-failure
                                (try
                                  (visit! (decode-wal-record! text))
                                  (catch Throwable error
                                    (if delay-failure-until-termination?
                                      (reset! first-failure error)
                                      (throw error))))))
                            (catch Throwable error
                              (if delay-failure-until-termination?
                                (when-not @utf8-failure
                                  (reset! utf8-failure error))
                                (throw error))))
                          (.reset line)
                          (recur (inc index) (inc index) (inc count)))
                        (recur (inc index) start count))))]
              (recur next-record-count))))))))

(defn- validate-wal! [path]
  ;; The old whole-file decoder classified an unterminated segment first and
  ;; whole-segment UTF-8 corruption second, before record parsing and limits.
  ;; Retain that ordering while scanning once by remembering failures until the
  ;; final raw byte is known to be LF.
  (visit-wal! path (fn [_] nil) true))

(defn- replay-wal! [path operations handle logical-database]
  (visit-wal!
   path
   (fn [sql]
     ((:analyze-execute! operations) handle sql logical-database)
     ((:execute-native! operations) handle sql []))
   false))

(defn- required-operation! [operations key]
  (when-not (fn? (get operations key))
    (fail! ::invalid-options "Durable open operations must be functions")))

(defn- resolve-store!
  [{:keys [store namespace-backend object-id]}]
  (cond
    (and store (or namespace-backend object-id))
    (fail! ::invalid-options
           "Choose either store or namespace-backend with object-id")

    store store

    (and namespace-backend object-id)
    (backend/object-backend namespace-backend object-id)

    (or namespace-backend object-id)
    (fail! ::invalid-options
           "namespace-backend and object-id must be supplied together")

    :else
    (fail! ::invalid-options
           "store or namespace-backend with object-id is required")))

(defn- default-open-operations []
  {:now-ms #(System/currentTimeMillis)
   :monotonic-ms! retry/monotonic-ms
   :await-backoff! retry/await-backoff!
   :durable-capability native/durable-capability
   :create-scratch! default-scratch!
   :cleanup-scratch! delete-tree!
   :open-native! (fn [scratch]
                   (native/open!
                    (str (.resolve ^Path scratch "data"))
                    {:backups-allowed-path (str scratch)}))
   :close-native! native/close!
   :restore-database! native/restore-database!
   :create-checkpoint!
   (fn [handle database scratch]
     (let [path (.resolve ^Path scratch
                          (str "checkpoint-" (UUID/randomUUID) ".tar.gz"))]
       (native/backup-database! handle database (str path))
       path))
   :delete-checkpoint! (fn [path] (Files/deleteIfExists ^Path path))
   :create-database! default-create-database!
   :use-database! default-use-database!
   :analyze-query! policy/analyze-query!
   :analyze-execute! policy/analyze-execute!
   :classification-sql! chdb/classification-sql
   :prepare-query! chdb/prepare-query
   :classify! native/classify-query!
   :query-native! (fn [handle sql params]
                    (chdb/execute-any handle sql params))
   :query-bytes-native! chdb/execute-query-bytes-handle
   :execute-native! (fn [handle sql params]
                      (chdb/execute-any handle sql params))
   :execute-prepared-native! chdb/execute-prepared-any})

(defn- epoch-ms->seconds
  "Convert the runtime wall-clock representation to the frozen V1 wire unit."
  [milliseconds]
  (/ (bigdec milliseconds) milliseconds-per-second))

(defn- epoch-seconds->ms
  "Convert a V1 wire expiry to the runtime writer's wall-clock unit."
  [seconds]
  (* (bigdec seconds) milliseconds-per-second))

(defn- validate-comparison-domain! [document clock-skew-ms]
  (when-let [expires-at (get-in document ["lease" "expires_at"])]
    (supported-derived-milliseconds!
     (+ (epoch-seconds->ms expires-at) clock-skew-ms)
     "The stored lease expiry and clock skew exceed the supported comparison range")))

(def ^:private recovery-operation-keys
  [:create-scratch! :cleanup-scratch! :open-native! :close-native!
   :restore-database! :create-database! :use-database! :analyze-execute!
   :execute-native!])

(defn- recover-snapshot!
  "Restore exactly `document`'s manifest into an already opened private handle."
  [store document operations scratch handle]
  (let [manifest (get document "manifest")
        logical-database (get manifest "db")]
    (if-let [base (get manifest "base")]
      (let [archive (download-reference!
                     store scratch "base.tar.gz" base nil)]
        (try
          ((:restore-database! operations)
           handle logical-database (str archive))
          (catch Throwable _
            (fail! ::engine-incompatible
                   "chDB could not restore a compatible Durable archive"))))
      ((:create-database! operations) handle logical-database))
    ((:use-database! operations) handle logical-database)
    (doseq [[index reference] (map-indexed vector (get manifest "wal"))]
      (let [path (download-reference!
                  store scratch (str "wal-" index ".jsonl") reference
                  writer/max-wal-segment-bytes)]
        ;; A corrupt tail must not leave a prefix applied. The first bounded
        ;; pass validates the complete immutable scratch copy without engine
        ;; effects; the second pass revalidates each record before replay.
        (validate-wal! path)
        (replay-wal! path operations handle logical-database)))
    logical-database))

(defn open-reader!
  "Open one immutable Durable V1 head snapshot without acquiring a lease."
  [{:keys [scratch-parent operations]
    :or {scratch-parent (System/getProperty "java.io.tmpdir")}
    :as options}]
  (let [store (resolve-store! options)
        operations (merge (default-open-operations) operations)]
    (doseq [key (concat [:durable-capability :classification-sql!
                         :query-native!
                         :query-bytes-native!]
                        recovery-operation-keys)]
      (required-operation! operations key))
    (let [capability ((:durable-capability operations))]
      (when-not (= :supported (:status capability))
        (fail! ::engine-incompatible "The running chDB core lacks Durable V1"))
      (let [snapshot (or (control/read-head-read-only! store)
                         (fail! ::not-found "The Durable object does not exist"))
            document (:head snapshot)
            scratch (atom nil)
            handle (atom nil)]
        (check-engine-compatibility! document (:native-version capability))
        (try
          (reset! scratch ((:create-scratch! operations) scratch-parent))
          (reset! handle ((:open-native! operations) @scratch))
          (let [database (recover-snapshot!
                          store document operations @scratch @handle)]
            (reader/start!
             {:handle @handle :database database
              :operations
              (assoc operations
                     :cleanup-scratch!
                     (fn [] ((:cleanup-scratch! operations) @scratch)))}))
          (catch Throwable primary
            (when @handle
              (try ((:close-native! operations) @handle) (catch Throwable _)))
            (when @scratch
              (try ((:cleanup-scratch! operations) @scratch) (catch Throwable _)))
            (throw primary)))))))

(defn open-writer!
  "Acquire, recover, renew, and return a serialized Durable V1 writer."
  [{:keys [owner instance database lease-ttl-ms clock-skew-ms force?
           heartbeat-interval-ms scratch-parent operations max-attempts
           retry-deadline-ms retry-initial-backoff-ms retry-max-backoff-ms]
    :or {lease-ttl-ms default-lease-ttl-ms
         clock-skew-ms default-clock-skew-ms
         max-attempts default-max-attempts
         retry-deadline-ms default-retry-deadline-ms
         retry-initial-backoff-ms default-retry-initial-backoff-ms
         retry-max-backoff-ms default-retry-max-backoff-ms
         force? false
         scratch-parent (System/getProperty "java.io.tmpdir")}
    :as options}]
  (writer/require-wal-byte-writer-capability!)
  (validate-lease-timing! lease-ttl-ms clock-skew-ms heartbeat-interval-ms)
  (let [configured-operations operations
        configured-preparation?
        (contains? configured-operations :prepare-query!)
        configured-prepared-execution?
        (contains? configured-operations :execute-prepared-native!)
        _ (when-not (= configured-preparation?
                       configured-prepared-execution?)
            (fail! ::invalid-options
                   "prepared-query operation overrides must be supplied together"))
        operations (merge (default-open-operations) configured-operations)
        ;; A test or embedding that replaces either legacy preparation or
        ;; execution operation must retain its old seam unless it explicitly
        ;; supplies the matching prepared pair as well.
        operations
        (if (and (not configured-preparation?)
                 (or (contains? configured-operations :classification-sql!)
                     (contains? configured-operations :execute-native!)))
          (dissoc operations :prepare-query! :execute-prepared-native!)
          operations)
        required [:now-ms :monotonic-ms! :await-backoff!
                  :durable-capability :create-scratch! :cleanup-scratch!
                  :open-native! :close-native! :restore-database!
                  :create-checkpoint! :delete-checkpoint!
                  :create-database! :use-database! :analyze-query!
                  :analyze-execute! :classification-sql! :classify!
                  :query-native!
                  :query-bytes-native!
                  :execute-native!]]
    (doseq [key required] (required-operation! operations key))
    (let [now-ms (sample-epoch-milliseconds! operations)
          initial-expiry-ms
          (supported-derived-milliseconds!
           (+ now-ms lease-ttl-ms)
           "The observed time and lease TTL exceed the supported epoch range")
          _ (when-not (< initial-expiry-ms
                         time-domain/max-safe-epoch-milliseconds)
              (fail! ::invalid-options
                     "The initial lease expiry lacks renewal headroom in the supported epoch range"))
          store (resolve-store! options)
          existing (control/read-head! store)]
      (when existing
        (validate-comparison-domain! (:head existing) clock-skew-ms))
      (let [capability ((:durable-capability operations))]
        (when-not (= :supported (:status capability))
          (fail! ::engine-incompatible "The running chDB core lacks Durable V1"))
        (let [running-version (:native-version capability)]
          (when existing
            (check-engine-compatibility! (:head existing) running-version))
          (let [retry-options
              {:max-attempts max-attempts
               :retry-deadline-ms retry-deadline-ms
               :retry-initial-backoff-ms retry-initial-backoff-ms
               :retry-max-backoff-ms retry-max-backoff-ms
               :monotonic-ms! (:monotonic-ms! operations)
               :await-backoff! (:await-backoff! operations)}
              now-seconds (epoch-ms->seconds now-ms)
              acquired (control/acquire!
                        store
                        (merge
                         retry-options
                         {:owner owner :instance instance
                          :expires-at
                          (epoch-ms->seconds initial-expiry-ms)
                          :now now-seconds
                          :clock-skew (epoch-ms->seconds clock-skew-ms)
                          :validate-existing-acquire-head!
                          #(validate-comparison-domain! % clock-skew-ms)
                          :force? force?
                          :database database :engine-version running-version
                          :backup-format reader-backup-format
                          :min-reader running-version}))
              token (:token acquired)
              document (:head acquired)
              scratch (atom nil)
              handle (atom nil)]
          (try
            (check-engine-compatibility! document running-version)
            (reset! scratch ((:create-scratch! operations) scratch-parent))
            (reset! handle ((:open-native! operations) @scratch))
            (let [logical-database
                  (recover-snapshot! store document operations @scratch @handle)]
              (let [renew-now (sample-epoch-milliseconds! operations)
                    current-expiry
                    (epoch-seconds->ms
                     (get-in document ["lease" "expires_at"]))
                    renewed-expiry
                    (supported-derived-milliseconds!
                     (max (inc current-expiry) (+ renew-now lease-ttl-ms))
                     "Lease renewal exceeds the supported epoch range")]
                (when (>= renew-now current-expiry)
                  (fail! ::control/lease-fenced
                         "The Durable writer lease expired during recovery"))
                (control/renew!
                 store token (epoch-ms->seconds renewed-expiry)
                 (assoc retry-options
                        :stopped?
                        #(>= (sample-epoch-milliseconds! operations)
                             current-expiry)))
                (writer/start!
                 {:store store :token token :handle @handle
                  :database logical-database
                  :engine-metadata
                  {:version running-version
                   :backup-format reader-backup-format
                   :min-reader running-version}
                  :lease-expiry renewed-expiry
                  :lease-ttl-ms (long lease-ttl-ms)
                  :heartbeat-interval-ms
                  (long (or heartbeat-interval-ms
                            (max 1 (quot lease-ttl-ms 3))))
                  :retry-options retry-options
                  :operations
                  (assoc operations
                         :renew!
                         (fn [store token expiry-ms operation-retry-options]
                           (update-in
                            (control/renew!
                             store token (epoch-ms->seconds expiry-ms)
                             operation-retry-options)
                            [:head "lease" "expires_at"]
                            epoch-seconds->ms))
                         :create-checkpoint!
                         (fn [handle database]
                           ((:create-checkpoint! operations)
                            handle database @scratch))
                         :commit-reference!
                         (fn [store token commit-options]
                           (control/commit-reference!
                            store token
                            (cond-> (merge commit-options retry-options)
                              (= :checkpoint (:kind commit-options))
                              (assoc :engine-metadata
                                     {:version running-version
                                      :backup-format reader-backup-format
                                      :min-reader running-version}))))
                         :cleanup-scratch!
                         (fn [] ((:cleanup-scratch! operations) @scratch)))})))
            (catch Throwable primary
              (when @handle
                (try ((:close-native! operations) @handle) (catch Throwable _)))
              (try (control/release! store token) (catch Throwable _))
              (when @scratch
                (try ((:cleanup-scratch! operations) @scratch) (catch Throwable _)))
              (throw primary)))))))))

(defn connection-role
  "Return `:writer` or `:reader` for an open Durable JDBC connection.

  Other driver types and closed connections fail at the JDBC extension
  boundary. This is a non-publishing capability check for integrations that
  must reject the wrong connection before performing schema or data writes."
  [connection]
  (shim/extension-operation
   #(let [shim-connection (proto/connection connection)
          {:keys [handle]}
          (shim/driver-context shim-connection :chdb-durable)]
      (if (reader/reader? handle) :reader :writer))))

(defn- jdbc-writer-handle [connection]
  (let [shim-connection (proto/connection connection)
        {:keys [handle]}
        (shim/driver-context shim-connection :chdb-durable)]
    (when (reader/reader? handle)
      (fail! ::read-only-required
             "A read-only Durable connection cannot publish state"))
    handle))

(defn flush!
  "Commit the pending recovery state of a Durable JDBC writer connection.

  Materialized mutations publish statement WAL. If a native bound mutation is
  pending, the call publishes a full checkpoint instead. It returns only after
  the manifest CAS is confirmed or reconciled. Other driver types and read-only
  Durable connections fail closed."
  [connection]
  (shim/extension-operation
   #(writer/flush! (jdbc-writer-handle connection))))

(defn checkpoint!
  "Publish and commit a full checkpoint for a Durable JDBC writer connection.

  Pending statement WAL is cleared only after the checkpoint head transition
  is confirmed or reconciled."
  [connection]
  (shim/extension-operation
   #(writer/checkpoint! (jdbc-writer-handle connection))))

(def durable-driver
  "`jdbc.core` adapter for map dbspecs carrying a scoped Durable backend.

  Reads and mutations retain native bound values. Because V1 statement WAL has
  no typed-parameter record, a successful parameterized mutation makes the next
  flush or close publish a full checkpoint."
  (reify driver/Driver
    (descriptor [_]
      {:id :chdb-durable
       :aliases #{"chdb-durable"}
       :uri-prefixes []
       :product-name "ClickHouse (chDB Durable V1)"
       :capabilities {:transactions :none :generated-keys :none
                      :query-bytes chdb/query-bytes-capability}
       :constraints {:active-storage-paths :one-per-process
                     :mutation-parameters :checkpoint-fallback}
       :schema-sql nil})
    (open-handle [_ spec]
      (let [spec (normalize-jdbc-dbspec! spec)
            common {:store (:backend spec)
                    :namespace-backend (:namespace-backend spec)
                    :object-id (:object-id spec)
                    :scratch-parent (or (:scratch-parent spec)
                                        (System/getProperty "java.io.tmpdir"))
                    :operations (:operations spec)}]
        (if (:read-only? spec)
          (open-reader! common)
          (open-writer!
           (merge common
                  {:owner (:owner spec)
                   :instance (:instance spec)
                   :database (:database spec)
                   :lease-ttl-ms (or (:lease-ttl-ms spec)
                                     default-lease-ttl-ms)
                   :clock-skew-ms (or (:clock-skew-ms spec)
                                      default-clock-skew-ms)
                   :heartbeat-interval-ms (:heartbeat-interval-ms spec)
                   :max-attempts (:max-attempts spec)
                   :retry-deadline-ms (:retry-deadline-ms spec)
                   :retry-initial-backoff-ms
                   (:retry-initial-backoff-ms spec)
                   :retry-max-backoff-ms (:retry-max-backoff-ms spec)
                   :force? (boolean (:force? spec))})))))
    (close-handle [_ handle]
      (if (reader/reader? handle)
        (reader/close! handle)
        (writer/close! handle)))
    (execute-handle [_ handle sql params]
      (if (reader/reader? handle)
        (reader/query! handle sql params)
        (writer/sql! handle sql params)))

    export/QueryBytesDriver
    (query-bytes-handle [_ handle sql params options]
      (if (reader/reader? handle)
        (reader/query-bytes! handle sql params options)
        (writer/query-bytes! handle sql params options)))))

(driver/register! durable-driver)
