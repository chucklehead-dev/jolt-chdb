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
            [jdbc.chdb.durable.head :as head]
            [jdbc.chdb.durable.policy :as policy]
            [jdbc.chdb.durable.reader :as reader]
            [jdbc.chdb.durable.retry :as retry]
            [jdbc.chdb.durable.time-domain :as time-domain]
            [jdbc.chdb.durable.writer :as writer]
            [jdbc.chdb.native :as native]
            [jdbc.proto :as proto])
  (:import [java.io ByteArrayOutputStream File]
           [java.nio ByteBuffer]
           [java.nio.charset CharacterCodingException Charset CodingErrorAction]
           [java.nio.file CopyOption Files OpenOption Path Paths StandardCopyOption]
           [java.nio.file.attribute FileAttribute PosixFilePermissions]
           [java.util UUID]))

(def reader-backup-format 1)
(def default-lease-ttl-ms 30000)
(def default-clock-skew-ms 0)
(def ^:private milliseconds-per-second 1000M)
(def default-max-attempts 4)
(def default-retry-deadline-ms 5000)
(def default-retry-initial-backoff-ms 10)
(def default-retry-max-backoff-ms 250)

(def ^:private recovery-phase-labels
  #{:base-download :base-hash :wal-download :wal-hash
    :wal-lf-scan :wal-record-buffer :wal-record-copy
    :wal-decode :wal-json-parse :wal-plan-retention
    :wal-replay-classification :wal-replay-native})

(def ^:private recovery-nano-time #(System/nanoTime))

(defn- notify-recovery-phase!
  [observe! phase status started bytes]
  ;; Instrumentation is diagnostic and must never replace a recovery result or
  ;; throwable. Events have a closed label/status vocabulary and scalar values;
  ;; paths, object keys, SQL, payloads, and exception data never cross the seam.
  (when (contains? recovery-phase-labels phase)
    (try
      (observe! {:phase phase
                 :status status
                 :calls 1
                 :nanos (max 0 (- (recovery-nano-time) started))
                 :bytes (max 0 (or bytes 0))})
      (catch Throwable _)))
  nil)

(defn- observed-recovery-phase
  [observe! phase bytes f]
  (if observe!
    (let [started (recovery-nano-time)]
      (try
        (let [value (f)]
          (notify-recovery-phase! observe! phase :complete started bytes)
          value)
        (catch Throwable primary
          (notify-recovery-phase! observe! phase :failed started bytes)
          (throw primary))))
    ;; Keep the default production path free of clock/counter reads.
    (f)))

(def ^:private common-dbspec-keys
  #{:vendor :backend :namespace-backend :object-id :scratch-parent
    :operations :read-only?})

(def ^:private writer-dbspec-keys
  (into common-dbspec-keys
        [:owner :instance :database :lease-ttl-ms :clock-skew-ms
         :heartbeat-interval-ms :force? :max-attempts :retry-deadline-ms
         :retry-initial-backoff-ms :retry-max-backoff-ms
         :checkpoint-wal-reference-threshold]))

(def ^:private snapshot-dbspec-keys
  (conj common-dbspec-keys :expected-normalized-head-sha256))

(def ^:private normalized-head-sha256-pattern #"[0-9a-f]{64}")

(def ^:private private-directory-attributes
  (into-array
   FileAttribute
   [(PosixFilePermissions/asFileAttribute
     (PosixFilePermissions/fromString "rwx------"))]))

(def ^:private atomic-move-options
  (into-array CopyOption [StandardCopyOption/ATOMIC_MOVE]))

(defn- fail! [type message]
  (throw (ex-info message {:type type})))

(defn- validate-recovery-phase-observer! [operations]
  (let [observe! (:recovery-phase! operations)]
    (when (and (some? observe!) (not (fn? observe!)))
      (fail! ::invalid-options "recovery-phase! must be a function")))
  (let [observe! (:writer-phase! operations)]
    (when (and (some? observe!) (not (fn? observe!)))
      (fail! ::invalid-options "writer-phase! must be a function")))
  operations)

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
  (when (some? (:checkpoint-wal-reference-threshold spec))
    (positive-integer! (:checkpoint-wal-reference-threshold spec)
                       "checkpoint-wal-reference-threshold"))
  (when (> (:retry-initial-backoff-ms spec)
           (:retry-max-backoff-ms spec))
    (fail! ::invalid-options
           "retry-initial-backoff-ms must not exceed retry-max-backoff-ms"))
  (when-not (instance? Boolean (:force? spec))
    (fail! ::invalid-options "force? must be boolean"))
  spec)

(defn- validate-expected-normalized-head-sha256! [options]
  (when (contains? options :expected-normalized-head-sha256)
    (when-not (and (string? (:expected-normalized-head-sha256 options))
                   (re-matches normalized-head-sha256-pattern
                               (:expected-normalized-head-sha256 options)))
      ;; A direct reader open has the same fail-closed public contract as the
      ;; dbspec constructor, including explicit nil rejection.
      (fail! ::invalid-options
             "expected-normalized-head-sha256 must be lowercase SHA-256")))
  options)

(defn- validate-snapshot-dbspec! [spec]
  (reject-unknown-dbspec-keys! spec snapshot-dbspec-keys)
  (when-not (true? (:read-only? spec))
    (fail! ::invalid-options "A snapshot dbspec must be read-only"))
  (validate-common-dbspec! spec)
  (validate-expected-normalized-head-sha256! spec))

(defn normalized-head-sha256
  "Return the normalized lowercase SHA-256 for a caller-supplied decoded head.

  This is a pure helper for constructing a snapshot pin. It does not read
  object storage and it cannot disclose a stored head, ETag, or reference."
  [document]
  (head/normalized-sha256 document))

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

(defn- default-cleanup-scratch! [scratch]
  ;; The process-lifetime native anchor still owns scratch/data after the
  ;; public handle closes. Removing that directory underneath the live engine
  ;; is unsafe. An external process owner may remove the scratch tree only
  ;; after this process exits.
  (let [active-path (:path (native/active-storage))
        scratch-data (when scratch
                       (native/canonical-storage-path
                        (str (.resolve ^Path scratch "data"))))]
    (when-not (= active-path scratch-data)
      (delete-tree! scratch)))
  nil)

(defn- require-fresh-default-native-lifetime! [operation-overrides]
  ;; Tests and embedders with a complete :open-native! seam own its lifecycle.
  ;; The production adapter creates a fresh recovery scratch path per open, so
  ;; an existing process anchor must reject before backend reads or lease CAS.
  (when (and (not (every? #(contains? operation-overrides %)
                          [:open-native! :close-native! :cleanup-scratch!]))
             (not= :cold (:phase (native/active-storage))))
    (fail! ::native-process-lifetime-exhausted
           (str "This process already owns a chDB storage lifetime; "
                "open an independent Durable snapshot in a fresh process"))))

(defn- download-reference!
  [store scratch label reference max-bytes reference-kind observe!]
  (let [size (get reference "size")]
    (when (and max-bytes (> size max-bytes))
      (fail! ::limit-exceeded "A Durable recovery object exceeds its limit"))
    (let [attempt (.resolve ^Path scratch
                            (str "." label "-" (UUID/randomUUID) ".part"))
          final (.resolve ^Path scratch label)]
      (try
        (let [download-phase (if (= :base reference-kind)
                               :base-download :wal-download)
              hash-phase (if (= :base reference-kind) :base-hash :wal-hash)
              result
              (observed-recovery-phase
               observe! download-phase size
               #(let [downloaded
                      (backend/download-to-file!
                       store (get reference "key") attempt)]
                  (when-not (= :downloaded (:status downloaded))
                    (fail! ::corrupt "A referenced Durable object is missing"))
                  (when-not (and (= size (:byte-count downloaded))
                                 (= size (Files/size attempt)))
                    (fail! ::corrupt
                           "A Durable recovery object has the wrong size"))
                  downloaded))]
          (observed-recovery-phase
           observe! hash-phase size
           #(when-not (= (get reference "sha256")
                         (digest/sha256-file attempt))
              (fail! ::corrupt
                     "A Durable recovery object has the wrong digest")))
          (Files/move attempt final atomic-move-options)
          final)
        (catch Throwable error
          (Files/deleteIfExists attempt)
          (throw error))))))

(def ^:private wal-read-buffer-bytes (* 64 1024))
(def ^:private replay-plan-wire-byte-limit (* 48 1024 1024))
(def ^:private replay-plan-record-limit 16384)
(def ^:private utf8-charset (Charset/forName "UTF-8"))

(def ^:private strict-utf8-malformed-probes
  [[:stray-continuation (byte-array [(unchecked-byte 0x80)])]
   [:truncated-continuation (byte-array [(unchecked-byte 0xe2)
                                         (unchecked-byte 0x82)])]
   [:invalid-continuation-after-valid-lead
    (byte-array [(unchecked-byte 0xe2) (unchecked-byte 0x28)
                 (unchecked-byte 0xa1)])]
   [:invalid-lead (byte-array [(unchecked-byte 0xff)])]
   [:obsolete-five-byte-lead
    (byte-array [(unchecked-byte 0xf8) (unchecked-byte 0x88)
                 (unchecked-byte 0x80) (unchecked-byte 0x80)
                 (unchecked-byte 0x80)])]
   [:two-byte-overlong (byte-array [(unchecked-byte 0xc0)
                                    (unchecked-byte 0xaf)])]
   [:three-byte-overlong (byte-array [(unchecked-byte 0xe0)
                                      (unchecked-byte 0x80)
                                      (unchecked-byte 0x80)])]
   [:four-byte-overlong (byte-array [(unchecked-byte 0xf0)
                                     (unchecked-byte 0x80)
                                     (unchecked-byte 0x80)
                                     (unchecked-byte 0x80)])]
   [:encoded-surrogate (byte-array [(unchecked-byte 0xed)
                                    (unchecked-byte 0xa0)
                                    (unchecked-byte 0x80)])]
   [:above-unicode-maximum (byte-array [(unchecked-byte 0xf4)
                                        (unchecked-byte 0x90)
                                        (unchecked-byte 0x80)
                                        (unchecked-byte 0x80)])]])

(defn- strict-utf8-decoder-capable? []
  (try
    (let [strict-decoder
          (fn []
            (doto (.newDecoder utf8-charset)
              (.onMalformedInput CodingErrorAction/REPORT)
              (.onUnmappableCharacter CodingErrorAction/REPORT)))
          strict-rejects?
          (fn [bytes]
            (try
              (.decode (strict-decoder) (ByteBuffer/wrap bytes))
              false
              (catch CharacterCodingException _ true)))
          replacement-visible?
          (fn [bytes]
            (not= -1 (.indexOf (String. bytes "UTF-8") (int 0xfffd))))]
      (and (= "β"
              (str (.decode (strict-decoder)
                            (ByteBuffer/wrap (.getBytes "β" "UTF-8")))))
           (= "�" (String. (.getBytes "�" "UTF-8") "UTF-8"))
           (every? (fn [[_ bytes]]
                     (and (replacement-visible? bytes)
                          (strict-rejects? bytes)))
                   strict-utf8-malformed-probes)))
    (catch Throwable _ false)))

(def ^:private strict-utf8-decoder-capable-result
  (delay (strict-utf8-decoder-capable?)))

(defn- require-strict-utf8-decoder-capability! []
  (when-not @strict-utf8-decoder-capable-result
    (fail! ::strict-utf8-decoder-unavailable
           "The running Jolt lacks strict UTF-8 decoder support"))
  true)

(defn- strict-decode-wal-text! [bytes]
  (try
    (let [decoder (.newDecoder utf8-charset)]
      (.onMalformedInput decoder CodingErrorAction/REPORT)
      (.onUnmappableCharacter decoder CodingErrorAction/REPORT)
      (str (.decode decoder (ByteBuffer/wrap bytes))))
    (catch CharacterCodingException _
      (fail! ::corrupt "A Durable WAL is not canonical UTF-8"))))

(defn- decode-wal-text! [bytes]
  ;; Jolt's String byte constructor reaches Chez's native UTF-8 decoder, while
  ;; CharsetDecoder deliberately models the JVM's incremental per-code-point
  ;; loop. The constructor exposes every malformed sequence as U+FFFD. A record
  ;; without that sentinel is therefore canonical immediately; a record with
  ;; it takes the strict path so a legitimate encoded U+FFFD remains accepted
  ;; while replacement caused by malformed input is still rejected.
  (let [text (String. bytes "UTF-8")]
    (if (= -1 (.indexOf text (int 0xfffd)))
      text
      (strict-decode-wal-text! bytes))))

(defn- exact-statement-bytes-exceed? [sql limit]
  (> (alength (.getBytes sql "UTF-8")) limit))

(defn- statement-bytes-exceed? [sql record-wire-bytes limit]
  (and (> record-wire-bytes limit)
       (exact-statement-bytes-exceed? sql limit)))

(defn- decode-wal-record!
  ([text record-wire-bytes]
   (decode-wal-record! text record-wire-bytes nil))
  ([text record-wire-bytes observe!]
   (let [record
         (try
           (observed-recovery-phase
            observe! :wal-json-parse record-wire-bytes
            #(json/read-str text))
           (catch Throwable _
             (fail! ::corrupt "A Durable WAL record cannot be decoded")))
         sql (get record "sql")]
     (when-not (and (map? record) (= #{"sql"} (set (keys record)))
                    (string? sql))
       (fail! ::corrupt "A Durable WAL record is invalid"))
     ;; A decoded JSON string cannot contain more UTF-8 bytes than its complete
     ;; JSON record: quotes, the key, and syntax add bytes, while every escape is
     ;; at least as long as the decoded scalar. Most records therefore prove the
     ;; 64 MiB statement bound from the streaming buffer's size without creating
     ;; and discarding another statement-sized byte array. Only a record already
     ;; above that bound needs the exact fallback.
     (when (statement-bytes-exceed?
            sql record-wire-bytes writer/max-statement-bytes)
       (fail! ::limit-exceeded "A Durable WAL statement exceeds 64 MiB"))
     sql)))

(defn- next-lf-index [^bytes chunk ^long start ^long end]
  ;; The explicit byte-array and primitive-index contract is material on Jolt:
  ;; it lowers the hot read to jolt-vaget instead of generic collection lookup.
  ;; END is returned when no raw LF occurs in the requested range.
  (loop [index start]
    (if (or (= index end)
            (= 10 (bit-and 255 (aget chunk index))))
      index
      (recur (unchecked-inc index)))))

(defn- observed-next-lf-index [observe! chunk start end]
  (if observe!
    (let [started (recovery-nano-time)]
      (try
        (let [index (next-lf-index chunk start end)
              visited (if (= index end)
                        (- end start)
                        (inc (- index start)))]
          (notify-recovery-phase!
           observe! :wal-lf-scan :complete started visited)
          index)
        (catch Throwable primary
          (notify-recovery-phase! observe! :wal-lf-scan :failed started 0)
          (throw primary))))
    (next-lf-index chunk start end)))

(defn- visit-wal!
  "Stream, validate, and visit each record without retaining another record.

  The caller supplies the already size/digest-verified private scratch file.
  A raw LF cannot occur inside a valid JSON string, so byte scanning preserves
  the same JSONL record boundary as the wire format while allowing strict UTF-8
  validation before decoding."
  ([path visit! delay-failure-until-termination?]
   (visit-wal! path visit! delay-failure-until-termination? nil))
  ([path visit! delay-failure-until-termination? observe!]
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
               ;; Zero bytes are the empty JSONL sequence. Readers tolerate that
               ;; noncanonical shape after the reference's size and digest have
               ;; verified, while writers continue to omit empty segments.
               (when (pos? (.size line))
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
                   (loop [start 0 count record-count]
                     (if (= start read-count)
                       count
                       (let [index
                             (observed-next-lf-index
                              observe! chunk start read-count)]
                         (if (= index read-count)
                           (do
                             (observed-recovery-phase
                              observe! :wal-record-buffer (- read-count start)
                              #(.write line chunk start (- read-count start)))
                             count)
                           (do
                             (when (< start index)
                               (observed-recovery-phase
                                observe! :wal-record-buffer (- index start)
                                #(.write line chunk start (- index start))))
                             (try
                               ;; UTF-8 has whole-segment precedence over record
                               ;; parsing and limits in the legacy decoder. Even
                               ;; after remembering a record failure, validate
                               ;; every later record's encoding before EOF.
                               (let [record-wire-bytes (.size line)
                                     record-bytes
                                     (observed-recovery-phase
                                      observe! :wal-record-copy
                                      record-wire-bytes
                                      #(.toByteArray line))
                                     text
                                     (observed-recovery-phase
                                      observe! :wal-decode
                                      record-wire-bytes
                                      #(decode-wal-text! record-bytes))]
                                 (when-not @first-failure
                                   (try
                                     (visit! (decode-wal-record!
                                              text record-wire-bytes observe!)
                                             record-wire-bytes)
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
                             (recur (inc index) (inc count)))))))]
               (recur next-record-count)))))))))

(defn- extend-replay-plan [plan sql record-wire-bytes]
  (when plan
    (let [next-wire-bytes (+ (:wire-bytes plan) record-wire-bytes)
          next-record-count (inc (:record-count plan))]
      (when (and (<= next-wire-bytes replay-plan-wire-byte-limit)
                 (<= next-record-count replay-plan-record-limit))
        (cond-> {:wire-bytes next-wire-bytes
                 :record-count next-record-count
                 :statements (conj (:statements plan) sql)}
          (contains? plan :statement-wire-bytes)
          (assoc :statement-wire-bytes
                 (conj (:statement-wire-bytes plan) record-wire-bytes)))))))

(defn- validate-wal!
  ;; The old whole-file decoder classified an unterminated segment first and
  ;; whole-segment UTF-8 corruption second, before record parsing and limits.
  ;; Retain that ordering while scanning once by remembering failures until the
  ;; final raw byte is known to be LF.
  ([path]
   (validate-wal! path nil))
  ([path observe!]
   (let [plan (atom (cond-> {:wire-bytes 0 :record-count 0 :statements []}
                      observe! (assoc :statement-wire-bytes [])))
         record-count
         (visit-wal!
          path
          (fn [sql record-wire-bytes]
            (observed-recovery-phase
             observe! :wal-plan-retention record-wire-bytes
             #(when @plan
                ;; `nil` is an irreversible bounded fallback for this segment.
                ;; Do not start retaining again after a later small record.
                (swap! plan extend-replay-plan sql record-wire-bytes))))
          true observe!)]
     (merge {:record-count record-count
             :statements (some-> @plan :statements)}
            (select-keys @plan [:statement-wire-bytes])))))

(defn- replay-statement!
  [sql operations handle logical-database record-wire-bytes observe!]
  (observed-recovery-phase
   observe! :wal-replay-classification record-wire-bytes
   #((:analyze-execute! operations) handle sql logical-database))
  (observed-recovery-phase
   observe! :wal-replay-native record-wire-bytes
   #((:execute-native! operations) handle sql [])))

(defn- replay-statements!
  [statements statement-wire-bytes operations handle logical-database observe!]
  (if statement-wire-bytes
    (doseq [[sql record-wire-bytes] (map vector statements statement-wire-bytes)]
      ;; Bytes are the exact JSON record bytes excluding its LF delimiter, the
      ;; same unit reported by the streaming fallback path.
      (replay-statement! sql operations handle logical-database
                         record-wire-bytes observe!))
    ;; Preserve the unobserved plan's old allocation and replay shape.
    (doseq [sql statements]
      (replay-statement! sql operations handle logical-database 0 observe!))))

(defn- replay-wal!
  ([path replay-plan operations handle logical-database]
   (replay-wal! path replay-plan operations handle logical-database nil))
  ([path replay-plan operations handle logical-database observe!]
   (if-some [statements (:statements replay-plan)]
     (replay-statements!
      statements (:statement-wire-bytes replay-plan)
      operations handle logical-database observe!)
     (visit-wal!
      path
      (fn [sql record-wire-bytes]
        (replay-statement!
         sql operations handle logical-database record-wire-bytes observe!))
      false observe!))))

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
   :cleanup-scratch! default-cleanup-scratch!
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
   :execute-prepared-native! chdb/execute-prepared-any
   :recovery-event! (fn [_] nil)})

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
   :execute-native! :recovery-event!])

(defn- observe-recovery! [operations event]
  ;; Recovery evidence is observation-only. A broken observer must not replace
  ;; a storage, validation, engine, or cleanup result.
  (try
    ((:recovery-event! operations) event)
    (catch Throwable _ nil)))

(defn- startup-failed
  "Return the public, stage-local failure envelope for a Durable open call.

  The original exception remains the immediate cause.  In particular, this
  boundary intentionally does not copy its message or ex-data: callers can
  classify the public stage without accidentally treating backend details as a
  stable or redacted API surface."
  [stage cause]
  (ex-info "Durable startup failed"
           {:type ::startup-failed
           :jdbc.chdb.durable/startup-stage stage}
           cause))

(def ^:private closed-startup-stages
  #{:capability :read-head :acquire-lease :create-scratch
    :open-native :recover :renew-lease :start-writer})

(defn- at-startup-stage!
  "Run one operational startup call and expose failures at `stage`.

  The envelope is intentionally total for the operation it owns: even a
  familiar backend/control category can carry unredacted implementation data.
  Input validation and compatibility checks that occur *between* operational
  calls remain outside this helper and retain their established contracts."
  [stage f]
  (when-not (contains? closed-startup-stages stage)
    (throw (IllegalArgumentException.
            (str "Unknown Durable startup stage: " stage))))
  (try
    (f)
    (catch Throwable cause
      (throw (startup-failed stage cause)))))

(defn- recover-snapshot!
  "Restore exactly `document`'s manifest into an already opened private handle."
  [store document operations scratch handle]
  (let [manifest (get document "manifest")
        logical-database (get manifest "db")]
    (if-let [base (get manifest "base")]
      (let [archive (download-reference!
                     store scratch "base.tar.gz" base nil :base
                     (:recovery-phase! operations))]
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
                  writer/max-wal-segment-bytes :wal
                  (:recovery-phase! operations))]
        (observe-recovery!
         operations {:event :durable/wal-integrity-verified :wal-index index})
        ;; A corrupt tail must not leave a prefix applied. Validation retains a
        ;; per-segment replay plan only within strict byte and record caps. A
        ;; larger segment discards the partial plan and uses the original
        ;; bounded second pass. Either path starts engine effects only after the
        ;; complete segment and its established error precedence are valid.
        (let [replay-plan
              (try
                (validate-wal! path (:recovery-phase! operations))
                (catch Throwable error
                  (observe-recovery!
                   operations
                   {:event :durable/wal-validation-failed :wal-index index})
                  (throw error)))]
          (observe-recovery!
           operations
           {:event :durable/wal-validation-complete
            :wal-index index :record-count (:record-count replay-plan)})
          (when (pos? (:record-count replay-plan))
            (observe-recovery!
             operations
             {:event :durable/wal-replay-started
              :wal-index index :record-count (:record-count replay-plan)}))
          (replay-wal!
           path replay-plan operations handle logical-database
           (:recovery-phase! operations)))))
    logical-database))

(defn open-reader!
  "Open one immutable Durable V1 head snapshot without acquiring a lease."
  [{:keys [scratch-parent operations]
    :or {scratch-parent (System/getProperty "java.io.tmpdir")}
    :as options}]
  (validate-expected-normalized-head-sha256! options)
  (require-strict-utf8-decoder-capability!)
  (require-fresh-default-native-lifetime! (or operations {}))
  (let [store (resolve-store! options)
        operations (validate-recovery-phase-observer!
                    (merge (default-open-operations) operations))]
    (doseq [key (concat [:durable-capability :classification-sql!
                         :query-native!
                         :query-bytes-native!]
                        recovery-operation-keys)]
      (required-operation! operations key))
    (let [capability (at-startup-stage!
                      :capability
                      #((:durable-capability operations)))]
      (when-not (= :supported (:status capability))
        (fail! ::engine-incompatible "The running chDB core lacks Durable V1"))
      (let [snapshot (or (at-startup-stage!
                          :read-head
                          #(control/read-head-read-only! store))
                         (fail! ::not-found "The Durable object does not exist"))
            document (:head snapshot)
            scratch (atom nil)
            handle (atom nil)]
        (when (contains? options :expected-normalized-head-sha256)
          ;; This pin is checked against the sole read-only snapshot before any
          ;; scratch, native, download, or recovery side effect.
          (when-not (= (:expected-normalized-head-sha256 options)
                       (normalized-head-sha256 document))
            (fail! ::snapshot-head-mismatch
                   "The Durable snapshot head does not match its expected digest")))
        (check-engine-compatibility! document (:native-version capability))
        (try
          (reset! scratch (at-startup-stage!
                           :create-scratch
                           #((:create-scratch! operations) scratch-parent)))
          (reset! handle (at-startup-stage!
                          :open-native
                          #((:open-native! operations) @scratch)))
          (let [database (at-startup-stage!
                          :recover
                          #(recover-snapshot!
                            store document operations @scratch @handle))]
            (reader/start!
             {:handle @handle :database database
              :recovered-document document
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
  "Acquire, recover, renew, and return a serialized Durable V1 writer.

  Operational startup failures expose only `::startup-failed` and one closed
  `:jdbc.chdb.durable/startup-stage` value: `:capability`, `:read-head`,
  `:acquire-lease`, `:create-scratch`, `:open-native`, `:recover`,
  `:renew-lease`, or `:start-writer`. The original exception is retained as
  the cause for in-process diagnostics but its message and data are not copied
  into the public envelope."
  [{:keys [owner instance database lease-ttl-ms clock-skew-ms force?
           heartbeat-interval-ms scratch-parent operations max-attempts
           retry-deadline-ms retry-initial-backoff-ms retry-max-backoff-ms
           checkpoint-wal-reference-threshold]
    :or {lease-ttl-ms default-lease-ttl-ms
         clock-skew-ms default-clock-skew-ms
         max-attempts default-max-attempts
         retry-deadline-ms default-retry-deadline-ms
         retry-initial-backoff-ms default-retry-initial-backoff-ms
         retry-max-backoff-ms default-retry-max-backoff-ms
         force? false
         scratch-parent (System/getProperty "java.io.tmpdir")}
    :as options}]
  (require-strict-utf8-decoder-capability!)
  (require-fresh-default-native-lifetime! (or operations {}))
  (validate-lease-timing! lease-ttl-ms clock-skew-ms heartbeat-interval-ms)
  (when (some? checkpoint-wal-reference-threshold)
    (positive-integer! checkpoint-wal-reference-threshold
                       "checkpoint-wal-reference-threshold"))
  (let [configured-operations operations
        configured-preparation?
        (contains? configured-operations :prepare-query!)
        configured-prepared-execution?
        (contains? configured-operations :execute-prepared-native!)
        _ (when-not (= configured-preparation?
                       configured-prepared-execution?)
            (fail! ::invalid-options
                   "prepared-query operation overrides must be supplied together"))
        operations (validate-recovery-phase-observer!
                    (merge (default-open-operations) configured-operations))
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
          existing (at-startup-stage! :read-head
                                      #(control/read-head! store))]
      (when existing
        (validate-comparison-domain! (:head existing) clock-skew-ms))
      (let [capability (at-startup-stage!
                        :capability
                        #((:durable-capability operations)))]
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
              acquired (at-startup-stage!
                        :acquire-lease
                        #(control/acquire!
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
                            :min-reader running-version})))
              token (:token acquired)
              document (:head acquired)
              scratch (atom nil)
              handle (atom nil)]
          (try
            (check-engine-compatibility! document running-version)
            (reset! scratch (at-startup-stage!
                             :create-scratch
                             #((:create-scratch! operations) scratch-parent)))
            (reset! handle (at-startup-stage!
                            :open-native
                            #((:open-native! operations) @scratch)))
            (let [logical-database
                  (at-startup-stage!
                   :recover
                   #(recover-snapshot!
                     store document operations @scratch @handle))]
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
                (at-startup-stage!
                 :renew-lease
                 #(control/renew!
                   store token (epoch-ms->seconds renewed-expiry)
                   (assoc retry-options
                          :stopped?
                          #(>= (sample-epoch-milliseconds! operations)
                               current-expiry))))
                (at-startup-stage!
                 :start-writer
                 #(writer/start!
                   {:store store :token token :handle @handle
                  :database logical-database
                    ;; The writer's transient WAL spool is private to this
                    ;; recovered engine lifetime. It is not a public API and
                    ;; does not extend scratch persistence beyond process use.
                    :wal-spool-parent @scratch
                    :checkpoint-wal-reference-threshold
                    checkpoint-wal-reference-threshold
                  :recovered-document document
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
                            (cond-> (assoc (merge commit-options retry-options)
                                           :phase-observe! (:writer-phase! operations))
                              (= :checkpoint (:kind commit-options))
                              (assoc :engine-metadata
                                     {:version running-version
                                      :backup-format reader-backup-format
                                      :min-reader running-version}))))
                         :cleanup-scratch!
                         (fn [] ((:cleanup-scratch! operations) @scratch)))}))))
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

(defn persistence-observation
  "Return closed, redacted persistence evidence for an open Durable JDBC
  connection. This is a pure projection: it does no storage I/O and cannot
  change native execution or Durable control state. It makes no delivery,
  timing, query-planner, object-store, or external-reader freshness claim.
  Closed and non-Durable JDBC connections fail at the extension boundary."
  [connection]
  (shim/extension-operation
   #(let [shim-connection (proto/connection connection)
          {:keys [handle]}
          (shim/read-only-driver-context shim-connection :chdb-durable)]
      (if (reader/reader? handle)
        (reader/persistence-observation handle)
        (writer/persistence-observation handle)))))

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

(defn execute-and-flush!
  "Execute one fully materialized Durable mutation and publish it atomically
  with respect to the writer queue.

  The returned value is the confirmed or reconciled publication receipt. This
  extension is for integrations that must not let another connection user run
  between mutation admission and its persistence barrier. Other driver types
  and read-only Durable connections fail closed."
  [connection sql]
  (shim/extension-operation
   #(writer/execute-and-flush! (jdbc-writer-handle connection) sql)))

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
       :constraints {:active-storage-paths :one-per-process-lifetime
                     :durable-native-lifetimes :one-per-process
                     :mutation-parameters :checkpoint-fallback}
       :schema-sql nil})
    (open-handle [_ spec]
      (let [spec (normalize-jdbc-dbspec! spec)
            common (cond-> {:store (:backend spec)
                            :namespace-backend (:namespace-backend spec)
                            :object-id (:object-id spec)
                            :scratch-parent (or (:scratch-parent spec)
                                                (System/getProperty "java.io.tmpdir"))
                            :operations (:operations spec)}
                     ;; `contains?` distinguishes an omitted optional pin from
                     ;; an explicit nil, which the snapshot constructor rejects.
                     (contains? spec :expected-normalized-head-sha256)
                     (assoc :expected-normalized-head-sha256
                            (:expected-normalized-head-sha256 spec)))]
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
