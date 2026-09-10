(ns jdbc.chdb.durable.control
  "Durable V1 lease fencing and reconciled head-CAS transitions.

  A backend instance is scoped to one Durable object, so the control key is
  always the V1 `head.json`. Every mutating operation rereads and validates
  that document, proves the caller's owner/instance/generation token, and uses
  the returned opaque ETag exactly once."
  (:require [clojure.string :as str]
            [jdbc.chdb.durable.backend :as backend]
            [jdbc.chdb.durable.digest :as digest]
            [jdbc.chdb.durable.head :as head]
            [jdbc.chdb.durable.retry :as retry])
  (:import [java.nio.file Files Path Paths]
           [java.nio.file.attribute FileAttribute PosixFilePermissions]
           [java.security MessageDigest]
           [java.util UUID]))

(def head-key "head.json")
(def ^:private default-commit-attempts 4)

(def forced-live-takeover-event
  "Stable, redacted event name returned after a forced live-lease takeover."
  :durable/forced-live-takeover)

(def protocol-source
  (assoc head/protocol-source :section "state-machine"))

(defn- fail!
  ([type message]
   (fail! type message nil))
  ([type message data]
   ;; Owner, instance, keys, paths, head bytes, and provider results are
   ;; deliberately absent. All can contain deployment identity or secrets.
   (throw (ex-info message (assoc (or data {}) :type type)))))

(defn- finite-number? [value]
  (and (number? value)
       (= value value)
       (not= value ##Inf)
       (not= value ##-Inf)))

(defn- nonblank! [value label]
  (when-not (and (string? value) (not (str/blank? value)))
    (fail! ::invalid-options (str label " must be a nonblank string")))
  value)

(defn- nonnegative-time! [value label]
  (when-not (and (finite-number? value) (not (neg? value)))
    (fail! ::invalid-options (str label " must be a finite nonnegative number")))
  value)

(defn- positive-attempts! [value]
  (when-not (and (integer? value) (pos? value))
    (fail! ::invalid-options "max-attempts must be a positive integer"))
  value)

(defn- retry-budget! [options]
  (try
    (retry/start options)
    (catch Throwable error
      (if (= ::retry/invalid-options (:type (ex-data error)))
        (fail! ::invalid-options (ex-message error))
        (throw error)))))

(defn- definite-retry-result! [result message]
  (case (:status result)
    :done (:value result)
    :stopped (fail! ::lease-fenced "The Durable writer has lost its lease")
    :deadline (fail! ::timeout message)
    :attempt-limit (fail! ::timeout message)))

(defn- ambiguity-aware-retry-result! [result timeout-message ambiguous-message]
  (case (:status result)
    :done (:value result)
    :stopped (fail! ::lease-fenced "The Durable writer has lost its lease")
    :deadline (if (= :reconcile (get-in result [:state :phase]))
                (fail! ::commit-ambiguous ambiguous-message)
                (fail! ::timeout timeout-message))
    :attempt-limit (if (= :reconcile (get-in result [:state :phase]))
                     (fail! ::commit-ambiguous ambiguous-message)
                     (fail! ::timeout timeout-message))))

(defn ownership
  "Return the opaque application-level fencing token carried by an active head."
  [document]
  (let [lease (get document "lease")]
    {:owner (get lease "owner")
     :instance (get lease "instance")
     :generation (get lease "generation")}))

(defn owns?
  "True only when identity and generation all match the current active lease."
  [document token]
  (let [{expected-owner :owner
         expected-instance :instance
         expected-generation :generation} token
        {actual-owner :owner
         actual-instance :instance
         actual-generation :generation} (ownership document)]
    (and (string? actual-owner)
         (= expected-owner actual-owner)
         (= expected-instance actual-instance)
         (= expected-generation actual-generation))))

(defn- assert-owned! [document token]
  (when-not (owns? document token)
    (fail! ::lease-fenced "The Durable writer has lost its lease"))
  document)

(defn read-head!
  "Read and writer-validate the current head with its opaque ETag, or nil."
  [store]
  (when-let [{:keys [bytes etag]} (backend/get-with-etag store head-key)]
    {:head (head/decode bytes :writer)
     :etag etag}))

(defn read-head-read-only!
  "Read and read-only-validate one immutable head snapshot, or nil."
  [store]
  (when-let [{:keys [bytes etag]} (backend/get-with-etag store head-key)]
    {:head (head/decode bytes :read-only)
     :etag etag}))

(defn fresh-head
  "Construct the generation-one active head for a new Durable object."
  [{:keys [owner instance expires-at database engine-version backup-format
           min-reader]}]
  (nonblank! owner "owner")
  (nonblank! instance "instance")
  (nonnegative-time! expires-at "expires-at")
  (nonblank! database "database")
  (nonblank! engine-version "engine-version")
  (when-not (and (integer? backup-format) (not (neg? backup-format)))
    (fail! ::invalid-options "backup-format must be a nonnegative integer"))
  (nonblank! min-reader "min-reader")
  (head/validate!
   {"protocol" {"version" 1 "reader_features" [] "writer_features" []}
    "engine" {"name" "chdb"
              "version" engine-version
              "backup_format" backup-format
              "min_reader" min-reader}
    "lease" {"generation" 1
             "owner" owner
             "instance" instance
             "expires_at" expires-at}
    "manifest" {"db" database "base" nil "wal" [] "seq" 0}}
   :writer))

(defn- released? [document]
  (nil? (get-in document ["lease" "owner"])))

(defn- expired? [document now clock-skew]
  (let [expires-at (get-in document ["lease" "expires_at"])]
    (and (number? expires-at)
         (> now (+ expires-at clock-skew)))))

(defn- forced-live-takeover? [document options]
  (and (:force? options)
       (not (released? document))
       (not (expired? document (:now options) (:clock-skew options)))))

(defn- takeover-warning [generation]
  {:event forced-live-takeover-event
   :severity :warning
   :protocol-version 1
   :lease-generation generation})

(defn- acquired-value [status desired etag token warning]
  {:status status
   :head desired
   :etag etag
   :token token
   :warnings (if warning [warning] [])})

(defn- next-generation [document]
  (let [generation (get-in document ["lease" "generation"])]
    (when (>= generation head/max-safe-integer)
      (fail! ::generation-exhausted
             "The Durable lease generation cannot be incremented safely"))
    (inc generation)))

(defn- active-lease [generation owner instance expires-at]
  {"generation" generation
   "owner" owner
   "instance" instance
   "expires_at" expires-at})

(defn- reread [store]
  (read-head! store))

(defn- encoded-head [document]
  ;; Reconciliation compares the semantic document obtained from stored bytes.
  ;; Canonicalize the intended value through those same bytes first: JSON may
  ;; spell an integral BigDecimal as an integer, and host numeric-map equality
  ;; is not a safe wire-format equivalence test.
  (let [bytes (head/encode document)]
    {:bytes bytes
     :head (head/decode bytes :writer)}))

(defn- acquire-attempt!
  [store options {:keys [phase desired token warning] :as state}]
  (if (= :reconcile phase)
    (if-let [latest (reread store)]
      (if (= desired (:head latest))
        {:status :done
         :value (acquired-value :reconciled desired (:etag latest)
                                token warning)}
        {:status :retry :state state})
      {:status :retry :state state})
    (if-let [snapshot (read-head! store)]
    (let [current (:head snapshot)]
      (when-not (or (released? current)
                    (expired? current (:now options) (:clock-skew options))
                    (:force? options))
        (fail! ::lease-held "Another writer holds the Durable lease"))
      (let [generation (next-generation current)
            warning (when (forced-live-takeover? current options)
                      (takeover-warning generation))
            intended (assoc current "lease"
                            (active-lease generation
                                          (:owner options)
                                          (:instance options)
                                          (:expires-at options)))
            {desired :head bytes :bytes} (encoded-head intended)
            token (ownership desired)
            result (backend/replace-if-match!
                    store head-key bytes (:etag snapshot))]
        (case (:status result)
          :replaced
          {:status :done
           :value (acquired-value :acquired desired (:etag result)
                                  token warning)}
          :ambiguous
          {:status :retry
           :state {:phase :reconcile :desired desired :token token
                   :warning warning}}
          :precondition-failed {:status :retry :state {:phase :cas}}
          (fail! ::backend-contract
                 "The Durable backend returned an unsupported CAS result"))))
    (let [{desired :head bytes :bytes} (encoded-head (fresh-head options))
          result (backend/put-bytes-if-absent! store head-key bytes)]
      (case (:status result)
        :created
        {:status :done
         :value (acquired-value :acquired desired (:etag result)
                                (ownership desired) nil)}
        :ambiguous
        {:status :retry
         :state {:phase :reconcile :desired desired
                 :token (ownership desired) :warning nil}}
        :precondition-failed {:status :retry :state {:phase :cas}}
        (fail! ::backend-contract
               "The Durable backend returned an unsupported create result"))))))

(defn acquire!
  "Acquire a missing, released, expired, or explicitly forced writer lease.

  expires-at, now, and clock-skew use the Protocol V1 wire unit of epoch
  seconds. The public Durable API converts its millisecond configuration at
  this boundary.

  Normal takeover is allowed only after `expires_at + clock-skew`; equality
  remains held. Every acquisition of an existing head increments its
  generation. The returned map always contains `:warnings`; a successful
  forced takeover of a still-live lease returns exactly one redacted warning.
  The bounded retry count covers CAS collisions; it does not wait for a live
  lease."
  [store {:keys [owner instance expires-at now clock-skew force? max-attempts]
          :or {clock-skew 0 force? false max-attempts 4}
          :as options}]
  (nonblank! owner "owner")
  (nonblank! instance "instance")
  (nonnegative-time! expires-at "expires-at")
  (nonnegative-time! now "now")
  (nonnegative-time! clock-skew "clock-skew")
  (positive-attempts! max-attempts)
  (when-not (> expires-at now)
    (fail! ::invalid-options
           "expires-at must be later than the acquisition time"))
  (let [options (assoc options
                       :clock-skew clock-skew
                       :force? force?
                       :max-attempts max-attempts)]
    (ambiguity-aware-retry-result!
     (retry/run! (retry-budget! options)
                 {:phase :cas}
                 (fn [_ state] (acquire-attempt! store options state)))
     "Durable lease acquisition exceeded its retry bounds"
     "Durable lease acquisition could not be proved before its deadline")))

(defn- renew-attempt! [store token expires-at attempt
                       {:keys [phase snapshot] :as state}]
  (if (= :reconcile phase)
    (if-let [latest (reread store)]
      (cond
        (not (owns? (:head latest) token))
        (fail! ::lease-fenced "The Durable writer has lost its lease")

        (>= (get-in (:head latest) ["lease" "expires_at"])
            expires-at)
        {:status :done
         :value {:status :reconciled
                 :head (:head latest) :etag (:etag latest) :token token}}

        :else {:status :retry :state state})
      {:status :retry :state state})
    (let [current (assert-owned! (:head snapshot) token)
        current-expiry (get-in current ["lease" "expires_at"])]
    (when-not (or (> expires-at current-expiry) (> attempt 1))
      (fail! ::invalid-options
             "A heartbeat must extend the current lease expiry"))
    (if (>= current-expiry expires-at)
      {:status :done
       :value {:status :reconciled
               :head current :etag (:etag snapshot) :token token}}
      (let [desired (assoc-in current ["lease" "expires_at"] expires-at)
            {canonical :head bytes :bytes} (encoded-head desired)
            result (backend/replace-if-match!
                    store head-key bytes (:etag snapshot))]
        (case (:status result)
          :replaced
          {:status :done
           :value {:status :committed
                   :head canonical :etag (:etag result) :token token}}

          :ambiguous
          {:status :retry :state {:phase :reconcile}}

          :precondition-failed
          (let [latest (or (reread store)
                           (fail! ::lease-fenced
                                  "The Durable head no longer exists"))]
            (cond
              (not (owns? (:head latest) token))
              (fail! ::lease-fenced "The Durable writer has lost its lease")

              (>= (get-in (:head latest) ["lease" "expires_at"])
                  expires-at)
              {:status :done
               :value {:status :reconciled
                       :head (:head latest) :etag (:etag latest) :token token}}

              :else {:status :retry
                     :state {:phase :cas :snapshot latest}}))

          (fail! ::backend-contract
                 "The Durable backend returned an unsupported CAS result")))))))

(defn renew!
  "Renew the current writer lease without changing its generation.

  expires-at uses the Protocol V1 wire unit of epoch seconds.

  Manifest commits may race the lease CAS. Definite same-owner conflicts retry
  from the latest head. An ambiguous renewal is reconciled when the same token
  still owns a lease whose expiry is at least the requested value, regardless
  of a later manifest-only advance."
  ([store token expires-at]
   (renew! store token expires-at {}))
  ([store token expires-at options]
   (nonnegative-time! expires-at "expires-at")
   (let [initial (or (read-head! store)
                     (fail! ::lease-fenced
                            "The Durable head no longer exists"))]
     (ambiguity-aware-retry-result!
      (retry/run!
       (retry-budget!
        (merge {:max-attempts default-commit-attempts} options))
       {:phase :cas :snapshot initial}
       (fn [attempt snapshot]
         (renew-attempt! store token expires-at attempt snapshot)))
      "Durable lease renewal exceeded its retry bounds"
      "The Durable lease renewal could not be proved before its deadline"))))

(defn- released-by-token? [document token]
  (let [lease (get document "lease")]
    (and (= (:generation token) (get lease "generation"))
         (nil? (get lease "owner"))
         (nil? (get lease "instance"))
         (nil? (get lease "expires_at")))))

(defn- release-attempt! [store token {:keys [phase snapshot] :as state}]
  (if (= :reconcile phase)
    (if-let [latest (reread store)]
      (cond
        (released-by-token? (:head latest) token)
        {:status :done
         :value {:status :reconciled :head (:head latest)
                 :etag (:etag latest) :token nil}}

        (not (owns? (:head latest) token))
        (fail! ::lease-fenced "The Durable writer has lost its lease")

        :else {:status :retry :state state})
      {:status :retry :state state})
    (let [current (assert-owned! (:head snapshot) token)
          desired (assoc current "lease"
                         (active-lease (:generation token) nil nil nil))
          {canonical :head bytes :bytes} (encoded-head desired)
          result (backend/replace-if-match!
                  store head-key bytes (:etag snapshot))]
      (case (:status result)
        :replaced
        {:status :done
         :value {:status :committed :head canonical
                 :etag (:etag result) :token nil}}

        :ambiguous {:status :retry :state {:phase :reconcile}}

        :precondition-failed
        (let [latest (or (reread store)
                         (fail! ::lease-fenced
                                "The Durable head no longer exists"))]
          (cond
            (released-by-token? (:head latest) token)
            {:status :done
             :value {:status :reconciled :head (:head latest)
                     :etag (:etag latest) :token nil}}

            (not (owns? (:head latest) token))
            (fail! ::lease-fenced "The Durable writer has lost its lease")

            :else {:status :retry
                   :state {:phase :cas :snapshot latest}}))

        (fail! ::backend-contract
               "The Durable backend returned an unsupported CAS result")))))

(defn release!
  "Release the current writer lease without changing its generation."
  ([store token] (release! store token {}))
  ([store token options]
   (let [initial (or (read-head! store)
                     (fail! ::lease-fenced
                            "The Durable head no longer exists"))]
     (ambiguity-aware-retry-result!
      (retry/run! (retry-budget! options)
                  {:phase :cas :snapshot initial}
                  (fn [_ state] (release-attempt! store token state)))
      "Durable lease release exceeded its retry bounds"
      "The Durable lease release could not be proved before its deadline"))))

(defn- byte->hex [value]
  (format "%02x" (bit-and 255 value)))

(defn- sha256 [bytes]
  (apply str
         (map byte->hex
              (.digest (MessageDigest/getInstance "SHA-256") bytes))))

(def ^:private private-directory-attributes
  (into-array
   FileAttribute
   [(PosixFilePermissions/asFileAttribute
     (PosixFilePermissions/fromString "rwx------"))]))

(defn- checked-token-generation [token current]
  (let [generation (:generation token)
        current-generation (get-in current ["lease" "generation"])]
    (when-not (and (map? token)
                   (string? (:owner token))
                   (not (str/blank? (:owner token)))
                   (string? (:instance token))
                   (not (str/blank? (:instance token)))
                   (integer? generation)
                   (pos? generation)
                   (<= generation head/max-safe-integer))
      (fail! ::lease-fenced "The Durable writer has no valid lease token"))
    ;; A stale generation may publish an unreachable immutable object, just as
    ;; the Quint transition permits. A future generation cannot have been
    ;; issued by any acquisition and would violate reference canonicality.
    (when (> generation current-generation)
      (fail! ::lease-fenced "The Durable writer generation is not reachable"))
    generation))

(declare verify-byte-reference!)

(defn- reconcile-publication!
  [status verify-reference! verify-created? options]
  (case status
    :created
    (do
      (when verify-created? (verify-reference!))
      :published)
    :precondition-failed
    (do
      (verify-reference!)
      :already-published)
    :ambiguous
    (ambiguity-aware-retry-result!
     (retry/run!
      (retry-budget! options)
      {:phase :reconcile}
      (fn [_ state]
        (try
          (verify-reference!)
          {:status :done :value :reconciled}
          (catch Throwable error
            (if (and (= ::object-unverified (:type (ex-data error)))
                     (= :missing (:reason (ex-data error))))
              {:status :retry :state state}
              (throw error))))))
     "The immutable Durable publication exceeded its retry bounds"
     "The immutable Durable publication outcome is unprovable")
    (fail! ::backend-contract
           "The Durable backend returned an unsupported publication result")))

(defn- unique-object-token []
  (let [uuid (UUID/randomUUID)]
    (when-not (= 4 (.version uuid))
      (fail! ::backend-contract
             "The runtime did not produce a UUIDv4 publication token"))
    (subs (str uuid) 0 8)))

(defn publish-wal-bytes!
  "Publish one bounded statement-WAL object under its exact V1 reference.

  The writer token supplies the remembered generation; the current head
  supplies the next manifest sequence. Publication is immutable and
  idempotent when an existing object verifies against the same size and full
  SHA-256 reference. A stale writer may leave an unreachable immutable object,
  but `commit-reference!` still fences it before verification or head CAS.

  Checkpoint archives must use the later streaming publication API rather than
  this payload-materializing helper."
  ([store token bytes]
   (publish-wal-bytes! store token bytes {}))
  ([store token bytes options]
   (when-not (bytes? bytes)
     (fail! ::invalid-options "WAL publication requires a byte array"))
   (let [snapshot (or (read-head! store)
                      (fail! ::lease-fenced "The Durable head no longer exists"))
         current (:head snapshot)
         generation (checked-token-generation token current)
         sequence (inc (get-in current ["manifest" "seq"]))]
     (when (> sequence head/max-safe-integer)
       (fail! ::sequence-exhausted
              "The Durable manifest sequence cannot be incremented safely"))
     (let [digest (sha256 bytes)
           reference {"key" (str "wal/" generation "-" sequence "-"
                                 (unique-object-token) ".jsonl")
                      "size" (alength bytes)
                      "sha256" digest}
           result (backend/put-bytes-if-absent!
                   store (get reference "key") bytes)]
       {:status (reconcile-publication!
                 (:status result)
                 #(verify-byte-reference! store reference)
                 false options)
        :reference reference
        :etag (:etag result)}))))

(defn verify-byte-reference!
  "Verify a bounded in-memory immutable object against a V1 reference.

  This is suitable for statement WAL objects. Checkpoints must use a streaming
  verifier and pass it to `commit-reference!`; this helper deliberately does
  not pretend a payload-sized allocation is acceptable for archives."
  [store reference]
  (let [bytes (backend/get-bytes store (get reference "key"))]
    (when-not bytes
      (fail! ::object-unverified "The immutable Durable object is missing"
             {:reason :missing}))
    (when-not (= (get reference "size") (alength bytes))
      (fail! ::object-unverified "The immutable Durable object size differs"
             {:reason :integrity}))
    (let [actual (sha256 bytes)]
      (when-not (= (get reference "sha256") actual)
        (fail! ::object-unverified
               "The immutable Durable object digest differs"
               {:reason :integrity})))
    reference))

(defn verify-file-reference!
  "Stream a backend object to private scratch and verify size plus SHA-256."
  [store reference]
  (let [scratch (Files/createTempDirectory
                 "jolt-chdb-verify-" private-directory-attributes)
        path (.resolve ^Path scratch "object.part")]
    (try
      (let [result (backend/download-to-file!
                    store (get reference "key") path)]
        (when-not (= :downloaded (:status result))
          (fail! ::object-unverified
                 "The immutable Durable file object is missing"
                 {:reason (if (= :not-found (:status result))
                            :missing
                            :integrity)}))
        (when-not (and (= (get reference "size") (:byte-count result))
                       (= (get reference "size") (Files/size path))
                       (= (get reference "sha256") (digest/sha256-file path)))
          (fail! ::object-unverified
                 "The immutable Durable file object could not be verified"
                 {:reason :integrity}))
        reference)
      (finally
        (Files/deleteIfExists path)
        (Files/deleteIfExists scratch)))))

(defn publish-checkpoint-file!
  "Conditionally publish one full checkpoint archive from a local file."
  ([store token file-path]
   (publish-checkpoint-file! store token file-path {}))
  ([store token file-path options]
   (let [path (if (instance? Path file-path)
               file-path
               (Paths/get (str file-path) (make-array String 0)))]
    (when-not (Files/isRegularFile path (make-array java.nio.file.LinkOption 0))
      (fail! ::invalid-options "Checkpoint publication requires a regular file"))
    (let [snapshot (or (read-head! store)
                       (fail! ::lease-fenced "The Durable head no longer exists"))
          current (:head snapshot)
          generation (checked-token-generation token current)
          sequence (inc (get-in current ["manifest" "seq"]))]
      (when (> sequence head/max-safe-integer)
        (fail! ::sequence-exhausted
               "The Durable manifest sequence cannot be incremented safely"))
      (let [reference {"key" (str "checkpoints/" generation "-" sequence "-"
                                   (unique-object-token) ".tar.gz")
                       "size" (Files/size path)
                       "sha256" (digest/sha256-file path)}
            result (backend/put-file-if-absent!
                    store (get reference "key") path)]
        {:status (reconcile-publication!
                  (:status result)
                  #(verify-file-reference! store reference)
                  true options)
         :reference reference
         :etag (:etag result)})))))

(defn- exact-transition-reference?
  [kind reference generation sequence]
  (let [prefix (case kind :wal "wal" :checkpoint "checkpoints")
        extension (case kind :wal "jsonl" :checkpoint "tar\\.gz")]
    (and (map? reference)
         (string? (get reference "key"))
         (boolean
          (re-matches
           (re-pattern
            (str prefix "/" generation "-" sequence
                 "-[0-9a-f]{8}\\." extension))
           (get reference "key"))))))

(defn- reference-transition
  [current token kind reference]
  (let [next-seq (inc (get-in current ["manifest" "seq"]))]
    (when (> next-seq head/max-safe-integer)
      (fail! ::sequence-exhausted
             "The Durable manifest sequence cannot be incremented safely"))
    (when-not (exact-transition-reference?
               kind reference (:generation token) next-seq)
      (fail! ::object-unverified
             "The immutable Durable reference is not current for this writer"))
    (let [desired
          (case kind
            :wal (-> current
                     (update-in ["manifest" "wal"] conj reference)
                     (assoc-in ["manifest" "seq"] next-seq))
            :checkpoint
            (-> current
                (assoc-in ["manifest" "base"] reference)
                (assoc-in ["manifest" "wal"] [])
                (assoc-in ["manifest" "seq"] next-seq)))]
      ;; This proves canonical key generation/sequence and retains all unknown
      ;; fields before any CAS reaches the backend.
      (head/validate! desired :writer)
      {:desired desired :next-seq next-seq})))

(defn- reference-landed?
  [kind reference next-seq latest]
  (let [manifest (get latest "manifest")]
    (and (= next-seq (get manifest "seq"))
         (case kind
           :wal (= reference (peek (get manifest "wal")))
           :checkpoint (and (= reference (get manifest "base"))
                            (empty? (get manifest "wal")))))))

(defn- commit-reference-attempt!
  [store token kind reference {:keys [phase snapshot] :as state}]
  (if (= :reconcile phase)
    (if-let [latest (reread store)]
      (let [{:keys [next-seq]} (:transition state)
            landed? #(reference-landed? kind reference next-seq %)]
        (cond
          (not (owns? (:head latest) token))
          (fail! ::lease-fenced "The Durable writer has lost its lease")

          (landed? (:head latest))
          {:status :done
           :value {:status :reconciled
                   :head (:head latest) :etag (:etag latest) :token token}}

          :else {:status :retry :state state}))
      {:status :retry :state state})
    (let [current (assert-owned! (:head snapshot) token)
        {:keys [desired next-seq]}
        (reference-transition current token kind reference)
        {canonical :head bytes :bytes} (encoded-head desired)
        landed? #(reference-landed? kind reference next-seq %)
        result (backend/replace-if-match!
                store head-key bytes (:etag snapshot))]
    (case (:status result)
      :replaced
      {:status :done
       :value {:status :committed
               :head canonical :etag (:etag result) :token token}}

      :ambiguous
      {:status :retry
       :state {:phase :reconcile
               :transition {:next-seq next-seq}}}

      :precondition-failed
      (let [latest (or (reread store)
                       (fail! ::lease-fenced
                              "The Durable head no longer exists"))]
        (cond
          (not (owns? (:head latest) token))
          (fail! ::lease-fenced "The Durable writer has lost its lease")

          (landed? (:head latest))
          {:status :done
           :value {:status :reconciled
                   :head (:head latest) :etag (:etag latest) :token token}}

          :else {:status :retry
                 :state {:phase :cas :snapshot latest}}))

      (fail! ::backend-contract
             "The Durable backend returned an unsupported CAS result")))))

(defn commit-reference!
  "Commit one already-published immutable WAL or checkpoint reference.

  `verify-reference!` must return truthy only after checking the backend
  object against the reference's size and SHA-256. The verification completes
  before the head CAS without excluding same-owner lease renewal. Definite
  heartbeat CAS conflicts retry from the latest owned head; ambiguous CAS
  outcomes never retry. Checkpoint commits replace the base and clear WAL; WAL
  commits append exactly one reference."
  [store token {:keys [kind reference verify-reference! max-attempts]
                :or {max-attempts default-commit-attempts}
                :as options}]
  (when-not (contains? #{:wal :checkpoint} kind)
    (fail! ::invalid-options "kind must be :wal or :checkpoint"))
  (when-not (fn? verify-reference!)
    (fail! ::invalid-options "verify-reference! must be callable"))
  (positive-attempts! max-attempts)
  ;; Reject an already-stale owner and a non-current reference before touching
  ;; the immutable object. Verification may block, so it deliberately happens
  ;; without locally excluding the writer's heartbeat.
  (let [initial (or (read-head! store)
                    (fail! ::lease-fenced "The Durable head no longer exists"))
        initial-current (assert-owned! (:head initial) token)]
    (reference-transition initial-current token kind reference)
    (when-not (verify-reference! store reference)
      (fail! ::object-unverified
             "The immutable Durable object could not be verified"))
    ;; A heartbeat may change only the lease expiry while verification is in
    ;; flight. Rebuild from the newest owned head and retry only a definite CAS
    ;; precondition failure. An ambiguous CAS is never retried: exact reread
    ;; reconciliation remains the sole authority against duplicate advancement.
    (ambiguity-aware-retry-result!
     (retry/run!
      (retry-budget! (assoc options :max-attempts max-attempts))
      {:phase :cas
       :snapshot (or (read-head! store)
                     (fail! ::lease-fenced
                            "The Durable head no longer exists"))}
      (fn [_ snapshot]
        (commit-reference-attempt! store token kind reference snapshot)))
     "Durable reference commit exceeded its retry bounds"
     "The Durable head update could not be proved before its deadline")))
