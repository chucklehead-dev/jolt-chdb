(ns jdbc.chdb-durable-writer-concurrency-test
  (:require [jdbc.chdb.durable.backend :as backend]
            [jdbc.chdb.durable.control :as control]
            [jdbc.chdb.durable.head :as head]
            [jdbc.chdb.durable.writer :as writer]
            [jdbc.chdb-durable-writer-test-support :as support]
            [jolt.fibers :as fibers]))

(def failures (atom 0))
(def ^:private base-options support/base-options)

(defn- check [label expected actual]
  (support/check failures label expected actual))

(defn- error-type [f]
  (support/error-type f))

(defn- fake-operations [calls close-count]
  (support/fake-operations calls close-count))

(defn- model-checkpoint-operations [calls close-count]
  (support/model-checkpoint-operations calls close-count))

(defn- block-first-manifest-cas-store
  [delegate entered release-cas]
  (let [blocked? (atom false)]
    (reify backend/ObjectBackend
      (get-bytes [_ key] (backend/get-bytes delegate key))
      (get-with-etag [_ key] (backend/get-with-etag delegate key))
      (put-file-if-absent! [_ key path]
        (backend/put-file-if-absent! delegate key path))
      (put-bytes-if-absent! [_ key bytes]
        (backend/put-bytes-if-absent! delegate key bytes))
      (replace-if-match! [_ key bytes etag]
        (let [document (when (= control/head-key key)
                         (head/decode bytes :writer))
              manifest-cas?
              (and document
                   (pos? (get-in document ["manifest" "seq"])))
              block?
              (and manifest-cas?
                   (compare-and-set! blocked? false true))]
          (when block?
            ;; Stop the worker after it has selected a stale ETag but before
            ;; the backend sees its manifest CAS. The heartbeat must remain
            ;; able to perform an independent CAS on the same head.
            (deliver entered true)
            @release-cas)
          (backend/replace-if-match! delegate key bytes etag)))
      (download-to-file! [_ key path]
        (backend/download-to-file! delegate key path)))))

(defn- block-manifest-ambiguous-reread-store
  [delegate entered release-reread]
  (let [reconciliation-armed? (atom false)
        blocked? (atom false)]
    (reify backend/ObjectBackend
      (get-bytes [_ key] (backend/get-bytes delegate key))
      (get-with-etag [_ key]
        (when (and (= control/head-key key)
                   @reconciliation-armed?
                   (compare-and-set! blocked? false true))
          ;; The manifest CAS has landed but its response is ambiguous. Stop
          ;; the worker's proof read while allowing a heartbeat's later read
          ;; and CAS to proceed independently.
          (deliver entered true)
          @release-reread)
        (backend/get-with-etag delegate key))
      (put-file-if-absent! [_ key path]
        (backend/put-file-if-absent! delegate key path))
      (put-bytes-if-absent! [_ key bytes]
        (backend/put-bytes-if-absent! delegate key bytes))
      (replace-if-match! [_ key bytes etag]
        (let [document (when (= control/head-key key)
                         (head/decode bytes :writer))
              manifest-cas?
              (and document
                   (pos? (get-in document ["manifest" "seq"])))
              result (backend/replace-if-match! delegate key bytes etag)]
          (if (and manifest-cas? (= :replaced (:status result)))
            (do (reset! reconciliation-armed? true)
                {:status :ambiguous})
            result)))
      (download-to-file! [_ key path]
        (backend/download-to-file! delegate key path)))))

(defn- conflict-every-manifest-cas-store
  [delegate token expiry replace-count]
  (reify backend/ObjectBackend
    (get-bytes [_ key] (backend/get-bytes delegate key))
    (get-with-etag [_ key] (backend/get-with-etag delegate key))
    (put-file-if-absent! [_ key path]
      (backend/put-file-if-absent! delegate key path))
    (put-bytes-if-absent! [_ key bytes]
      (backend/put-bytes-if-absent! delegate key bytes))
    (replace-if-match! [_ key bytes etag]
      (let [document (when (= control/head-key key)
                       (head/decode bytes :writer))]
        (when (and document
                   (pos? (get-in document ["manifest" "seq"])))
          (swap! replace-count inc)
          (control/renew! delegate token (swap! expiry inc)))
        (backend/replace-if-match! delegate key bytes etag)))
    (download-to-file! [_ key path]
      (backend/download-to-file! delegate key path))))


(defn run-checks! []
  (reset! failures 0)
  (println "Durable writer blocked-I/O concurrency")
  (let [store (backend/memory-backend)
        acquired (control/acquire! store base-options)
        calls (atom [])
        close-count (atom 0)
        now (atom 100M)
        publish-entered (promise)
        release-publish (promise)
        heartbeat-tick (promise)
        heartbeat-renewed (promise)
        waits (atom 0)
        operations
        (-> (fake-operations calls close-count)
            (assoc
             :now-ms #(deref now)
             :await-heartbeat!
             (fn [stop _]
               (if (= 1 (swap! waits inc))
                 (do @heartbeat-tick :tick)
                 (do @stop (swap! calls conj :heartbeat-stop) :stop)))
             :renew!
             (fn [store token expiry]
               (swap! calls conj :renew)
               (let [result (control/renew! store token expiry)]
                 (deliver heartbeat-renewed result)
                 result))
             :publish-wal!
             (fn [store token payload]
               (swap! calls conj :publish-begin)
               (deliver publish-entered true)
               @release-publish
               (swap! calls conj :publish-end)
               (control/publish-wal-bytes! store token payload))
             :commit-reference!
             (fn [store token request]
               (swap! calls conj :commit)
               (control/commit-reference! store token request))
             :release!
             (fn [store token]
               (swap! calls conj :release)
               (control/release! store token))
             :close-native!
             (fn [_]
               (swap! calls conj :native-close)
               (swap! close-count inc))
             :cleanup-scratch! (fn [] (swap! calls conj :cleanup))))
        durable-writer
        (writer/start!
         {:store store :token (:token acquired) :handle :fake-handle
          :database "default" :lease-expiry 1000M
          :lease-ttl-ms 300 :heartbeat-interval-ms 100
          :operations operations})]
    (writer/execute! durable-writer "INSERT INTO t VALUES (1)")
    (let [closing
          (fibers/spawn
           #(try
              (writer/close! durable-writer)
              :closed
              (catch Throwable error (:type (ex-data error)))))]
      @publish-entered
      (deliver heartbeat-tick true)
      (check "heartbeat renews while close-time WAL publication is blocked"
             true (not= ::timeout
                        (deref heartbeat-renewed 1000 ::timeout)))
      (reset! now 1000M)
      (deliver release-publish true)
      (check "renewal carries a blocked close-time publication through expiry"
             :closed (fibers/join closing))
      (check "publication does not exclude heartbeat from the head"
             [:publish-begin :renew :publish-end :commit
              :heartbeat-stop :release :native-close :cleanup]
             (filterv keyword? @calls))))

  (let [store (backend/memory-backend)
        acquired (control/acquire! store base-options)
        calls (atom [])
        close-count (atom 0)
        now (atom 100M)
        verify-entered (promise)
        release-verify (promise)
        heartbeat-tick (promise)
        heartbeat-renewed (promise)
        waits (atom 0)
        base (model-checkpoint-operations calls close-count)
        verify-reference! (:verify-checkpoint-reference! base)
        operations
        (assoc
         base
         :now-ms #(deref now)
         :await-heartbeat!
         (fn [stop _]
           (if (= 1 (swap! waits inc))
             (do @heartbeat-tick :tick)
             (do @stop (swap! calls conj :heartbeat-stop) :stop)))
         :renew!
         (fn [store token expiry]
           (swap! calls conj :renew)
           (let [result (control/renew! store token expiry)]
             (deliver heartbeat-renewed result)
             result))
         :verify-checkpoint-reference!
         (fn [store reference]
           (swap! calls conj :verify-begin)
           (deliver verify-entered true)
           @release-verify
           (swap! calls conj :verify-end)
           (verify-reference! store reference))
         :release!
         (fn [store token]
           (swap! calls conj :release)
           (control/release! store token))
         :close-native!
         (fn [_]
           (swap! calls conj :native-close)
           (swap! close-count inc))
         :cleanup-scratch! (fn [] (swap! calls conj :cleanup)))
        durable-writer
        (writer/start!
         {:store store :token (:token acquired) :handle :fake-handle
          :database "default" :lease-expiry 1000M
          :lease-ttl-ms 300 :heartbeat-interval-ms 100
          :operations operations})]
    (writer/sql! durable-writer "INSERT INTO t VALUES (?)" [42])
    (let [closing (fibers/spawn #(writer/close! durable-writer))]
      @verify-entered
      (deliver heartbeat-tick true)
      (check "heartbeat renews while close-time verification is blocked"
             true (not= ::timeout
                        (deref heartbeat-renewed 1000 ::timeout)))
      (reset! now 1000M)
      (deliver release-verify true)
      (fibers/join closing)
      (check "verification does not exclude heartbeat from the head"
             [:verify-begin :renew :verify-end
              :heartbeat-stop :release :native-close :cleanup]
             (filterv keyword? @calls))))

  (let [delegate (backend/memory-backend)
        manifest-cas-entered (promise)
        release-manifest-cas (promise)
        store (block-first-manifest-cas-store
               delegate manifest-cas-entered release-manifest-cas)
        acquired (control/acquire! store base-options)
        calls (atom [])
        close-count (atom 0)
        now (atom 100M)
        heartbeat-tick (promise)
        heartbeat-renewed (promise)
        waits (atom 0)
        operations
        (-> (fake-operations calls close-count)
            (assoc
             :now-ms #(deref now)
             :await-heartbeat!
             (fn [stop _]
               (if (= 1 (swap! waits inc))
                 (do @heartbeat-tick :tick)
                 (do @stop (swap! calls conj :heartbeat-stop) :stop)))
             :renew!
             (fn [store token expiry]
               (swap! calls conj :renew-begin)
               (let [result (control/renew! store token expiry)]
                 (swap! calls conj :renew-end)
                 (deliver heartbeat-renewed result)
                 result))
             :commit-reference!
             (fn [store token request]
               (swap! calls conj :commit-begin)
               (let [result (control/commit-reference! store token request)]
                 (swap! calls conj :commit-end)
                 result))
             :release!
             (fn [store token]
               (swap! calls conj :release)
               (control/release! store token))
             :close-native!
             (fn [_]
               (swap! calls conj :native-close)
               (swap! close-count inc))
             :cleanup-scratch! (fn [] (swap! calls conj :cleanup))))
        durable-writer
        (writer/start!
         {:store store :token (:token acquired) :handle :fake-handle
          :database "default" :lease-expiry 1000M
          :lease-ttl-ms 300 :heartbeat-interval-ms 100
          :operations operations})]
    (writer/execute! durable-writer "INSERT INTO t VALUES (1)")
    (let [flushing (fibers/spawn #(writer/flush! durable-writer))]
      @manifest-cas-entered
      (deliver heartbeat-tick true)
      (check "heartbeat CAS completes while a manifest CAS is blocked"
             :committed
             (:status (deref heartbeat-renewed 1000 {:status ::timeout})))
      ;; The old manifest attempt now loses to the renewal ETag. Its bounded
      ;; same-owner retry must preserve that expiry and advance exactly once.
      (reset! now 1000M)
      (deliver release-manifest-cas true)
      (check "blocked manifest CAS retries after concurrent heartbeat"
             :committed (:status (fibers/join flushing))))
    (let [current (:head (control/read-head! store))]
      (check "manifest retry preserves renewal and advances exactly once"
             [1 1001 1]
             [(get-in current ["manifest" "seq"])
              (get-in current ["lease" "expires_at"])
              (count (get-in current ["manifest" "wal"]))]))
    (check "head CAS does not exclude heartbeat from the writer"
           [:commit-begin :renew-begin :renew-end :commit-end]
           (filterv #{:commit-begin :renew-begin :renew-end :commit-end}
                    @calls))
    (writer/close! durable-writer))

  (let [delegate (backend/memory-backend)
        reconciliation-entered (promise)
        release-reconciliation (promise)
        store (block-manifest-ambiguous-reread-store
               delegate reconciliation-entered release-reconciliation)
        acquired (control/acquire! store base-options)
        calls (atom [])
        close-count (atom 0)
        heartbeat-tick (promise)
        heartbeat-renewed (promise)
        waits (atom 0)
        operations
        (-> (fake-operations calls close-count)
            (assoc
             :now-ms (fn [] 100M)
             :await-heartbeat!
             (fn [stop _]
               (if (= 1 (swap! waits inc))
                 (do @heartbeat-tick :tick)
                 (do @stop :stop)))
             :renew!
             (fn [store token expiry]
               (let [result (control/renew! store token expiry)]
                 (deliver heartbeat-renewed result)
                 result))))
        durable-writer
        (writer/start!
         {:store store :token (:token acquired) :handle :fake-handle
          :database "default" :lease-expiry 1000M
          :lease-ttl-ms 300 :heartbeat-interval-ms 100
          :operations operations})]
    (writer/execute! durable-writer "INSERT INTO t VALUES (1)")
    (let [flushing (fibers/spawn #(writer/flush! durable-writer))]
      @reconciliation-entered
      (deliver heartbeat-tick true)
      ;; Because the already-landed manifest is retained in renewal's desired
      ;; head, this backend also loses the renewal CAS response. The heartbeat
      ;; must still complete by its semantic expiry reread while the worker's
      ;; earlier reconciliation read remains blocked.
      (check "heartbeat renews while manifest reconciliation read is blocked"
             true
             (not= ::timeout
                   (deref heartbeat-renewed 1000 ::timeout)))
      (deliver release-reconciliation true)
      (check "manifest reconciliation accepts its effect after renewal"
             :reconciled (:status (fibers/join flushing))))
    (check "ambiguous manifest and later renewal preserve both effects"
           [1 1001 1]
           (let [latest (:head (control/read-head! store))]
             [(get-in latest ["manifest" "seq"])
              (get-in latest ["lease" "expires_at"])
              (count (get-in latest ["manifest" "wal"]))]))
    (writer/close! durable-writer))

  (let [delegate (backend/memory-backend)
        reconciliation-entered (promise)
        release-reconciliation (promise)
        store (block-manifest-ambiguous-reread-store
               delegate reconciliation-entered release-reconciliation)
        acquired (control/acquire! store base-options)
        close-count (atom 0)
        operations
        (assoc
         (fake-operations (atom []) close-count)
         :now-ms (fn [] 100M)
         :await-heartbeat! (fn [stop _] @stop :stop))
        durable-writer
        (writer/start!
         {:store store :token (:token acquired) :handle :fake-handle
          :database "default" :lease-expiry 1000M
          :lease-ttl-ms 300 :heartbeat-interval-ms 100
          :operations operations})]
    (writer/execute! durable-writer "INSERT INTO t VALUES (1)")
    (let [flushing
          (fibers/spawn
           #(error-type (fn [] (writer/flush! durable-writer))))]
      @reconciliation-entered
      ;; The manifest CAS has landed, but writer 1 has not completed its proof
      ;; read. Writer 2 may acquire the expired lease without rewriting that
      ;; manifest. Writer 1 must observe the new generation and self-fence.
      (control/acquire!
       store (assoc base-options :owner "writer-2" :instance "instance-2"
                    :now 1000M :expires-at 2000M))
      (deliver release-reconciliation true)
      (check "takeover fences an ambiguous manifest reconciliation"
             ::control/lease-fenced (fibers/join flushing)))
    (let [latest (:head (control/read-head! store))]
      (check "takeover preserves the already-landed recovery reference"
             [2 "writer-2" 1 1 1]
             [(get-in latest ["lease" "generation"])
              (get-in latest ["lease" "owner"])
              (get-in latest ["manifest" "seq"])
              (count (get-in latest ["manifest" "wal"]))
              (:pending-statements (writer/status durable-writer))]))
    (check "fenced reconciliation cleanup preserves the takeover"
           ::control/lease-fenced
           (error-type #(writer/close! durable-writer))))

  (let [delegate (backend/memory-backend)
        acquired (control/acquire! delegate base-options)
        token (:token acquired)
        replace-count (atom 0)
        expiry (atom 1000M)
        store (conflict-every-manifest-cas-store
               delegate token expiry replace-count)
        close-count (atom 0)
        verify-count (atom 0)
        base-operations (fake-operations (atom []) close-count)
        operations
        (assoc
         base-operations
         :commit-reference!
         (fn [store token request]
           (control/commit-reference!
            store token
            (assoc request
                   :max-attempts 2
                   :verify-reference!
                   (fn [store reference]
                     (swap! verify-count inc)
                     (control/verify-byte-reference! store reference))))))
        durable-writer
        (writer/start!
         {:store store :token token :handle :fake-handle
          :database "default" :operations operations})]
    (writer/execute! durable-writer "INSERT INTO t VALUES (1)")
    (check "manifest retry exhaustion is reported to the writer"
           ::control/timeout
           (error-type #(writer/flush! durable-writer)))
    (check "exhausted commit verifies once, advances zero times, and retains WAL"
           [2 1 0 1]
           [@replace-count @verify-count
            (get-in (:head (control/read-head! delegate)) ["manifest" "seq"])
            (:pending-statements (writer/status durable-writer))])
    (check "close retains the same exhausted persistence obligation"
           ::control/timeout
           (error-type #(writer/close! durable-writer))))

  (let [delegate (backend/memory-backend)
        acquired (control/acquire! delegate base-options)
        token (:token acquired)
        replace-count (atom 0)
        expiry (atom 1000M)
        now (atom 100M)
        waits (atom [])
        store (conflict-every-manifest-cas-store
               delegate token expiry replace-count)
        close-count (atom 0)
        operations
        (assoc (fake-operations (atom []) close-count)
               :now-ms (fn [] @now)
               :await-heartbeat! (fn [stop _] @stop :stop))
        durable-writer
        (writer/start!
         {:store store :token token :handle :fake-handle
          :database "default" :lease-expiry 1000M
          :lease-ttl-ms 300 :heartbeat-interval-ms 100
          :retry-options
          {:max-attempts 4 :retry-deadline-ms 5000
           :retry-initial-backoff-ms 10 :retry-max-backoff-ms 250
           :monotonic-ms! (fn [] @now)
           :await-backoff! (fn [milliseconds]
                             (swap! waits conj milliseconds)
                             (reset! now 1000M))}
          :operations operations})]
    (writer/execute! durable-writer "INSERT INTO t VALUES (1)")
    (check "lease expiry during backoff self-fences the writer"
           ::control/lease-fenced
           (error-type #(writer/flush! durable-writer)))
    (check "self-fencing stops before a second CAS and retains WAL"
           [1 [10] false 1]
           [@replace-count @waits (:writable? (writer/status durable-writer))
            (:pending-statements (writer/status durable-writer))])
    (check "close retains the self-fenced persistence obligation"
           ::control/lease-fenced
           (error-type #(writer/close! durable-writer))))

  (let [store (backend/memory-backend)
        acquired (control/acquire! store base-options)
        publish-entered (promise)
        release-publish (promise)
        close-count (atom 0)
        operations
        (assoc
         (fake-operations (atom []) close-count)
         :now-ms (fn [] 100M)
         :await-heartbeat! (fn [stop _] @stop :stop)
         :publish-wal!
         (fn [store token payload]
           (deliver publish-entered true)
           @release-publish
           (control/publish-wal-bytes! store token payload)))
        durable-writer
        (writer/start!
         {:store store :token (:token acquired) :handle :fake-handle
          :database "default" :lease-expiry 1000M
          :lease-ttl-ms 300 :heartbeat-interval-ms 100
          :operations operations})]
    (writer/execute! durable-writer "INSERT INTO t VALUES (1)")
    (let [flushing (fibers/spawn
                    #(error-type (fn [] (writer/flush! durable-writer))))]
      @publish-entered
      (control/acquire!
       store (assoc base-options :owner "writer-2" :instance "instance-2"
                    :now 1000M :expires-at 2000M))
      (deliver release-publish true)
      (check "takeover during blocked publication fences the old flush"
             ::control/lease-fenced (fibers/join flushing)))
    (check "blocked-publication takeover retains pending WAL"
           [1 0 "writer-2" 2]
           [(:pending-statements (writer/status durable-writer))
            (get-in (:head (control/read-head! store)) ["manifest" "seq"])
            (get-in (:head (control/read-head! store)) ["lease" "owner"])
            (get-in (:head (control/read-head! store)) ["lease" "generation"])])
    (check "fenced publication cleanup preserves the takeover"
           ::control/lease-fenced
           (error-type #(writer/close! durable-writer))))

  (let [store (backend/memory-backend)
        acquired (control/acquire! store base-options)
        verify-entered (promise)
        release-verify (promise)
        close-count (atom 0)
        base (model-checkpoint-operations (atom []) close-count)
        verify-reference! (:verify-checkpoint-reference! base)
        operations
        (assoc
         base
         :now-ms (fn [] 100M)
         :await-heartbeat! (fn [stop _] @stop :stop)
         :verify-checkpoint-reference!
         (fn [store reference]
           (deliver verify-entered true)
           @release-verify
           (verify-reference! store reference)))
        durable-writer
        (writer/start!
         {:store store :token (:token acquired) :handle :fake-handle
          :database "default" :lease-expiry 1000M
          :lease-ttl-ms 300 :heartbeat-interval-ms 100
          :operations operations})]
    (writer/sql! durable-writer "INSERT INTO t VALUES (?)" [42])
    (let [flushing (fibers/spawn
                    #(error-type (fn [] (writer/flush! durable-writer))))]
      @verify-entered
      (control/acquire!
       store (assoc base-options :owner "writer-2" :instance "instance-2"
                    :now 1000M :expires-at 2000M))
      (deliver release-verify true)
      (check "takeover during blocked verification fences before head CAS"
             ::control/lease-fenced (fibers/join flushing)))
    (check "blocked-verification takeover retains checkpoint obligation"
           [true 0 "writer-2" 2]
           [(:checkpoint-required? (writer/status durable-writer))
            (get-in (:head (control/read-head! store)) ["manifest" "seq"])
            (get-in (:head (control/read-head! store)) ["lease" "owner"])
            (get-in (:head (control/read-head! store)) ["lease" "generation"])])
    (check "fenced verification cleanup preserves the takeover"
           ::control/lease-fenced
           (error-type #(writer/close! durable-writer))))

  (when-not (zero? @failures)
    (throw (ex-info (str @failures " Durable writer concurrency checks failed")
                    {:failures @failures})))
  (println "all Durable writer concurrency checks passed")
  true)

(defn -main [& _]
  (run-checks!))
