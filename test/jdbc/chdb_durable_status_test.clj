(ns jdbc.chdb-durable-status-test
  (:require [db.jdbc]
            [jdbc.chdb.durable :as durable]
            [jdbc.chdb.durable.backend :as backend]
            [jdbc.chdb.durable.control :as control]
            [jdbc.chdb.durable.reader :as reader]
            [jdbc.chdb.durable.writer :as writer]
            [jdbc.chdb-durable-open-test-support :as open-support]
            [jdbc.chdb-durable-writer-test-support :as support]
            [jdbc.core :as jdbc]
            [jolt.fibers :as fibers]))

(def failures (atom 0))

(def ^:private status-keys
  #{:jdbc.chdb.durable.status/version :role :lifecycle :availability
    :unavailable-reason :persistence-state :view-current?
    :view-current-reason :observed-manifest-sequence
    :last-successful-persistence :last-persistence-error})

(defn- check [label expected actual]
  (support/check failures label expected actual))

(defn- caught [f]
  (try (f) nil (catch Throwable error error)))

(defn- new-writer
  ([] (new-writer {}))
  ([operation-overrides]
   (let [store (backend/memory-backend)
         acquired (control/acquire! store support/base-options)
         calls (atom [])
         close-count (atom 0)
         operations (merge (support/fake-operations calls close-count)
                           operation-overrides)
         durable-writer
         (writer/start!
          {:store store :token (:token acquired) :handle :fake-handle
           :database "default"
           :manifest-sequence
           (get-in (:head acquired) ["manifest" "seq"])
           :operations operations})]
     {:store store :calls calls :close-count close-count
      :writer durable-writer})))

(defn- close-ignoring-error! [durable-writer]
  (try (writer/close! durable-writer) (catch Throwable _ nil)))

(defn- counting-backend [delegate calls]
  (reify backend/ObjectBackend
    (get-bytes [_ key]
      (swap! calls inc)
      (backend/get-bytes delegate key))
    (get-with-etag [_ key]
      (swap! calls inc)
      (backend/get-with-etag delegate key))
    (put-file-if-absent! [_ key path]
      (swap! calls inc)
      (backend/put-file-if-absent! delegate key path))
    (put-bytes-if-absent! [_ key bytes]
      (swap! calls inc)
      (backend/put-bytes-if-absent! delegate key bytes))
    (replace-if-match! [_ key bytes etag]
      (swap! calls inc)
      (backend/replace-if-match! delegate key bytes etag))
    (download-to-file! [_ key path]
      (swap! calls inc)
      (backend/download-to-file! delegate key path))))

(defn- base-status [role lifecycle availability unavailable-reason
                    persistence-state view-current? view-current-reason
                    sequence last-success last-error]
  {:jdbc.chdb.durable.status/version 1
   :role role
   :lifecycle lifecycle
   :availability availability
   :unavailable-reason unavailable-reason
   :persistence-state persistence-state
   :view-current? view-current?
   :view-current-reason view-current-reason
   :observed-manifest-sequence sequence
   :last-successful-persistence last-success
   :last-persistence-error last-error})

(defn- run-writer-status-checks! []
  (println "Durable public writer status")
  (let [{:keys [writer]} (new-writer)]
    (check "fresh recovered writer exposes only a confirmed local boundary"
           (base-status :writer :open :available :none :current true :none
                        0 :unavailable :none)
           (writer/public-status writer))
    (writer/execute! writer "INSERT INTO telemetry VALUES ('private-value')")
    (let [pending (writer/public-status writer)]
      (check "a completed mutation is pending until its head commit"
             [:pending false :pending-persistence 0]
             ((juxt :persistence-state :view-current? :view-current-reason
                    :observed-manifest-sequence)
              pending))
      (check "status does not copy SQL from the pending WAL"
             false (.contains (pr-str pending) "private-value")))
    (let [result (writer/flush! writer)
          status (writer/public-status writer)]
      (check "confirmed WAL advances the observed manifest sequence"
             [:committed :current true 1
              {:operation :flush :boundary :wal
               :manifest-sequence 1 :confirmation :committed}]
             [(:status result) (:persistence-state status)
              (:view-current? status) (:observed-manifest-sequence status)
              (:last-successful-persistence status)]))
    (writer/flush! writer)
    (check "empty flush confirms the unchanged boundary"
           {:operation :flush :boundary :empty
            :manifest-sequence 1 :confirmation :unchanged}
           (:last-successful-persistence (writer/public-status writer)))
    (writer/close! writer)
    (let [closed (writer/public-status writer)]
      (check "closed writer retains no live availability claim"
             [:closed :unavailable :closed :unavailable :unavailable]
             ((juxt :lifecycle :availability :unavailable-reason
                    :persistence-state :view-current?)
              closed))))

  (let [{:keys [writer]}
        (new-writer
         {:commit-reference!
          (fn [_ _ _]
            {:status :reconciled
             :head {"manifest" {"seq" 1}
                    "private" "must-not-cross-status"}
             :etag "private-etag" :token {:owner "private-owner"}})})]
    (writer/execute! writer "INSERT INTO t VALUES (1)")
    (writer/flush! writer)
    (let [status (writer/public-status writer)]
      (check "reconciled commit is an equally confirmed boundary"
             [1 :reconciled :current]
             [(:observed-manifest-sequence status)
              (get-in status [:last-successful-persistence :confirmation])
              (:persistence-state status)])
      (check "raw control result fields never cross the projection"
             false (.contains (pr-str status) "private")))
    (writer/close! writer))

  (let [secret "ambiguous-secret"
        primary (ex-info secret {:type ::control/commit-ambiguous
                                 :credential secret})
        {:keys [writer]}
        (new-writer {:commit-reference! (fn [& _] (throw primary))})]
    (writer/execute! writer "INSERT INTO t VALUES (2)")
    (let [actual (caught #(writer/flush! writer))
          status (writer/public-status writer)]
      (check "persistence failure preserves exact Throwable identity"
             true (identical? primary actual))
      (check "ambiguous commit never becomes false currentness"
             [:unconfirmed :unavailable :commit-unconfirmed 0 :ambiguous]
             [(:persistence-state status) (:view-current? status)
              (:view-current-reason status)
              (:observed-manifest-sequence status)
              (:last-persistence-error status)])
      (check "ambiguous error message and data remain private"
             false (.contains (pr-str status) secret)))
    (close-ignoring-error! writer))

  (let [{:keys [writer]}
        (new-writer
         {:commit-reference!
          (fn [& _]
            {:status :attempted
             :head {"manifest" {"seq" 99}}
             :secret "attempt-is-not-ack"})})]
    (writer/execute! writer "INSERT INTO t VALUES (3)")
    (let [error (caught #(writer/flush! writer))
          status (writer/public-status writer)]
      (check "attempt-only result is rejected by the causal control"
             ::writer/persistence-unconfirmed (:type (ex-data error)))
      (check "attempt-only result cannot clear or advance status"
             [:pending false 0 :failed]
             [(:persistence-state status) (:view-current? status)
              (:observed-manifest-sequence status)
              (:last-persistence-error status)]))
    (close-ignoring-error! writer))

  (let [{:keys [store writer]}
        (let [store (backend/memory-backend)
              acquired (control/acquire! store support/base-options)
              calls (atom [])
              close-count (atom 0)]
          {:store store
           :writer
           (writer/start!
            {:store store :token (:token acquired) :handle :fake-handle
             :database "default" :manifest-sequence 0
             :engine-metadata
             {:version "26.7.2-rc.2" :backup-format 1
              :min-reader "26.7.2-rc.2"}
             :operations
             (support/model-checkpoint-operations calls close-count)})})]
    (writer/sql! writer "INSERT INTO t VALUES (?)" [42])
    (check "bound mutation is not current before checkpoint fallback"
           [:pending false]
           ((juxt :persistence-state :view-current?)
            (writer/public-status writer)))
    (writer/flush! writer)
    (check "flush records its actual checkpoint boundary"
           {:operation :flush :boundary :checkpoint
            :manifest-sequence 1 :confirmation :committed}
           (:last-successful-persistence (writer/public-status writer)))
    (check "checkpoint fallback committed the same observed sequence"
           1 (get-in (:head (control/read-head! store)) ["manifest" "seq"]))
    (writer/close! writer)))

(defn- run-concurrency-checks! []
  (println "Durable status concurrency trace")
  (let [entered (promise)
        release (promise)
        delegate (backend/memory-backend)
        acquired (control/acquire! delegate support/base-options)
        calls (atom [])
        close-count (atom 0)
        durable-writer
        (writer/start!
         {:store delegate :token (:token acquired) :handle :fake-handle
          :database "default" :manifest-sequence 0
          :operations
          (assoc (support/fake-operations calls close-count)
                 :commit-reference!
                 (fn [store token request]
                   (deliver entered true)
                   @release
                   (control/commit-reference! store token request)))})]
    (writer/execute! durable-writer "INSERT INTO t VALUES (4)")
    (let [flushing (fibers/spawn #(writer/flush! durable-writer))]
      @entered
      (check "blocked commit cannot expose its future sequence"
             [:pending false 0]
             ((juxt :persistence-state :view-current?
                    :observed-manifest-sequence)
              (writer/public-status durable-writer)))
      (deliver release true)
      (fibers/join flushing)
      (check "completed commit atomically publishes the new boundary"
             [:current true 1]
             ((juxt :persistence-state :view-current?
                    :observed-manifest-sequence)
              (writer/public-status durable-writer))))
    (writer/close! durable-writer)))

(defn- run-public-boundary-checks! []
  (println "Durable JDBC status boundary")
  (let [delegate (backend/memory-backend)
        backend-calls (atom 0)
        store (counting-backend delegate backend-calls)
        calls (atom [])
        close-count (atom 0)
        cleanup-count (atom 0)
        clocks (atom [0M 1M 2M 3M 4M])
        operations (open-support/fake-open-operations
                    calls clocks close-count cleanup-count)
        connection
        (jdbc/connection
         {:vendor "chdb-durable" :backend store
          :owner "private-owner" :instance "private-instance"
          :database "private-database" :lease-ttl-ms 300M
          :operations operations})]
    (try
      (reset! backend-calls 0)
      (let [status (durable/status connection)]
        (check "public status is the exact closed v1 projection"
               status-keys (set (keys status)))
        (check "public status performs no backend I/O"
               0 @backend-calls)
        (check "public projection contains no ownership or database values"
               false
               (boolean
                (some #(.contains (pr-str status) %)
                      ["private-owner" "private-instance" "private-database"]))))
      (finally (.close connection))))

  (let [delegate (backend/memory-backend)
        _ (control/acquire! delegate support/base-options)
        backend-calls (atom 0)
        store (counting-backend delegate backend-calls)
        calls (atom [])
        close-count (atom 0)
        cleanup-count (atom 0)
        clocks (atom [0M])
        connection
        (jdbc/connection
         {:vendor "chdb-durable" :backend store :read-only? true
          :operations (open-support/fake-open-operations
                       calls clocks close-count cleanup-count)})]
    (try
      (reset! backend-calls 0)
      (let [status (durable/status connection)]
        (check "reader reports an observed snapshot, never guessed freshness"
               [:reader :snapshot :unavailable
                :snapshot-not-revalidated 0]
               [(:role status) (:persistence-state status)
                (:view-current? status) (:view-current-reason status)
                (:observed-manifest-sequence status)])
        (check "reader status does not reread its backend"
               0 @backend-calls))
      (finally (.close connection))))

  (with-open [ordinary (jdbc/connection "chdb::memory:")]
    (check "ordinary chDB connection is rejected at the extension boundary"
           true (some? (caught #(durable/status ordinary)))))
  (let [closed (jdbc/connection "chdb::memory:")]
    (.close closed)
    (check "closed connection is rejected at the extension boundary"
           true (some? (caught #(durable/status closed))))))

(defn run-checks! []
  (reset! failures 0)
  (run-writer-status-checks!)
  (run-concurrency-checks!)
  (run-public-boundary-checks!)
  (when-not (zero? @failures)
    (throw (ex-info (str @failures " Durable status checks failed")
                    {:failures @failures})))
  (println "all Durable status checks passed")
  true)

(defn -main [& _]
  (run-checks!))
