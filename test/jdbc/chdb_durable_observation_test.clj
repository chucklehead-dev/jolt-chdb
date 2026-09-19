(ns jdbc.chdb-durable-observation-test
  (:require [db.jdbc]
            [jdbc.chdb.durable :as durable]
            [jdbc.chdb.durable.backend :as backend]
            [jdbc.chdb.durable.control :as control]
            [jdbc.chdb.durable.observation :as observation]
            [jdbc.chdb.durable.reader :as reader]
            [jdbc.chdb.durable.writer :as writer]
            [jdbc.chdb-durable-open-test-support :as open-support]
            [jdbc.chdb-durable-writer-test-support :as writer-support]
            [jdbc.core :as jdbc]
            [jdbc.proto :as proto]
            [jolt.host :as host]))

(def failures (atom 0))

(defn- check [label expected actual]
  (if (= expected actual)
    (println "  ok  " label)
    (do (swap! failures inc)
        (println "  FAIL" label "- expected" (pr-str expected)
                 "got" (pr-str actual)))))

(defn- error? [f]
  (try (f) false (catch Throwable _ true)))

(defn- counted-backend [delegate calls]
  (letfn [(count! [operation f]
            (swap! calls conj operation)
            (f))]
    (reify backend/ObjectBackend
      (get-bytes [_ key] (count! :get-bytes #(backend/get-bytes delegate key)))
      (get-with-etag [_ key]
        (count! :get-with-etag #(backend/get-with-etag delegate key)))
      (put-file-if-absent! [_ key path]
        (count! :put-file #(backend/put-file-if-absent! delegate key path)))
      (put-bytes-if-absent! [_ key bytes]
        (count! :put-bytes #(backend/put-bytes-if-absent! delegate key bytes)))
      (replace-if-match! [_ key bytes etag]
        (count! :replace #(backend/replace-if-match! delegate key bytes etag)))
      (download-to-file! [_ key path]
        (count! :download #(backend/download-to-file! delegate key path))))))

(defn- direct-observation-checks! []
  (let [acquired (control/acquire! (backend/memory-backend)
                                   writer-support/base-options)
        recovered (:head acquired)
        reference {"key" "wal/1-1-00000001.jsonl" "size" 1
                   "sha256" (apply str (repeat 64 "0"))}
        checkpoint {"key" "checkpoints/1-2-00000002.tar.gz" "size" 1
                    "sha256" (apply str (repeat 64 "0"))}
        wal-head (-> recovered
                     (assoc-in ["manifest" "seq"] 1)
                     (assoc-in ["manifest" "wal"] [reference]))
        checkpoint-head (-> recovered
                            (assoc-in ["manifest" "seq"] 2)
                            (assoc-in ["manifest" "base"] checkpoint)
                            (assoc-in ["manifest" "wal"] []))
        state (observation/start :writer recovered)]
    (check "recovered writer carries only a scalar boundary"
           {:availability :available :role :writer :state :recovered
            :view-current? true :confirmed-boundary :recovered
            :confirmed-sequence 0 :last-successful-persistence :unavailable}
           (observation/projection state))
    (observation/pending! state)
    (check "native mutation admission is pending before any persistence result"
           [:pending false] ((juxt :state :view-current?) (observation/projection state)))
    (observation/unconfirmed! state)
    (observation/confirmed! state :wal reference
                            {:status :committed :head wal-head
                             :opaque-secret "must-not-escape"})
    (check "a later confirmed WAL cannot clear an earlier ambiguous WAL"
           :unconfirmed (:state (observation/projection state)))
    (observation/confirmed! state :checkpoint checkpoint
                            {:status :reconciled :head checkpoint-head
                             :opaque-secret "must-not-escape"})
    (check "only a canonical reconciled checkpoint clears uncertainty"
           {:availability :available :role :writer :state :confirmed
            :view-current? true :confirmed-boundary :checkpoint
            :confirmed-sequence 2 :last-successful-persistence :checkpoint}
           (observation/projection state))
    (observation/unavailable! state)
    (check "terminal observation is unavailable rather than stale"
           {:availability :unavailable} (observation/projection state))))

(defn- jdbc-observation-checks! []
  (let [calls (atom [])
        storage-calls (atom [])
        close-count (atom 0)
        cleanup-count (atom 0)
        clocks (atom [1000M 1001M 1002M 1003M 1004M])
        store (counted-backend (backend/memory-backend) storage-calls)
        operations (assoc (open-support/fake-open-operations
                           calls clocks close-count cleanup-count)
                          :execute-native!
                          (fn [_ sql _]
                            (swap! calls conj [:execute sql])
                            {:labels [] :rows [] :count 0}))
        connection
        (jdbc/connection {:vendor "chdb-durable" :backend store
                          :owner "private-owner" :instance "private-instance"
                          :database "private-db" :lease-ttl-ms 300M
                          :operations operations})]
    (try
      (reset! storage-calls [])
      (reset! calls [])
      ;; Durable correctly advertises no SQL transaction support, so no public
      ;; API can leave it pending. This controlled shim-state probe instead
      ;; proves the observation boundary does not invoke the old generic
      ;; driver-context path, which would issue native BEGIN here.
      (host/ref-put! (proto/connection connection) :tx-pending true)
      (let [initial (durable/persistence-observation connection)]
        (check "JDBC observation is a zero-I/O projection" [] @storage-calls)
        (check "JDBC observation never starts a deferred native transaction"
               [] @calls)
        (check "JDBC writer starts with recovered evidence"
               [:writer :recovered true :recovered 0 :unavailable]
               ((juxt :role :state :view-current? :confirmed-boundary
                      :confirmed-sequence :last-successful-persistence) initial))
        (check "JDBC projection does not leak private state"
               #{:availability :role :state :view-current? :confirmed-boundary
                 :confirmed-sequence :last-successful-persistence}
               (set (keys initial)))
        ;; Restore the controlled negative state before exercising ordinary
        ;; Durable writer operations below.
        (host/ref-put! (proto/connection connection) :tx-pending false))
      (check "empty flush is not a persistence success"
             :empty (:status (durable/flush! connection)))
      (check "empty flush preserves recovered observation"
             :recovered (:state (durable/persistence-observation connection)))
      (jdbc/execute! connection "INSERT INTO private_table VALUES (7)")
      (check "admitted JDBC mutation becomes pending before flush"
             [:pending false]
             ((juxt :state :view-current?)
              (durable/persistence-observation connection)))
      (check "canonical control WAL establishes a public witness"
             :committed (:status (durable/flush! connection)))
      (check "public witness contains no reference or payload"
             [:confirmed :wal :wal]
             ((juxt :state :confirmed-boundary :last-successful-persistence)
              (durable/persistence-observation connection)))
      (finally
        (.close connection)))
    (check "closed JDBC connection rejects persistence observation"
           true (error? #(durable/persistence-observation connection)))))

(defn- reader-observation-checks! []
  (let [recovered (:head (control/acquire! (backend/memory-backend)
                                           writer-support/base-options))
        reader (reader/start! {:handle :private-handle :database "private-db"
                               :recovered-document recovered
                               :operations {:close-native! (fn [_] nil)
                                            :cleanup-scratch! (fn [] nil)}})]
    (try
      (check "reader recovery is a snapshot rather than current-head claim"
             {:availability :available :role :reader :state :snapshot
              :view-current? false :confirmed-boundary :recovered
              :confirmed-sequence 0 :last-successful-persistence :unavailable}
             (reader/persistence-observation reader))
      (finally (reader/close! reader)))
    (check "closed reader observation is unavailable"
           {:availability :unavailable} (reader/persistence-observation reader))))

(defn- forced-teardown-observation-checks! []
  ;; A terminal worker failure owns cleanup without a public close caller. Make
  ;; native cleanup wait at a deterministic point and observe during that wait:
  ;; unavailable must be published before any potentially blocking teardown.
  (let [store (backend/memory-backend)
        acquired (control/acquire! store writer-support/base-options)
        native-close-entered (promise)
        release-native-close (promise)
        terminal (ex-info "writer worker failed" {:type ::writer-worker-failed})
        durable-writer
        (writer/start!
         {:store store :token (:token acquired) :handle :private-writer-handle
          :database "private-db" :recovered-document (:head acquired)
          :operations
          (assoc (writer-support/fake-operations (atom []) (atom 0))
                 :take-request! (fn [_] (throw terminal))
                 :close-native! (fn [_]
                                  (deliver native-close-entered true)
                                  @release-native-close))})]
    @native-close-entered
    (check "forced writer teardown makes observation unavailable before native close"
           {:availability :unavailable}
           (writer/persistence-observation durable-writer))
    (deliver release-native-close true)
    (check "forced writer teardown retains its terminal failure"
           true (error? #(writer/close! durable-writer))))
  (let [recovered (:head (control/acquire! (backend/memory-backend)
                                           writer-support/base-options))
        native-close-entered (promise)
        release-native-close (promise)
        terminal (ex-info "reader worker failed" {:type ::reader-worker-failed})
        durable-reader
        (reader/start!
         {:handle :private-reader-handle :database "private-db"
          :recovered-document recovered
          :operations
          {:take-request! (fn [_] (throw terminal))
           :close-native! (fn [_]
                            (deliver native-close-entered true)
                            @release-native-close)
           :cleanup-scratch! (fn [] nil)}})]
    @native-close-entered
    (check "forced reader teardown makes observation unavailable before native close"
           {:availability :unavailable}
           (reader/persistence-observation durable-reader))
    (deliver release-native-close true)
    (check "forced reader teardown retains its terminal failure"
           true (error? #(reader/close! durable-reader)))))

(defn- writer-error-transition-checks! []
  (let [store (backend/memory-backend)
        acquired (control/acquire! store writer-support/base-options)
        fault (atom :definite)
        calls (atom [])
        close-count (atom 0)
        metadata {:version "26.7.3" :backup-format 1 :min-reader "26.7.3"}
        operations
        (assoc (writer-support/model-checkpoint-operations calls close-count)
               :commit-reference!
               (fn [current-store token options]
                 (case @fault
                   :definite (throw (ex-info "definite publication failure"
                                             {:type ::definite-failure}))
                   :ambiguous (throw (ex-info "unprovable head outcome"
                                              {:type ::control/commit-ambiguous}))
                   :canonical
                   (control/commit-reference!
                    current-store token (assoc options :engine-metadata metadata)))))
        durable-writer
        (writer/start! {:store store :token (:token acquired) :handle :private-handle
                        :database "private-db" :engine-metadata metadata
                        :recovered-document (:head acquired) :operations operations})]
    (try
      (writer/execute! durable-writer "INSERT INTO t VALUES (1)")
      (check "definite flush failure leaves persistence evidence pending"
             true (error? #(writer/flush! durable-writer)))
      (check "definite failure is pending, not a false confirmation"
             :pending (:state (writer/persistence-observation durable-writer)))
      (reset! fault :ambiguous)
      (check "ambiguous control failure is visible as unconfirmed"
             true (error? #(writer/flush! durable-writer)))
      (check "ambiguous control result poisons writer observation"
             :unconfirmed (:state (writer/persistence-observation durable-writer)))
      (reset! fault :canonical)
      (writer/flush! durable-writer)
      (check "subsequent canonical WAL retains earlier uncertainty"
             :unconfirmed (:state (writer/persistence-observation durable-writer)))
      (writer/execute! durable-writer "INSERT INTO t VALUES (2)")
      (writer/checkpoint! durable-writer)
      (check "canonical checkpoint is the only operation that clears ambiguity"
             [:confirmed :checkpoint true]
             ((juxt :state :confirmed-boundary :view-current?)
              (writer/persistence-observation durable-writer)))
      (finally (writer/close! durable-writer)))))

(defn- writer-fence-check! []
  (let [store (backend/memory-backend)
        acquired (control/acquire! store writer-support/base-options)
        writer
        (writer/start!
         {:store store :token (:token acquired) :handle :private-handle
          :database "private-db" :recovered-document (:head acquired)
          :lease-expiry 1 :lease-ttl-ms 3 :heartbeat-interval-ms 1
          :operations (assoc (writer-support/fake-operations (atom []) (atom 0))
                             :now-ms (constantly 1)
                             :await-heartbeat! (fn [_ _] :stop))})]
    (try
      (check "fenced writer rejects mutation"
             true (error? #(writer/execute! writer "INSERT INTO t VALUES (1)")))
      (check "fenced writer observation is unavailable"
             {:availability :unavailable} (writer/persistence-observation writer))
      (finally (try (writer/close! writer) (catch Throwable _ nil))))))

(defn run-checks! []
  (reset! failures 0)
  (direct-observation-checks!)
  (reader-observation-checks!)
  (with-redefs [writer/require-wal-byte-writer-capability! (constantly true)]
    (jdbc-observation-checks!)
    (forced-teardown-observation-checks!)
    (writer-error-transition-checks!)
    (writer-fence-check!))
  (when-not (zero? @failures)
    (throw (ex-info (str @failures " Durable persistence observation checks failed")
                    {:failures @failures})))
  (println "all Durable persistence observation checks passed")
  true)

(defn -main [& _] (run-checks!))
