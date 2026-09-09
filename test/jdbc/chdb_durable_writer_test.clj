(ns jdbc.chdb-durable-writer-test
  (:require [clojure.data.json :as json]
            [clojure.string :as str]
            [hegel.core :as h]
            [hegel.stateful :as hs]
            [jdbc.chdb :as chdb]
            [jdbc.chdb.durable.backend :as backend]
            [jdbc.chdb.durable.control :as control]
            [jdbc.chdb.durable.writer :as writer]
            [jolt.fibers :as fibers]))

(def failures (atom 0))

(def ^:private base-options
  {:owner "writer-1"
   :instance "instance-1"
   :expires-at 1000M
   :now 0M
   :clock-skew 0M
   :database "default"
   :engine-version "26.7.2-rc.2"
   :backup-format 1
   :min-reader "26.7.2-rc.2"})

(defn- check [label expected actual]
  (if (= expected actual)
    (println "  ok  " label)
    (do
      (swap! failures inc)
      (println "  FAIL" label "- expected" (pr-str expected)
               "got" (pr-str actual)))))

(defn- error-type [f]
  (try (f) nil (catch Throwable error (:type (ex-data error)))))

(defn- fake-operations [calls close-count]
  {:classification-sql! (fn [sql _] sql)
   :classify! (fn [_ sql _]
                {:query-class (if (str/starts-with? sql "SELECT")
                                :read-only :mutating)
                 :statement-count 1 :has-secrets false
                 :writes-only-target-database true
                 :changes-database-lifecycle false})
   :analyze-query! (fn [_ sql database]
                     (swap! calls conj [:analyze-query sql database]))
   :analyze-execute! (fn [_ sql database]
                       (swap! calls conj [:analyze-execute sql database]))
   :execute-native! (fn [_ sql _]
                      (swap! calls conj [:execute sql])
                      {:sql sql})
   :query-native! (fn [_ sql _]
                    (swap! calls conj [:execute sql])
                    {:sql sql})
   :close-native! (fn [_] (swap! close-count inc))
   :cleanup-scratch! (fn [] nil)})

(defn- model-checkpoint-operations [calls close-count]
  (let [bytes (.getBytes "abc" "UTF-8")]
    (assoc (fake-operations calls close-count)
           :create-checkpoint! (fn [_ _] :model-checkpoint)
           :delete-checkpoint! (fn [_] nil)
           :publish-checkpoint!
           (fn [store token _]
             (let [sequence (inc (get-in (:head (control/read-head! store))
                                         ["manifest" "seq"]))
                   reference
                   {"key" (str "checkpoints/" (:generation token) "-"
                               sequence "-" (format "%08x" sequence)
                               ".tar.gz")
                    "size" 3
                    "sha256" "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"}]
               (backend/put-bytes-if-absent!
                store (get reference "key") bytes)
               {:status :published :reference reference}))
           :verify-checkpoint-reference! control/verify-byte-reference!)))

(defn- checkpoint-fault-operations
  [calls close-count fault-state published-reference]
  (let [base (model-checkpoint-operations calls close-count)
        create-checkpoint! (:create-checkpoint! base)
        publish-checkpoint! (:publish-checkpoint! base)]
    (assoc
     base
     :create-checkpoint!
     (fn [handle database]
       (swap! calls conj [:backup])
       (if (= :backup-failure @fault-state)
         (throw (ex-info "checkpoint backup failed"
                         {:type ::backup-failure}))
         (create-checkpoint! handle database)))
     :delete-checkpoint!
     (fn [_] (swap! calls conj [:delete-checkpoint]))
     :publish-checkpoint!
     (fn [store token path]
       (swap! calls conj [:publish-checkpoint])
       (case @fault-state
         :upload-failure
         (throw (ex-info "checkpoint upload failed"
                         {:type ::control/object-unverified}))

         :upload-ambiguous
         (throw (ex-info "checkpoint upload outcome is ambiguous"
                         {:type ::control/commit-ambiguous}))

         :ownership-takeover
         (let [publication (publish-checkpoint! store token path)]
           (when-not @published-reference
             (reset! published-reference (:reference publication))
             (control/acquire!
              store
              (assoc base-options
                     :owner "writer-2" :instance "instance-2"
                     :now 1000M :expires-at 2000M)))
           publication)

         (publish-checkpoint! store token path))))))

(defn- ambiguous-head-store [delegate]
  (reify backend/ObjectBackend
    (get-bytes [_ key] (backend/get-bytes delegate key))
    (get-with-etag [_ key] (backend/get-with-etag delegate key))
    (put-file-if-absent! [_ key path]
      (backend/put-file-if-absent! delegate key path))
    (put-bytes-if-absent! [_ key bytes]
      (backend/put-bytes-if-absent! delegate key bytes))
    (replace-if-match! [_ key bytes etag]
      (if (= control/head-key key)
        {:status :ambiguous}
        (backend/replace-if-match! delegate key bytes etag)))
    (download-to-file! [_ key path]
      (backend/download-to-file! delegate key path))))

(defn- new-writer
  ([] (new-writer (atom []) (atom 0)))
  ([calls close-count]
   (new-writer calls close-count (fake-operations calls close-count)))
  ([calls close-count operations]
   (let [store (backend/memory-backend)
         acquired (control/acquire! store base-options)]
     {:store store
      :calls calls
      :close-count close-count
      :writer
      (writer/start!
       {:store store
        :token (:token acquired)
        :handle :fake-handle
        :database "default"
        :operations operations})})))

(defn- stored-wal-lines [store]
  (let [head (:head (control/read-head! store))]
    (mapv
     (fn [reference]
       (->> (String. (backend/get-bytes store (get reference "key")) "UTF-8")
            str/split-lines
            (mapv #(json/read-str %))))
     (get-in head ["manifest" "wal"]))))

(defn- run-deterministic-checks! []
  (println "Durable V1 serialized writer operations")
  (let [{:keys [store calls close-count writer]} (new-writer)]
    (check "read query crosses classification before execution"
           {:sql "SELECT 1"}
           (writer/query! writer "SELECT 1"))
    (check "query does not enter the statement WAL"
           {:lifecycle :open :writable? true
            :pending-wal-bytes 0 :pending-statements 0
            :checkpoint-required? false}
           (writer/status writer))
    (writer/execute! writer "INSERT INTO t VALUES (1)")
    (writer/execute! writer "INSERT INTO t VALUES (2)")
    (check "successful mutations remain pending before flush"
           2 (:pending-statements (writer/status writer)))
    (check "flush commits the complete pending segment"
           :committed (:status (writer/flush! writer)))
    (check "flush clears only the committed pending segment"
           0 (:pending-statements (writer/status writer)))
    (check "committed WAL preserves statement and line order"
           [[{"sql" "INSERT INTO t VALUES (1)"}
             {"sql" "INSERT INTO t VALUES (2)"}]]
           (stored-wal-lines store))
    (check "empty flush does not advance the manifest"
           [:empty 1]
           [(:status (writer/flush! writer))
            (get-in (:head (control/read-head! store)) ["manifest" "seq"])])
    (writer/execute! writer "INSERT INTO t VALUES (3)")
    (writer/close! writer)
    (writer/close! writer)
    (check "close flushes, releases, and closes exactly once"
           [2 nil 1 :closed]
           [(get-in (:head (control/read-head! store)) ["manifest" "seq"])
            (get-in (:head (control/read-head! store)) ["lease" "owner"])
            @close-count
            (:lifecycle (writer/status writer))])
    (check "operations after close fail with the public closed category"
           ::writer/closed
           (error-type #(writer/query! writer "SELECT 1")))
    (check "classification precedes every engine call"
           [[:analyze-query "SELECT 1" "default"]
            [:execute "SELECT 1"]
            [:analyze-execute "INSERT INTO t VALUES (1)" "default"]
            [:execute "INSERT INTO t VALUES (1)"]]
           (subvec @calls 0 4)))

  (let [{:keys [writer calls]} (new-writer)]
    (try
      (with-redefs [writer/max-statement-bytes 3]
        (check "statement limit rejects before classification and execution"
               ::writer/limit-exceeded
               (error-type #(writer/execute! writer "1234"))))
      (check "limit rejection has no engine side effect" [] @calls)
      (finally (writer/close! writer))))

  (let [{:keys [writer calls]} (new-writer)]
    (try
      (with-redefs [writer/max-statement-bytes 3]
        (check "bound mutation SQL retains the same pre-engine size limit"
               ::writer/limit-exceeded
               (error-type #(writer/sql! writer "1234" [42]))))
      (check "bound mutation limit rejection has no engine side effect"
             [] @calls)
      (finally (writer/close! writer))))

  (let [secret "durable-invalid-bound-secret"
        calls (atom [])
        close-count (atom 0)
        operations (assoc (fake-operations calls close-count)
                          :classification-sql! chdb/classification-sql)
        {:keys [writer]} (new-writer calls close-count operations)
        error (try
                (writer/sql! writer "INSERT INTO t VALUES (?)"
                             [{:secret secret}])
                nil
                (catch Throwable thrown thrown))]
    (try
      (check "invalid bound values fail before native execution"
             true (some? error))
      (check "invalid bound-value diagnostics do not retain the value"
             false (str/includes? (pr-str [(ex-message error)
                                           (ex-data error)])
                                  secret))
      (check "invalid bound values have no engine side effect" [] @calls)
      (finally (writer/close! writer))))

  (let [calls (atom [])
        close-count (atom 0)
        publications (atom [])
        operations
        (assoc (fake-operations calls close-count)
               :execute-native!
               (fn [_ sql params]
                 (swap! calls conj [:execute sql (vec params)])
                 {:sql sql :params (vec params)})
               :create-checkpoint!
               (fn [_ _]
                 (swap! calls conj [:backup])
                 :checkpoint-path)
               :delete-checkpoint! (fn [_] nil)
               :publish-checkpoint!
               (fn [_ _ _]
                 (swap! publications conj :checkpoint)
                 {:status :published
                  :reference {"key" "checkpoints/1-1-00000012.tar.gz"
                              "size" 3
                              "sha256" "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"}})
               :publish-wal!
               (fn [_ _ _]
                 (swap! publications conj :wal)
                 (throw (ex-info "WAL must not publish" {})))
               :commit-reference!
               (fn [_ _ request]
                 (swap! calls conj [:commit (:kind request)])
                 {:status :committed}))
        {:keys [writer]} (new-writer calls close-count operations)]
    (try
      (check "generic SQL routing preserves read-only parameters"
             {:sql "SELECT ?"}
             (writer/sql! writer "SELECT ?" [42]))
      (check "parameterized mutations preserve native bound values"
             {:sql "INSERT INTO t VALUES (?)" :params [42]}
             (writer/sql! writer "INSERT INTO t VALUES (?)" [42]))
      (check "bound mutations require a checkpoint without entering V1 WAL"
             [true 0 0]
             ((juxt :checkpoint-required? :pending-statements
                    :pending-wal-bytes)
              (writer/status writer)))
      (writer/sql! writer "INSERT INTO t VALUES (42)" [])
      (check "mixed materialized mutations remain pending until the checkpoint"
             [true 1]
             ((juxt :checkpoint-required? :pending-statements)
              (writer/status writer)))
      (check "flush checkpoints the complete state after a bound mutation"
             :committed (:status (writer/flush! writer)))
      (check "checkpoint commit clears both pending recovery requirements"
             [false 0 0]
             ((juxt :checkpoint-required? :pending-statements
                    :pending-wal-bytes)
              (writer/status writer)))
      (check "bound mutation flush never publishes a value-bearing V1 WAL"
             [:checkpoint] @publications)
      (check "bound values reach only the native execution operation"
             true (boolean
                   (some #{[:execute "INSERT INTO t VALUES (?)" [42]]}
                         @calls)))
      (finally (writer/close! writer))))

  (let [calls (atom [])
        close-count (atom 0)
        {:keys [store writer]}
        (new-writer calls close-count
                    (model-checkpoint-operations calls close-count))]
    (writer/sql! writer "INSERT INTO t VALUES (?)" [9])
    (writer/close! writer)
    (let [head (:head (control/read-head! store))]
      (check "successful close checkpoints an unflushed bound mutation"
             [1 true [] nil]
             [(get-in head ["manifest" "seq"])
              (boolean (get-in head ["manifest" "base"]))
              (get-in head ["manifest" "wal"])
              (get-in head ["lease" "owner"])])))

  (let [calls (atom [])
        close-count (atom 0)
        checkpoint-error
        (ex-info "checkpoint commit ambiguous"
                 {:type ::control/commit-ambiguous})
        operations
        (assoc (fake-operations calls close-count)
               :create-checkpoint! (fn [_ _] :checkpoint-path)
               :delete-checkpoint! (fn [_] nil)
               :publish-checkpoint!
               (fn [_ _ _]
                 {:status :published
                  :reference {"key" "checkpoints/1-1-00000013.tar.gz"
                              "size" 3
                              "sha256" "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"}})
               :commit-reference! (fn [_ _ _] (throw checkpoint-error)))
        {:keys [writer]} (new-writer calls close-count operations)]
    (writer/execute! writer "INSERT INTO t VALUES (1)")
    (writer/sql! writer "INSERT INTO t VALUES (?)" [2])
    (check "failed checkpoint fallback returns the storage failure unchanged"
           ::control/commit-ambiguous
           (error-type #(writer/flush! writer)))
    (check "failed checkpoint fallback retains WAL and checkpoint requirement"
           [1 true]
           ((juxt :pending-statements :checkpoint-required?)
            (writer/status writer)))
    (check "failed checkpoint fallback close still reaches terminal cleanup"
           ::control/commit-ambiguous
           (error-type #(writer/close! writer)))
    (check "failed close retains recovery state and closes the engine once"
           [:closed true 1 1]
           (let [status (writer/status writer)]
             [(:lifecycle status) (:checkpoint-required? status)
              (:pending-statements status) @close-count])))

  (doseq [[fault expected-error expected-calls]
          [[:backup-failure ::backup-failure
            [[:backup]]]
           [:upload-failure ::control/object-unverified
            [[:backup] [:publish-checkpoint] [:delete-checkpoint]]]
           [:upload-ambiguous ::control/commit-ambiguous
            [[:backup] [:publish-checkpoint] [:delete-checkpoint]]]]]
    (let [calls (atom [])
          close-count (atom 0)
          fault-state (atom fault)
          published-reference (atom nil)
          operations (checkpoint-fault-operations
                      calls close-count fault-state published-reference)
          {:keys [store writer]} (new-writer calls close-count operations)]
      (writer/execute! writer "INSERT INTO t VALUES (1)")
      (writer/sql! writer "INSERT INTO t VALUES (?)" [2])
      (check (str (name fault) " returns its exact failure category")
             expected-error
             (error-type #(writer/flush! writer)))
      (check (str (name fault) " retains both recovery obligations")
             [true 1 0]
             (let [status (writer/status writer)]
               [(:checkpoint-required? status)
                (:pending-statements status)
                (get-in (:head (control/read-head! store))
                        ["manifest" "seq"])]))
      (check (str (name fault) " reaches only its expected checkpoint stages")
             expected-calls
             (filterv #(contains? #{:backup :publish-checkpoint
                                    :delete-checkpoint}
                                  (first %))
                      @calls))
      (reset! fault-state nil)
      (check (str (name fault) " retained state commits on retry")
             :committed (:status (writer/flush! writer)))
      (check (str (name fault) " retry clears obligations after head commit")
             [false 0 1 true]
             (let [status (writer/status writer)]
               [(:checkpoint-required? status)
                (:pending-statements status)
                (get-in (:head (control/read-head! store))
                        ["manifest" "seq"])
                (boolean
                 (get-in (:head (control/read-head! store))
                         ["manifest" "base"]))]))
      (writer/close! writer)))

  (let [calls (atom [])
        close-count (atom 0)
        fault-state (atom :ownership-takeover)
        published-reference (atom nil)
        operations (checkpoint-fault-operations
                    calls close-count fault-state published-reference)
        {:keys [store writer]} (new-writer calls close-count operations)]
    (writer/execute! writer "INSERT INTO t VALUES (1)")
    (writer/sql! writer "INSERT INTO t VALUES (?)" [2])
    (check "takeover during checkpoint publication fences the stale writer"
           ::control/lease-fenced
           (error-type #(writer/flush! writer)))
    (let [status (writer/status writer)
          head (:head (control/read-head! store))]
      (check "fenced checkpoint fallback retains both recovery obligations"
             [true 1 0 "writer-2" 2]
             [(:checkpoint-required? status)
              (:pending-statements status)
              (get-in head ["manifest" "seq"])
              (get-in head ["lease" "owner"])
              (get-in head ["lease" "generation"])]))
    (check "takeover leaves the old generation checkpoint unreachable"
           [true nil]
           [(some? (backend/get-bytes
                    store (get @published-reference "key")))
            (get-in (:head (control/read-head! store)) ["manifest" "base"])])
    (check "fenced close returns the ownership failure"
           ::control/lease-fenced
           (error-type #(writer/close! writer)))
    (check "fenced close cleans the native handle without clearing recovery state"
           [:closed true 1 1]
           (let [status (writer/status writer)]
             [(:lifecycle status) (:checkpoint-required? status)
              (:pending-statements status) @close-count])))

  (let [calls (atom [])
        close-count (atom 0)
        first-sql "x\\y"
        second-sql "β"
        first-line-bytes (alength (.getBytes
                                   (str (json/write-str {"sql" first-sql}) "\n")
                                   "UTF-8"))
        {:keys [writer]} (new-writer calls close-count)]
    (try
      (with-redefs [writer/max-wal-segment-bytes first-line-bytes]
        (writer/execute! writer first-sql)
        (check "an escaped WAL record may exactly fill the segment limit"
               first-line-bytes (:pending-wal-bytes (writer/status writer)))
        (check "a multibyte record that crosses the cumulative limit is rejected"
               ::writer/limit-exceeded
               (error-type #(writer/execute! writer second-sql)))
        (check "segment rejection preserves the prior WAL and has no new engine call"
               [1 [[:analyze-execute first-sql "default"] [:execute first-sql]]]
               [(:pending-statements (writer/status writer)) @calls]))
      (finally (writer/close! writer))))

  (let [calls (atom [])
        close-count (atom 0)
        classifier-error (ex-info "classified failure" {:type ::classified-failure})
        operations (assoc (fake-operations calls close-count)
                          :analyze-execute!
                          (fn [_ _ _] (throw classifier-error)))
        {:keys [writer]} (new-writer calls close-count operations)]
    (try
      (check "classifier failure is returned unchanged"
             ::classified-failure
             (error-type #(writer/execute! writer "INSERT INTO t VALUES (1)")))
      (check "classifier failure reaches neither engine nor WAL"
             [0 []]
             [(:pending-statements (writer/status writer)) @calls])
      (finally (writer/close! writer))))

  (let [calls (atom [])
        close-count (atom 0)
        operations (assoc (fake-operations calls close-count)
                          :execute-native!
                          (fn [_ sql _]
                            (swap! calls conj [:execute sql])
                            (throw (ex-info "engine failure" {:type ::engine-failure}))))
        {:keys [writer]} (new-writer calls close-count operations)]
    (try
      (check "engine mutation failure is returned unchanged"
             ::engine-failure
             (error-type #(writer/execute! writer "INSERT INTO t VALUES (1)")))
      (check "engine mutation failure appends no WAL record"
             0 (:pending-statements (writer/status writer)))
      (finally (writer/close! writer))))

  (let [calls (atom [])
        close-count (atom 0)
        entered (promise)
        release-first (promise)
        operations
        (assoc (fake-operations calls close-count)
               :execute-native!
               (fn [_ sql _]
                 (swap! calls conj [:begin sql])
                 (when (= sql "first")
                   (deliver entered true)
                   @release-first)
                 (swap! calls conj [:end sql])
                 sql))
        {:keys [writer]} (new-writer calls close-count operations)
        first-call (fibers/spawn #(writer/execute! writer "first"))]
    @entered
    (let [second-call (fibers/spawn #(writer/execute! writer "second"))]
      (loop [remaining 1000]
        (when (and (pos? remaining) (zero? (.size (:queue writer))))
          (Thread/yield)
          (recur (dec remaining))))
      (deliver release-first true)
      (fibers/join first-call)
      (fibers/join second-call)
      (check "the explicit queue executes concurrent calls in FIFO order"
             [[:begin "first"] [:end "first"]
              [:begin "second"] [:end "second"]]
             (filterv #(contains? #{:begin :end} (first %)) @calls)))
    (writer/close! writer))

  (let [store (backend/memory-backend)
        acquired (control/acquire! store base-options)
        calls (atom [])
        close-count (atom 0)
        now (atom 1000M)
        writer
        (writer/start!
         {:store store :token (:token acquired) :handle :fake-handle
          :database "default" :lease-expiry 1000M
          :lease-ttl-ms 300 :heartbeat-interval-ms 100
          :operations
          (assoc (fake-operations calls close-count)
                 :now-ms #(deref now)
                 :await-heartbeat! (fn [stop _] @stop :stop))})]
    (check "a locally expired lease fences a mutation before engine access"
           ::control/lease-fenced
           (error-type #(writer/execute! writer "INSERT INTO t VALUES (1)")))
    (check "local self-fencing is visible without exposing lease identity"
           [false []]
           [(:writable? (writer/status writer)) @calls])
    (check "self-fenced close still performs cleanup"
           ::control/lease-fenced
           (error-type #(writer/close! writer)))
    (check "self-fenced cleanup closes the engine exactly once"
           1 @close-count))

  (let [store (backend/memory-backend)
        acquired (control/acquire! store base-options)
        calls (atom [])
        close-count (atom 0)
        now (atom 100M)
        execute-entered (promise)
        release-execute (promise)
        heartbeat-tick (promise)
        heartbeat-renewed (promise)
        waits (atom 0)
        operations
        (-> (fake-operations calls close-count)
            (assoc
             :now-ms #(deref now)
             :execute-native!
             (fn [_ _ _]
               (swap! calls conj :execute-begin)
               (deliver execute-entered true)
               @release-execute
               (swap! calls conj :execute-end)
               :executed)
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
               (swap! calls conj :publish)
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
          :operations operations})
        executing
        (fibers/spawn
         #(writer/execute! durable-writer "INSERT INTO t VALUES (1)"))]
    @execute-entered
    (let [closing
          (fibers/spawn
           #(try
              (writer/close! durable-writer)
              :closed
              (catch Throwable error (:type (ex-data error)))))]
      (loop [remaining 1000]
        (when (and (pos? remaining)
                   (not= :closing (:lifecycle (writer/status durable-writer))))
          (Thread/yield)
          (recur (dec remaining))))
      (deliver heartbeat-tick true)
      (check "heartbeat remains live after close admission while work drains"
             true (not= ::timeout
                        (deref heartbeat-renewed 1000 ::timeout)))
      ;; The renewal extends the lease from 1000 to 1001. A stopped heartbeat
      ;; leaves the close-time flush exactly at expiry and therefore fenced.
      (reset! now 1000M)
      (deliver release-execute true)
      (fibers/join executing)
      (check "close-time flush succeeds under the renewed lease"
             :closed (fibers/join closing))
      (check "close stops and joins heartbeat before release and cleanup"
             [:execute-begin :renew :execute-end :publish :commit
              :heartbeat-stop :release :native-close :cleanup]
             (filterv keyword? @calls))
      (check "close commits pending WAL and releases ownership"
             [1 nil 0 :closed 1]
             [(get-in (:head (control/read-head! store)) ["manifest" "seq"])
              (get-in (:head (control/read-head! store)) ["lease" "owner"])
              (:pending-statements (writer/status durable-writer))
              (:lifecycle (writer/status durable-writer))
              @close-count])))

  (let [store (backend/memory-backend)
        acquired (control/acquire! store base-options)
        calls (atom [])
        close-count (atom 0)
        entered (promise)
        release-execute (promise)
        heartbeat-tick (promise)
        heartbeat-renewed (promise)
        waits (atom 0)
        operations
        (-> (fake-operations calls close-count)
            (assoc :now-ms (fn [] 100M)
                   :create-checkpoint!
                   (fn [_ _]
                     (deliver entered true)
                     @release-execute
                     :checkpoint-path)
                   :delete-checkpoint! (fn [_] nil)
                   :publish-checkpoint!
                   (fn [_ _ _]
                     {:status :published
                      :reference {"key" "checkpoints/1-1-00000011.tar.gz"
                                  "size" 3
                                  "sha256" "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"}})
                   :commit-reference! (fn [_ _ _] {:status :committed})
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
        writer
        (writer/start!
         {:store store :token (:token acquired) :handle :fake-handle
          :database "default" :lease-expiry 1000M
          :lease-ttl-ms 300 :heartbeat-interval-ms 100
          :operations operations})
        executing (fibers/spawn #(writer/checkpoint! writer))]
    @entered
    (deliver heartbeat-tick true)
    @heartbeat-renewed
    (check "heartbeat renews while a long checkpoint is in flight"
           1001 (get-in (:head (control/read-head! store))
                        ["lease" "expires_at"]))
    (deliver release-execute true)
    (fibers/join executing)
    (writer/close! writer))

  (let [delegate (backend/memory-backend)
        acquired (control/acquire! delegate base-options)
        close-count (atom 0)
        writer
        (writer/start!
         {:store (ambiguous-head-store delegate)
          :token (:token acquired)
          :handle :fake-handle
          :database "default"
          :operations (fake-operations (atom []) close-count)})]
    (writer/execute! writer "INSERT INTO t VALUES (1)")
    (check "unprovable flush failure is distinguishable"
           ::control/commit-ambiguous
           (error-type #(writer/flush! writer)))
    (check "failed flush retains the complete pending WAL"
           1 (:pending-statements (writer/status writer)))
    (check "failed close still closes the native engine"
           ::control/commit-ambiguous
           (error-type #(writer/close! writer)))
    (check "failed close reaches the terminal lifecycle exactly once"
           [:closed 1]
           [(:lifecycle (writer/status writer)) @close-count])
    (check "a repeated failed close returns the same persistence category"
           ::control/commit-ambiguous
           (error-type #(writer/close! writer)))
    (check "a repeated failed close does not repeat native cleanup"
           1 @close-count))

  (let [store (backend/memory-backend)
        acquired (control/acquire! store base-options)
        heartbeat-entered (promise)
        heartbeat-error (ex-info "heartbeat scheduler failed"
                                 {:type ::heartbeat-failed})
        close-count (atom 0)
        durable-writer
        (writer/start!
         {:store store :token (:token acquired) :handle :fake-handle
          :database "default" :lease-expiry 1000M
          :lease-ttl-ms 300 :heartbeat-interval-ms 100
          :operations
          (assoc (fake-operations (atom []) close-count)
                 :now-ms (fn [] 100M)
                 :await-heartbeat!
                 (fn [_ _]
                   (deliver heartbeat-entered true)
                   (throw heartbeat-error)))})]
    @heartbeat-entered
    (let [returned
          (try
            (writer/close! durable-writer)
            nil
            (catch Throwable error error))]
      (check "close rethrows the exact owned heartbeat failure"
             true (identical? heartbeat-error returned)))
    (check "heartbeat failure still releases and closes owned resources"
           [nil 1 :closed]
           [(get-in (:head (control/read-head! store)) ["lease" "owner"])
            @close-count
            (:lifecycle (writer/status durable-writer))]))

  (let [calls (atom [])
        close-count (atom 0)
        take-entered (promise)
        release-take (promise)
        terminal (ex-info "worker queue failed" {:type ::worker-failed})
        operations
        (assoc (fake-operations calls close-count)
               :take-request!
               (fn [_]
                 (deliver take-entered true)
                 @release-take
                 (throw terminal)))
        {:keys [writer]} (new-writer calls close-count operations)]
    @take-entered
    (let [waiting (fibers/spawn
                   #(try
                      (writer/query! writer "SELECT 1")
                      nil
                      (catch Throwable error error)))]
      (loop [remaining 1000]
        (when (and (pos? remaining) (zero? (.size (:queue writer))))
          (Thread/yield)
          (recur (dec remaining))))
      (deliver release-take true)
      (check "terminal worker failure resolves a queued caller unchanged"
             true (identical? terminal (fibers/join waiting))))
    (check "terminal worker failure closes and cleans the writer"
           [:closed 1]
           [(:lifecycle (writer/status writer)) @close-count])
    (check "close after terminal worker failure returns the same cause"
           true
           (identical?
            terminal
            (try
              (writer/close! writer)
              nil
              (catch Throwable error error))))))

(defn- model-valid? [state]
  (let [status (writer/status (:writer state))
        head (:head (control/read-head! (:store state)))]
    (and (= (:pending state) (:pending-statements status))
         (= (:checkpoint-required? state)
            (:checkpoint-required? status))
         (= (:committed state) (get-in head ["manifest" "seq"])))))

(defn- execute-step [state]
  (writer/execute! (:writer state)
                   (str "INSERT INTO t VALUES (" (:next-id state) ")"))
  (-> state (update :pending inc) (update :next-id inc)))

(defn- query-step [state]
  (writer/query! (:writer state) "SELECT 1")
  state)

(defn- parameterized-step [state]
  (writer/sql! (:writer state) "INSERT INTO t VALUES (?)"
               [(:next-id state)])
  (-> state
      (assoc :checkpoint-required? true)
      (update :next-id inc)))

(defn- flush-step [state]
  (let [had-pending? (or (pos? (:pending state))
                         (:checkpoint-required? state))]
    (writer/flush! (:writer state))
    (cond-> (assoc state :pending 0 :checkpoint-required? false)
      had-pending? (update :committed inc))))

(defn- run-stateful-property! []
  (println "Durable writer Hegel queue/WAL state machine")
  (let [result
        (h/run-test!
         {:name "chdb/durable-writer-queue-wal"
          :database ""
          :derandomize? true
          :verbosity :quiet
          :test-cases 30
          :stateful-step-count 16}
         (fn [_]
           (let [calls (atom [])
                 close-count (atom 0)
                 {:keys [writer store]}
                 (new-writer calls close-count
                             (model-checkpoint-operations calls close-count))]
             (try
               (hs/run!
                {:initial-state {:writer writer :store store
                                 :pending 0 :checkpoint-required? false
                                 :committed 0 :next-id 1}
                 :rules [(hs/rule :execute execute-step)
                         (hs/rule :execute-parameterized parameterized-step)
                         (hs/rule :query query-step)
                         (hs/rule :flush flush-step)]
                 :invariants [(hs/invariant :model-matches-writer
                                            model-valid?)]})
               (finally (writer/close! writer))))))]
    (println "  hegel writer seed" (:seed result)
             "valid" (:valid-test-cases result))
    (when-not (and (:passed? result) (not (:flaky? result)))
      (swap! failures inc)
      (println "  FAIL writer property" (pr-str result)))))

(defn run-checks! []
  (reset! failures 0)
  (run-deterministic-checks!)
  (run-stateful-property!)
  (when-not (zero? @failures)
    (throw (ex-info (str @failures " Durable writer checks failed")
                    {:failures @failures})))
  (println "all Durable writer checks passed")
  true)

(defn -main [& _]
  (run-checks!))
