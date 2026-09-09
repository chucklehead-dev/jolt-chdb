(ns jdbc.chdb-durable-open-test
  (:require [db.jdbc]
            [db.export :as export]
            [jdbc.chdb.durable :as durable]
            [jdbc.chdb.durable.backend :as backend]
            [jdbc.chdb.durable.control :as control]
            [jdbc.chdb-durable-open-test-support :as support]
            [jdbc.chdb.durable.reader :as reader]
            [jdbc.chdb.durable.writer :as writer]
            [jdbc.core :as jdbc]
            [jolt.fibers :as fibers])
  (:import [java.nio.file Files Path]))

(def failures (atom 0))

(def ^:private initial-options
  {:owner "old-writer" :instance "old-instance"
   :expires-at 100M :now 0M :clock-skew 0M
   :database "tenant`one" :engine-version "26.7.2-rc.2"
   :backup-format 1 :min-reader "26.7.2-rc.2"})

(defn- check [label expected actual]
  (if (= expected actual)
    (println "  ok  " label)
    (do
      (swap! failures inc)
      (println "  FAIL" label "- expected" (pr-str expected)
               "got" (pr-str actual)))))

(defn- error-type [f]
  (try
    (f)
    nil
    (catch Throwable error
      (loop [current error]
        (when current
          (or (:type (ex-data current))
              (recur (.getCause current))))))))

(defn- prepare-raw-wal-store! [store payload]
  (let [token (:token (control/acquire! store initial-options))
        publication (control/publish-wal-bytes! store token payload)]
    (control/commit-reference!
     store token {:kind :wal :reference (:reference publication)
                  :verify-reference! control/verify-byte-reference!})
    (control/release! store token)
    store))

(defn- prepared-raw-wal-store [payload]
  (prepare-raw-wal-store! (backend/memory-backend) payload))

(defn- prepared-wal-store []
  (prepared-raw-wal-store
   (.getBytes "{\"sql\":\"INSERT INTO t VALUES (1)\"}\n" "UTF-8")))

(defn- prepared-checkpoint-store []
  (let [store (backend/memory-backend)
        token (:token (control/acquire! store initial-options))
        reference {"key" "checkpoints/1-1-00000011.tar.gz"
                   "size" 3
                   "sha256" "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"}]
    (backend/put-bytes-if-absent! store (get reference "key")
                                  (.getBytes "abc" "UTF-8"))
    (control/commit-reference!
     store token {:kind :checkpoint :reference reference
                  :verify-reference! control/verify-byte-reference!})
    (control/release! store token)
    store))

(defn- append-wal! [store sql]
  (let [token (:token
               (control/acquire!
                store (assoc initial-options
                             :owner "later-writer" :instance "later-instance"
                             :now 200M :expires-at 300M)))
        payload (.getBytes
                 (str "{\"sql\":\"" sql "\"}\n") "UTF-8")
        publication (control/publish-wal-bytes! store token payload)]
    (control/commit-reference!
     store token {:kind :wal :reference (:reference publication)
                  :verify-reference! control/verify-byte-reference!})
    (control/release! store token)))

(defn- run-deterministic-checks! []
  (println "Durable V1 public writer open and recovery")
  (check "release precedence orders release after its prerelease"
         true (pos? (durable/compare-release-versions
                     "26.7.2" "26.7.2-rc.2")))
  (check "release precedence compares numeric identifiers numerically"
         true (neg? (durable/compare-release-versions
                     "26.7.2-rc.2" "26.7.2-rc.10")))
  (check "future backup formats fail closed"
         ::durable/engine-incompatible
         (error-type
          #(durable/check-engine-compatibility!
            {"engine" {"backup_format" 2 "min_reader" "26.7.0"}}
            "26.7.2")))
  (check "an older running core fails the minimum-reader gate"
         ::durable/engine-incompatible
         (error-type
          #(durable/check-engine-compatibility!
            {"engine" {"backup_format" 1 "min_reader" "26.8.0"}}
            "26.7.2")))

  (let [namespace (backend/memory-backend)
        alpha-store (backend/object-backend namespace "alpha")
        _ (prepare-raw-wal-store!
           alpha-store
           (.getBytes "{\"sql\":\"INSERT INTO t VALUES (7)\"}\n" "UTF-8"))
        calls (atom [])
        clocks (atom [0M])
        close-count (atom 0)
        cleanup-count (atom 0)
        operations (support/fake-open-operations calls clocks close-count cleanup-count)]
    (let [opened (durable/open-reader!
                  {:namespace-backend namespace
                   :object-id "alpha"
                   :operations operations})]
      (check "namespace/object open recovers the selected object's WAL"
             ["INSERT INTO t VALUES (7)"]
             (mapv second (filter #(= :execute (first %)) @calls)))
      (reader/close! opened))
    (check "a sibling object remains absent"
           ::durable/not-found
           (error-type #(durable/open-reader!
                         {:namespace-backend namespace
                          :object-id "beta"
                          :operations operations})))
    (check "sibling lookup does not create a head"
           nil (backend/get-with-etag namespace "beta/head.json"))
    (check "namespace/object cannot be combined with an already-scoped store"
           ::durable/invalid-options
           (error-type #(durable/open-reader!
                         {:store alpha-store
                          :namespace-backend namespace
                          :object-id "alpha"
                          :operations operations})))
    (check "namespace and object identity are an indivisible pair"
           ::durable/invalid-options
           (error-type #(durable/open-reader!
                         {:namespace-backend namespace
                          :operations operations}))))

  (let [store (prepared-wal-store)
        calls (atom [])
        clocks (atom [200M 201M])
        close-count (atom 0)
        cleanup-count (atom 0)
        opened (durable/open-writer!
                {:store store :owner "new-writer" :instance "new-instance"
                 :database "ignored-for-existing" :lease-ttl-ms 100M
                 :operations (support/fake-open-operations calls clocks close-count
                                                           cleanup-count)})]
    (check "writer open creates, selects, and replays the stored database"
           [[:create "tenant`one"] [:use "tenant`one"]
            [:analyze-execute "INSERT INTO t VALUES (1)" "tenant`one"]
            [:execute "INSERT INTO t VALUES (1)"]]
           (filterv #(contains? #{:create :use :analyze-execute :execute}
                                (first %))
                    @calls))
    (check "writer open renews only after recovery"
           [2 0.301M]
           [(get-in (:head (control/read-head! store)) ["lease" "generation"])
            (get-in (:head (control/read-head! store)) ["lease" "expires_at"])])
    (writer/execute! opened "INSERT INTO t VALUES (2)")
    (check "queued checkpoint commits a new base and covers pending WAL"
           :committed (:status (writer/checkpoint! opened)))
    (check "checkpoint advances once, replaces base, and clears WAL buffers"
           [2 true [] 0]
           [(get-in (:head (control/read-head! store)) ["manifest" "seq"])
            (boolean (get-in (:head (control/read-head! store))
                             ["manifest" "base"]))
            (get-in (:head (control/read-head! store)) ["manifest" "wal"])
            (:pending-statements (writer/status opened))])
    (writer/close! opened)
    (check "public close flushes, releases, closes, and removes scratch"
           [2 nil 1 1 :closed]
           [(get-in (:head (control/read-head! store)) ["manifest" "seq"])
            (get-in (:head (control/read-head! store)) ["lease" "owner"])
            @close-count @cleanup-count (:lifecycle (writer/status opened))]))

  (let [store (prepared-checkpoint-store)
        calls (atom [])
        clocks (atom [200M 201M])
        close-count (atom 0)
        cleanup-count (atom 0)
        operations (assoc (support/fake-open-operations calls clocks close-count cleanup-count)
                          :restore-database!
                          (fn [_ _ _]
                            (throw (ex-info "restore failed" {}))))]
    (check "a compatible full-archive restore failure is engine-incompatible"
           ::durable/engine-incompatible
           (error-type
            #(durable/open-writer!
              {:store store :owner "new-writer" :instance "new-instance"
               :database "ignored" :lease-ttl-ms 100M
               :operations operations})))
    (check "failed recovery closes, releases, and removes scratch"
           [1 1 nil]
           [@close-count @cleanup-count
            (get-in (:head (control/read-head! store)) ["lease" "owner"])]))

  (let [namespace (backend/memory-backend)
        store (backend/object-backend namespace "jdbc-object")
        calls (atom [])
        clocks (atom [0M 1M])
        close-count (atom 0)
        cleanup-count (atom 0)
        operations
        (assoc (support/fake-open-operations calls clocks close-count cleanup-count)
               :query-native!
               (fn [_ _ params]
                 {:labels ["value"] :rows [[(first params)]] :count 1})
               :execute-native!
               (fn [_ sql params]
                 (swap! calls conj [:jdbc-execute sql (vec params)])
                 {:labels [] :rows [] :count 0}))]
    (with-open [connection
                (jdbc/connection
                 {:vendor "chdb-durable"
                  :namespace-backend namespace :object-id "jdbc-object"
                  :owner "jdbc-writer" :instance "jdbc-instance"
                  :database "default" :lease-ttl-ms 300M
                  :operations operations})]
      (check "Durable writer connection reports its role"
             :writer (durable/connection-role connection))
      (check "jdbc.core adapter preserves bound parameters for reads"
             [{:value 42}]
             (jdbc/fetch connection ["SELECT ?" 42]))
      (check "writer adapter routes query-bytes through its queue"
             [1 2 3]
             (vec (:bytes (export/query-bytes
                           connection ["SELECT ?" 42]
                           {:format :parquet}))))
      (check "jdbc.core adapter admits fully materialized mutations"
             0 (jdbc/execute! connection "INSERT INTO t VALUES (42)"))
      (check "Durable JDBC flush publishes the pending WAL"
             :committed (:status (durable/flush! connection)))
      (check "jdbc.core admits a native bound Durable mutation"
             0 (jdbc/execute! connection
                              ["INSERT INTO t VALUES (?)" 43]))
      (check "Durable JDBC flush checkpoints bound values"
             :committed (:status (durable/flush! connection)))
      (check "the native operation receives the original bound value"
             true (boolean
                   (some #{[:jdbc-execute "INSERT INTO t VALUES (?)" [43]]}
                         @calls)))
      (jdbc/execute! connection "INSERT INTO t VALUES (43)")
      (check "Durable JDBC checkpoint publishes the complete database"
             :committed (:status (durable/checkpoint! connection))))
    (check "jdbc.core close owns Durable release and cleanup"
           [nil 1 1]
           [(get-in (:head (control/read-head! store)) ["lease" "owner"])
            @close-count @cleanup-count])
    (check "checkpoint replaces earlier WAL at the JDBC extension boundary"
           [3 true []]
           (let [head (:head (control/read-head! store))]
             [(get-in head ["manifest" "seq"])
              (some? (get-in head ["manifest" "base"]))
              (get-in head ["manifest" "wal"])])))

  (let [store (backend/memory-backend)
        calls (atom [])
        clocks (atom [0M])
        close-count (atom 0)
        cleanup-count (atom 0)]
    (check "read-only open reports not-found"
           ::durable/not-found
           (error-type
            #(durable/open-reader!
              {:store store
               :operations (support/fake-open-operations
                            calls clocks close-count cleanup-count)})))
    (check "read-only not-found does not create head.json"
           nil (backend/get-with-etag store control/head-key)))

  (let [store (prepared-wal-store)
        calls (atom [])
        clocks (atom [0M])
        close-count (atom 0)
        cleanup-count (atom 0)
        base-operations (support/fake-open-operations
                         calls clocks close-count cleanup-count)
        create-scratch! (:create-scratch! base-operations)
        operations
        (assoc base-operations
               :create-scratch!
               (fn [parent]
                 ;; This happens after the reader's one head read. Recovery
                 ;; must remain pinned to the earlier manifest snapshot.
                 (append-wal! store "INSERT INTO t VALUES (2)")
                 (create-scratch! parent))
               :analyze-query!
               (fn [_ sql database]
                 (when-not (= "SELECT ?" sql)
                   (throw (ex-info "not read-only"
                                   {:type ::durable/read-only-required})))
                 (swap! calls conj [:analyze-query sql database])))]
    (with-open [connection
                (jdbc/connection
                 {:vendor "chdb-durable" :backend store :read-only? true
                  :operations operations})]
      (check "Durable reader connection reports its role"
             :reader (durable/connection-role connection))
      (check "reader restores only the first immutable manifest snapshot"
             ["INSERT INTO t VALUES (1)"]
             (mapv second (filter #(= :execute (first %)) @calls)))
      (check "reader preserves bound parameters"
             [{:value 9}] (jdbc/fetch connection ["SELECT ?" 9]))
      (check "reader rejects a mutation before native execution"
             ::durable/read-only-required
             (error-type #(jdbc/execute! connection
                                          "INSERT INTO t VALUES (3)")))
      (check "reader routes query-bytes through its serialized queue"
             [1 2 3]
             (vec (:bytes (export/query-bytes
                           connection ["SELECT ?" 7]
                           {:format :arrow}))))
      (check "read-only Durable JDBC connections cannot flush"
             ::durable/read-only-required
             (error-type #(durable/flush! connection))))
    (check "reader close closes and cleans without changing lease state"
           [1 1 nil]
           [@close-count @cleanup-count
            (get-in (:head (control/read-head! store)) ["lease" "owner"])]))

  (let [store (prepared-raw-wal-store
               (byte-array [(unchecked-byte 0xc3) 0x28 0x0a]))
        calls (atom [])
        clocks (atom [0M])
        close-count (atom 0)
        cleanup-count (atom 0)]
    (check "reader rejects noncanonical UTF-8 WAL bytes"
           ::durable/corrupt
           (error-type
            #(durable/open-reader!
              {:store store
               :operations (support/fake-open-operations
                            calls clocks close-count cleanup-count)})))
    (check "corrupt reader recovery still closes and cleans"
           [1 1]
           [@close-count @cleanup-count]))

  (let [store (prepared-wal-store)
        calls (atom [])
        clocks (atom [0M])
        close-count (atom 0)
        cleanup-count (atom 0)
        take-entered (promise)
        release-take (promise)
        operations
        (assoc (support/fake-open-operations calls clocks close-count cleanup-count)
               :take-request!
               (fn [_]
                 (deliver take-entered true)
                 @release-take
                 (throw (ex-info "reader worker failed"
                                 {:type ::reader-worker-failed}))))
        opened (durable/open-reader! {:store store :operations operations})]
    @take-entered
    (let [queued (fibers/spawn
                  #(error-type #(reader/query! opened "SELECT ?" [1])))]
      (loop [remaining 1000]
        (when (and (pos? remaining) (zero? (.size (:queue opened))))
          (Thread/yield)
          (recur (dec remaining))))
      (deliver release-take true)
      (check "terminal reader worker failure resolves a queued query"
             ::reader-worker-failed (fibers/join queued)))
    (check "terminal reader worker failure closes and cleans"
           [:closed 1 1]
           [(:lifecycle (reader/status opened))
            @close-count @cleanup-count])
    (check "reader close after terminal failure returns the same cause"
           ::reader-worker-failed
           (error-type #(reader/close! opened))))
  (with-open [connection (jdbc/connection "chdb::memory:")]
    (check "ordinary chDB connection is not a Durable role"
           true
           (try
             (durable/connection-role connection)
             false
             (catch Throwable _ true))))
  true)

(defn run-checks! []
  (reset! failures 0)
  (run-deterministic-checks!)
  (when-not (zero? @failures)
    (throw (ex-info (str @failures " Durable open checks failed")
                    {:failures @failures})))
  (println "all Durable open checks passed")
  true)

(defn -main [& _]
  (run-checks!))
