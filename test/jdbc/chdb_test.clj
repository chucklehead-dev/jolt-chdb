(ns jdbc.chdb-test
  (:require [db.jdbc]
            [db.driver :as driver]
            [honey.sql :as sql]
            [jdbc.chdb :as chdb]
            [jdbc.chdb.native :as native]
            [jdbc.chdb-property-test :as property]
            [jdbc.core :as jdbc]
            [jdbc.proto :as proto]))

(def failures (atom 0))

(defn- check [label expected actual]
  (if (= expected actual)
    (println "  ok  " label)
    (do
      (swap! failures inc)
      (println "  FAIL" label "- expected" (pr-str expected) "got" (pr-str actual)))))

(defn- throws? [f]
  (try (f) false (catch Throwable _ true)))

(defn- run-query-checks []
  (println "chDB query and compatibility checks")
  (with-open [conn (jdbc/connection "chdb::memory:")]
    (check "database product metadata" "ClickHouse (chDB)"
           (.getDatabaseProductName (.getMetaData (proto/connection conn))))
    (check "URI dbspec retains the default logical database" "default"
           (:database (jdbc/fetch-one conn "select currentDatabase() database")))
    (check "DDL update count" 0
           (jdbc/execute! conn "create table event (id Int64, name String) engine=Memory"))
    (check "typed positional insert count" 1
           (jdbc/execute! conn ["insert into event values (?, ?)" 1 "one"]))
    (check "parameterized rows" [{:id 1 :name "one"}]
           (jdbc/fetch conn ["select id, name from event where id = ?" 1]))
    (check "backslashes survive String parameter parsing" "a\\b"
           (:value (jdbc/fetch-one conn ["select ? as value" "a\\b"])))
    (check "all byte values use lossless ClickHouse escapes" "5C00FF"
           (:value (jdbc/fetch-one
                    conn
                    ["select hex(?) as value" (byte-array [92 0 -1])])))
    (check "literal and comments do not consume placeholders" "ok"
           (:value (jdbc/fetch-one
                    conn
                    ["select '?' as literal, ? as value /* ? */ -- ?\n" "ok"])))
    (check "nullable typed parameter" 1
           (:missing (jdbc/fetch-one
                      conn
                      ["select isNull(?) as missing"
                       (chdb/typed-param "Nullable(String)" nil)])))
    (check "bare nil requires an explicit ClickHouse type" true
           (throws? #(jdbc/fetch conn ["select ?" nil])))

    (let [query (sql/format {:select [:id :name]
                             :from [:event]
                             :where [:= :id 1]})]
      (check "basic HoneySQL select vector" [{:id 1 :name "one"}]
             (jdbc/fetch conn query)))

    (jdbc/execute! conn "create table streamed (id Int64, name String) engine=Memory")
    (chdb/stream-insert! conn
                         "insert into streamed"
                         ["{\"id\":2,\"name\":\"two\"}\n"
                          (byte-array (map int (.getBytes "{\"id\":3,\"name\":\"three\"}\n")))])
    (check "streaming insert owns and consumes every chunk"
           [{:id 2 :name "two"} {:id 3 :name "three"}]
           (jdbc/fetch conn "select id, name from streamed order by id"))

    (let [body-ran (atom false)]
      (check "transaction-less driver rejects before body" true
             (throws? #(jdbc/atomic conn (reset! body-ran true))))
      (check "rejected transaction body did not run" false @body-ran))
    (check "generated-key requests are rejected" true
           (throws? #(jdbc/insert! conn :event {:id 4 :name "four"}
                                  {:returning true}))))

  (let [conn (jdbc/connection "chdb::memory:")]
    (.close conn)
    (.close conn)
    (check "close is idempotent and use-after-close fails" true
           (throws? #(jdbc/fetch conn "select 1")))))

(defn- run-logical-database-checks []
  (println "chDB logical database selection")
  (check "descriptor advertises logical database dbspec support"
         {:spec-key :database :create-if-missing true :identifier :ascii-simple}
         (get-in (driver/driver-descriptor chdb/chdb-driver)
                 [:constraints :logical-databases]))
  (with-open [default-conn (jdbc/connection {:vendor "chdb" :name ":memory:"})]
    (check "map dbspec without logical database retains default" "default"
           (:database (jdbc/fetch-one default-conn
                                      "select currentDatabase() database"))))
  (let [base {:vendor "chdb" :name ":memory:"}
        a (jdbc/connection (assoc base :database "logical_a"))
        b (jdbc/connection (assoc base :database :logical_b))]
    (try
      (check "string logical database is selected" "logical_a"
             (:database (jdbc/fetch-one a "select currentDatabase() database")))
      (check "keyword logical database is selected" "logical_b"
             (:database (jdbc/fetch-one b "select currentDatabase() database")))
      (jdbc/execute! a "create table same_name (value String) engine=Memory")
      (jdbc/execute! b "create table same_name (value String) engine=Memory")
      (jdbc/execute! a ["insert into same_name values (?)" "from-a"])
      (jdbc/execute! b ["insert into same_name values (?)" "from-b"])
      (check "same table name is isolated in first logical database"
             [{:value "from-a"}]
             (jdbc/fetch a "select value from same_name"))
      (check "same table name is isolated in second logical database"
             [{:value "from-b"}]
             (jdbc/fetch b "select value from same_name"))
      (check "logical databases retain one physical storage claim" 2
             (:references (native/active-storage)))
      (finally
        (.close b)
        (.close a))))
  (let [before (native/active-storage)]
    (check "hostile logical database identifier is rejected" true
           (throws? #(jdbc/connection
                      {:vendor "chdb" :name ":memory:"
                       :database "safe`; DROP DATABASE default; --"})))
    (check "overlength logical database identifier is rejected" true
           (throws? #(jdbc/connection
                      {:vendor "chdb" :name ":memory:"
                       :database (apply str (repeat 256 "a"))})))
    (check "non-string logical database identifier is rejected" true
           (throws? #(jdbc/connection
                      {:vendor "chdb" :name ":memory:" :database 42})))
    (check "namespaced keyword logical database is rejected" true
           (throws? #(jdbc/connection
                      {:vendor "chdb" :name ":memory:" :database :tenant/data})))
    (check "invalid names are rejected before claiming native storage" before
           (native/active-storage))))

(defn- run-storage-checks []
  (println "chDB process storage ownership")
  (let [a (native/open! ":memory:")
        b (native/open! ":memory:")]
    (try
      (check "same-path handles share the process claim" 2
             (:references (native/active-storage)))
      (check "a different path fails while memory storage is live" true
             (throws? #(native/open! "/tmp/jolt-chdb-forbidden-second-path")))
      (finally
        (native/close! b)
        (native/close! a))))
  (check "last close releases process storage claim"
         {:path nil :references 0}
         (native/active-storage)))

(defn -main [& _]
  (reset! failures 0)
  (run-query-checks)
  (run-logical-database-checks)
  (run-storage-checks)
  (property/run-properties!)
  (if (zero? @failures)
    (println "all checks passed")
    (throw (ex-info (str @failures " checks failed") {:failures @failures}))))
