(ns jdbc.chdb-test
  (:require [clojure.string :as str]
            [db.jdbc]
            [db.driver :as driver]
            [db.export :as export]
            [honey.sql :as sql]
            [jdbc.chdb :as chdb]
            [jdbc.chdb-durable-head-test :as durable-head]
            [jdbc.chdb-durable-head-whitespace-test :as durable-head-whitespace]
            [jdbc.chdb-durable-head-depth-test :as durable-head-depth]
            [jdbc.chdb-durable-backend-test :as durable-backend]
            [jdbc.chdb-durable-aspect-manifest-test :as durable-aspect-manifest]
            [jdbc.chdb-durable-control-test :as durable-control]
            [jdbc.chdb-durable-itf-test :as durable-itf]
            [jdbc.chdb-durable-engine-metadata-itf-test :as durable-engine-metadata-itf]
            [jdbc.chdb-durable-writer-test :as durable-writer]
            [jdbc.chdb-durable-writer-concurrency-test :as durable-writer-concurrency]
            [jdbc.chdb-durable-worker-join-test :as durable-worker-join]
            [jdbc.chdb-durable-open-test :as durable-open]
            [jdbc.chdb-durable-compatibility-test :as durable-compatibility]
            [jdbc.chdb-durable-epoch-seconds-test :as durable-epoch-seconds]
            [jdbc.chdb-durable-dbspec-test :as durable-dbspec]
            [jdbc.chdb-durable-local-test :as durable-local]
            [jdbc.chdb-durable-s3-test :as durable-s3]
            [jdbc.chdb-durable-policy-test :as durable-policy]
            [jdbc.chdb-durable-retry-test :as durable-retry]
            [jdbc.chdb.native :as native]
            [jdbc.chdb-property-test :as property]
            [jdbc.core :as jdbc]
            [jdbc.proto :as proto]
            [jolt.ffi :as ffi]))

(def failures (atom 0))

(defn- check [label expected actual]
  (if (= expected actual)
    (println "  ok  " label)
    (do
      (swap! failures inc)
      (println "  FAIL" label "- expected" (pr-str expected) "got" (pr-str actual)))))

(defn- throws? [f]
  (try (f) false (catch Throwable _ true)))

(defn- rejected [f]
  (try (f) nil (catch Throwable error error)))

(defn- sql-exception? [error]
  (instance? java.sql.SQLException error))

(defn- cause-data [error]
  (some-> error ex-cause ex-data))

(defn- ascii [bytes]
  (apply str (map #(char (bit-and (int %) 255)) bytes)))

(defn- byte-prefix [bytes n]
  (ascii (take n bytes)))

(defn- byte-suffix [bytes n]
  (ascii (take-last n bytes)))

(defn- run-ffi-write-order-checks []
  (println "chDB FFI write argument order")
  (let [allocated (atom [])
        writes (atom [])
        buffers [{:pointer 101 :length 11}
                 {:pointer 202 :length 22}]]
    (with-redefs [ffi/alloc (fn [_] :array-pointer)
                  ffi/sizeof (fn [_] 8)
                  ffi/write (fn [pointer type value offset]
                              (swap! writes conj [pointer type value offset]))]
      (#'chdb/pointer-array! allocated buffers)
      (check "pointer array writes values before offsets"
             [[:array-pointer :pointer 101 0]
              [:array-pointer :pointer 202 8]]
             @writes)
      (reset! writes [])
      (#'chdb/length-array! allocated buffers)
      (check "length array writes values before offsets"
             [[:array-pointer :size_t 11 0]
              [:array-pointer :size_t 22 8]]
             @writes))))

(def expected-query-bytes-capability
  {:version 1
   :formats {:arrow {:content-type "application/vnd.apache.arrow.file"
                     :extension "arrow"}
             :parquet {:content-type "application/vnd.apache.parquet"
                       :extension "parquet"}}
   :limits {:max-rows 100000 :max-bytes 67108864
            :default-max-rows 100000 :default-max-bytes 67108864}
   :staging :memory})

(def query-statistics-keys
  #{:elapsed-seconds :result-rows :result-bytes
    :storage-rows-read :storage-bytes-read
    :rows-written :bytes-written})

(defn- run-query-checks []
  (println "chDB query and compatibility checks")
  (check "classification SQL retains types but no parameter values"
         "select {p1:Int64}, {p2:String}"
         (chdb/classification-sql "select ?, ?" [42 "private-value"]))
  (check "classification SQL does not expose a secret parameter"
         false
         (str/includes?
          (chdb/classification-sql "select ?" ["private-value"])
          "private-value"))
  (let [secret "unsupported-parameter-secret"
        error (try
                (chdb/classification-sql "select ?" [{:secret secret}])
                nil
                (catch Throwable thrown thrown))]
    (check "unsupported parameter diagnostics omit the parameter value"
           false
           (str/includes? (pr-str [(ex-message error) (ex-data error)])
                          secret)))
  (with-open [conn (jdbc/connection "chdb::memory:")]
    (check "database product metadata" "ClickHouse (chDB)"
           (.getDatabaseProductName (.getMetaData (proto/connection conn))))
    (check "URI dbspec retains the default logical database" "default"
           (:database (jdbc/fetch-one conn "select currentDatabase() database")))
    (check "DDL update count" 0
           (jdbc/execute! conn "create table event (id Int64, name String) engine=Memory"))
    (check "typed positional insert count" 1
           (jdbc/execute! conn ["insert into event values (?, ?)" 1 "one"]))
    (let [accessor (fn [_]
                     (throw (ex-info "statistics accessor ran" {})))]
      (with-redefs [native/chdb-result-elapsed accessor
                    native/chdb-result-rows-read accessor
                    native/chdb-result-bytes-read accessor
                    native/chdb-result-storage-rows-read accessor
                    native/chdb-result-storage-bytes-read accessor
                    native/chdb-result-rows-written accessor
                    native/chdb-result-bytes-written accessor]
        (check "unobserved success performs no statistics FFI calls" 1
               (:n (jdbc/fetch-one conn "select 1 n")))
        (check "active collector performs statistics FFI calls" true
               (throws? #(chdb/with-query-statistics
                          (fn [] (jdbc/fetch-one conn "select 1 n")))))))
    (let [{:keys [result queries]}
          (chdb/with-query-statistics
           #(jdbc/fetch conn "select id, name from event order by id"))
          statistics (first queries)]
      (check "statistics collector preserves the JDBC result"
             [{:id 1 :name "one"}] result)
      (check "one native query produces one statistics record" 1
             (count queries))
      (check "result statistics expose the complete stable C API"
             query-statistics-keys
             (set (keys statistics)))
      (check "SELECT statistics report its result row" 1
             (:result-rows statistics))
      (check "SELECT statistics report serialized result bytes" true
             (pos? (:result-bytes statistics)))
      (check "SELECT statistics do not claim writes" [0 0]
             [(:rows-written statistics) (:bytes-written statistics)])
      (check "native elapsed time is a plausible double" true
             (let [elapsed (:elapsed-seconds statistics)]
               (and (number? elapsed) (<= 0.0 elapsed) (< elapsed 60.0)))))
    (let [statistics
          (-> (chdb/with-query-statistics
               #(jdbc/fetch-one conn
                                "select sum(number) total from numbers(1000)"))
              :queries first)]
      (check "native statistics distinguish result from storage rows"
             [1 1000]
             [(:result-rows statistics) (:storage-rows-read statistics)])
      (check "native statistics report exact generated storage bytes"
             8000 (:storage-bytes-read statistics)))
    (let [{:keys [result queries]}
          (chdb/with-query-statistics
           #(jdbc/execute! conn ["insert into event values (?, ?)" 2 "two"]))
          statistics (first queries)]
      (check "statistics collector preserves the update count" 1 result)
      (check "INSERT statistics report one written row" 1
             (:rows-written statistics))
      (check "INSERT statistics report native written bytes" true
             (pos? (:bytes-written statistics))))
    (let [capture
          (chdb/with-query-statistics
           #(do (jdbc/fetch-one conn "select 1 n")
                (jdbc/fetch-one conn "select 2 n")
                :complete))]
      (check "collector preserves an arbitrary thunk result" :complete
             (:result capture))
      (check "collector retains native query order" 2
             (count (:queries capture))))
    (let [outer
          (chdb/with-query-statistics
           #(chdb/with-query-statistics
             #(jdbc/fetch-one conn "select 3 n")))]
      (check "nested collectors both observe the query" [1 1]
             [(count (:queries outer))
              (count (get-in outer [:result :queries]))]))
    (let [data (try
                 (chdb/with-query-statistics
                  #(jdbc/fetch conn "select no_such_column"))
                 nil
                 (catch Throwable error (cause-data error)))]
      (check "failed native query retains its statistics" true
             (= query-statistics-keys
                (set (keys (:db.chdb/query-statistics data))))))
    (let [data (try
                 (chdb/with-query-statistics
                  #(do (jdbc/fetch-one conn "select 1 n")
                       (jdbc/fetch conn "select no_such_column")))
                 nil
                 (catch Throwable error (cause-data error)))]
      (check "throwing thunk retains all completed query statistics" 2
             (count (:db.chdb/query-statistics-collected data))))
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

(defn- run-encoded-query-checks []
  (println "chDB bounded Arrow/Parquet queries")
  (check "public capability has truthful formats, staging, defaults, and limits"
         expected-query-bytes-capability chdb/query-bytes-capability)
  (check "descriptor advertises the exact neutral query-bytes capability"
         expected-query-bytes-capability
         (export/query-bytes-capability chdb/chdb-driver))
  (check "advertising driver implements QueryBytesDriver" true
         (satisfies? export/QueryBytesDriver chdb/chdb-driver))
  (with-open [conn (jdbc/connection "chdb::memory:")]
    (let [arrow (chdb/query-bytes
                 conn ["select throwIf(? != 7), ? as value" 7 "bound-arrow"]
                 {:format :arrow :max-rows 10 :max-bytes 1048576})
          parquet (export/query-bytes
                   conn ["select throwIf(? != 8), ? as value" 8 "bound-parquet"]
                   {:format :parquet :max-rows 10 :max-bytes 1048576})
          public-keys #{:format :content-type :extension :byte-count :bytes}]
      (check "compatibility wrapper returns exactly the neutral five keys"
             public-keys (set (keys arrow)))
      (check "generic SPI returns exactly the neutral five keys"
             public-keys (set (keys parquet)))
      (check "Arrow metadata"
             {:format :arrow :content-type "application/vnd.apache.arrow.file"
              :extension "arrow"}
             (select-keys arrow [:format :content-type :extension]))
      (check "Arrow file prefix" "ARROW1" (byte-prefix (:bytes arrow) 6))
      (check "Arrow file suffix" "ARROW1" (byte-suffix (:bytes arrow) 6))
      (check "Arrow byte count owns exact copied array length"
             (:byte-count arrow) (alength (:bytes arrow)))
      (check "Parquet metadata"
             {:format :parquet :content-type "application/vnd.apache.parquet"
              :extension "parquet"}
             (select-keys parquet [:format :content-type :extension]))
      (check "Parquet file prefix" "PAR1" (byte-prefix (:bytes parquet) 4))
      (check "Parquet file suffix" "PAR1" (byte-suffix (:bytes parquet) 4))
      (check "Parquet byte count owns exact copied array length"
             (:byte-count parquet) (alength (:bytes parquet))))

    (let [error (rejected
                 #(export/query-bytes
                   conn "select 1" {:format :arrow :unknown true}))]
      (check "neutral option failure is a SQLException" true
             (sql-exception? error))
      (check "neutral option failure preserves boundary cause data" true
             (:db.export/query-bytes (cause-data error))))

    (let [error (rejected
                 #(chdb/query-bytes
                   conn "create table forbidden (n Int64) engine=Memory"
                   {:format :parquet}))]
      (check "compatibility wrapper driver rejection is a SQLException" true
             (sql-exception? error))
      (check "compatibility wrapper preserves chDB cause data" true
             (:db.chdb/query-bytes (cause-data error))))

    (check "HoneySQL formatted SQL vector exports unchanged" "PAR1"
           (byte-prefix
            (:bytes (chdb/query-bytes
                     conn (sql/format {:select [[1 :one]]})
                     {:format :parquet})) 4))

    (check "row cap is enforced by ClickHouse before returning bytes" true
           (throws? #(chdb/query-bytes conn "select number from numbers(2)"
                                       {:format :arrow :max-rows 1
                                        :max-bytes 1048576})))
    (check "ordinary JDBC works after Arrow row-cap recovery" 9
           (:n (jdbc/fetch-one conn "select 9 n")))
    (check "byte cap is enforced by ClickHouse before returning bytes" true
           (throws? #(chdb/query-bytes
                      conn "select number, cityHash64(number) from numbers(100000)"
                      {:format :parquet :max-rows 100000 :max-bytes 1000})))
    (check "ordinary JDBC works after Parquet byte-cap recovery" 10
           (:n (jdbc/fetch-one conn "select 10 n")))
    (check "WITH SELECT is accepted by the read-only shape" "with-ok"
           (let [result (chdb/query-bytes
                         conn "with 'with-ok' as value select value"
                         {:format :parquet})]
             (when (= "PAR1" (byte-prefix (:bytes result) 4)) "with-ok")))
    (check "semicolon inside a quoted value is not a statement separator" "PAR1"
           (byte-prefix
            (:bytes (chdb/query-bytes conn "select ';' as value"
                                      {:format :parquet})) 4))
    (check "unbalanced caller SQL cannot escape the bounded subquery" true
           (throws? #(chdb/query-bytes
                      conn "select 1) AS escaped; DROP TABLE encoded_escape; SELECT * FROM ("
                      {:format :arrow})))
    (check "DDL cannot occupy the encoded SELECT subquery" true
           (throws? #(chdb/query-bytes
                      conn "create table encoded_escape (n Int64) engine=Memory"
                      {:format :parquet})))
    (check "failed DDL did not create a table" 0
           (:n (jdbc/fetch-one
                conn
                "select count() n from system.tables where database=currentDatabase() and name='encoded_escape'")))

    ;; Count around the real destructor. These cases use real native results but
    ;; force each Jolt-side failure edge after query execution.
    (let [real-destroy native/chdb-destroy-query-result
          destroys (atom 0)]
      (with-redefs [native/chdb-destroy-query-result
                    (fn [result]
                      (swap! destroys inc)
                      (real-destroy result))]
        (chdb/query-bytes conn "select 1" {:format :parquet})
        (check "successful encoded result is destroyed exactly once" 1 @destroys)))

    (let [real-destroy native/chdb-destroy-query-result
          real-length native/chdb-result-length
          destroys (atom 0)]
      (with-redefs [native/chdb-destroy-query-result
                    (fn [result]
                      (swap! destroys inc)
                      (real-destroy result))
                    native/chdb-result-length
                    (fn [result]
                      ;; Preserve a real result and only falsify its reported
                      ;; serialized length beyond the caller cap.
                      (inc (max 1024 (real-length result))))]
        (check "post-serialization cap rejects before copying" true
               (throws? #(chdb/query-bytes conn "select 1"
                                           {:format :parquet :max-bytes 1024})))
        (check "oversized encoded result is destroyed exactly once" 1 @destroys)))

    (let [real-destroy native/chdb-destroy-query-result
          destroys (atom 0)]
      (with-redefs [native/chdb-destroy-query-result
                    (fn [result]
                      (swap! destroys inc)
                      (real-destroy result))
                    native/chdb-result-length (fn [_] 0)
                    native/chdb-result-buffer (fn [_] ffi/null)]
        (let [result (chdb/query-bytes conn "select 1 where 0" {:format :arrow})]
          (check "zero-length native result copies to an empty owned array"
                 0 (alength (:bytes result))))
        (check "zero-length encoded result is destroyed exactly once" 1 @destroys)))

    (let [real-destroy native/chdb-destroy-query-result
          destroys (atom 0)]
      (with-redefs [native/chdb-destroy-query-result
                    (fn [result]
                      (swap! destroys inc)
                      (real-destroy result))
                    native/chdb-result-buffer (fn [_] ffi/null)]
        (check "positive encoded result rejects a null buffer" true
               (throws? #(chdb/query-bytes conn "select 1" {:format :arrow})))
        (check "null-buffer encoded result is destroyed exactly once" 1 @destroys)))

    (let [real-destroy native/chdb-destroy-query-result
          destroys (atom 0)]
      (with-redefs [native/chdb-destroy-query-result
                    (fn [result]
                      (swap! destroys inc)
                      (real-destroy result))
                    ffi/read-array (fn [& _] (throw (ex-info "injected copy failure" {})))]
        (check "binary copy failure propagates" true
               (throws? #(chdb/query-bytes conn "select 1" {:format :parquet})))
        (check "copy-failed encoded result is destroyed exactly once" 1 @destroys)))

    (let [real-destroy native/chdb-destroy-query-result
          destroyed (atom [])]
      (with-redefs [native/chdb-destroy-query-result
                    (fn [result]
                      (swap! destroyed conj result)
                      (real-destroy result))]
        (check "native query error propagates" true
               (throws? #(chdb/query-bytes conn "select no_such_column"
                                           {:format :arrow})))
        ;; One failed user result and one successful stale-format reset result.
        (check "native error and recovery results are each destroyed exactly once"
               [1 1]
               (sort (vals (frequencies @destroyed))))))

    (let [error (rejected
                 #(chdb/query-bytes conn "select no_such_column"
                                    {:format :arrow}))
          statistics (:db.chdb/query-statistics (cause-data error))]
      (check "failed encoded query retains complete native statistics"
             query-statistics-keys (set (keys statistics))))

    (let [capture
          (chdb/with-query-statistics
           #(try
              (chdb/query-bytes conn "select no_such_column"
                                {:format :parquet})
              (catch Throwable _ :handled)))]
      (check "collector observes one failed encoded user query"
             [:handled 1]
             [(:result capture) (count (:queries capture))])
      (check "collector does not expose the internal format-reset query"
             query-statistics-keys
             (set (keys (first (:queries capture))))))

    (let [native-calls (atom 0)]
      (with-redefs [native/chdb-query-with-params-n
                    (fn [& _] (swap! native-calls inc) ffi/null)]
        (check "unsupported format fails" true
               (throws? #(chdb/query-bytes conn "select 1" {:format :csv})))
        (check "over-hard row cap fails" true
               (throws? #(chdb/query-bytes
                          conn "select 1"
                          {:format :arrow
                           :max-rows (inc chdb/max-encoded-result-rows)})))
        (check "over-hard byte cap fails" true
               (throws? #(chdb/query-bytes
                          conn "select 1"
                          {:format :parquet
                           :max-bytes (inc chdb/max-encoded-result-bytes)})))
        (check "invalid encoded options never invoke native query" 0 @native-calls)))

    (let [copied (:bytes (chdb/query-bytes conn "select 42 n"
                                           {:format :parquet}))]
      (jdbc/fetch-one conn "select 43 n")
      (check "owned bytes survive a later native result" "PAR1"
             (byte-prefix copied 4))))

  (let [conn (jdbc/connection "chdb::memory:")
        copied (:bytes (chdb/query-bytes conn "select 44 n" {:format :arrow}))]
    (.close conn)
    (check "owned bytes survive connection close" "ARROW1"
           (byte-prefix copied 6)))

  (let [conn (jdbc/connection "chdb::memory:")
        real-close-conn native/chdb-close-conn
        native-close-calls (atom 0)
        destroy-calls (atom 0)
        destroy-error (ex-info "injected failed-result destruction" {:stage :destroy})]
    (try
      (with-redefs
       [native/chdb-destroy-query-result
        (fn [_]
          (swap! destroy-calls inc)
          (throw destroy-error))
        native/chdb-close-conn
        (fn [owner]
          (swap! native-close-calls inc)
          (real-close-conn owner))]
       (let [error (rejected
                    #(chdb/query-bytes conn "select no_such_column"
                                       {:format :arrow}))]
         (check "failed-result destruction failure is a SQLException"
                true (sql-exception? error))
         (check "failed-result destruction is attempted exactly once"
                1 @destroy-calls)
         (check "failed-result destruction failure closes native state once"
                1 @native-close-calls)
         (check "destruction retirement preserves the native query error"
                true (boolean (re-find #"no_such_column"
                                       (:query-error (cause-data error)))))
         (check "destruction retirement preserves the destructor throwable"
                true (identical? destroy-error
                                 (some-> error ex-cause ex-cause)))
         (check "destruction retirement rejects every later query"
                true (throws? #(jdbc/fetch-one conn "select 1")))
         (.close conn)
         (check "explicit close after destruction retirement is idempotent"
                1 @native-close-calls)))
      (finally (.close conn))))

  (let [conn (jdbc/connection "chdb::memory:")
        real-query native/chdb-query-with-params-n
        real-destroy native/chdb-destroy-query-result
        real-close native/close!
        close-error (ex-info "injected retirement close failure"
                             {:stage :close}
                             (ex-info "nested close cause" {:native :close}))
        native-calls (atom 0)
        user-result-destroyed? (atom false)
        recovery-saw-destroy? (atom false)]
    (try
      (let [error
            (with-redefs
             [native/chdb-query-with-params-n
              (fn [& args]
                (let [call (swap! native-calls inc)]
                  (if (= call 2)
                    (do (reset! recovery-saw-destroy? @user-result-destroyed?)
                        ffi/null)
                    (apply real-query args))))
              native/chdb-destroy-query-result
              (fn [result]
                (reset! user-result-destroyed? true)
                (real-destroy result))
              native/close!
              (fn [handle]
                (real-close handle)
                (throw close-error))]
             (try
               (chdb/query-bytes conn "select no_such_column"
                                 {:format :parquet})
               nil
               (catch Throwable error error)))]
        (check "failed encoded result is destroyed before recovery"
               true @recovery-saw-destroy?)
        (check "failed format recovery is a SQLException" true
               (sql-exception? error))
        (check "failed format recovery retires the connection"
               true (:db.chdb/connection-retired (cause-data error)))
        (check "recovery failure preserves the original native query error"
               true (boolean (re-find #"no_such_column"
                                      (:query-error (cause-data error)))))
        (check "recovery failure retains its nested recovery cause"
               true (some? (some-> error ex-cause ex-cause)))
        (check "recovery retirement retains the close throwable and its cause"
               true
               (let [retained (:close-error (cause-data error))]
                 (and (identical? close-error retained)
                      (= {:native :close} (some-> retained ex-cause ex-data)))))
        (check "retired connection rejects every later query"
               true (throws? #(jdbc/fetch-one conn "select 1"))))
      (finally (.close conn))))

  (let [conn (jdbc/connection "chdb::memory:")
        native-calls (atom 0)
        destroys (atom 0)]
    (try
      (let [error
            (with-redefs
             [native/chdb-query-with-params-n
              (fn [& _] (swap! native-calls inc) ffi/null)
              native/chdb-destroy-query-result
              (fn [_] (swap! destroys inc))]
             (try
               (chdb/query-bytes conn "select 1" {:format :arrow})
               nil
               (catch Throwable error error)))]
        (check "null native encoded result fails" true (some? error))
        (check "null native result was invoked once" 1 @native-calls)
        (check "null native result is never passed to destroy" 0 @destroys)
        (check "null native result is a SQLException" true
               (sql-exception? error))
        (check "null native result retires the uncertain connection"
               true (:db.chdb/connection-retired (cause-data error)))
        (check "connection retired after null result rejects later JDBC"
               true (throws? #(jdbc/fetch-one conn "select 1"))))
      (finally (.close conn))))

  (with-open [sqlite (jdbc/connection "sqlite::memory:")]
    (check "encoded query rejects a non-chDB connection" true
           (throws? #(chdb/query-bytes sqlite "select 1" {:format :arrow}))))
  (let [closed (jdbc/connection "chdb::memory:")]
    (.close closed)
    (check "encoded query rejects a closed connection" true
           (throws? #(chdb/query-bytes closed "select 1" {:format :arrow})))))

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
  (run-ffi-write-order-checks)
  (run-query-checks)
  (run-encoded-query-checks)
  (run-logical-database-checks)
  (run-storage-checks)
  (durable-head/run-checks!)
  (durable-head-whitespace/run-checks!)
  (durable-head-depth/run-checks!)
  (durable-backend/run-checks!)
  (durable-aspect-manifest/run-checks!)
  (durable-control/run-checks!)
  (durable-itf/run-checks!)
  (durable-engine-metadata-itf/run-checks!)
  (durable-writer/run-checks!)
  (durable-writer-concurrency/run-checks!)
  (durable-worker-join/run-checks!)
  (durable-open/run-checks!)
  (durable-compatibility/run-checks!)
  (durable-epoch-seconds/run-checks!)
  (durable-dbspec/run-checks!)
  (durable-local/run-checks!)
  (durable-s3/run-checks!)
  (durable-policy/run-checks!)
  (durable-retry/run-checks!)
  (property/run-properties!)
  (if (zero? @failures)
    (println "all checks passed")
    (throw (ex-info (str @failures " checks failed") {:failures @failures}))))
