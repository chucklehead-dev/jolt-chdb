(ns jdbc.chdb-durable-secret-conformance-test
  (:require [clojure.data.json :as json]
            [clojure.string :as str]
            [jdbc.chdb :as chdb]
            [jdbc.chdb.durable.backend :as backend]
            [jdbc.chdb.durable.control :as control]
            [jdbc.chdb.durable.policy :as policy]
            [jdbc.chdb.durable.reader :as reader]
            [jdbc.chdb.durable.writer :as writer]
            [jdbc.chdb-durable-writer-test-support :as support]))

(def failures (atom 0))

(defn- check [label expected actual]
  (support/check failures label expected actual))

(defn- rejected [f]
  (try
    (f)
    nil
    (catch Throwable error error)))

(defn- throwable-diagnostics [error]
  (loop [current error
         diagnostics []]
    (if current
      (recur (.getCause current)
             (conj diagnostics
                   [(str current) (ex-message current) (ex-data current)]))
      (pr-str diagnostics))))

(defn- redacted-error? [secret error]
  (let [diagnostics (throwable-diagnostics error)]
    (and (some? error)
         (not (str/includes? diagnostics secret))
         (not (str/includes? diagnostics "s3(")))))

(defn- acquired-writer [operations]
  (let [store (backend/memory-backend)
        acquired (control/acquire! store support/base-options)
        operations (merge {:query-bytes-native! (fn [_ _ _ _] nil)}
                          operations)]
    {:store store
     :writer
     (writer/start!
      {:store store
       :token (:token acquired)
       :handle :fake-handle
       :database "default"
       :operations operations})}))

(defn- analysis [query-class has-secrets]
  {:query-class query-class
   :statement-count 1
   :has-secrets has-secrets
   :writes-only-target-database true
   :changes-database-lifecycle false})

(defn- stored-text [store]
  (let [head (:head (control/read-head! store))
        references (get-in head ["manifest" "wal"])]
    (str (String. (backend/get-bytes store control/head-key) "UTF-8")
         (apply str
                (map #(String. (backend/get-bytes store (get % "key"))
                               "UTF-8")
                     references)))))

(defn- stored-wal-sql [store]
  (let [head (:head (control/read-head! store))]
    (mapv (fn [line] (get (json/read-str line) "sql"))
          (mapcat #(str/split-lines
                    (String. (backend/get-bytes store (get % "key")) "UTF-8"))
                  (get-in head ["manifest" "wal"])))))

(defn- run-secret-mutation-case! []
  (println "Pinned secret-bearing mutation refusal")
  (let [secret "SUPERSECRETKEY123"
        application-sql
        (str "INSERT INTO spans FORMAT JSONEachRow\n"
             "{\"attributes\":{\"app.token\":"
             "\"application-visible-value\"}}")
        secret-sql
        (str "INSERT INTO t SELECT 1 FROM s3('https://x/y.csv', "
             "'AKIAEXAMPLE', '" secret "')")
        engine-calls (atom [])
        close-count (atom 0)
        operations
        (assoc (support/fake-operations engine-calls close-count)
               :classify!
               (fn [_ sql _]
                 (if (= secret-sql sql)
                   (analysis :mutating true)
                   (analysis :mutating false))))
        {:keys [store writer]} (acquired-writer operations)]
    (try
      ;; Match the upstream pending==1 control: one earlier mutation remains
      ;; pending, while the secret-bearing mutation never reaches the engine.
      (writer/sql! writer application-sql [])
      (let [calls-before @engine-calls
            error (rejected #(writer/sql! writer secret-sql []))]
        (check "secret-bearing mutation is rejected by the public writer"
               ::policy/rejected (:type (ex-data error)))
        (check "secret-bearing mutation error quotes neither SQL nor secret"
               true (redacted-error? secret error))
        (check "secret-bearing mutation leaves the earlier pending count at one"
               1 (:pending-statements (writer/status writer)))
        (check "secret-bearing mutation reaches no native execution"
               calls-before @engine-calls)
        (writer/flush! writer)
        (check "redaction never rewrites persisted WAL or telemetry-shaped attribute values"
               [application-sql] (stored-wal-sql store))
        (check "published head and WAL contain no rejected credential"
               false (str/includes? (stored-text store) secret)))
      (finally
        (writer/close! writer)))))

(defn- run-success-value-transparency! []
  (println "Successful secret-bearing query values remain unchanged")
  (let [value {:rows [[1]]
               :attributes {"app.token" "application-visible-value"
                            "service.name" "oscope"}}
        bytes (.getBytes "application-visible-bytes" "UTF-8")
        close-count (atom 0)
        operations
        (assoc (support/fake-operations (atom []) close-count)
               :classify! (fn [_ _ _] (analysis :read-only true))
               :analyze-query! (fn [_ _ _] (analysis :read-only true))
               :query-native! (fn [_ _ _] value)
               :query-bytes-native! (fn [_ _ _ _] bytes))
        {:keys [writer]} (acquired-writer operations)]
    (try
      (check "secret-bearing success preserves telemetry-shaped values exactly"
             true (identical? value (writer/query! writer "SELECT 1" [])))
      (check "secret-bearing encoded success preserves bytes exactly"
             true (identical? bytes
                              (writer/query-bytes! writer "SELECT 1" []
                                                   {:format :arrow})))
      (finally
        (writer/close! writer)))
    (let [snapshot
          (reader/start!
           {:handle :fake-reader
            :database "default"
            :operations
            {:analyze-query! (fn [_ _ _] (analysis :read-only true))
             :query-native! (fn [_ _ _] value)
             :query-bytes-native! (fn [_ _ _ _] bytes)
             :close-native! (fn [_] nil)}})]
      (try
        (check "immutable reader success preserves telemetry-shaped values exactly"
               true (identical? value (reader/query! snapshot "SELECT 1" [])))
        (check "immutable reader encoded success preserves bytes exactly"
               true (identical? bytes
                                (reader/query-bytes! snapshot "SELECT 1" []
                                                     {:format :arrow})))
        (finally
          (reader/close! snapshot))))))

(defn- run-secret-read-case! []
  (println "Pinned secret-bearing read failure redaction")
  (let [secret "SUPERSECRETKEY123"
        sql (str "SELECT * FROM s3('not a url', 'AKIAEXAMPLE', '"
                 secret "')")
        close-count (atom 0)
        calls (atom [])
        query-calls (atom 0)
        leaking-error
        (fn []
          (ex-info (str "engine echoed " sql)
                   {:engine/sql sql :engine/secret secret}
                   (ex-info (str "nested " secret) {:nested secret})))
        operations
        (assoc (support/fake-operations calls close-count)
               :classify! (fn [_ _ _] (analysis :read-only true))
               :analyze-query! (fn [_ _ _] (analysis :read-only true))
               :query-native!
               (fn [_ _ _]
                 ;; Deliberately model a hostile engine diagnostic. The public
                 ;; Durable boundary must not retain its message, data, or cause.
                 (swap! query-calls inc)
                 (throw (leaking-error)))
               :query-bytes-native!
               (fn [_ _ _ _]
                 (swap! query-calls inc)
                 (throw (leaking-error))))
        {:keys [store writer]} (acquired-writer operations)]
    (try
      (let [errors [(rejected #(writer/sql! writer sql []))
                    (rejected #(writer/query! writer sql []))
                    (rejected #(writer/query-bytes! writer sql []
                                                     {:format :arrow}))]]
        (check "secret-bearing public writer reads reach the engine"
               3 @query-calls)
        (check "secret-bearing engine failures are redacted through their cause chains"
               [true true true]
               (mapv #(redacted-error? secret %) errors))
        (check "redacted failures retain one stable public SQL category"
               [[::policy/secret-query-failed true nil]
                [::policy/secret-query-failed true nil]
                [::policy/secret-query-failed true nil]]
               (mapv (fn [error]
                       [(:type (ex-data error))
                        (:jdbc/sql-error (ex-data error))
                        (.getCause error)])
                     errors))
        (check "secret-bearing read writes no pending WAL"
               [0 0 false]
               ((juxt :pending-statements :pending-wal-bytes
                      :checkpoint-required?)
                (writer/status writer)))
        (check "flush after a failed secret-bearing read publishes nothing"
               :empty (:status (writer/flush! writer)))
        (check "head and object-store diagnostics retain no read credential"
               false (str/includes? (stored-text store) secret)))
      (finally
        (writer/close! writer)))

    (let [reader-calls (atom 0)
          snapshot
          (reader/start!
           {:handle :fake-reader
            :database "default"
            :operations
            {:classification-sql! (fn [statement _] statement)
             :analyze-query! (fn [_ _ _] (analysis :read-only true))
             :query-native! (fn [_ _ _]
                              (swap! reader-calls inc)
                              (throw (leaking-error)))
             :query-bytes-native! (fn [_ _ _ _]
                                    (swap! reader-calls inc)
                                    (throw (leaking-error)))
             :close-native! (fn [_] nil)}})]
      (try
        (let [errors [(rejected #(reader/query! snapshot sql []))
                      (rejected #(reader/query-bytes! snapshot sql []
                                                       {:format :arrow}))]]
          (check "secret-bearing public snapshot reads reach the engine"
                 2 @reader-calls)
          (check "snapshot query and encoded-query errors share redaction"
                 [true true]
                 (mapv #(redacted-error? secret %) errors)))
        (finally
          (reader/close! snapshot))))))

(defn- run-nonsecret-error-identity-control! []
  (let [engine-error (ex-info "ordinary engine detail" {:ordinary true})
        close-count (atom 0)
        operations
        (assoc (support/fake-operations (atom []) close-count)
               :classify! (fn [_ _ _] (analysis :read-only false))
               :query-native! (fn [_ _ _] (throw engine-error)))
        {:keys [writer]} (acquired-writer operations)]
    (try
      (check "non-secret engine failures retain exact throwable identity"
             true (identical? engine-error
                              (rejected #(writer/sql! writer "SELECT 1" []))))
      (finally
        (writer/close! writer)))))

(defn- shape-analysis [shape mutation-shape read-shape]
  (cond
    (= shape mutation-shape) (analysis :mutating true)
    (= shape read-shape) (analysis :read-only true)
    (= shape "SELECT {p1:Int64}") (analysis :read-only false)
    (= shape "INSERT INTO t VALUES ({p1:Int64})")
    (analysis :mutating false)
    :else (analysis :unknown false)))

(defn- run-parameter-secret-shape-case! []
  (println "Parameterized secret-bearing SQL shape")
  (let [secret "BOUNDSECRETKEY456"
        mutation-sql "INSERT INTO t SELECT 1 FROM s3(?, ?, ?)"
        read-sql "SELECT * FROM s3(?, ?, ?)"
        params ["https://x/y.csv" "AKIAEXAMPLE" secret]
        mutation-shape
        "INSERT INTO t SELECT 1 FROM s3({p1:String}, {p2:String}, {p3:String})"
        read-shape
        "SELECT * FROM s3({p1:String}, {p2:String}, {p3:String})"]
    (let [shapes (atom [])
          engine-calls (atom 0)
          close-count (atom 0)
          operations
          (assoc (support/fake-operations (atom []) close-count)
                 :classification-sql! chdb/classification-sql
                 :classify!
                 (fn [_ shape _]
                   (swap! shapes conj shape)
                   (shape-analysis shape mutation-shape read-shape))
                 :execute-native!
                 (fn [_ _ _]
                   (swap! engine-calls inc)
                   (throw (ex-info "secret mutation reached engine"
                                   {:secret secret}))))
          {:keys [store writer]} (acquired-writer operations)]
      (try
        (let [error (rejected #(writer/sql! writer mutation-sql params))]
          (check "bound secret mutation classifier sees only its typed-placeholder shape"
                 [mutation-shape false]
                 [(first @shapes) (str/includes? (pr-str @shapes) secret)])
          (check "bound secret mutation is refused at the secrets policy reason"
                 [::policy/rejected :secrets]
                 ((juxt :type :reason) (ex-data error)))
          (check "bound secret mutation is refused before native execution and WAL"
                 [0 0 false]
                 [@engine-calls (:pending-statements (writer/status writer))
                  (str/includes? (stored-text store) secret)]))
        (finally
          (writer/close! writer))))

    (let [shapes (atom [])
          engine-calls (atom 0)
          close-count (atom 0)
          operations
          (assoc (support/fake-operations (atom []) close-count)
                 :classification-sql! chdb/classification-sql
                 :classify!
                 (fn [_ shape _]
                   (swap! shapes conj shape)
                   (shape-analysis shape mutation-shape read-shape))
                 :query-native!
                 (fn [_ _ bound]
                   (swap! engine-calls inc)
                   (throw (ex-info (str "engine echoed " (last bound))
                                   {:bound-parameters (vec bound)}
                                   (ex-info "nested parameter failure"
                                            {:nested-parameters
                                             (vec bound)})))))
          {:keys [store writer]} (acquired-writer operations)]
      (try
        (let [error (rejected #(writer/sql! writer read-sql params))]
          (check "bound secret read classifier sees only its typed-placeholder shape"
                 [read-shape false]
                 [(first @shapes) (str/includes? (pr-str @shapes) secret)])
          (check "bound secret read parameter data and nested cause are redacted"
                 [1 ::policy/secret-query-failed true nil]
                 [@engine-calls (:type (ex-data error))
                  (redacted-error? secret error) (.getCause error)])
          (check "bound secret read leaves flush empty and storage secret-free"
                 [:empty 0 false]
                 [(:status (writer/flush! writer))
                  (:pending-statements (writer/status writer))
                  (str/includes? (stored-text store) secret)]))
        (finally
          (writer/close! writer))))

    (let [calls (atom [])
          close-count (atom 0)
          operations
          (assoc (support/fake-operations calls close-count)
                 :classification-sql! chdb/classification-sql
                 :classify!
                 (fn [_ shape _]
                   (shape-analysis shape mutation-shape read-shape))
                 :execute-native!
                 (fn [_ sql bound]
                   (swap! calls conj [:execute sql (vec bound)])
                   {:sql sql :params (vec bound)})
                 :query-native!
                 (fn [_ sql bound]
                   (swap! calls conj [:query sql (vec bound)])
                   {:sql sql :params (vec bound)})
                 :create-checkpoint! (fn [_ _] :checkpoint)
                 :delete-checkpoint! (fn [_] nil)
                 :publish-checkpoint!
                 (fn [_ _ _] {:status :published :reference {}})
                 :commit-reference! (fn [_ _ _] {:status :committed}))
          {:keys [writer]} (acquired-writer operations)]
      (try
        (check "ordinary bound SELECT remains admitted"
               {:sql "SELECT ?" :params [42]}
               (writer/sql! writer "SELECT ?" [42]))
        (check "ordinary bound INSERT remains admitted"
               {:sql "INSERT INTO t VALUES (?)" :params [42]}
               (writer/sql! writer "INSERT INTO t VALUES (?)" [42]))
        (finally
          (writer/close! writer))))))

(defn- run-false-secret-flag-mutants! []
  (let [secret "BOUNDSECRETKEY456"
        params ["https://x/y.csv" "AKIAEXAMPLE" secret]
        mutation-calls (atom 0)
        read-calls (atom 0)
        close-count (atom 0)
        mutation-operations
        (assoc (support/fake-operations (atom []) close-count)
               :classification-sql! chdb/classification-sql
               :classify! (fn [_ _ _] (analysis :mutating false))
               :execute-native!
               (fn [_ _ _]
                 (swap! mutation-calls inc)
                 (throw (ex-info (str "mutant echoed " secret)
                                 {:secret secret}))))
        read-operations
        (assoc (support/fake-operations (atom []) close-count)
               :classification-sql! chdb/classification-sql
               :classify! (fn [_ _ _] (analysis :read-only false))
               :query-native!
               (fn [_ _ _]
                 (swap! read-calls inc)
                 (throw (ex-info (str "mutant echoed " secret)
                                 {:secret secret}))))
        mutation-writer (:writer (acquired-writer mutation-operations))
        read-writer (:writer (acquired-writer read-operations))]
    (try
      (let [mutation-error
            (rejected
             #(writer/sql! mutation-writer
                           "INSERT INTO t SELECT 1 FROM s3(?, ?, ?)" params))
            read-error
            (rejected
             #(writer/sql! read-writer "SELECT * FROM s3(?, ?, ?)" params))]
        (check "false has-secrets mutation mutant reaches native execution"
               1 @mutation-calls)
        (check "false has-secrets mutation mutant leaks its engine failure"
               false (redacted-error? secret mutation-error))
        (check "false has-secrets read mutant reaches native execution"
               1 @read-calls)
        (check "false has-secrets read mutant leaks its engine failure"
               false (redacted-error? secret read-error)))
      (finally
        (writer/close! mutation-writer)
        (writer/close! read-writer)))))

(defn- run-redaction-oracle-control! []
  (let [secret "SUPERSECRETKEY123"
        leaking-mutant
        (ex-info (str "chDB query failed: s3(..., '" secret "')")
                 {:sql (str "SELECT FROM s3('" secret "')")})]
    (check "redaction oracle rejects a secret-leaking engine-error mutant"
           false (redacted-error? secret leaking-mutant))))

(defn run-checks! []
  (reset! failures 0)
  (run-secret-mutation-case!)
  (run-secret-read-case!)
  (run-success-value-transparency!)
  (run-nonsecret-error-identity-control!)
  (run-parameter-secret-shape-case!)
  (run-false-secret-flag-mutants!)
  (run-redaction-oracle-control!)
  (when-not (zero? @failures)
    (throw (ex-info (str @failures " Durable secret conformance checks failed")
                    {:failures @failures})))
  (println "all Durable secret conformance checks passed")
  true)

(defn -main [& _]
  (run-checks!))
