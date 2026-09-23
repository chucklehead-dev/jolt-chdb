(ns jdbc.chdb-durable-native-test
  (:require [clojure.string :as str]
            [db.jdbc]
            [jdbc.chdb :as chdb]
            [jdbc.chdb.durable :as durable]
            [jdbc.chdb.durable.backend :as backend]
            [jdbc.chdb.durable.control :as control]
            [jdbc.chdb.durable.local-posix :as local-posix]
            [jdbc.chdb.durable.json-rows :as json-rows]
            [jdbc.chdb.native :as native]
            [jdbc.chdb.durable.policy :as policy]
            [jdbc.chdb.durable.reader :as reader]
            [jdbc.chdb.durable.wal :as wal]
            [jdbc.chdb.durable.writer :as writer]
            [jdbc.core :as jdbc]
            [jolt.ffi :as ffi])
  (:import [java.util Arrays]))

(def failures (atom 0))

(defn- check [label expected actual]
  (if (= expected actual)
    (println "  ok  " label)
    (do
      (swap! failures inc)
      (println "  FAIL" label "- expected" (pr-str expected)
               "got" (pr-str actual)))))

(defn- rejected [f]
  (try (f) nil (catch Throwable error error)))

(defn- throwable-diagnostics [error]
  (loop [current error
         diagnostics []]
    (if current
      (recur (.getCause current)
             (conj diagnostics
                   [(str current) (ex-message current) (ex-data current)]))
      (pr-str diagnostics))))

(defn- excludes-secret? [secret error]
  (let [diagnostics (throwable-diagnostics error)]
    (and (some? error)
         (not (str/includes? diagnostics secret))
         (not (str/includes? diagnostics "s3(")))))

(defn- durable-control-text [store]
  (let [head (:head (control/read-head! store))
        wal (get-in head ["manifest" "wal"])]
    (str (String. (backend/get-bytes store control/head-key) "UTF-8")
         (apply str
                (map #(String. (backend/get-bytes store (get % "key"))
                               "UTF-8")
                     wal)))))

(defn- scalar [handle sql]
  (-> (chdb/execute-any handle sql []) :rows first first str))

(defn- unsigned-prefix [bytes length]
  (mapv #(bit-and 255 %) (take length bytes)))

(defn- run-shared-query-buffer-checks []
  (println "Request-local native SQL buffer")
  (doseq [sql ["" "INSERT INTO t VALUES (1)"
               "INSERT INTO `данные` VALUES ('β🌍')"
               (str "SELECT 'a" (char 0) "b'")
               "SELECT '?' AS literal"]]
    (let [expected (.getBytes sql "UTF-8")
          frees (atom 0)
          native-free ffi/free]
      (with-redefs [ffi/free (fn [pointer]
                              (swap! frees inc)
                              (native-free pointer))]
        (native/with-query-buffer
         sql
         (fn [{:keys [pointer length]}]
           (check (str "exact UTF-8 byte count " (pr-str sql))
                  (alength expected) length)
           (check (str "exact UTF-8 bytes " (pr-str sql))
                  (vec expected)
                  (vec (ffi/read-array pointer length)))
           (let [other (ffi/alloc (max 1 (inc (* 4 (count sql)))))]
             (try
               (check (str "classifier and execution encoders agree " (pr-str sql))
                      [(alength expected) (vec expected)]
                      (let [n (ffi/write-bytes other sql)]
                        [n (vec (ffi/read-array other n))]))
               (finally (native-free other))))))
        (check (str "SQL pointer freed once " (pr-str sql)) 1 @frees))))
  (let [frees (atom 0)
        native-free ffi/free]
    (with-redefs [ffi/free (fn [pointer]
                            (swap! frees inc)
                            (native-free pointer))]
      (check "request failure propagates"
             ::buffer-failure
             (:type (ex-data
                     (rejected
                      #(native/with-query-buffer
                        "SELECT 1"
                        (fn [_] (throw (ex-info "failed" {:type ::buffer-failure}))))))))
      (check "request failure frees SQL pointer once" 1 @frees)))
  ;; A Jolt string normally contains Unicode scalar values. If this runtime
  ;; permits a lone surrogate, both encoders must still agree or fail before
  ;; a pointer is made available to native code.
  (when-let [malformed (try (str (char 0xD800))
                            (catch Throwable _ nil))]
    (let [managed (rejected #(.getBytes malformed "UTF-8"))
          foreign (rejected #(native/with-query-buffer malformed (fn [_] nil)))]
      (check "malformed Unicode is treated consistently"
             (boolean managed) (boolean foreign))
      (when-not managed
        (let [expected (.getBytes malformed "UTF-8")]
          (native/with-query-buffer
           malformed
           (fn [{:keys [pointer length]}]
             (check "representable surrogate has exact encoded bytes"
                    [(alength expected) (vec expected)]
                    [length (vec (ffi/read-array pointer length))])))))))
  (check "quoted question mark preserves exact prepared SQL"
         "SELECT '?' AS literal"
         (chdb/prepared-sql
          (chdb/prepare-query "SELECT '?' AS literal" [])))
  (check "unbound code placeholder is refused before native execution"
         true (boolean (rejected #(chdb/prepare-query "SELECT ?" []))))
  (let [other (chdb/prepare-query "SELECT 2" [])]
    (with-redefs [chdb/prepare-query (fn [_ _] other)
                  chdb/execute-prepared-any (fn [_ prepared]
                                              (check "rewrite mismatch uses ordinary execution"
                                                     true (identical? other prepared))
                                              :fallback)]
      (check "prepared SQL mismatch falls back without shared pointer"
             :fallback
             (chdb/execute-any-with-query-buffer :handle "SELECT 1"
                                                 {:pointer :unused :length 8})))))

(defn- run-buffered-admission-checks []
  (println "Request-local buffered Durable admission")
  (let [with-buffer! (:with-native-admitted-buffer!
                      ((ns-resolve 'jdbc.chdb.durable 'default-open-operations)))
        sql "INSERT INTO `данные` VALUES ('β')"
        accepted {:query-class :mutating :statement-count 1
                  :has-secrets false :writes-only-target-database true
                  :changes-database-lifecycle false}]
    (doseq [[label analysis expected-execution]
            [["accepted" accepted 1]
             ["multi-statement" (assoc accepted :statement-count 2) 0]
             ["secret" (assoc accepted :has-secrets true) 0]
             ["cross-database" (assoc accepted :writes-only-target-database false) 0]]]
      (let [classified (atom nil)
            executed (atom nil)
            frees (atom [])
            native-free ffi/free]
        (with-redefs [native/classify-query-buffer!
                      (fn [_ buffer _]
                        (reset! classified buffer)
                        analysis)
                      chdb/execute-any-with-query-buffer
                      (fn [_ _ buffer]
                        (reset! executed buffer)
                        :native-result)
                      ffi/free (fn [pointer]
                                 (swap! frees conj pointer)
                                 (native-free pointer))]
          (let [result (rejected
                        #(with-buffer! :handle sql "default"
                           (fn [analysis execute!]
                             (policy/authorize-execute! analysis)
                             (execute!))))]
            (check (str label " executes only after policy")
                   expected-execution (if @executed 1 0))
            (when (= label "accepted")
              (check "accepted buffer reaches both native calls unchanged"
                     true (identical? @classified @executed))
              (check "accepted result survives buffer cleanup"
                     nil result))
            (when-not (= label "accepted")
              (check (str label " is rejected before native mutation")
                     ::policy/rejected (:type (ex-data result))))
            (check (str label " buffer is freed exactly once")
                   1 (count @frees))))))
    (let [frees (atom 0)
          native-free ffi/free]
      (with-redefs [native/classify-query-buffer! (fn [_ _ _] accepted)
                    chdb/execute-any-with-query-buffer
                    (fn [_ _ _] (throw (ex-info "native failed" {:type ::native-failure})))
                    ffi/free (fn [pointer]
                               (swap! frees inc)
                               (native-free pointer))]
        (check "native error propagates after admission"
               ::native-failure
               (:type (ex-data
                       (rejected
                        #(with-buffer! :handle sql "default"
                           (fn [analysis execute!]
                             (policy/authorize-execute! analysis)
                             (execute!)))))))
        (check "native error frees SQL buffer once" 1 @frees)))))

(defn- run-buffered-prepared-checks []
  (println "Request-local prepared JDBC SQL buffer")
  (let [with-buffer! (:with-native-prepared-buffer!
                      ((ns-resolve 'jdbc.chdb.durable 'default-open-operations)))
        secret "prepared-bound-secret"
        prepared (chdb/prepare-query "INSERT INTO t VALUES (?)" [secret])
        sql (chdb/prepared-sql prepared)
        accepted {:query-class :mutating :statement-count 1
                  :has-secrets false :writes-only-target-database true
                  :changes-database-lifecycle false}]
    (doseq [[label analysis expected-execution]
            [["accepted" accepted 1]
             ["multi-statement" (assoc accepted :statement-count 2) 0]
             ["secret" (assoc accepted :has-secrets true) 0]
             ["cross-database" (assoc accepted :writes-only-target-database false) 0]]]
      (let [classified (atom nil)
            executed (atom nil)
            frees (atom 0)
            native-free ffi/free]
        (with-redefs [native/classify-query-buffer!
                      (fn [_ buffer _]
                        (reset! classified buffer)
                        (check (str label " classifier sees only typed SQL")
                               false
                               (str/includes?
                                (String. (ffi/read-array (:pointer buffer)
                                                         (:length buffer)) "UTF-8")
                                secret))
                        (check (str label " classifier sees exact prepared SQL")
                               sql
                               (String. (ffi/read-array (:pointer buffer)
                                                        (:length buffer)) "UTF-8"))
                        analysis)
                      chdb/execute-prepared-any-with-query-buffer
                      (fn [_ execution-prepared buffer]
                        (reset! executed buffer)
                        (check (str label " executes same prepared request")
                               true (identical? prepared execution-prepared))
                        :native-result)
                      ffi/free (fn [pointer]
                                 (swap! frees inc)
                                 (native-free pointer))]
          (let [result (rejected
                        #(with-buffer! :handle prepared "default"
                           (fn [analysis execute!]
                             (policy/authorize-execute! analysis)
                             (execute!))))]
            (check (str label " executes only after policy")
                   expected-execution (if @executed 1 0))
            (if (= label "accepted")
              (do
                (check "accepted classification/execution share native buffer"
                       true (identical? @classified @executed))
                (check "accepted result survives buffer cleanup" nil result))
              (check (str label " rejects before native mutation")
                     ::policy/rejected (:type (ex-data result))))
            (check (str label " SQL buffer freed exactly once") 1 @frees)))))
    (let [frees (atom 0)
          native-free ffi/free]
      (with-redefs [native/classify-query-buffer!
                    (fn [_ _ _] (throw (ex-info "classified failure"
                                                  {:type ::classifier-failure})))
                    chdb/execute-prepared-any-with-query-buffer
                    (fn [& _] (throw (ex-info "should not execute"
                                               {:type ::unexpected-execution})))
                    ffi/free (fn [pointer]
                               (swap! frees inc)
                               (native-free pointer))]
        (check "classification failure propagates without execution"
               ::classifier-failure
               (:type (ex-data (rejected
                                #(with-buffer! :handle prepared "default"
                                   (fn [_ execute!] (execute!)))))))
        (check "classification failure frees SQL buffer" 1 @frees)))
    (let [frees (atom 0)
          native-free ffi/free]
      (with-redefs [native/classify-query-buffer! (fn [_ _ _] accepted)
                    chdb/execute-prepared-any-with-query-buffer
                    (fn [& _] (throw (ex-info "native failed"
                                               {:type ::native-failure})))
                    ffi/free (fn [pointer]
                               (swap! frees inc)
                               (native-free pointer))]
        (check "native failure propagates after prepared admission"
               ::native-failure
               (:type (ex-data
                       (rejected
                        #(with-buffer! :handle prepared "default"
                           (fn [analysis execute!]
                             (policy/authorize-execute! analysis)
                             (execute!)))))))
        (check "native failure frees prepared SQL buffer" 1 @frees)))))

(defn- run-real-native-prepared-checks [handle]
  ;; Native storage identity is process-wide. Reuse the core phase's owned
  ;; handle, rather than opening :memory: before that phase claims core/db.
  (let [prepared (chdb/prepare-query "SELECT ?" [42])]
    (native/with-query-buffer
     (chdb/prepared-sql prepared)
     (fn [query-buffer]
       (check "real native prepared execution accepts shared SQL and bound value"
              "42"
              (-> (chdb/execute-prepared-any-with-query-buffer
                   handle prepared query-buffer)
                  :rows first first str))))))

(defn- required-env [name]
  (or (some-> (System/getenv name) str/trim not-empty)
      (throw (ex-info "durable native subprocess environment is incomplete"
                      {:environment name}))))

(defn- process-store [object-id]
  (let [namespace (local-posix/local-backend
                   (required-env "JOLT_CHDB_NATIVE_OBJECT_ROOT"))]
    {:namespace namespace
     :store (backend/object-backend namespace object-id)}))

(defn- scratch-options []
  {:scratch-parent (required-env "JOLT_CHDB_NATIVE_SCRATCH_ROOT")})

(defn- force-managed-wal? []
  (= "1" (System/getenv "JOLT_CHDB_FORCE_MANAGED_WAL")))

(defn- call-with-wal-selection [f]
  (if (force-managed-wal?)
    (with-redefs [wal/native-line-enabled? (constantly false)]
      (f))
    (f)))

(defn- run-durable-object-e2e [phase]
  (println "Durable native object checkpoint and cross-process reopen" phase)
  (let [{:keys [namespace store]} (process-store "native-object")]
    (case phase
      :writer
      (let [opened (durable/open-writer!
                    (merge
                     {:namespace-backend namespace :object-id "native-object"
                      :owner "native-e2e-writer"
                      :instance "native-e2e-instance" :database "snapshot"
                      :lease-ttl-ms 30000}
                     (scratch-options)))]
    (try
      (writer/execute! opened
                       "CREATE TABLE t (id UInt32) ENGINE = MergeTree ORDER BY id")
      (writer/execute! opened "INSERT INTO t VALUES (1),(2),(3)")
      (writer/execute!
       opened
       (str "CREATE TABLE bound_values (id Int64, text String, flag Bool, "
            "note Nullable(String), raw String) ENGINE = MergeTree ORDER BY id"))
      (writer/sql! opened
                   (str "INSERT INTO bound_values VALUES (?, ?, ?, ?, ?)")
                   [7 "a\\β" true
                    (chdb/typed-param "Nullable(String)" nil)
                    (byte-array [92 0 -1])])
      (check "native bound mutation selects checkpoint fallback"
             [true 3]
             ((juxt :checkpoint-required? :pending-statements)
              (writer/status opened)))
      (check "flush commits a full checkpoint for native bound values"
             :committed (:status (writer/flush! opened)))
      (finally
        (writer/close! opened)))
    (let [head (:head (control/read-head! store))]
      (check "checkpoint head has one base and no WAL"
             [1 true [] nil]
             [(get-in head ["manifest" "seq"])
              (boolean (get-in head ["manifest" "base"]))
              (get-in head ["manifest" "wal"])
              (get-in head ["lease" "owner"])])))

      :reader
      (let [opened (durable/open-reader!
                    (merge {:namespace-backend namespace
                            :object-id "native-object"}
                           (scratch-options)))]
      (try
        (check "read-only reopen restores checkpoint rows"
               3 (-> (reader/query! opened "SELECT count() n FROM t" [])
                     :rows first first))
        (check "checkpoint reopen preserves every representative bound value"
               [7 "a\\β" true 1 "5C00FF"]
               (-> (reader/query!
                    opened
                    (str "SELECT id, text, flag, isNull(note), hex(raw) "
                         "FROM bound_values") [])
                   :rows first))
        (check "read-only reopen exports owned Arrow bytes"
               [65 82 82 79 87 49]
               (unsigned-prefix
                (:bytes (reader/query-bytes!
                         opened "SELECT * FROM t ORDER BY id" []
                         {:format :arrow :max-rows 10
                          :max-bytes (* 4 1024 1024)}))
                6))
        (finally
          (reader/close! opened)))))))

(defn- durable-json-row [id]
  {"id" id "message" (str "event?=" id " \\ λ😀")})

(defn- run-durable-json-rows-e2e [phase]
  (println "Durable ordered JSONEachRow admission and fresh-process readback" phase)
  (let [{:keys [namespace store]} (process-store "native-json-rows")
        object-id "native-json-rows"
        scratch-parent (required-env "JOLT_CHDB_NATIVE_SCRATCH_ROOT")]
    (case phase
      :writer
      (with-open [connection
                  (jdbc/connection
                   (durable/writer-dbspec
                    {:namespace-backend namespace :object-id object-id
                     :scratch-parent scratch-parent
                     :owner "json-rows-writer" :instance "json-rows-instance"
                     :database "json_rows" :lease-ttl-ms 30000}))]
        (jdbc/execute! connection
                       (str "CREATE TABLE events (id UInt32, message String) "
                            "ENGINE = MergeTree ORDER BY id"))
        (let [context (json-rows/open-writer connection {:parallelism 4})]
          (try
            (json-rows/admit-rows!
             context "events" ["id" "message"]
             (mapv durable-json-row (range 256)))
            (check "admission-only rows are not yet in the Durable head"
                   [] (get-in (:head (control/read-head! store))
                              ["manifest" "wal"]))
            (check "explicit flush confirms the first 256 rows"
                   :committed (:status (durable/flush! connection)))
            (check "atomic row insert confirms the second 256 rows"
                   :committed
                   (:status
                    (json-rows/insert-rows-and-flush!
                     context "events" ["id" "message"]
                     (mapv durable-json-row (range 256 512)))))
            (finally (json-rows/close! context)))))

      :reader
      (with-open [connection
                  (jdbc/connection
                   (durable/snapshot-dbspec
                    {:namespace-backend namespace :object-id object-id
                     :scratch-parent scratch-parent}))]
        (let [actual (jdbc/fetch connection
                                 "SELECT id, message FROM events ORDER BY id")
              expected (mapv (fn [id]
                               {:id id :message (get (durable-json-row id)
                                                     "message")})
                             (range 512))]
          (check "fresh process reads every ordered row and exact value"
                 expected (vec actual)))))))

(defn- run-durable-local-recovery-e2e [phase]
  (println "Durable native local object WAL and checkpoint recovery" phase)
  (let [{:keys [namespace store]} (process-store "native-recovery")
        object-id "native-recovery"
        database "数据库-α"]
    (case phase
      :wal-writer
      (let [opened (durable/open-writer!
                    (merge
                     {:namespace-backend namespace :object-id object-id
                      :owner "wal-writer" :instance "wal-instance"
                     :database database :lease-ttl-ms 30000}
                     (scratch-options)))
            create-sql "CREATE TABLE `данные` (id UInt32) ENGINE = MergeTree ORDER BY id"
            insert-sql "INSERT INTO `данные` VALUES (10)"
            expected (byte-array
                      (map unchecked-byte
                           (mapcat seq (map wal/portable-line-bytes
                                            [create-sql insert-sql]))))]
        (try
          (when (force-managed-wal?)
            (check "stock native WAL test observes optional encoder disabled"
                   false (wal/native-line-enabled?)))
          (call-with-wal-selection
           #(do
              (writer/execute!
               opened create-sql)
              (writer/execute! opened insert-sql)
              (check "WAL-only flush commits"
                     :committed (:status (writer/flush! opened)))))
          (let [reference (first (get-in (:head (control/read-head-read-only! store))
                                         ["manifest" "wal"]))]
            (check "stock native writer persists forced managed WAL bytes"
                   true
                   (Arrays/equals expected
                                  (backend/get-bytes store (get reference "key")))))
          (finally
            (writer/close! opened))))

      :wal-reader
      (do
       (let [head (:head (control/read-head-read-only! store))]
        (check "WAL-only head has no base and one ordered WAL"
               [1 nil 1]
               [(get-in head ["manifest" "seq"])
                (get-in head ["manifest" "base"])
                (count (get-in head ["manifest" "wal"]))]))
      (let [opened (durable/open-reader!
                    (merge {:namespace-backend namespace :object-id object-id}
                           (scratch-options)))]
        (try
          (check "fresh reader replays WAL-only mutation"
                 10 (-> (reader/query! opened "SELECT sum(id) FROM `данные`" [])
                        :rows first first))
          (finally
            (reader/close! opened)))))

      :checkpoint-writer
      (let [opened (durable/open-writer!
                    (merge
                     {:namespace-backend namespace :object-id object-id
                      :owner "checkpoint-writer" :instance "checkpoint-instance"
                      :database "ignored-for-existing" :lease-ttl-ms 30000}
                     (scratch-options)))]
        (try
          (writer/execute! opened "INSERT INTO `данные` VALUES (20)")
          (check "checkpoint after WAL commits"
                 :committed (:status (writer/checkpoint! opened)))
          (finally
            (writer/close! opened))))

      :checkpoint-reader
      (do
       (let [head (:head (control/read-head-read-only! store))]
        (check "checkpoint folds prior and pending WAL into one base"
               [2 true [] database]
               [(get-in head ["manifest" "seq"])
                (boolean (get-in head ["manifest" "base"]))
                (get-in head ["manifest" "wal"])
                (get-in head ["manifest" "db"])]))
      (let [opened (durable/open-reader!
                    (merge {:namespace-backend namespace :object-id object-id}
                           (scratch-options)))]
        (try
          (check "fresh reader restores the folded checkpoint"
                 30 (-> (reader/query! opened "SELECT sum(id) FROM `данные`" [])
                        :rows first first))
          (finally
            (reader/close! opened))))))))

(defn- run-analysis-checks [handle]
  (println "Durable native query classification")
  (let [cases [["SELECT 1" :read-only 1]
               ["INSERT INTO mem.t VALUES (1)" :mutating 1]
               ["CREATE FUNCTION f AS (x) -> x + 1" :mutating-global 1]
               ["USE other" :control 1]
               ["SELECT FROM WHERE ((" :unknown 0]
               ["SELECT 1; SELECT 2" :read-only 2]
               ["SELECT 1; INSERT INTO mem.t VALUES (1)" :mutating 2]
               ["SELECT 1; USE other" :control 2]]]
    (doseq [[sql class count] cases]
      (let [analysis (native/classify-query! handle sql "mem")]
        (check (str "classifies " sql) class (:query-class analysis))
        (check (str "counts " sql) count (:statement-count analysis)))))
  (check "target write is contained"
         true (:writes-only-target-database
               (native/classify-query! handle "INSERT INTO mem.t VALUES (1)" "mem")))
  (check "cross-database write is not contained"
         false (:writes-only-target-database
                (native/classify-query! handle "INSERT INTO other.t VALUES (1)" "mem")))
  (check "database lifecycle is identified"
         true (:changes-database-lifecycle
               (native/classify-query! handle "CREATE DATABASE d" "mem")))
  (check "secret-bearing SQL is identified"
         true (:has-secrets
               (native/classify-query!
                handle
                "CREATE USER u IDENTIFIED WITH sha256_password BY 'hunter2'"
                nil)))
  (let [secret "SUPERSECRETKEY123"
        mutation-sql
        (str "INSERT INTO mem.t SELECT 1 FROM s3('https://x/y.csv', "
             "'AKIAEXAMPLE', '" secret "')")
        read-sql
        (str "SELECT * FROM s3('not a url', 'AKIAEXAMPLE', '"
             secret "')")]
    (check "pinned s3 mutation is classified as secret-bearing mutation"
           [:mutating true]
           ((juxt :query-class :has-secrets)
            (native/classify-query! handle mutation-sql "mem")))
    (check "pinned s3 read is classified as secret-bearing read"
           [:read-only true]
           ((juxt :query-class :has-secrets)
            (native/classify-query! handle read-sql "mem"))))
  (let [secret "BOUNDSECRETKEY456"
        params ["https://x/y.csv" "AKIAEXAMPLE" secret]
        mutation-sql "INSERT INTO mem.t SELECT 1 FROM s3(?, ?, ?)"
        read-sql "SELECT * FROM s3(?, ?, ?)"
        mutation-shape
        "INSERT INTO mem.t SELECT 1 FROM s3({p1:String}, {p2:String}, {p3:String})"
        read-shape
        "SELECT * FROM s3({p1:String}, {p2:String}, {p3:String})"
        actual-mutation-shape (chdb/classification-sql mutation-sql params)
        actual-read-shape (chdb/classification-sql read-sql params)]
    (check "bound s3 mutation exposes only its exact typed-placeholder shape"
           [mutation-shape false]
           [actual-mutation-shape
            (str/includes? actual-mutation-shape secret)])
    (check "bound s3 read exposes only its exact typed-placeholder shape"
           [read-shape false]
           [actual-read-shape (str/includes? actual-read-shape secret)])
    (check "native AST marks bound s3 mutation shape secret-bearing"
           [:mutating true]
           ((juxt :query-class :has-secrets)
            (native/classify-query! handle actual-mutation-shape "mem")))
    (check "native AST marks bound s3 read shape secret-bearing"
           [:read-only true]
           ((juxt :query-class :has-secrets)
            (native/classify-query! handle actual-read-shape "mem"))))
  (let [analysis (native/classify-query! handle (str "SELECT 1" (char 0) "; USE other") nil)]
    (check "embedded NUL is parsed by explicit byte length" :unknown (:query-class analysis))
    (check "embedded NUL cannot hide a replayable statement" 0 (:statement-count analysis)))
  (check "classification does not execute an INSERT"
         "3" (scalar handle "SELECT count() FROM mem.t"))
  (native/classify-query! handle "USE other" nil)
  (check "classification does not change the current database"
         "default" (scalar handle "SELECT currentDatabase()"))
  (check "real read crosses the query policy" :read-only
         (:query-class (policy/analyze-query! handle "SELECT 1" "mem")))
  (check "real contained mutation crosses the execute policy" :mutating
         (:query-class
          (policy/analyze-execute! handle "INSERT INTO mem.t VALUES (4)" "mem")))
  (check "execute admission analysis does not execute the mutation" "3"
         (scalar handle "SELECT count() FROM mem.t"))
  (doseq [[label operation reason]
          [["real multi-statement query"
            #(policy/analyze-query! handle "SELECT 1; SELECT 2" "mem") :statement-count]
           ["real cross-database mutation"
            #(policy/analyze-execute! handle "INSERT INTO other.t VALUES (1)" "mem")
            :target-database]
           ["real global mutation"
            #(policy/analyze-execute! handle "CREATE FUNCTION f AS (x) -> x + 1" "mem")
            :query-class]
           ["real control statement"
            #(policy/analyze-execute! handle "USE other" "mem") :query-class]
           ["real secret-bearing mutation"
            #(policy/analyze-execute!
              handle
              "CREATE NAMED COLLECTION nc AS access_key_id = 'AKIA', secret_access_key = 's3cr3t'"
              "mem")
            :query-class]]]
    (let [data (some-> (rejected operation) ex-data)]
      (check (str label " fails closed") ::policy/rejected (:type data))
      (check (str label " reports the policy boundary") reason (:reason data)))))

(defn- run-layout-mutants []
  (println "Durable query-analysis fail-closed controls")
  (doseq [[label raw]
          [["truncated struct" {:struct-size 12 :statement-count 1 :flags 0 :query-class 0}]
           ["unknown enum" {:struct-size 16 :statement-count 1 :flags 0 :query-class 99}]
           ["unknown flag" {:struct-size 16 :statement-count 1 :flags 8 :query-class 0}]]]
    (check (str label " is rejected")
           ::native/invalid-query-analysis
           (-> (rejected #(#'native/validate-query-analysis raw)) ex-data :type))))

(defn- run-backup-checks [handle root backups]
  (println "Durable native backup and restore")
  (let [awkward "durable-obj`1"
        awkward-sql "`durable-obj``1`"
        archive (str backups "/it's a backup.tar.gz")]
    (chdb/execute-any handle (str "CREATE DATABASE " awkward-sql) [])
    (chdb/execute-any handle
                      (str "CREATE TABLE " awkward-sql
                           ".t (id UInt32, name String) ENGINE = MergeTree ORDER BY id") [])
    (chdb/execute-any handle
                      (str "INSERT INTO " awkward-sql ".t VALUES (1,'a'),(2,'b'),(3,'c')") [])
    (native/backup-database! handle awkward archive)
    (check "backup creates the archive" true (.isFile (java.io.File. archive)))
    (check "existing destination is refused"
           ::native/durable-operation-failed
           (-> (rejected #(native/backup-database! handle awkward archive)) ex-data :type))
    (chdb/execute-any handle (str "DROP DATABASE " awkward-sql) [])
    (native/restore-database! handle awkward archive)
    (check "restored data is intact" "3"
           (scalar handle (str "SELECT count() FROM " awkward-sql ".t")))
    (check "restore preserves current database" "default"
           (scalar handle "SELECT currentDatabase()"))
    (check "relative backup path is refused"
           ::native/durable-operation-failed
           (-> (rejected #(native/backup-database! handle awkward "relative.tar.gz")) ex-data :type))
    (check "path outside backups.allowed_path is refused"
           ::native/durable-operation-failed
           (-> (rejected #(native/backup-database!
                           handle awkward (str root "/outside.tar.gz"))) ex-data :type))
    (check "missing backup directory is refused"
           ::native/durable-operation-failed
           (-> (rejected #(native/backup-database!
                           handle awkward (str backups "/missing/backup.tar.gz"))) ex-data :type))
    (check "missing restore archive is refused"
           ::native/durable-operation-failed
           (-> (rejected #(native/restore-database!
                           handle awkward (str backups "/missing.tar.gz"))) ex-data :type))
    (check "empty database is refused"
           ::native/durable-operation-failed
           (-> (rejected #(native/backup-database!
                           handle "" (str backups "/empty.tar.gz"))) ex-data :type))
    (check "empty archive path is refused"
           ::native/durable-operation-failed
           (-> (rejected #(native/backup-database! handle awkward "")) ex-data :type))
    (let [injected (str "x` TO File('" backups
                        "/inj.tar.gz'); DROP DATABASE " awkward-sql " --")]
      (check "injection-shaped database name is refused"
             ::native/durable-operation-failed
             (-> (rejected #(native/backup-database!
                             handle injected (str backups "/injection.tar.gz"))) ex-data :type))
      (check "injection shape did not execute" "3"
             (scalar handle (str "SELECT count() FROM " awkward-sql ".t"))))
    (let [utf8-db "数据库-α"
          utf8-sql "`数据库-α`"
          utf8-archive (str backups "/备份-β.tar.gz")]
      (chdb/execute-any handle (str "CREATE DATABASE " utf8-sql) [])
      (chdb/execute-any handle
                        (str "CREATE TABLE " utf8-sql
                             ".`данные` (id UInt32) ENGINE = MergeTree ORDER BY id") [])
      (chdb/execute-any handle
                        (str "INSERT INTO " utf8-sql ".`данные` VALUES (10),(20)") [])
      (native/backup-database! handle utf8-db utf8-archive)
      (chdb/execute-any handle (str "DROP DATABASE " utf8-sql) [])
      (native/restore-database! handle utf8-db utf8-archive)
      (check "non-ASCII name and path round trip" "30"
             (scalar handle (str "SELECT sum(id) FROM " utf8-sql ".`данные`"))))
    (let [incremental (str backups "/incremental.tar.gz")]
      (chdb/execute-any handle
                        (str "INSERT INTO " awkward-sql ".t VALUES (4,'d')") [])
      (native/backup-database! handle awkward incremental archive)
      (chdb/execute-any handle (str "DROP DATABASE " awkward-sql) [])
      (native/restore-database! handle awkward incremental)
      (check "incremental restore includes base and new rows" "4"
             (scalar handle (str "SELECT count() FROM " awkward-sql ".t")))
      (check "incremental restore includes its own row" "d"
             (scalar handle (str "SELECT name FROM " awkward-sql ".t WHERE id = 4"))))
    (check "incremental backup requires its base"
           ::native/durable-operation-failed
           (-> (rejected #(native/backup-database!
                           handle awkward (str backups "/orphan.tar.gz")
                           (str backups "/not-a-base.tar.gz"))) ex-data :type))))

(defn- run-native-secret-boundary-checks [phase]
  (println "Durable native secret-bearing public boundary" phase)
  (case phase
    :mutation
    (let [secret "SUPERSECRETKEY123"
        mutation-sql "INSERT INTO t SELECT 1 FROM s3(?, ?, ?)"
        params ["https://x/y.csv" "AKIAEXAMPLE" secret]
        store (backend/memory-backend)
        opened (durable/open-writer!
                (merge
                 {:store store :owner "native-secret-mutation"
                  :instance "native-secret-mutation-instance"
                  :database "secret_mutation" :lease-ttl-ms 30000}
                 (scratch-options)))]
    (try
      (writer/execute!
       opened "CREATE TABLE t (n Int64) ENGINE=MergeTree ORDER BY n")
      (let [error (rejected #(writer/sql! opened mutation-sql params))]
        (check "native pinned secret mutation is refused with redacted diagnostics"
               [::policy/rejected :secrets true nil]
               [(:type (ex-data error)) (:reason (ex-data error))
                (excludes-secret? secret error) (.getCause error)])
        (check "native pinned secret mutation leaves one earlier WAL entry"
               1 (:pending-statements (writer/status opened))))
      (finally
        (writer/close! opened)))
      (check "native bound secret mutation leaves stored control text secret-free"
             false (str/includes? (durable-control-text store) secret)))

    :read
    (let [secret "SUPERSECRETKEY123"
        read-sql "SELECT * FROM s3(?, ?, ?)"
        params ["not a url" "AKIAEXAMPLE" secret]
        store (backend/memory-backend)
        opened (durable/open-writer!
                (merge
                 {:store store :owner "native-secret-read"
                  :instance "native-secret-read-instance"
                  :database "secret_read" :lease-ttl-ms 30000}
                 (scratch-options)))]
    (try
      (let [error (rejected #(writer/sql! opened read-sql params))]
        (check "native pinned secret read reaches a redacted SQL failure"
               [::policy/secret-query-failed true true nil]
               [(:type (ex-data error))
                (:jdbc/sql-error (ex-data error))
                (excludes-secret? secret error)
                (.getCause error)])
        (check "native pinned secret read writes no WAL"
               [0 :empty]
               [(:pending-statements (writer/status opened))
                (:status (writer/flush! opened))]))
      (finally
        (writer/close! opened)))
      (check "native bound secret read leaves stored control text secret-free"
             false (str/includes? (durable-control-text store) secret)))))

(defn- check-runtime! []
  (let [library (System/getenv "JOLT_CHDB_LIB")]
    (when-not (and library (.isFile (java.io.File. library)))
      (throw (ex-info "durable native qualification requires JOLT_CHDB_LIB"
                      {:library library})))
    (check "qualification library has the exact stable version"
           "26.7.3" (:native-version (native/durable-capability)))
    (check "all Durable V1 symbols resolve"
           :supported (:status (native/durable-capability)))))

(defn- run-core-native-checks []
  (run-layout-mutants)
  (let [before (native/active-storage)]
    (check "backup configuration cannot turn :memory: into a persistent path"
           ::native/invalid-open-options
           (-> (rejected #(native/open! ":memory:" {:backups-allowed-path "/tmp"}))
               ex-data :type))
    (check "rejected :memory: options do not claim process storage"
           before (native/active-storage)))
  (let [root-file (java.io.File. (required-env "JOLT_CHDB_NATIVE_CORE_ROOT"))
        _ (.mkdirs root-file)
        root (.getAbsolutePath root-file)
        backups-file (java.io.File. root-file "backups")
        _ (.mkdirs backups-file)
        backups (.getAbsolutePath backups-file)
        handle (native/open! (str root "/db") {:backups-allowed-path backups})]
    (try
      (chdb/execute-any handle "CREATE DATABASE mem" [])
      (chdb/execute-any handle
                        "CREATE TABLE mem.t (id UInt32) ENGINE = MergeTree ORDER BY id" [])
      (chdb/execute-any handle "INSERT INTO mem.t VALUES (1),(2),(3)" [])
      (run-real-native-prepared-checks handle)
      (run-analysis-checks handle)
      (run-backup-checks handle root backups)
      (finally
        (native/close! handle)))
    (check "classification on a closed handle fails without a native call"
           true (boolean (rejected #(native/classify-query! handle "SELECT 1" nil))))
    (check "backup on a closed handle fails without a native call"
           true (boolean (rejected #(native/backup-database! handle "mem" "/tmp/x"))))
    (check "restore on a closed handle fails without a native call"
           true (boolean (rejected #(native/restore-database! handle "mem" "/tmp/x"))))))

(defn -main [& [mode]]
  (reset! failures 0)
  (check-runtime!)
  (case mode
    "core" (do (run-shared-query-buffer-checks)
               (run-buffered-admission-checks)
               (run-buffered-prepared-checks)
               (run-core-native-checks))
    "object-writer" (run-durable-object-e2e :writer)
    "object-reader" (run-durable-object-e2e :reader)
    "json-rows-writer" (run-durable-json-rows-e2e :writer)
    "json-rows-reader" (run-durable-json-rows-e2e :reader)
    "wal-writer" (run-durable-local-recovery-e2e :wal-writer)
    "wal-reader" (run-durable-local-recovery-e2e :wal-reader)
    "checkpoint-writer" (run-durable-local-recovery-e2e :checkpoint-writer)
    "checkpoint-reader" (run-durable-local-recovery-e2e :checkpoint-reader)
    "secret-mutation" (run-native-secret-boundary-checks :mutation)
    "secret-read" (run-native-secret-boundary-checks :read)
    (throw (ex-info "unknown Durable native subprocess phase" {:mode mode})))
  (if (zero? @failures)
    (println "all Durable native checks passed" mode)
    (throw (ex-info (str @failures " Durable native checks failed")
                    {:failures @failures}))))
