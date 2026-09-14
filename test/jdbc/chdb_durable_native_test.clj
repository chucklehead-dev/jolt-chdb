(ns jdbc.chdb-durable-native-test
  (:require [clojure.string :as str]
            [jdbc.chdb :as chdb]
            [jdbc.chdb.durable :as durable]
            [jdbc.chdb.durable.backend :as backend]
            [jdbc.chdb.durable.control :as control]
            [jdbc.chdb.durable.local-posix :as local-posix]
            [jdbc.chdb.native :as native]
            [jdbc.chdb.durable.policy :as policy]
            [jdbc.chdb.durable.reader :as reader]
            [jdbc.chdb.durable.writer :as writer]
            [jolt.ffi :as ffi]))

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

(defn- delete-tree! [file]
  (when (.exists file)
    (doseq [child (reverse (file-seq file))]
      (.delete child))))

(defn- scalar [handle sql]
  (-> (chdb/execute-any handle sql []) :rows first first str))

(defn- unsigned-prefix [bytes length]
  (mapv #(bit-and 255 %) (take length bytes)))

(defn- run-durable-object-e2e []
  (println "Durable native object checkpoint and read-only reopen")
  (let [store (backend/memory-backend)
        opened (durable/open-writer!
                {:store store :owner "native-e2e-writer"
                 :instance "native-e2e-instance" :database "snapshot"
                 :lease-ttl-ms 30000})]
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
              (get-in head ["lease" "owner"])]))
    (let [opened (durable/open-reader! {:store store})]
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
          (reader/close! opened))))))

(defn- run-durable-local-recovery-e2e []
  (println "Durable native local object WAL and checkpoint recovery")
  (let [root-file (java.io.File/createTempFile "jolt-chdb-recovery-" "")
        _ (.delete root-file)
        _ (.mkdirs root-file)
        namespace (local-posix/local-backend (.getAbsolutePath root-file))
        object-id "native-recovery"
        database "数据库-α"]
    (try
      (let [opened (durable/open-writer!
                    {:namespace-backend namespace :object-id object-id
                     :owner "wal-writer" :instance "wal-instance"
                     :database database :lease-ttl-ms 30000})]
        (try
          (writer/execute!
           opened
           "CREATE TABLE `данные` (id UInt32) ENGINE = MergeTree ORDER BY id")
          (writer/execute! opened "INSERT INTO `данные` VALUES (10)")
          (check "WAL-only flush commits"
                 :committed (:status (writer/flush! opened)))
          (finally
            (writer/close! opened))))
      (let [head (:head (control/read-head-read-only!
                         (backend/object-backend namespace object-id)))]
        (check "WAL-only head has no base and one ordered WAL"
               [1 nil 1]
               [(get-in head ["manifest" "seq"])
                (get-in head ["manifest" "base"])
                (count (get-in head ["manifest" "wal"]))]))
      (let [opened (durable/open-reader!
                    {:namespace-backend namespace :object-id object-id})]
        (try
          (check "fresh reader replays WAL-only mutation"
                 10 (-> (reader/query! opened "SELECT sum(id) FROM `данные`" [])
                        :rows first first))
          (finally
            (reader/close! opened))))
      (let [opened (durable/open-writer!
                    {:namespace-backend namespace :object-id object-id
                     :owner "checkpoint-writer" :instance "checkpoint-instance"
                     :database "ignored-for-existing" :lease-ttl-ms 30000})]
        (try
          (writer/execute! opened "INSERT INTO `данные` VALUES (20)")
          (check "checkpoint after WAL commits"
                 :committed (:status (writer/checkpoint! opened)))
          (finally
            (writer/close! opened))))
      (let [head (:head (control/read-head-read-only!
                         (backend/object-backend namespace object-id)))]
        (check "checkpoint folds prior and pending WAL into one base"
               [2 true [] database]
               [(get-in head ["manifest" "seq"])
                (boolean (get-in head ["manifest" "base"]))
                (get-in head ["manifest" "wal"])
                (get-in head ["manifest" "db"])]))
      (let [opened (durable/open-reader!
                    {:namespace-backend namespace :object-id object-id})]
        (try
          (check "fresh reader restores the folded checkpoint"
                 30 (-> (reader/query! opened "SELECT sum(id) FROM `данные`" [])
                        :rows first first))
          (finally
            (reader/close! opened))))
      (finally
        (delete-tree! root-file)))))

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

(defn- run-native-secret-boundary-checks []
  (println "Durable native secret-bearing public boundary")
  (let [secret "SUPERSECRETKEY123"
        mutation-sql "INSERT INTO t SELECT 1 FROM s3(?, ?, ?)"
        params ["https://x/y.csv" "AKIAEXAMPLE" secret]
        store (backend/memory-backend)
        opened (durable/open-writer!
                {:store store :owner "native-secret-mutation"
                 :instance "native-secret-mutation-instance"
                 :database "secret_mutation" :lease-ttl-ms 30000})]
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

  (let [secret "SUPERSECRETKEY123"
        read-sql "SELECT * FROM s3(?, ?, ?)"
        params ["not a url" "AKIAEXAMPLE" secret]
        store (backend/memory-backend)
        opened (durable/open-writer!
                {:store store :owner "native-secret-read"
                 :instance "native-secret-read-instance"
                 :database "secret_read" :lease-ttl-ms 30000})]
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
           false (str/includes? (durable-control-text store) secret))))

(defn -main [& _]
  (reset! failures 0)
  (let [library (System/getenv "JOLT_CHDB_LIB")]
    (when-not (and library (.isFile (java.io.File. library)))
      (throw (ex-info "durable native qualification requires JOLT_CHDB_LIB"
                      {:library library})))
    (check "qualification library has the exact stable version"
           "26.7.3" (:native-version (native/durable-capability)))
    (check "all Durable V1 symbols resolve"
           :supported (:status (native/durable-capability))))
  (run-layout-mutants)
  (let [before (native/active-storage)]
    (check "backup configuration cannot turn :memory: into a persistent path"
           ::native/invalid-open-options
           (-> (rejected #(native/open! ":memory:" {:backups-allowed-path "/tmp"}))
               ex-data :type))
    (check "rejected :memory: options do not claim process storage"
           before (native/active-storage)))
  (let [root-file (java.io.File/createTempFile "jolt-chdb-durable-" "")
        _ (.delete root-file)
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
      (run-analysis-checks handle)
      (run-backup-checks handle root backups)
      (finally
        (native/close! handle)
        (delete-tree! root-file))))
  (let [closed (native/open! ":memory:")]
    (native/close! closed)
    (check "classification on a closed handle fails without a native call"
           true (boolean (rejected #(native/classify-query! closed "SELECT 1" nil))))
    (check "backup on a closed handle fails without a native call"
           true (boolean (rejected #(native/backup-database! closed "mem" "/tmp/x"))))
    (check "restore on a closed handle fails without a native call"
           true (boolean (rejected #(native/restore-database! closed "mem" "/tmp/x")))))
  (run-durable-object-e2e)
  (run-durable-local-recovery-e2e)
  (run-native-secret-boundary-checks)
  (if (zero? @failures)
    (println "all Durable native checks passed")
    (throw (ex-info (str @failures " Durable native checks failed")
                    {:failures @failures}))))
