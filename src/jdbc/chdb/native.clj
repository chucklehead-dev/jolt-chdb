(ns jdbc.chdb.native
  "Owned libchdb v26.7.0 binding. Application code should use jdbc.chdb."
  (:require [clojure.string :as str]
            [jdbc.chdb.abi :as abi]
            [jolt.ffi :as ffi]))

(def version "26.7.0")

(def assets
  {[:linux "amd64"]
   {:name "linux-x86_64-libchdb.tar.gz"
    :sha256 "4ba2b740f2b107be53157d8226cae1edb25b498d69511cdb3c340db3c95c2905"}
   [:linux "arm64"]
   {:name "linux-aarch64-libchdb.tar.gz"
    :sha256 "d934104b492848d2500b48c0a67c580afc50bd968bebbe435c1b13970ef20384"}
   [:darwin "arm64"]
   {:name "macos-arm64-libchdb.tar.gz"
    :sha256 "297001823683e8189ba7ab5bb7b47f2e14a2ad2484da222a3da62708d2e7f1c4"}
   [:darwin "amd64"]
   {:name "macos-x86_64-libchdb.tar.gz"
    :sha256 "5ea7705db21a63b4af7c5bff43e12fdbebe4e6cb459f7335221687493180fbeb"}})

(defn- nonblank-env [name]
  (let [value (System/getenv name)]
    (when-not (str/blank? value) value)))

(defn platform []
  (let [os-name (str/lower-case (or (System/getProperty "os.name") ""))
        arch-name (str/lower-case (or (System/getProperty "os.arch") ""))
        os (cond
             (str/includes? os-name "linux") :linux
             (or (str/includes? os-name "mac") (str/includes? os-name "darwin")) :darwin
             :else nil)
        arch (cond
               (contains? #{"x86_64" "amd64" "x64"} arch-name) "amd64"
               (contains? #{"aarch64" "arm64"} arch-name) "arm64"
               :else nil)]
    (when-not (and os arch (get assets [os arch]))
      (throw (ex-info "jolt-chdb has no native release for this platform"
                      {:os os-name :arch arch-name})))
    {:os os :arch arch :library-name (if (= :darwin os) "libchdb.dylib" "libchdb.so")}))

(defn cache-directory []
  (or (nonblank-env "JOLT_CHDB_CACHE_DIR")
      (let [root (or (nonblank-env "XDG_CACHE_HOME")
                     (str (System/getProperty "user.home") "/.cache"))]
        (str root "/jolt-chdb/" version "/" (name (:os (platform))) "-" (:arch (platform))))))

(defn library-path []
  (or (nonblank-env "JOLT_CHDB_LIB")
      (str (cache-directory) "/" (:library-name (platform)))))

(defonce ^:private loaded-library (atom nil))

(defn ensure-loaded! []
  (or @loaded-library
      (locking loaded-library
        (or @loaded-library
            (let [path (library-path)]
              (when-not (.isFile (java.io.File. path))
                (throw (ex-info (str "libchdb is not installed at " path
                                     "; run jolt -m jdbc.chdb.install or set JOLT_CHDB_LIB")
                                {:path path :type ::library-missing})))
              (let [library (ffi/load-library path)]
                (reset! loaded-library library)
                library))))))

(abi/defjoltfn chdb-version :version)
(abi/defjoltfn chdb-set-signal-handlers-enabled :set-signal-handlers-enabled)
(abi/defjoltfn chdb-connect :connect)
(abi/defjoltfn chdb-close-conn :close-conn)
(abi/defjoltfn chdb-query-with-params-n :query-with-params-n)
(abi/defjoltfn chdb-destroy-query-result :destroy-query-result)
(abi/defjoltfn chdb-result-buffer :result-buffer)
(abi/defjoltfn chdb-result-length :result-length)
(abi/defjoltfn chdb-result-elapsed :result-elapsed)
(abi/defjoltfn chdb-result-rows-read :result-rows-read)
(abi/defjoltfn chdb-result-bytes-read :result-bytes-read)
(abi/defjoltfn chdb-result-storage-rows-read :result-storage-rows-read)
(abi/defjoltfn chdb-result-storage-bytes-read :result-storage-bytes-read)
(abi/defjoltfn chdb-result-rows-written :result-rows-written)
(abi/defjoltfn chdb-result-bytes-written :result-bytes-written)
(abi/defjoltfn chdb-result-error :result-error)
(abi/defjoltfn chdb-stream-insert-n :stream-insert-n)
(abi/defjoltfn chdb-stream-append :stream-append)
(abi/defjoltfn chdb-stream-done :stream-done)
(abi/defjoltfn chdb-stream-cancel-insert :stream-cancel-insert)
(abi/defjoltfn chdb-stream-insert-error :stream-insert-error)
(abi/defjoltfn chdb-destroy-insert-stream :destroy-insert-stream)

;; These bindings remain lazy on the stable 26.7.0 production library. Public
;; Durable wrappers must call durable-capability first and must never reach a
;; missing symbol. Keeping them here makes the descriptor-to-Jolt signature
;; path compile-checked before the native pin moves.
(abi/defjoltfn ^:private chdb-backup-database-n :backup-database-n)
(abi/defjoltfn ^:private chdb-restore-database-n :restore-database-n)
(abi/defjoltfn ^:private chdb-classify-query-n :classify-query-n)

(def query-analysis-layout
  "Compiled mirror of chdb_query_analysis_v1 from the pinned chdb.h."
  (abi/jolt-layout :query-analysis-v1))

(defn contract-capability
  "Report whether the loaded libchdb exports every symbol in `contract-id`.
  Missing optional contracts are data, not namespace-load failures."
  [contract-id]
  (let [functions (abi/contract-functions contract-id)
        contract (abi/contract-spec contract-id)]
    ;; Validate the requested descriptor contract before native loading so an
    ;; invalid contract cannot be masked by an installation or loader error.
    (ensure-loaded!)
    (let [symbols (into (sorted-map)
                        (map (fn [[function-id {:keys [symbol]}]]
                               [function-id
                                {:symbol symbol
                                 :available? (boolean (ffi/find-symbol symbol))}]))
                        functions)
          missing (into []
                        (keep (fn [[function-id {:keys [available?]}]]
                                (when-not available? function-id)))
                        symbols)]
      (cond-> {:status (if (empty? missing) :supported :unsupported)
               :contract contract-id
               :native-version (chdb-version)
               :minimum-native-version (:minimum-native-version contract)
               :symbols symbols
               :provenance (abi/source-provenance)}
        (seq missing) (assoc :type ::unsupported-core :missing missing)))))

(defn durable-capability
  "Report Durable V1 availability without making it a production requirement."
  []
  (contract-capability :durable-v1))

(defonce ^:private signals-disabled? (atom false))
(defonce ^:private storage-state (atom {:path nil :references 0}))

(defn- disable-signal-handlers! []
  (when-not @signals-disabled?
    (locking signals-disabled?
      (when-not @signals-disabled?
        (chdb-set-signal-handlers-enabled 0)
        (reset! signals-disabled? true)))))

(defn- normalized-path [path]
  (if (= path ":memory:")
    path
    (.getAbsolutePath (java.io.File. path))))

(defn- claim-path! [path]
  (swap! storage-state
         (fn [{current :path n :references :as state}]
           (cond
             (zero? n) {:path path :references 1}
             (= current path) (assoc state :references (inc n))
             :else (throw (ex-info "chDB supports one storage path per process"
                                   {:active-path current :requested-path path
                                    :jdbc/sql-error true})))))
  path)

(defn- release-path! [path]
  (swap! storage-state
         (fn [{current :path n :references :as state}]
           (if (and (= current path) (pos? n))
             (if (= n 1) {:path nil :references 0}
                 (assoc state :references (dec n)))
             state))))

(defrecord ChdbHandle [owner connection path closed? lock])

(defn open!
  ([path] (open! path {}))
  ([path {:keys [backups-allowed-path]}]
   (when (and (= path ":memory:") backups-allowed-path)
     (throw (ex-info "backups.allowed_path requires a persistent chDB path"
                     {:type ::invalid-open-options
                      :path path :option :backups-allowed-path})))
   (ensure-loaded!)
   (disable-signal-handlers!)
   (let [path (claim-path! (normalized-path path))]
    (try
      (let [connect (fn [argc argv]
                      (let [owner (chdb-connect argc argv)]
                        (when (ffi/null? owner)
                          (throw (ex-info "chDB connection failed"
                                          {:path path :jdbc/sql-error true})))
                        (let [connection (ffi/read owner :pointer)]
                          (when (ffi/null? connection)
                            (chdb-close-conn owner)
                            (throw (ex-info "chDB returned a null connection"
                                            {:path path :jdbc/sql-error true})))
                          (->ChdbHandle owner connection path (atom false) (Object.)))))]
        ;; The C API's documented in-memory mode is argc=0/argv=NULL. Passing
        ;; --path=:memory: creates a persistent directory literally named
        ;; :memory:, which is both surprising and unsafe for tests.
        (if (= path ":memory:")
          (connect 0 ffi/null)
          (let [args (cond-> ["chdb" (str "--path=" path)]
                       backups-allowed-path
                       (conj (str "--backups.allowed_path="
                                  (normalized-path backups-allowed-path))))]
            (ffi/with-c-string-array [argv (count args)] args
              (connect (count args) argv)))))
      (catch Throwable t
        (release-path! path)
        (throw t))))))

(defn close! [handle]
  (locking (:lock handle)
    (when (compare-and-set! (:closed? handle) false true)
      ;; Keep the process-wide path claimed if the native destructor itself
      ;; fails; ownership would then be uncertain and opening a different path
      ;; would be unsound.
      (chdb-close-conn (:owner handle))
      (release-path! (:path handle))))
  nil)

(defn with-live-handle [handle f]
  (locking (:lock handle)
    (when @(:closed? handle)
      (throw (ex-info "chDB connection is closed"
                      {:db.chdb/closed true :jdbc/sql-error true})))
    (f (:connection handle))))

(defn active-storage [] @storage-state)

(def ^:private query-classes
  {0 :read-only
   1 :mutating
   2 :mutating-global
   3 :control
   4 :unknown})

(def ^:private query-analysis-flags
  {1 :has-secrets
   2 :writes-only-target-database
   4 :changes-database-lifecycle})

(defonce ^:private durable-support
  ;; A process cannot replace the library after ffi/load-library. Resolve the
  ;; optional contract once so the per-statement classification path does not
  ;; repeat eight symbol lookups.
  (delay (durable-capability)))

(defn- require-durable! []
  (let [capability @durable-support]
    (when-not (= :supported (:status capability))
      (throw (ex-info "loaded libchdb does not provide Durable V1"
                      capability)))
    capability))

(defn- allocated-utf8! [allocated value]
  (let [bytes (.getBytes (str value) "UTF-8")
        length (alength bytes)
        pointer (ffi/alloc (max 1 length))]
    (swap! allocated conj pointer)
    (when (pos? length) (ffi/write-array pointer bytes))
    {:pointer pointer :length length}))

(defn- validate-query-analysis
  [{:keys [struct-size statement-count flags query-class] :as raw}]
  (let [known-flags (reduce bit-or 0 (keys query-analysis-flags))
        unknown-flags (bit-and flags (bit-not known-flags))
        class (get query-classes query-class)]
    (when-not (= 16 struct-size)
      (throw (ex-info "chDB returned an incompatible query-analysis layout"
                      {:type ::invalid-query-analysis :analysis raw})))
    (when-not class
      (throw (ex-info "chDB returned an unknown query class"
                      {:type ::invalid-query-analysis :analysis raw})))
    (when-not (zero? unknown-flags)
      (throw (ex-info "chDB returned unknown query-analysis flags"
                      {:type ::invalid-query-analysis
                       :unknown-flags unknown-flags :analysis raw})))
    {:query-class class
     :statement-count statement-count
     :flags (into #{} (keep (fn [[mask flag]]
                              (when-not (zero? (bit-and flags mask)) flag)))
                  query-analysis-flags)
     :has-secrets (not (zero? (bit-and flags 1)))
     :writes-only-target-database (not (zero? (bit-and flags 2)))
     :changes-database-lifecycle (not (zero? (bit-and flags 4)))}))

(defn classify-query!
  "Classify SQL with the connection's parser without executing it. A nil
  target database skips write-containment analysis. Unknown ABI values fail
  closed instead of being treated as a permitted statement."
  [handle sql target-database]
  (require-durable!)
  (with-live-handle
   handle
   (fn [connection]
     (let [allocated (atom [])]
       (try
         (let [sql-buffer (allocated-utf8! allocated sql)
               target-buffer (when-not (nil? target-database)
                               (allocated-utf8! allocated target-database))
               analysis (ffi/alloc (ffi/layout-size query-analysis-layout))]
           (swap! allocated conj analysis)
           (ffi/write-array analysis (byte-array 16))
           (ffi/write-field analysis query-analysis-layout [:struct-size] 16)
           (when-not
            (zero?
             (chdb-classify-query-n
              connection
              (:pointer sql-buffer) (:length sql-buffer)
              (if target-buffer (:pointer target-buffer) ffi/null)
              (if target-buffer (:length target-buffer) 0)
              analysis))
             (throw (ex-info "chDB query classification failed"
                             {:type ::classification-failed
                              :jdbc/sql-error true})))
           (validate-query-analysis
            {:struct-size (ffi/read-field analysis query-analysis-layout [:struct-size])
             :statement-count (ffi/read-field analysis query-analysis-layout [:statement-count])
             :flags (ffi/read-field analysis query-analysis-layout [:flags])
             :query-class (ffi/read-field analysis query-analysis-layout [:query-class])}))
         (finally
           (doseq [pointer (reverse @allocated)] (ffi/free pointer))))))))

(defn- consume-durable-result! [operation result]
  (when (ffi/null? result)
    (throw (ex-info (str "chDB " (name operation) " returned a null result")
                    {:type ::durable-null-result :operation operation
                     :jdbc/sql-error true})))
  (try
    (when-let [message (chdb-result-error result)]
      (throw (ex-info (str "chDB " (name operation) " failed: " message)
                      {:type ::durable-operation-failed :operation operation
                       :jdbc/sql-error true})))
    nil
    (finally (chdb-destroy-query-result result))))

(defn backup-database!
  "Back up one database to an absolute archive path. Optional base-file-path
  requests an incremental backup. Database and paths remain separate ABI
  arguments and are never interpolated into SQL."
  ([handle database file-path]
   (backup-database! handle database file-path nil))
  ([handle database file-path base-file-path]
   (require-durable!)
   (with-live-handle
    handle
    (fn [connection]
      (let [allocated (atom [])]
        (try
          (let [database-buffer (allocated-utf8! allocated database)
                file-buffer (allocated-utf8! allocated file-path)
                base-buffer (when base-file-path
                              (allocated-utf8! allocated base-file-path))]
            (consume-durable-result!
             :backup
             (chdb-backup-database-n
              connection
              (:pointer database-buffer) (:length database-buffer)
              (:pointer file-buffer) (:length file-buffer)
              (if base-buffer (:pointer base-buffer) ffi/null)
              (if base-buffer (:length base-buffer) 0))))
          (finally
            (doseq [pointer (reverse @allocated)] (ffi/free pointer)))))))))

(defn restore-database!
  "Restore one database from an archive produced by backup-database!."
  [handle database file-path]
  (require-durable!)
  (with-live-handle
   handle
   (fn [connection]
     (let [allocated (atom [])]
       (try
         (let [database-buffer (allocated-utf8! allocated database)
               file-buffer (allocated-utf8! allocated file-path)]
           (consume-durable-result!
            :restore
            (chdb-restore-database-n
             connection
             (:pointer database-buffer) (:length database-buffer)
             (:pointer file-buffer) (:length file-buffer))))
         (finally
           (doseq [pointer (reverse @allocated)] (ffi/free pointer))))))))
