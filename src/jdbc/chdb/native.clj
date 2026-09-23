(ns jdbc.chdb.native
  "Owned libchdb v26.7.3 binding. Application code should use jdbc.chdb."
  (:require [clojure.string :as str]
            [jdbc.chdb.abi :as abi]
            [jdbc.chdb.durable.compatibility :as compatibility]
            [jolt.ffi :as ffi]))

(def version "26.7.3")

(def assets
  {[:linux "amd64"]
   {:name "linux-x86_64-libchdb.tar.gz"
    :sha256 "bc33260c32acf78eade2ac41a9115f38e00404651fa42e3bb4c419e4f011c031"}
   [:linux "arm64"]
   {:name "linux-aarch64-libchdb.tar.gz"
    :sha256 "d153adad1ff39b2e3caf0417f09d8bd9edd41939c7c67a3c4978a61e73fb9227"}
   [:darwin "arm64"]
   {:name "macos-arm64-libchdb.tar.gz"
    :sha256 "5640e50dccf711bf3dd5551333d08e43f433edf7bd94b2289f36c2539e627762"}
   [:darwin "amd64"]
   {:name "macos-x86_64-libchdb.tar.gz"
    :sha256 "af5ded3ed3e84c31af1cd198dcf459f11d2b6aad4f6ddeccc04b8a519b0300fc"}})

(defn- nonblank-env [name]
  (let [value (System/getenv name)]
    (when-not (str/blank? value) value)))

(defn library-name
  "Return the filename packaged by the pinned chDB release archive.
  chDB's macOS archives deliberately retain the ELF-style `.so` filename."
  [_os]
  "libchdb.so")

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
    {:os os :arch arch :library-name (library-name os)}))

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

;; Keep the versioned contract check at the public Durable boundary even though
;; the packaged 26.7.3 library provides these symbols. JOLT_CHDB_LIB may select
;; a different library, which must fail closed before an optional symbol call.
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
    (let [native-version (chdb-version)
          minimum-native-version (:minimum-native-version contract)
          version-supported?
          (and (compatibility/release-version? native-version)
               (not (neg? (compatibility/compare-release-versions
                            native-version minimum-native-version))))
          symbols (into (sorted-map)
                        (map (fn [[function-id {:keys [symbol]}]]
                               [function-id
                                {:symbol symbol
                                 :available? (boolean (ffi/find-symbol symbol))}]))
                        functions)
          missing (into []
                        (keep (fn [[function-id {:keys [available?]}]]
                                (when-not available? function-id)))
                        symbols)]
      (cond-> {:status (if (and version-supported? (empty? missing))
                         :supported :unsupported)
               :contract contract-id
               :native-version native-version
               :minimum-native-version minimum-native-version
               :symbols symbols
               :provenance (abi/source-provenance)}
        (not version-supported?)
        (assoc :type ::unsupported-version)
        (and version-supported? (seq missing))
        (assoc :type ::unsupported-core :missing missing)))))

(defn durable-capability
  "Report Durable V1 availability for the supported native release floor."
  []
  (contract-capability :durable-v1))

(defonce ^:private driver-support (delay (contract-capability :driver)))

(defn- require-driver! []
  (let [capability @driver-support]
    (when-not (= :supported (:status capability))
      (throw (ex-info "loaded libchdb is below the supported driver contract"
                      (assoc capability :jdbc/sql-error true))))
    capability))

(defonce ^:private signals-disabled? (atom false))
(defonce ^:private storage-lock (Object.))
(defonce ^:private storage-state
  (atom {:phase :cold
         :path nil
         :references 0
         :bootstrap-options nil
         :anchor-owner nil}))

(defn- close-anchor-at-process-exit! []
  ;; Claim the retained owner before entering native code. Shutdown hooks may be
  ;; reached from ordinary return, System/exit, or a handled termination signal;
  ;; every path must observe the same exactly-once owner transition. Public
  ;; owners remain independently closeable, but no new owner may be opened once
  ;; process teardown has begun.
  (locking storage-lock
    (let [{:keys [phase anchor-owner]} @storage-state]
      (when (and (= :anchored phase) anchor-owner)
        (swap! storage-state assoc
               :phase :exiting
               :anchor-owner nil)
        (chdb-close-conn anchor-owner)
        (swap! storage-state assoc :phase :exit-closed))))
  nil)

(defn- register-anchor-shutdown-hook! []
  (.addShutdownHook
   (Runtime/getRuntime)
   (Thread. close-anchor-at-process-exit!)))

(defn- disable-signal-handlers! []
  (when-not @signals-disabled?
    (locking signals-disabled?
      (when-not @signals-disabled?
        (chdb-set-signal-handlers-enabled 0)
        (reset! signals-disabled? true)))))

(defn canonical-storage-path
  "Returns the canonical process-lifetime identity for a chDB storage path."
  [path]
  (if (= path ":memory:")
    path
    ;; getCanonicalPath is available on the supported Jolt, Babashka, and JVM
    ;; hosts.  Unlike getAbsolutePath it removes lexical aliases and resolves
    ;; existing symlink prefixes, so the process claim follows physical path
    ;; identity instead of caller spelling.
    (.getCanonicalPath (java.io.File. path))))

(defn canonical-archive-path
  "Canonicalize an absolute native backup or restore path.

  chDB compares an archive destination with `backups.allowed_path` by its
  supplied spelling. Normalize only absolute paths so a macOS `/var` scratch
  alias agrees with the canonical `/private/var` bootstrap option, while
  retaining chDB's rejection of relative archive paths."
  [path]
  (let [file (java.io.File. path)]
    (if (.isAbsolute file)
      (canonical-storage-path path)
      path)))

(defn- terminal-storage-state []
  {:phase :terminal
   :path nil
   :references 0
   :bootstrap-options nil
   :anchor-owner nil})

(defn- require-process-path! [requested-path bootstrap-options]
  (let [{:keys [phase path] active-options :bootstrap-options :as state}
        @storage-state]
    (cond
      (= phase :terminal)
      (throw (ex-info "chDB native lifecycle is terminal after a failed bootstrap"
                      {:type ::terminal-native-lifecycle
                       :requested-path requested-path
                       :jdbc/sql-error true}))

      (contains? #{:exiting :exit-closed} phase)
      (throw (ex-info "chDB native lifecycle is closing for process exit"
                      {:type ::process-exiting
                       :requested-path requested-path
                       :jdbc/sql-error true}))

      (and (= phase :anchored) (not= path requested-path))
      (throw (ex-info "chDB storage path is immutable for this process"
                      {:type ::different-process-path
                       :active-path path :requested-path requested-path
                       :jdbc/sql-error true}))

      (and (= phase :anchored) (not= active-options bootstrap-options))
      (throw (ex-info "chDB bootstrap options are immutable for this process"
                      {:type ::different-process-options
                       :active-options active-options
                       :requested-options bootstrap-options
                       :jdbc/sql-error true}))

      :else state)))

(defn- release-reference! [path]
  (swap! storage-state
         (fn [{current :path n :references :as state}]
           (if (and (= current path) (pos? n))
             (assoc state :references (dec n))
             state))))

(defrecord ChdbHandle [owner connection path closed? lock])

(defn- connect-owned! [path backups-allowed-path bootstrap-attempt]
  (let [connect
        (fn [argc argv]
          (when bootstrap-attempt
            (reset! (:native-attempted? bootstrap-attempt) true))
          (let [owner (chdb-connect argc argv)]
            (when (ffi/null? owner)
              ;; The API returned no owner, so there is no connection to close
              ;; and no last-close boundary was crossed.  This is the only
              ;; native-attempt failure proven safe to retry in-process.
              (when bootstrap-attempt
                (reset! (:null-owner? bootstrap-attempt) true))
              (throw (ex-info "chDB connection failed"
                              {:type ::null-native-owner
                               :path path :jdbc/sql-error true})))
            (try
              (let [connection (ffi/read owner :pointer)]
                (when (ffi/null? connection)
                  (throw (ex-info "chDB returned a null connection"
                                  {:path path :jdbc/sql-error true})))
                {:owner owner :connection connection})
              (catch Throwable error
                ;; A non-null owner must not escape on an invalid/read-failed
                ;; bootstrap.  Closing it may cross final shutdown, so the
                ;; caller also makes the bootstrap lifecycle terminal.
                (try
                  (chdb-close-conn owner)
                  (catch Throwable _ nil))
                (throw error)))))]
    ;; The C API's documented in-memory mode is argc=0/argv=NULL. Passing
    ;; --path=:memory: creates a persistent directory literally named
    ;; :memory:, which is both surprising and unsafe for tests.
    (if (= path ":memory:")
      (connect 0 ffi/null)
      (let [args (cond-> ["chdb" (str "--path=" path)]
                   backups-allowed-path
                   (conj (str "--backups.allowed_path="
                              (canonical-storage-path backups-allowed-path))))]
        (ffi/with-c-string-array [argv (count args)] args
          (connect (count args) argv))))))

(defn- ensure-anchor! [path backups-allowed-path]
  (let [bootstrap-options {:backups-allowed-path backups-allowed-path}
        bootstrap-attempt {:native-attempted? (atom false)
                           :null-owner? (atom false)}]
    (require-process-path! path bootstrap-options)
    (when (= :cold (:phase @storage-state))
      (try
        (let [{:keys [owner]}
              (connect-owned! path backups-allowed-path bootstrap-attempt)]
          (try
            ;; Register while the caller holds storage-lock and before
            ;; publishing. A concurrently starting shutdown hook therefore
            ;; cannot observe a cold state after registration or close the
            ;; anchor before the first public owner has been established.
            (register-anchor-shutdown-hook!)
            ;; Publish only a fully validated native owner. Its connection is
            ;; never exposed or used for public queries; retaining the owner
            ;; keeps the embedded engine alive across logical last close. The
            ;; registered hook releases it explicitly before host teardown.
            (reset! storage-state
                    {:phase :anchored
                     :path path
                     :references 0
                     :bootstrap-options bootstrap-options
                     :anchor-owner owner})
            (catch Throwable error
              ;; A registered hook is part of publishing native ownership. If
              ;; registration fails, retire the unpublishable owner once and
              ;; let the outer bootstrap boundary make this process terminal.
              (try
                (chdb-close-conn owner)
                (catch Throwable _ nil))
              (throw error))))
        (catch Throwable error
          ;; Before-native failures cannot have initialized chDB.  A documented
          ;; null-owner return owns nothing and is likewise safe to retry.  Any
          ;; other exception after entering chdb_connect has uncertain engine
          ;; ownership and must make the process lifecycle terminal.
          (when (and @(:native-attempted? bootstrap-attempt)
                     (not @(:null-owner? bootstrap-attempt)))
            (reset! storage-state (terminal-storage-state)))
          (throw error))))
    nil))

(defn open!
  ([path] (open! path {}))
  ([path {:keys [backups-allowed-path]}]
   (when (and (= path ":memory:") backups-allowed-path)
     (throw (ex-info "backups.allowed_path requires a persistent chDB path"
                     {:type ::invalid-open-options
                      :path path :option :backups-allowed-path})))
   (require-driver!)
   (disable-signal-handlers!)
   (let [path (canonical-storage-path path)
         backups-allowed-path (some-> backups-allowed-path
                                      canonical-storage-path)]
     (locking storage-lock
       (ensure-anchor! path backups-allowed-path)
       (let [{:keys [owner connection]}
             (connect-owned! path backups-allowed-path nil)]
         (swap! storage-state update :references inc)
         (->ChdbHandle owner connection path (atom false) (Object.)))))))

(defn close! [handle]
  (locking (:lock handle)
    (when (compare-and-set! (:closed? handle) false true)
      ;; Keep the process-wide path claimed if the native destructor itself
      ;; fails; ownership would then be uncertain and opening a different path
      ;; would be unsound.
      (locking storage-lock
        ;; Serialize public-owner destruction with the process anchor hook so
        ;; the native reference-count boundary has one total close order.
        (chdb-close-conn (:owner handle))
        (release-reference! (:path handle)))))
  nil)

(defn with-live-handle [handle f]
  (locking (:lock handle)
    (when @(:closed? handle)
      (throw (ex-info "chDB connection is closed"
                      {:db.chdb/closed true :jdbc/sql-error true})))
    (f (:connection handle))))

(defn active-storage []
  (let [{:keys [phase path references]} @storage-state]
    {:phase phase
     :path path
     :references references
     :anchored? (= phase :anchored)}))

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

(defn with-query-buffer
  "Call f with one request-owned exact UTF-8 SQL buffer. The pointer is valid
  only during f and must never be retained by native code or the caller."
  [sql f]
  (let [allocated (atom [])]
    (try
      (f (allocated-utf8! allocated sql))
      (finally
        (doseq [pointer (reverse @allocated)] (ffi/free pointer))))))

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

(defn classify-query-buffer!
  "Classify an owned, exact-length SQL buffer. The caller keeps it live until
  this call returns; unknown ABI values fail closed."
  [handle sql-buffer target-database]
  (require-durable!)
  (with-live-handle
   handle
   (fn [connection]
     (let [allocated (atom [])]
       (try
         (let [target-buffer (when-not (nil? target-database)
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

(defn classify-query!
  "Classify SQL with the connection's parser without executing it. A nil
  target database skips write-containment analysis."
  [handle sql target-database]
  (with-query-buffer
    sql #(classify-query-buffer! handle % target-database)))

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
                file-buffer (allocated-utf8! allocated
                                             (canonical-archive-path file-path))
                base-buffer (when base-file-path
                              (allocated-utf8! allocated
                                              (canonical-archive-path base-file-path)))]
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
               file-buffer (allocated-utf8! allocated
                                            (canonical-archive-path file-path))]
           (consume-durable-result!
            :restore
            (chdb-restore-database-n
             connection
             (:pointer database-buffer) (:length database-buffer)
             (:pointer file-buffer) (:length file-buffer))))
         (finally
           (doseq [pointer (reverse @allocated)] (ffi/free pointer))))))))
