(ns jdbc.chdb.native
  "Owned libchdb v26.7.0 binding. Application code should use jdbc.chdb."
  (:require [clojure.string :as str]
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

(ffi/defcfn chdb-version "chdb_version" [] :string)
(ffi/defcfn chdb-set-signal-handlers-enabled "chdb_set_signal_handlers_enabled" [:int] :void)
(ffi/defcfn chdb-connect "chdb_connect" [:int :pointer] :pointer :blocking)
(ffi/defcfn chdb-close-conn "chdb_close_conn" [:pointer] :void :blocking)
(ffi/defcfn chdb-query-with-params-n "chdb_query_with_params_n"
  [:pointer :pointer :size_t :pointer :size_t
   :pointer :pointer :pointer :pointer :size_t]
  :pointer :blocking)
(ffi/defcfn chdb-destroy-query-result "chdb_destroy_query_result" [:pointer] :void :blocking)
(ffi/defcfn chdb-result-buffer "chdb_result_buffer" [:pointer] :pointer)
(ffi/defcfn chdb-result-length "chdb_result_length" [:pointer] :size_t)
(ffi/defcfn chdb-result-rows-written "chdb_result_rows_written" [:pointer] :uint64)
(ffi/defcfn chdb-result-error "chdb_result_error" [:pointer] :string)

(ffi/defcfn chdb-stream-insert-n "chdb_stream_insert_n"
  [:pointer :pointer :size_t :pointer :size_t] :pointer :blocking)
(ffi/defcfn chdb-stream-append "chdb_stream_append" [:pointer :pointer :size_t] :int :blocking)
(ffi/defcfn chdb-stream-done "chdb_stream_done" [:pointer] :pointer :blocking)
(ffi/defcfn chdb-stream-cancel-insert "chdb_stream_cancel_insert" [:pointer] :void :blocking)
(ffi/defcfn chdb-stream-insert-error "chdb_stream_insert_error" [:pointer] :string)
(ffi/defcfn chdb-destroy-insert-stream "chdb_destroy_insert_stream" [:pointer] :void :blocking)

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

(defn open! [path]
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
          (ffi/with-c-string-array [argv 2] ["chdb" (str "--path=" path)]
            (connect 2 argv))))
      (catch Throwable t
        (release-path! path)
        (throw t)))))

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
