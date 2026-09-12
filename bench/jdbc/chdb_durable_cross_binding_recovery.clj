(ns jdbc.chdb-durable-cross-binding-recovery
  "Read-only Jolt half of the matched chdb-rust Durable recovery oracle."
  (:require [clojure.data.json :as json]
            [clojure.string :as str]
            [db.jdbc]
            [jdbc.chdb.durable :as durable]
            [jdbc.chdb.durable.backend :as backend]
            [jdbc.chdb.durable.digest :as digest]
            [jdbc.chdb.native :as native]
            [jdbc.core :as jdbc]
            [jolt.host :as host])
  (:import [java.io File]
           [java.nio.file CopyOption Files LinkOption OpenOption Path Paths
            StandardCopyOption]))

(def ^:private aggregate-sql
  (str "SELECT count() n, sum(TraceFlags) flags, "
       "sum(SeverityNumber) severity_sum, sum(length(Body)) body_bytes, "
       "countIf(position(Body, '?') > 0) question_bodies, "
       "min(TraceId) min_trace, max(TraceId) max_trace, "
       "min(SpanId) min_span, max(SpanId) max_span FROM otel_logs"))

(def ^:private no-open-options (make-array OpenOption 0))
(def ^:private no-link-options (make-array LinkOption 0))
(def ^:private no-copy-options (make-array CopyOption 0))

(defn- fail! [message]
  (throw (ex-info message {:type ::invalid-oracle})))

(defn- exact-keys! [label expected value]
  (when-not (and (map? value) (= expected (set (keys value))))
    (fail! (str label " has missing or unknown fields")))
  value)

(defn- required-env [name]
  (or (not-empty (System/getenv name))
      (fail! (str "required environment variable " name " is missing"))))

(defn- path [text]
  (Paths/get text (make-array String 0)))

(defn- safe-key! [key]
  (let [parts (when (string? key) (str/split key #"/" -1))]
    (when-not (and (seq parts)
                   (not (str/starts-with? key "/"))
                   (not (str/includes? key "\\"))
                   (every? #(and (not (str/blank? %))
                                 (not (contains? #{"." ".."} %)))
                           parts))
      (fail! "fixture key is not a safe relative path"))
    key))

(defn- resolve-key [^Path root key]
  (let [resolved (.normalize (.resolve root (safe-key! key)))]
    (when-not (.startsWith resolved root)
      (fail! "fixture key escaped its namespace root"))
    resolved))

(deftype ^:private RawReadOnlyBackend [^Path root]
  backend/ObjectBackend
  (get-bytes [_ key]
    (let [target (resolve-key root key)]
      (when (Files/exists target no-link-options)
        (Files/readAllBytes target))))
  (get-with-etag [this key]
    (when-let [bytes (backend/get-bytes this key)]
      ;; Read-only recovery never returns this token to the backend. Keep it a
      ;; content identity so the report does not manufacture provider state.
      {:bytes bytes :etag (digest/sha256-file (resolve-key root key))}))
  (put-file-if-absent! [_ _ _]
    (fail! "cross-binding recovery backend is read-only"))
  (put-bytes-if-absent! [_ _ _]
    (fail! "cross-binding recovery backend is read-only"))
  (replace-if-match! [_ _ _ _]
    (fail! "cross-binding recovery backend is read-only"))
  (download-to-file! [_ key destination]
    (let [source (resolve-key root key)
          target (path (str destination))]
      (if (Files/exists source no-link-options)
        (do
          (Files/copy source target no-copy-options)
          {:status :downloaded :byte-count (Files/size target)})
        {:status :not-found}))))

(defn- raw-read-only-backend [root]
  (let [resolved (.normalize (.toAbsolutePath (path root)))]
    (when-not (Files/isDirectory resolved no-link-options)
      (fail! "fixture namespace root is not a directory"))
    (RawReadOnlyBackend. resolved)))

(defn- entry [^Path base ^File file]
  (let [target (.toPath file)
        key (str/replace (str (.relativize base target)) "\\" "/")]
    (cond
      (Files/isSymbolicLink target)
      {:key key :kind "symlink" :bytes nil :sha256 nil
       :target (str (Files/readSymbolicLink target))}

      (Files/isRegularFile target no-link-options)
      {:key key :kind "file" :bytes (Files/size target)
       :sha256 (digest/sha256-file target) :target nil}

      :else nil)))

(defn- inventory [root object-id]
  (let [base (.resolve (path root) object-id)]
    (->> (file-seq (.toFile base))
         (keep #(when-not (= base (.toPath ^File %)) (entry base %)))
         (sort-by :key)
         vec)))

(defn- descriptor [descriptor-path]
  (json/read-str (slurp descriptor-path) :key-fn keyword))

(defn- run-manifest [manifest-path]
  (json/read-str (slurp manifest-path) :key-fn keyword))

(defn- validate-harness-state! [state]
  (exact-keys! "harness state"
               #{:schema_version :head :parent :tree :status :tracked_patch
                 :untracked_files :state_sha256} state)
  (exact-keys! "tracked patch" #{:file_name :bytes :sha256} (:tracked_patch state))
  (doseq [entry (:untracked_files state)]
    (exact-keys! "untracked file" #{:path :bytes :mode :sha256} entry))
  state)

(defn- validate-run-manifest! [manifest]
  (exact-keys! "run manifest" #{:schema_version :config :harness_state :schedule :run_id} manifest)
  (exact-keys! "run config"
               #{:object_id :database :trials :batch_size :warmup_batches
                 :measured_batches :total_rows} (:config manifest))
  (validate-harness-state! (:harness_state manifest))
  (doseq [entry (:schedule manifest)]
    (exact-keys! "schedule entry"
                 #{:ordinal :phase :runtime :trial :report_file :time_file} entry))
  (when-not (= 1 (:schema_version manifest))
    (fail! "run manifest schema version is unsupported"))
  (when-not (= (:total_rows (:config manifest))
               (* (:batch_size (:config manifest))
                  (+ (:warmup_batches (:config manifest))
                     (:measured_batches (:config manifest)))))
    (fail! "run manifest total rows does not match its workload"))
  (let [entries (volatile! [])
        add! (fn [phase runtime trial label]
               (let [ordinal (count @entries)]
                 (vswap! entries conj
                         {:ordinal ordinal :phase phase :runtime runtime :trial trial
                          :report_file (str runtime "-" label ".json")
                          :time_file (str runtime "-" label ".time")})))]
    (add! "prime" "rust" nil "prime")
    (add! "prime" "jolt" nil "prime")
    (doseq [trial (range 1 (inc (get-in manifest [:config :trials])))
            runtime (if (odd? trial) ["rust" "jolt"] ["jolt" "rust"])]
      (add! "measured" runtime trial (str "trial-" trial)))
    (when-not (= @entries (:schedule manifest))
      (fail! "run schedule is not the exact required prime/alternating order")))
  manifest)

(defn- validate-fixture! [root object-id descriptor]
  (exact-keys! "fixture descriptor"
               #{:schema_version :run_id :producer :config :expected :manifest
                 :inventory :inventory_sha256} descriptor)
  (exact-keys! "fixture config"
               #{:object_id :database :batch_size :warmup_batches
                 :measured_batches :total_rows} (:config descriptor))
  (exact-keys! "expected aggregate"
               #{:n :flags :severity_sum :body_bytes :question_bodies
                 :min_trace :max_trace :min_span :max_span} (:expected descriptor))
  (exact-keys! "fixture manifest" #{:db :base :wal :seq} (:manifest descriptor))
  (exact-keys! "producer provenance"
               #{:runtime :chdb_rust_git_sha :chdb_rust_crate_version
                 :oracle_crate_version :rustc_version :cargo_version
                 :engine_source :native_version :harness_state :native_library
                 :native_header :executable} (:producer descriptor))
  (validate-harness-state! (get-in descriptor [:producer :harness_state]))
  (doseq [[label identity] [["producer native library" (get-in descriptor [:producer :native_library])]
                            ["producer native header" (get-in descriptor [:producer :native_header])]
                            ["producer executable" (get-in descriptor [:producer :executable])]]]
    (exact-keys! label #{:file_name :bytes :sha256} identity))
  (doseq [reference (concat (when-let [base (get-in descriptor [:manifest :base])] [base])
                            (get-in descriptor [:manifest :wal]))]
    (exact-keys! "manifest object reference" #{:key :size :sha256} reference))
  (doseq [entry (:inventory descriptor)]
    (exact-keys! "inventory entry" #{:key :kind :bytes :sha256 :target} entry))
  (when-not (= 1 (:schema_version descriptor))
    (fail! "fixture descriptor schema version is unsupported"))
  (when-not (= object-id (get-in descriptor [:config :object_id]))
    (fail! "fixture descriptor object id does not match"))
  (let [actual (inventory root object-id)]
    (when-not (= (:inventory descriptor) actual)
      (fail! "fixture inventory differs from its descriptor"))
    actual))

(defn- file-metadata [env-name]
  (let [file (File. (required-env env-name))]
    (when-not (.isFile file) (fail! (str env-name " is not a file")))
    {:file_name (.getName file)
     :bytes (.length file)
     :sha256 (digest/sha256-file (.toPath file))}))

(defn- runtime-metadata []
  (let [harness (json/read-str (slurp (required-env "BENCH_HARNESS_STATE_FILE"))
                               :key-fn keyword)]
    (validate-harness-state! harness)
    {:runtime "jolt"
     :jolt_version (required-env "BENCH_JOLT_VERSION")
     :jolt_source_sha (required-env "BENCH_JOLT_SOURCE_SHA")
     :jolt_sdescribe (file-metadata "BENCH_JOLT_DESCRIBE")
     :jolt_config_mode "Srepro-project-only"
     :jolt_cache_scope "run-scoped-isolated-after-prime"
     :scheme_version (host/scheme-version)
     :machine_type (host/machine-type)
     :native_version (native/chdb-version)
     :harness_state harness
     :native_library (file-metadata "BENCH_NATIVE_LIBRARY")
     :native_header (file-metadata "BENCH_NATIVE_HEADER")
     :executable (file-metadata "BENCH_JOLT_BIN")}))

(defn- write-json! [output value]
  (let [target (path output)
        parent (.getParent target)]
    (when parent (Files/createDirectories parent (make-array java.nio.file.attribute.FileAttribute 0)))
    (spit output (str (json/write-str value) "\n"))))

(defn- recover! [root object-id descriptor-path manifest-path ordinal output]
  (let [process-started (System/currentTimeMillis)
        fixture (descriptor descriptor-path)
        plan (validate-run-manifest! (run-manifest manifest-path))
        schedule (get (:schedule plan) ordinal)
        _ (when-not (and schedule (= ordinal (:ordinal schedule))
                         (= "jolt" (:runtime schedule))
                         (= (.getName (File. output)) (:report_file schedule))
                         (= (:run_id fixture) (:run_id plan)))
            (fail! "recovery invocation differs from the run schedule"))
        before (validate-fixture! root object-id fixture)
        store (raw-read-only-backend root)
        expected (:expected fixture)
        start (System/nanoTime)
        actual
        (with-open [reader (jdbc/connection
                            (durable/snapshot-dbspec
                             {:namespace-backend store :object-id object-id}))]
          (let [actual
                (into {} (map (fn [[key value]] [key (str value)]))
                      (jdbc/fetch-one reader aggregate-sql))]
            ;; Keep the nine-field reconciliation inside the same measured
            ;; open/query/close region as the Rust oracle.
            (when-not (= expected actual)
              (throw (ex-info "cross-binding recovery aggregate mismatch"
                              {:type ::reconciliation-failed
                               :expected expected :actual actual})))
            actual))
        elapsed (- (System/nanoTime) start)
        after (validate-fixture! root object-id fixture)
        process-finished (System/currentTimeMillis)]
    (when-not (= before after)
      (fail! "read-only recovery changed fixture bytes or links"))
    (let [rows (get-in fixture [:config :total_rows])
          report
          {:schema_version 1
           :run_id (:run_id plan)
           :schedule_ordinal ordinal
           :phase (:phase schedule)
           :runtime (runtime-metadata)
           :trial (:trial schedule)
           :process_id (Long/parseLong (first (str/split (slurp "/proc/self/stat") #" ")))
           :process_started_epoch_ms process-started
           :process_finished_epoch_ms process-finished
           :cache_condition "warm-provider-cache-fresh-process-engine-and-scratch"
           :fixture
           {:inventory_sha256 (:inventory_sha256 fixture)
            :batch_size (get-in fixture [:config :batch_size])
            :warmup_batches (get-in fixture [:config :warmup_batches])
            :measured_batches (get-in fixture [:config :measured_batches])
            :wal_segments (count (get-in fixture [:manifest :wal]))
            :recovered_rows rows}
           :recovery
           {:elapsed_ns elapsed
            :rows_per_second (/ (* (double rows) 1000000000.0) elapsed)
            :expected expected :actual actual
            :inventory_unchanged true}}]
      (write-json! output report)
      (println (json/write-str
                {:status "ok" :runtime "jolt" :trial (:trial schedule)
                 :phase (:phase schedule)
                 :elapsed_ns elapsed :recovered_rows rows})))))

(defn -main [& args]
  (when-not (= 6 (count args))
    (fail! "expected ROOT OBJECT_ID DESCRIPTOR_JSON RUN_MANIFEST_JSON ORDINAL OUTPUT_JSON"))
  (let [[root object-id descriptor-path manifest-path ordinal output] args
        ordinal (try (Long/parseLong ordinal)
                     (catch Throwable _ (fail! "ordinal must be a non-negative integer")))]
    (when (neg? ordinal) (fail! "ordinal must be a non-negative integer"))
    (recover! root object-id descriptor-path manifest-path ordinal output)))
