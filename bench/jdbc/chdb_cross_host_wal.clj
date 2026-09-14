(ns jdbc.chdb-cross-host-wal
  "Phase 0 cross-host characterization of pure WAL byte/JSON costs.

  This intentionally does not copy Durable's private record validation or
  claim that Babashka/JVM can open or replay a Durable database."
  (:require [clojure.edn :as edn]
            [clojure.java.io :as io]
            [clojure.string :as str]
            [jdbc.chdb-cross-host-report :as report])
  (:import [java.nio ByteBuffer]
           [java.nio.charset CharacterCodingException Charset CodingErrorAction]
           [java.nio.file Files Path Paths]
           [java.security MessageDigest]
           [java.util Arrays]))

(def ^:private runtimes #{:jolt :babashka :jvm})
(def ^:private utf8 (Charset/forName "UTF-8"))
(def ^:private no-path-parts (make-array String 0))

(defn- fail! [message data]
  (throw (ex-info message (assoc data :type ::invalid-benchmark))))

(defn- required-env [name]
  (or (not-empty (System/getenv name))
      (fail! (str "required environment variable " name " is missing") {})))

(defn- parse-positive [label text maximum]
  (let [value (try (Long/parseLong text) (catch Throwable _ nil))]
    (when-not (and value (pos? value) (<= value maximum))
      (fail! (str label " must be a bounded positive integer") {}))
    value))

(defn- bytes->hex [bytes]
  (apply str (map #(format "%02x" (bit-and 255 %)) bytes)))

(defn- sha256-bytes [bytes]
  (bytes->hex (.digest (MessageDigest/getInstance "SHA-256") bytes)))

(defn- resource-bytes [resource-name]
  (let [resource (io/resource resource-name)]
    (when-not resource
      (fail! "required benchmark resource is missing"
             {:resource resource-name}))
    {:location (str resource)
     :bytes (.getBytes (slurp resource) "UTF-8")}))

(defn- resource-identity [resource-name]
  (let [{:keys [bytes]} (resource-bytes resource-name)]
    {:resource resource-name
     :bytes (alength bytes)
     :sha256 (sha256-bytes bytes)}))

(defn- pinned-resource-identity [resource-name expected-git-sha]
  (let [{:keys [location bytes]} (resource-bytes resource-name)]
    ;; Git deps resolve beneath a directory named by their immutable SHA. This
    ;; rejects a built-in, Maven, or stale checkout that happens to expose the
    ;; same namespace before any timings are accepted.
    (when-not (str/includes? location expected-git-sha)
      (fail! "loaded benchmark resource does not match its exact git pin"
             {:resource resource-name}))
    {:resource resource-name
     :bytes (alength bytes)
     :sha256 (sha256-bytes bytes)}))

(defn- versioned-resource-identity [resource-name expected-fragment]
  (let [{:keys [location bytes]} (resource-bytes resource-name)]
    (when-not (str/includes? location expected-fragment)
      (fail! "loaded benchmark resource does not match its coordinate"
             {:resource resource-name}))
    {:resource resource-name
     :bytes (alength bytes)
     :sha256 (sha256-bytes bytes)}))

(defn- strict-decode [bytes]
  (try
    (let [decoder (.newDecoder utf8)]
      (.onMalformedInput decoder CodingErrorAction/REPORT)
      (.onUnmappableCharacter decoder CodingErrorAction/REPORT)
      (str (.decode decoder (ByteBuffer/wrap bytes))))
    (catch CharacterCodingException _
      (fail! "selected WAL record is not strict UTF-8" {}))))

(defn- scan-record-boundaries [^bytes bytes ordinal]
  (when-not (and (pos? (alength bytes))
                 (= 10 (bit-and 255 (aget bytes (dec (alength bytes))))))
    (fail! "WAL fixture must be non-empty and LF terminated" {}))
  (let [result
        (loop [index 0 start 0 current 1 record-count 0
               target-start -1 target-end -1]
          (if (= index (alength bytes))
            {:record-count record-count
             :record-start target-start
             :record-end-exclusive target-end}
            (if (= 10 (bit-and 255 (aget bytes index)))
              (let [target? (= current ordinal)]
                (recur (inc index) (inc index) (inc current) (inc record-count)
                       (if target? start target-start)
                       (if target? index target-end)))
              (recur (inc index) start current record-count
                     target-start target-end))))]
    (when (neg? (:record-start result))
      (fail! "record ordinal exceeds the WAL fixture" {:ordinal ordinal}))
    result))

(defn- metrics-provider [runtime]
  (case runtime
    :jolt ((requiring-resolve
            'jdbc.chdb-cross-host-jolt-metrics/provider))
    :jvm ((requiring-resolve
           'jdbc.chdb-cross-host-jvm-metrics/provider))
    {:support {:calling-thread-cpu :unsupported
               :managed-allocation :unsupported
               :gc :unsupported}
     :snapshot nil}))

(defn- counter-delta [before after]
  (when (and before after)
    (into (sorted-map)
          (map (fn [[key value]] [key (- value (get before key 0))]))
          after)))

(defn- consume-result [value]
  ;; Make the measured result observable without timing a digest per sample.
  (cond
    (string? value) (count value)
    (integer? value) value
    :else (hash value)))

(defn- measured-phase [measurement inputs expected-result]
  (merge {:status :complete
          :expected-result expected-result}
         inputs
         measurement))

(defn- not-run-phase [reason]
  {:status :not-run :reason reason
   :warmups 0 :samples 0})

(defn- json-parser [runtime data-json-sha]
  (let [profile (keyword
                 (or (System/getenv "BENCH_JSON_PARSER")
                     (if (= runtime :babashka)
                       "babashka-bundled-cheshire"
                       "casselc-data-json")))]
    (case profile
      :babashka-bundled-cheshire
      (do
        (when-not (= runtime :babashka)
          (fail! "bundled Cheshire is only valid on Babashka" {}))
        {:read-str (requiring-resolve 'cheshire.core/parse-string)
         :identity
         {:implementation profile
          :coordinate :babashka/bundled-cheshire
          :version :bundled-with-runtime
          :provenance {:runtime :babashka
                       :runtime-version (required-env "BENCH_RUNTIME_VERSION")
                       :runtime-revision (required-env "BENCH_RUNTIME_REVISION")}}})

      :casselc-data-json
      (let [read-str (requiring-resolve 'clojure.data.json/read-str)]
        {:read-str read-str
         :identity (assoc
                    (pinned-resource-identity "clojure/data/json.clj"
                                              data-json-sha)
                    :implementation profile
                    :coordinate 'io.github.casselc/data.json
                    :git-sha data-json-sha
                    :version data-json-sha)})

      :upstream-data-json
      (let [version (required-env "BENCH_JSON_PARSER_VERSION")]
        {:read-str (requiring-resolve 'clojure.data.json/read-str)
         :identity (assoc
                    (versioned-resource-identity
                     "clojure/data/json.clj" (str "/" version "/"))
                    :implementation profile
                    :coordinate 'org.clojure/data.json
                    :version version
                    :artifact-sha256
                    (required-env "BENCH_JSON_PARSER_ARTIFACT_SHA256"))})

      :jvm-cheshire
      (let [version (required-env "BENCH_JSON_PARSER_VERSION")]
        {:read-str (requiring-resolve 'cheshire.core/parse-string)
         :identity (assoc
                    (versioned-resource-identity
                     "cheshire/core.clj" (str "/" version "/"))
                    :implementation profile
                    :coordinate 'cheshire/cheshire
                    :version version
                    :artifact-sha256
                    (required-env "BENCH_JSON_PARSER_ARTIFACT_SHA256"))})

      (fail! "unknown JSON parser profile" {:profile profile}))))

(defn- warm! [warmups f]
  (dotimes [_ warmups] (consume-result (f))))

(defn- measure-samples [samples {:keys [snapshot]} f]
  (let [sink (volatile! 0)
        before (when snapshot (snapshot))
        elapsed
        (loop [remaining samples values []]
          (if (zero? remaining)
            values
            (let [start (System/nanoTime)
                  value (f)
                  nanos (- (System/nanoTime) start)]
              (vswap! sink bit-xor (consume-result value))
              (recur (dec remaining) (conj values nanos)))))
        after (when snapshot (snapshot))]
    {:latency (report/distribution elapsed)
     :resource-delta (counter-delta before after)
     :sink @sink}))

(defn- measure-once [{:keys [snapshot]} f]
  (let [before (when snapshot (snapshot))
        start (System/nanoTime)
        value (f)
        nanos (- (System/nanoTime) start)
        after (when snapshot (snapshot))]
    {:value value
     :latency {:count 1
               :total-ns nanos
               :max-ns nanos
               :p50-supported? false
               :p95-supported? false
               :p99-supported? false
               :samples-ns [nanos]}
     :resource-delta (counter-delta before after)
     :sink (consume-result value)}))

(defn- measure-boundary-phase
  ([bytes ordinal expected-record-count metrics scanner]
   (measure-boundary-phase bytes ordinal expected-record-count metrics
                           scanner measure-once))
  ([bytes ordinal expected-record-count metrics scanner measurer]
   (let [{:keys [value] :as measurement}
         (measurer metrics #(scanner bytes ordinal))]
     (when-not (= expected-record-count (:record-count value))
       (fail! "WAL boundary count differs from the caller-verified fixture count" {}))
     {:value value
      :result
      (measured-phase
       (assoc (dissoc measurement :value) :warmups 0 :samples 1)
       {:input-bytes (alength bytes)
        :includes-record-selection true}
       value)})))

(defn- artifact-path [output suffix]
  (str/replace output #"\.edn$" suffix))

(defn- append-checkpoint! [path value]
  ;; One bounded EDN event per closed write. A killed later phase therefore
  ;; cannot erase the already completed work; at worst its own final line is
  ;; absent or partial and every previous newline remains parseable.
  (spit path (report/render value) :append true))

(defn- append-csv! [path host phase phase-result]
  (let [latency (:latency phase-result)
        row (str (name host) "," (name phase) "," (name (:status phase-result)) ","
                 (or (:warmups phase-result) 0) "," (or (:samples phase-result) 0) ","
                 (or (:count latency) 0) "," (or (:total-ns latency) "") ","
                 (or (:max-ns latency) "") "\n")]
    (spit path row :append true)))

(defn- error-summary [throwable]
  {:class (str (type throwable))
   :type (:type (ex-data throwable))})

(defn- start-jvm-profile []
  (when-let [path (System/getenv "BENCH_JFR_PATH")]
    (try
      (let [start (requiring-resolve 'jdbc.chdb-cross-host-jvm-profile/start!)]
        {:path path :status :recording :recording (start path)})
      (catch Throwable throwable
        {:path path :status :failed :stage :start
         :error (error-summary throwable)}))))

(defn- stop-jvm-profile [profile]
  (if-not (= :recording (:status profile))
    profile
    (try
      (let [stop (requiring-resolve 'jdbc.chdb-cross-host-jvm-profile/stop!)]
        (stop (:recording profile))
        (-> profile (dissoc :recording) (assoc :status :complete)))
      (catch Throwable throwable
        (-> profile
            (dissoc :recording)
            (assoc :status :failed :stage :stop
                   :error (error-summary throwable)))))))

(defn- runtime-id []
  (let [runtime (keyword (required-env "BENCH_RUNTIME"))]
    (when-not (contains? runtimes runtime)
      (fail! "BENCH_RUNTIME is not a supported host" {:runtime runtime}))
    runtime))

(defn- load-reused-boundaries
  [path expected-sha expected-bytes expected-count ordinal]
  (let [text (slurp path)
        source (edn/read-string text)
        fixture (:fixture source)
        boundaries (select-keys fixture
                                [:record-count :record-start
                                 :record-end-exclusive])]
    (when-not (and (= :complete (:status source))
                   (= :jvm (get-in source [:host :runtime]))
                   (= :casselc-data-json
                      (get-in source [:libraries :json-parser :implementation]))
                   (= expected-sha (:sha256 fixture))
                   (= expected-bytes (:bytes fixture))
                   (= expected-count (:record-count fixture))
                   (= ordinal (:record-ordinal fixture))
                   (integer? (:record-start boundaries))
                   (integer? (:record-end-exclusive boundaries))
                   (<= 0 (:record-start boundaries)
                       (:record-end-exclusive boundaries)
                       expected-bytes))
      (fail! "reused JVM boundary report does not match this fixture" {}))
    (assoc boundaries
           :source-report-sha256
           (sha256-bytes (.getBytes text "UTF-8")))))

(defn run-report [fixture expected-sha ordinal warmups samples output]
  (let [fixture-path (Paths/get fixture no-path-parts)
        runtime (runtime-id)
        data-json-sha (required-env "BENCH_DATA_JSON_GIT_SHA")
        expected-record-count
        (parse-positive "expected record count"
                        (required-env "BENCH_EXPECTED_RECORD_COUNT") Long/MAX_VALUE)
        host {:runtime runtime
            :runtime-version (required-env "BENCH_RUNTIME_VERSION")
            :runtime-revision (required-env "BENCH_RUNTIME_REVISION")
            :executable-sha256
            (required-env "BENCH_RUNTIME_EXECUTABLE_SHA256")
            :os (System/getProperty "os.name")
            :arch (System/getProperty "os.arch")
            :java (System/getProperty "java.runtime.version")
            :matrix-order (required-env "BENCH_MATRIX_ORDER")
            :matrix-position
            (parse-positive "matrix position"
                            (required-env "BENCH_MATRIX_POSITION") 5)}
        harness {:repo-head (required-env "BENCH_REPO_HEAD")
                 :repo-parent (required-env "BENCH_REPO_PARENT")
                 :repo-tree (required-env "BENCH_REPO_TREE")
                 :wal-source-sha256 (required-env "BENCH_WAL_SOURCE_SHA256")
                 :report-source-sha256 (required-env "BENCH_REPORT_SOURCE_SHA256")
                 :jolt-metrics-source-sha256
                 (required-env "BENCH_JOLT_METRICS_SOURCE_SHA256")
                 :jvm-metrics-source-sha256
                 (required-env "BENCH_JVM_METRICS_SOURCE_SHA256")
                 :jvm-profile-source-sha256
                 (required-env "BENCH_JVM_PROFILE_SOURCE_SHA256")
                 :jvm-scan-source-sha256
                 (required-env "BENCH_JVM_SCAN_SOURCE_SHA256")
                 :runner-sha256 (required-env "BENCH_RUNNER_SHA256")
                 :scan-scope :one-wal-segment
                 :parse-scope :one-selected-record}
        fixture-base {:file-name (.getName (.toFile ^Path fixture-path))
                      :sha256 expected-sha
                      :record-ordinal ordinal
                      :record-count expected-record-count}
        journal-path (artifact-path output ".journal.edn")
        csv-path (artifact-path output ".csv")
        checkpoint-base {:schema-version report/schema-version
                         :event :phase
                         :host host
                         :harness harness
                         :fixture fixture-base}
        _journal-init (spit journal-path "")
        _csv-init (spit csv-path
                        "runtime,phase,status,warmups,samples,count,total_ns,max_ns\n")
        _host-start
        (append-checkpoint! journal-path
                            {:schema-version report/schema-version
                             :event :host :status :started
                             :host host :harness harness :fixture fixture-base})
        profile (atom (if (= runtime :jvm)
                        (or (start-jvm-profile)
                            {:status :not-run :reason :no-jfr-path})
                        {:status :not-run :reason :not-a-jvm-runtime}))
        metrics (metrics-provider runtime)
        phases (atom (sorted-map))
        checkpoint!
        (fn [phase result]
          (swap! phases assoc phase result)
          (append-checkpoint! journal-path
                              (assoc checkpoint-base :phase phase :result result))
          (append-csv! csv-path runtime phase result)
          result)
        phase-start!
        (fn [phase]
          (append-checkpoint! journal-path
                              (assoc checkpoint-base :phase phase
                                     :result {:status :started})))
        fail-phase!
        (fn [phase throwable]
          (let [result {:status :failed
                        :warmups 0 :samples 0
                        :error {:class (str (type throwable))
                                :type (:type (ex-data throwable))}}]
            (checkpoint! phase result)
            (throw throwable)))
        run-once!
        (fn [phase inputs f summarize]
          (phase-start! phase)
          (try
            (let [{:keys [value] :as measurement} (measure-once metrics f)
                  summary (summarize value)]
              (checkpoint!
               phase
               (measured-phase
                (assoc (dissoc measurement :value) :warmups 0 :samples 1)
                inputs summary))
              value)
            (catch Throwable throwable (fail-phase! phase throwable))))
        run-sampled!
        (fn [phase inputs expected-result f]
          (phase-start! phase)
          (try
            (checkpoint!
             phase
             (measured-phase
              (assoc (measure-samples samples metrics f)
                     :warmups warmups :samples samples)
              inputs expected-result))
            (catch Throwable throwable (fail-phase! phase throwable))))
        completed (atom nil)]
    (try
      (when-not (Files/isRegularFile fixture-path
                                     (make-array java.nio.file.LinkOption 0))
        (fail! "WAL fixture is not a regular file" {}))
      (let [bytes
            (run-once! :file-read {:file-name (:file-name fixture-base)}
                       #(Files/readAllBytes fixture-path) alength)
            actual-sha
            (run-once!
             :sha256 {:input-bytes (alength bytes)}
             #(sha256-bytes bytes)
             (fn [value]
               (when-not (= expected-sha value)
                 (fail! "WAL fixture digest does not match" {}))
               value))
            reused-path (System/getenv "BENCH_REUSE_BOUNDARY_REPORT")
            boundaries
            (if reused-path
              (let [value (load-reused-boundaries
                           reused-path actual-sha (alength bytes)
                           expected-record-count ordinal)]
                (phase-start! :record-boundary-scan)
                (checkpoint! :record-boundary-scan
                             (assoc (not-run-phase
                                     :reused-primary-jvm-boundaries)
                                    :source-report-sha256
                                    (:source-report-sha256 value)))
                (dissoc value :source-report-sha256))
              (do
                (phase-start! :record-boundary-scan)
                (try
                  (let [{:keys [value result]}
                        (measure-boundary-phase
                         bytes ordinal expected-record-count metrics
                         scan-record-boundaries)]
                    (checkpoint! :record-boundary-scan result)
                    value)
                  (catch Throwable throwable
                    (fail-phase! :record-boundary-scan throwable)))))
            primitive-boundaries
            (if (and (= runtime :jvm) (not reused-path))
              (let [scanner
                    (requiring-resolve
                     'jdbc.chdb-cross-host-jvm-scan/scan-record-boundaries)]
                (phase-start! :jvm-primitive-boundary-scan)
                (try
                  (let [{:keys [value result]}
                        (measure-boundary-phase
                         bytes ordinal expected-record-count metrics scanner)]
                    (when-not (= boundaries value)
                      (fail! "JVM primitive boundary scan differs from shared scanner" {}))
                    (checkpoint! :jvm-primitive-boundary-scan result)
                    value)
                  (catch Throwable throwable
                    (fail-phase! :jvm-primitive-boundary-scan throwable))))
              (do
                (phase-start! :jvm-primitive-boundary-scan)
                (checkpoint! :jvm-primitive-boundary-scan
                             (not-run-phase
                              (if reused-path
                                :reused-primary-jvm-boundaries
                                :jvm-only-control)))))
            record-bytes
            (run-once!
             :record-copy
             {:record-start (:record-start boundaries)
              :record-end-exclusive (:record-end-exclusive boundaries)}
             #(Arrays/copyOfRange bytes (:record-start boundaries)
                                  (:record-end-exclusive boundaries))
             (fn [value]
               {:bytes (alength value)
                :sha256 (sha256-bytes value)}))
            record-text
            (run-once! :strict-utf8-decode {:input-bytes (alength record-bytes)}
                       #(strict-decode record-bytes) count)
            parser
            (run-once! :parser-load {:runtime runtime}
                       #(json-parser runtime data-json-sha) :identity)
            read-json (:read-str parser)
            sql
            (run-once!
             :json-oracle {:input-chars (count record-text)}
             #(get (read-json record-text) "sql")
             (fn [value]
               (when-not (string? value)
                 (fail! "selected JSON record has no string sql value" {}))
               {:sql-chars (count value)
                :sql-sha256 (sha256-bytes (.getBytes value "UTF-8"))}))
            json-phases
            [[:json-read-str {:input-chars (count record-text)} (count sql)
              #(get (read-json record-text) "sql")]
             [:decode-and-json-read-str {:input-bytes (alength record-bytes)}
              (count sql) #(get (read-json (strict-decode record-bytes)) "sql")]]]
        (doseq [[phase _ _ f] json-phases]
          (try (warm! warmups f)
               (catch Throwable throwable (fail-phase! phase throwable))))
        (doseq [[phase inputs expected-result f] json-phases]
          (run-sampled! phase inputs expected-result f))
        (reset!
         completed
         {:libraries
          {:json-parser (:identity parser)
           :abi (resource-identity "jdbc/chdb/abi.edn")
           :compatibility (resource-identity "jdbc/chdb/ffi-compatibility.edn")
           :jolt-compiler
           {:source-sha (required-env "BENCH_JOLT_SOURCE_SHA")
            :executable-sha256 (required-env "BENCH_JOLT_EXECUTABLE_SHA256")}
           :native
           {:version (required-env "BENCH_NATIVE_VERSION")
            :library-sha256 (required-env "BENCH_NATIVE_LIBRARY_SHA256")}}
          :fixture
          (merge fixture-base
                 {:bytes (alength bytes)
                  :sha256 actual-sha
                  :record-start (:record-start boundaries)
                  :record-end-exclusive (:record-end-exclusive boundaries)
                  :record-bytes (alength record-bytes)
                  :record-sha256 (sha256-bytes record-bytes)
                  :decoded-chars (count record-text)
                  :json {:status :verified
                         :sql-chars (count sql)
                         :sql-sha256
                         (sha256-bytes (.getBytes sql "UTF-8"))}})}))
      (catch Throwable throwable
        (append-checkpoint! journal-path
                            {:schema-version report/schema-version
                             :event :host :status :failed
                             :host host :harness harness :fixture fixture-base
                             :error (error-summary throwable)})
        (throw throwable))
      (finally
        (when (= runtime :jvm)
          (reset! profile (stop-jvm-profile @profile)))))
    (let [{:keys [libraries fixture]} @completed
          value
          {:schema-version report/schema-version
           :status :complete
           :scope :phase-0-characterization
           :claim :pure-wal-byte-and-json-costs-only
           :host host
           :harness harness
           :libraries libraries
           :fixture fixture
           :measurement {:clock :monotonic-nanoseconds
                         :warmups warmups :samples samples
                         :resource-support (:support metrics)
                         :resource-metadata (:metadata metrics)
                         :phases @phases}
           :profile {:jfr @profile}
           :limitations
           [:no-durable-open-or-replay
            :no-production-record-validation-on-babashka-or-jvm
            :babashka-json-parser-is-bundled-cheshire-not-pinned-data-json
            :allocation-counters-are-host-specific
            :jvm-gc-counters-are-process-global]
           :comparison-axes
           {:runtime-controlled [:jolt :jvm]
            :runtime-controlled-json-parser :casselc-data-json
            :natural-host
            {:jolt :casselc-data-json
             :babashka :babashka-bundled-cheshire
             :jvm [:casselc-data-json :upstream-data-json :jvm-cheshire]}
            :raw-scan-and-decode-controlled [:jolt :babashka :jvm]}}]
      (append-checkpoint! journal-path
                          {:schema-version report/schema-version
                           :event :host :status :complete
                           :host host :harness harness :fixture fixture})
      value)))

(defn -main [& args]
  (when-not (= 6 (count args))
    (fail! "expected WAL_JSONL SHA256 RECORD_ORDINAL WARMUPS SAMPLES OUTPUT_EDN" {}))
  (let [[fixture digest ordinal warmups samples output] args
        ordinal (parse-positive "record ordinal" ordinal Long/MAX_VALUE)
        warmups (parse-positive "warmups" warmups report/max-samples)
        samples (parse-positive "samples" samples report/max-samples)
        value (run-report fixture digest ordinal warmups samples output)]
    (spit output (report/render value))
    (println (pr-str {:status :ok :runtime (get-in value [:host :runtime])
                      :output output}))))
