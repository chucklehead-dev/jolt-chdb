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

(defn- count-newlines [bytes]
  (loop [index 0 count 0]
    (if (= index (alength bytes))
      count
      (recur (inc index)
             (if (= 10 (bit-and 255 (aget bytes index)))
               (inc count) count)))))

(defn- selected-record [bytes ordinal]
  (when-not (and (pos? (alength bytes))
                 (= 10 (bit-and 255 (aget bytes (dec (alength bytes))))))
    (fail! "WAL fixture must be non-empty and LF terminated" {}))
  (loop [index 0 start 0 current 1]
    (if (= index (alength bytes))
      (fail! "record ordinal exceeds the WAL fixture" {:ordinal ordinal})
      (if (= 10 (bit-and 255 (aget bytes index)))
        (if (= current ordinal)
          (Arrays/copyOfRange bytes start index)
          (recur (inc index) (inc index) (inc current)))
        (recur (inc index) start current)))))

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
  (merge {:status :measured
          :expected-result expected-result}
         inputs
         measurement))

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

(defn- measure [warmups samples {:keys [snapshot]} f]
  (dotimes [_ warmups] (consume-result (f)))
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

(defn- runtime-id []
  (let [runtime (keyword (required-env "BENCH_RUNTIME"))]
    (when-not (contains? runtimes runtime)
      (fail! "BENCH_RUNTIME is not a supported host" {:runtime runtime}))
    runtime))

(defn run-report [fixture expected-sha ordinal warmups samples]
  (let [fixture-path (Paths/get fixture no-path-parts)
        _ (when-not (Files/isRegularFile fixture-path (make-array java.nio.file.LinkOption 0))
            (fail! "WAL fixture is not a regular file" {}))
        bytes (Files/readAllBytes fixture-path)
        actual-sha (sha256-bytes bytes)
        _ (when-not (= expected-sha actual-sha)
            (fail! "WAL fixture digest does not match" {}))
        record-bytes (selected-record bytes ordinal)
        record-text (strict-decode record-bytes)
        runtime (runtime-id)
        data-json-sha (required-env "BENCH_DATA_JSON_GIT_SHA")
        parser (json-parser runtime data-json-sha)
        read-json (:read-str parser)
        sql (get (read-json record-text) "sql")
        _ (when-not (string? sql)
            (fail! "selected JSON record has no string sql value" {}))
        metrics (metrics-provider runtime)
        measure-raw? (not= "0" (System/getenv "BENCH_MEASURE_RAW"))
        json-input {:input-chars (count record-text)}
        composed-input {:input-bytes (alength record-bytes)}
        json-phases
        [[:json-read-str
          (measured-phase
           (measure warmups samples metrics #(get (read-json record-text) "sql"))
           json-input (count sql))]
         [:decode-and-json-read-str
          (measured-phase
           (measure warmups samples metrics
                    #(get (read-json (strict-decode record-bytes)) "sql"))
           composed-input (count sql))]]
        phases
        (into
         (sorted-map)
         (concat
          (if measure-raw?
            [[:raw-lf-scan
              (measured-phase
               (measure warmups samples metrics #(count-newlines bytes))
               {:input-bytes (alength bytes)} (count-newlines bytes))]
             [:strict-utf8-decode
              (measured-phase
               (measure warmups samples metrics #(strict-decode record-bytes))
               {:input-bytes (alength record-bytes)} (count record-text))]]
            [[:raw-lf-scan
              {:status :not-run :reason :measured-once-per-host}]
             [:strict-utf8-decode
              {:status :not-run :reason :measured-once-per-host}]])
          json-phases))]
    {:schema-version report/schema-version
     :scope :phase-0-characterization
     :claim :pure-wal-byte-and-json-costs-only
     :host {:runtime runtime
            :runtime-version (required-env "BENCH_RUNTIME_VERSION")
            :runtime-revision (required-env "BENCH_RUNTIME_REVISION")
            :executable-sha256
            (required-env "BENCH_RUNTIME_EXECUTABLE_SHA256")
            :os (System/getProperty "os.name")
            :arch (System/getProperty "os.arch")
            :java (System/getProperty "java.runtime.version")}
     :libraries
     {:json-parser (:identity parser)
      :abi (resource-identity "jdbc/chdb/abi.edn")
      :compatibility (resource-identity "jdbc/chdb/ffi-compatibility.edn")
      :jolt-compiler
      {:source-sha (required-env "BENCH_JOLT_SOURCE_SHA")
       :executable-sha256 (required-env "BENCH_JOLT_EXECUTABLE_SHA256")}
      :native
      {:version (required-env "BENCH_NATIVE_VERSION")
       :library-sha256 (required-env "BENCH_NATIVE_LIBRARY_SHA256")}}
     :fixture {:file-name (.getName (.toFile ^Path fixture-path))
               :bytes (alength bytes)
               :sha256 actual-sha
               :record-ordinal ordinal
               :record-count (count-newlines bytes)
               :record-bytes (alength record-bytes)
               :record-sha256 (sha256-bytes record-bytes)
               :decoded-chars (count record-text)
               :json {:status :verified
                      :sql-chars (count sql)
                      :sql-sha256 (sha256-bytes (.getBytes sql "UTF-8"))}}
     :measurement {:clock :monotonic-nanoseconds
                   :warmups warmups :samples samples
                   :resource-support (:support metrics)
                   :phases phases}
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
      :raw-scan-and-decode-controlled [:jolt :babashka :jvm]}}))

(defn -main [& args]
  (when-not (= 6 (count args))
    (fail! "expected WAL_JSONL SHA256 RECORD_ORDINAL WARMUPS SAMPLES OUTPUT_EDN" {}))
  (let [[fixture digest ordinal warmups samples output] args
        ordinal (parse-positive "record ordinal" ordinal Long/MAX_VALUE)
        warmups (parse-positive "warmups" warmups report/max-samples)
        samples (parse-positive "samples" samples report/max-samples)
        value (run-report fixture digest ordinal warmups samples)]
    (spit output (report/render value))
    (println (pr-str {:status :ok :runtime (get-in value [:host :runtime])
                      :output output}))))
