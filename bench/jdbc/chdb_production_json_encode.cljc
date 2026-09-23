(ns jdbc.chdb-production-json-encode
  "Encoder-only comparison of the exact 512-row Durable ClickStack log fixture.
  No chDB, WAL, flush, or receiver work is measured."
  (:require [clojure.string :as str]
            [jdbc.chdb-durable-log-fixture :as fixture]
            [jdbc.chdb.json-each-row :as ordered]
            #?(:bb [cheshire.core :as native-json]
               :default [clojure.data.json :as data-json]))
  (:import [java.security MessageDigest]))

(defn- fail! [reason data]
  (throw (ex-info "Invalid cross-host JSON benchmark" (assoc data :reason reason))))

(defn- sha256-bytes [bytes]
  (apply str (map #(format "%02x" (bit-and 255 %))
                  (.digest (MessageDigest/getInstance "SHA-256") bytes))))

(defn- sha256 [text]
  (sha256-bytes (.getBytes text "UTF-8")))

(defn- canonical [value]
  (cond
    (map? value) (into (sorted-map) (map (fn [[k v]] [k (canonical v)]) value))
    (vector? value) (mapv canonical value)
    :else value))

(defn- encode-production [rows writer]
  ;; Match the production payload shape AND its UTF-8 materialization pass.
  ;; The opt-in ordered encoder does both inside encode-rows!, so stopping at
  ;; text alone would bias the cross-host timing against that API.
  (let [payload (apply str (map #(str (writer %) "\n") rows))
        bytes (.getBytes payload "UTF-8")]
    {:payload payload :utf8 bytes :byte-count (alength bytes)}))

(defn- selected-encoder [profile]
  (case profile
    :data-json-production
    #?(:bb (fail! :data-json-not-native-on-bb {:profile profile})
       :default {:encode #(encode-production % data-json/write-str)
                 :close (fn [] :closed)})
    :ordered-four
    #?(:jolt (let [context (ordered/open-encoder {:parallelism 4})]
               {:encode #(ordered/encode-rows! context %)
                :close #(ordered/close! context)})
       :default (fail! :ordered-four-is-jolt-only {:profile profile}))
    :cheshire-production
    #?(:bb {:encode #(encode-production % native-json/generate-string)
            :close (fn [] :closed)}
       :clj (do (require 'cheshire.core)
                {:encode #(encode-production %
                                             (ns-resolve 'cheshire.core
                                                         'generate-string))
                 :close (fn [] :closed)})
       :jolt (fail! :cheshire-is-not-jolt-profile {:profile profile}))
    (fail! :unknown-profile {:profile profile})))

(defn- parse-positive [name text]
  (let [value (try (Long/parseLong text) (catch Throwable _ nil))]
    (when-not (and value (pos? value) (<= value 10000))
      (fail! :invalid-count {:field name :value text}))
    value))

(defn- percentile [values fraction]
  (nth values (dec (int (Math/ceil (* fraction (count values)))))))

(defn- summary [samples]
  (let [ordered (vec (sort samples))]
    {:count (count ordered)
     :p50-ns (percentile ordered 0.5)
     :p95-ns (percentile ordered 0.95)
     :p99-ns (percentile ordered 0.99)
     :max-ns (peek ordered)}))

(defn report! [profile warmups samples]
  (let [rows (fixture/rows-512)
        semantic-sha (sha256 (pr-str (canonical rows)))
        {:keys [encode close]} (selected-encoder profile)]
    (try
      (let [baseline (encode rows)
            payload (:payload baseline)
            digest (sha256 payload)
            byte-count (:byte-count baseline)
            lines (str/split-lines payload)
            ;; Reuse each host's native parser to detect silently different
            ;; values before treating a hash/length as meaningful parity.
            parse #?(:bb native-json/parse-string
                     :default data-json/read-str)]
        (when-not (and (= 512 (count rows)) (= 512 (count lines))
                       (= byte-count (alength (:utf8 baseline)))
                       (= digest (sha256-bytes (:utf8 baseline)))
                       (str/ends-with? payload "\n")
                       (= rows (mapv parse lines)))
          (fail! :fixture-or-decoded-value-mismatch
                 {:profile profile :rows (count rows) :lines (count lines)}))
        (dotimes [_ warmups]
          (when-not (= digest (sha256 (:payload (encode rows))))
            (fail! :warmup-bytes-changed {:profile profile})))
        (let [sink (volatile! 0)
              durations
              (when (pos? samples)
                (vec (repeatedly samples
                                 (fn []
                                   (let [start (System/nanoTime)
                                         result (encode rows)
                                         elapsed (- (System/nanoTime) start)]
                                     (when-not (and (= digest (sha256 (:payload result)))
                                                    (= byte-count (:byte-count result)))
                                       (fail! :sample-bytes-changed
                                              {:profile profile}))
                                     (vswap! sink bit-xor (:byte-count result))
                                     elapsed)))))]
          {:schema 1 :scope :encode-only :profile profile
           :runtime #?(:bb :babashka :jolt :jolt :clj :jvm)
           :runtime-version #?(:bb (or (System/getenv "BENCH_RUNTIME_VERSION")
                                      "unknown")
                               :jolt (or (System/getenv "BENCH_RUNTIME_VERSION")
                                        "unknown")
                               :clj (clojure-version))
           :runtime-binary-sha256
           #?(:jolt (or (System/getenv "BENCH_RUNTIME_BINARY_SHA256") "unknown")
              :default nil)
           :harness-head (or (System/getenv "BENCH_HARNESS_HEAD") "unknown")
           :fixture-source-sha256
           (or (System/getenv "BENCH_FIXTURE_SOURCE_SHA256") "unknown")
           :codec-version (or (System/getenv "BENCH_CODEC_VERSION") "unknown")
           :fixture {:shape :clickstack-otel-log-jsoneachrow
                     :rows 512 :start-index 0 :question-mark? false
                     :semantic-sha256 semantic-sha
                     :utf8-bytes byte-count :utf8-sha256 digest
                     :utf8-array-sha256 (sha256-bytes (:utf8 baseline))
                     :decoded-value-parity :passed}
           :measurement (if durations
                          (assoc (summary durations) :warmups warmups
                                 :samples samples :sink @sink)
                          {:status :verification-only :warmups warmups})}))
      (finally (close)))))

(defn -main [& args]
  (let [[profile warmups samples output] args]
    (when-not (and output (= 4 (count args)))
      (fail! :usage {:expected "PROFILE WARMUPS SAMPLES OUTPUT"}))
    (let [report (report! (keyword profile)
                          (parse-positive :warmups warmups)
                          (if (= "verify" samples) 0
                              (parse-positive :samples samples)))]
      (spit output (str (pr-str report) "\n"))
      (println (pr-str report)))))
