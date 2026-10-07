(ns jdbc.chdb.direct-bytes-bench
  (:require [jdbc.chdb.direct-bytes-test :as kernel]
            [otel.exporter.chdb :as exporter]
            [otel.exporter.chdb-benchmark :as fixture]
            [clojure.data.json :as json]
            [clojure.data.json.jolt-native :as native]
            [jdbc.chdb.durable.digest :as digest]
            [jolt.host :as host]))

(defn baseline [rows]
  (binding [json/*experimental-native-writer* (native/load-payload-string-caching-writer!)]
    (.getBytes (#'exporter/json-each-row-payload rows) "UTF-8")))

(defn candidate [rows] (kernel/encode rows (* 128 1024 1024)))
(defn guarded [rows] (kernel/guarded-encode rows (* 128 1024 1024)))

(defn sample [f rows]
  (f rows)
  (let [before (+ (host/gc-bytes) (host/bytes-allocated))
        cpu (host/cpu-nanos) start (System/nanoTime)
        sizes (mapv (fn [_] (alength (f rows))) (range 3))]
    {:iterations 3 :bytes sizes
     :cpu-nanos (- (host/cpu-nanos) cpu)
     :nanos (- (System/nanoTime) start)
     :estimated-scheme-allocation (- (+ (host/gc-bytes) (host/bytes-allocated)) before)}))

(defn -main [output]
  (let [service "direct-byte-buffer-component"
        resource (#'fixture/resource service)
        metrics (first (#'fixture/metrics 0 5000))
        scope (:scope metrics)
        signals (into
                  {:spans (mapv #(#'exporter/span-row (#'fixture/span service %) nil true) (range 5000))
                   :logs (mapv #(#'exporter/log-row (#'fixture/log-record service %) nil true) (range 5000))}
                  (map (fn [metric]
                         [(:type metric)
                          (mapv #(#'exporter/metric-row resource scope metric % nil true)
                                (:data-points metric))]) (:metrics metrics)))
        report {:scope :prepared-stock-rows-encoding-only-not-durable-or-ingest-qualification
                :runtime-version (host/scheme-version)
                :row-count 5000
                :json-source (:file (meta #'json/write))
                :exporter-source (:file (meta #'exporter/span-row))
                :signals
                (into {} (map
                           (fn [[signal rows]]
                             (let [a (digest/sha256-bytes (baseline rows))
                                   b (digest/sha256-bytes (candidate rows))
                                   c (digest/sha256-bytes (guarded rows))]
                               (assert (= a b) (str "wire mismatch: " signal))
                               (assert (= a c) (str "guarded wire mismatch: " signal))
                               [signal {:sha256 a
                                        :samples (mapv
                                                   (fn [arm]
                                                     (assoc (sample (case arm :baseline baseline :candidate candidate :guarded guarded) rows)
                                                            :arm arm))
                                                   [:baseline :candidate :guarded :guarded :candidate :baseline])}])) signals))}]
    (spit output (pr-str report))
    (doseq [[signal result] (:signals report)]
      (prn {:signal signal :wire-match true
            :samples (mapv #(select-keys % [:arm :nanos :estimated-scheme-allocation])
                           (:samples result))}))))
