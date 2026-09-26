(ns chez-json.bench
  "Bounded encoding probe; no database, WAL, flush, or receiver timing."
  (:require [clojure.data.json :as json]
            [clojure.string :as str])
  (:import [java.security MessageDigest]))

(load-file "bench/chez-json/fixture.clj")
(in-ns 'chez-json.bench)

(defn sha256 [bytes]
  (apply str (map #(format "%02x" (bit-and 255 %))
                  (.digest (MessageDigest/getInstance "SHA-256") bytes))))

(defn summary [samples]
  (let [ordered (vec (sort samples))
        percentile #(nth ordered (dec (int (Math/ceil (* % (count ordered))))))]
    {:count (count samples) :total-ns (reduce + samples)
     :p50-ns (percentile 0.50) :p95-ns (percentile 0.95)
     :p99-ns (percentile 0.99) :max-ns (peek ordered)
     :p99-qualified? (>= (count samples) 100)}))

(defn select-encoder [mode]
  (case mode
    "data-json"
    {:encode #(chez-json.fixture/payload % {}) :options {}
     :input :actual-jolt-values :oracle :exact-data-json-utf8}
    (do
      (load-file "bench/chez-json/bridge.clj")
      (in-ns 'chez-json.bench)
      ((ns-resolve 'chez-json.bridge 'load-encoder!) "bench/chez-json/encoder.ss")
      (let [encode (ns-resolve 'chez-json.bridge 'encode-rows)
            options (case mode
                      "chez-default" {}
                      "chez-minimal" chez-json.fixture/minimal-options
                      (throw (ex-info "Unknown encoder mode" {:mode mode})))]
        {:encode #(encode % options) :options options
         :input :actual-jolt-values-no-preconversion
         :oracle :decoded-values-and-row-order}))))

(defn check-payload! [rows payload]
  (let [lines (str/split-lines payload)]
    (when-not (and (str/ends-with? payload "\n")
                   (= (count rows) (count lines))
                   (= rows (mapv json/read-str lines)))
      (throw (ex-info "Decoded payload or row-order mismatch" {})))))

(defn timed-encode [encode rows]
  ;; Both paths receive the very same materialized Clojure vector of maps.
  ;; The first stopwatch includes all traversal, serialization and text
  ;; assembly; the second isolates the required UTF-8 materialization.
  (let [start (System/nanoTime)
        payload (encode rows)
        text-done (System/nanoTime)
        bytes (.getBytes payload "UTF-8")
        end (System/nanoTime)]
    {:payload payload :utf8 bytes
     :encode-and-assembly-ns (- text-done start)
     :utf8-ns (- end text-done) :total-ns (- end start)}))

(defn measure! [mode n warmups samples directory]
  (let [rows (chez-json.fixture/rows n)
        {:keys [encode options input oracle]} (select-encoder mode)
        reference (chez-json.fixture/payload rows {})
        initial (timed-encode encode rows)
        baseline (:payload initial)
        byte-count (alength (:utf8 initial))
        digest (sha256 (:utf8 initial))]
    (check-payload! rows baseline)
    (when (and (= oracle :exact-data-json-utf8) (not= reference baseline))
      (throw (ex-info "Byte-compatible mode differs from data.json" {:mode mode})))
    (dotimes [_ warmups] (timed-encode encode rows))
    (let [results
          (mapv (fn [_]
                  (let [result (timed-encode encode rows)]
                    ;; Hashing, equality, and reports are outside the timer.
                    (when-not (and (= baseline (:payload result))
                                   (= byte-count (alength (:utf8 result)))
                                   (= digest (sha256 (:utf8 result))))
                      (throw (ex-info "Measured bytes changed" {:mode mode})))
                    (select-keys result [:encode-and-assembly-ns :utf8-ns :total-ns])))
                (range samples))
          total (summary (mapv :total-ns results))
          report
          {:schema 1 :scope :encode-only :mode (keyword mode)
           :runtime :jolt :input input :options options :oracle oracle
           :data-json-sha "1b0716268232a79dd2b2fdb968cca171414bd589"
           :fixture {:shape :clickstack-otel-log-jsoneachrow :rows n
                     :start-index 0 :question-mark? false
                     :utf8-bytes byte-count :utf8-sha256 digest
                     :decoded-value-parity :passed
                     :default-reference-byte-equal? (= reference baseline)
                     :default-reference-utf8-sha256
                     (sha256 (.getBytes reference "UTF-8"))}
           :measurement
           {:warmups warmups :samples samples
            :encode-and-assembly (summary (mapv :encode-and-assembly-ns results))
            :utf8 (summary (mapv :utf8-ns results)) :total total
            :rows-per-second (/ (* 1.0e9 n) (:p50-ns total))
            :bytes-per-second (/ (* 1.0e9 byte-count) (:p50-ns total))
            :raw-samples results}
           :limitations
           [:encoding-and-text-assembly-integrated-in-production-api
            :native-port-drain-cost-measured-separately-in-bench-ss
            :no-ingestion-or-durability-qualification]}]
      (.mkdirs (java.io.File. directory))
      (when (= "1" (System/getenv "BENCH_RETAIN_PAYLOAD"))
        (spit (str directory "/" mode "-" n ".jsonl") baseline))
      (spit (str directory "/" mode "-" n ".edn") (str (pr-str report) "\n"))
      (println (pr-str (update report :measurement dissoc :raw-samples)))
      report)))

(defn parity! []
  (load-file "bench/chez-json/bridge.clj")
  (in-ns 'chez-json.bench)
  ((ns-resolve 'chez-json.bridge 'load-encoder!) "bench/chez-json/encoder.ss")
  (let [encode (ns-resolve 'chez-json.bridge 'encode-value)
        encode-rows (ns-resolve 'chez-json.bridge 'encode-rows)
        cases (chez-json.fixture/corpus)
        float-cases (chez-json.fixture/finite-float-cases)]
    (doseq [collision-keys chez-json.fixture/full-hash-collision-groups]
      (when-not (and (= (count collision-keys) (count (set collision-keys)))
                     (apply = (map hash collision-keys)))
        (throw (ex-info "Full-hash collision fixture does not collide"
                        {:keys collision-keys :hashes (mapv hash collision-keys)}))))
    (doseq [{:keys [label value]} float-cases]
      (when (or (.isNaN ^Double value) (.isInfinite ^Double value))
        (throw (ex-info "Float grid produced a non-finite value" {:label label}))))
    (doseq [{:keys [label value options]} cases]
      (let [expected (chez-json.fixture/write-json value options)
            actual (encode value (or options {}))]
        (when-not (= expected actual)
          (throw (ex-info "Bridge exact byte parity failed"
                          {:label label :expected expected :actual actual})))
        (when-not (= (json/read-str expected) (json/read-str actual))
          (throw (ex-info "Bridge decoded parity failed"
                          {:label label :expected expected :actual actual})))))
    (doseq [value [Double/NaN Double/POSITIVE_INFINITY Double/NEGATIVE_INFINITY
                   (Object.) \x #{1 2}]]
      (when-not (try (encode value) false (catch Throwable _ true))
        (throw (ex-info "Unsupported bridge value was accepted" {}))))
    (doseq [options [{:unknown true} {:escape-slash :yes}
                     {:namespaced/escape-slash false}]]
      (when-not (try (encode 1 options) false (catch Throwable _ true))
        (throw (ex-info "Unsupported bridge option was accepted" {}))))
    (when-not (and (= "" (encode-rows []))
                   (try (encode-rows '(1 2)) false (catch Throwable _ true)))
      (throw (ex-info "Bridge rows collection contract failed" {})))
    (doseq [n chez-json.fixture/row-counts]
      (let [rows (chez-json.fixture/rows n)
            actual (encode-rows rows)
            minimal (encode-rows rows chez-json.fixture/minimal-options)]
        (when-not (= (chez-json.fixture/payload rows {}) actual)
          (throw (ex-info "Telemetry default byte parity failed" {:rows n})))
        (when-not (= (chez-json.fixture/payload rows chez-json.fixture/minimal-options)
                     minimal)
          (throw (ex-info "Telemetry minimal-option byte parity failed" {:rows n})))
        (check-payload! rows actual)
        (check-payload! rows minimal)))
    (println (pr-str {:status :passed :corpus-cases (count cases)
                      :finite-float-grid-cases (count float-cases)
                      :full-hash-collision-groups
                      chez-json.fixture/full-hash-collision-groups
                      :full-hash-equality :passed
                      :unsupported-cases 6 :row-counts chez-json.fixture/row-counts
                      :unsupported-option-cases 3 :empty-and-invalid-rows :passed
                      :exact-corpus-bytes :passed
                      :exact-default-row-bytes :passed
                      :exact-minimal-option-row-bytes :passed
                      :default-decoded-parity true :minimal-decoded-parity true}))))

(defn -main [& args]
  (case (first args)
    "export" (println (pr-str (chez-json.fixture/export-native! (second args))))
    "parity" (parity!)
    "measure" (let [[_ mode n warmups samples directory] args]
                (when-not (and directory (= 6 (count args)))
                  (throw (ex-info "usage: measure MODE ROWS WARMUPS SAMPLES OUTPUT_DIR" {})))
                (let [counts (mapv #(Long/parseLong %) [n warmups samples])]
                  (when-not (and (contains? (set chez-json.fixture/row-counts) (first counts))
                                 (<= 0 (second counts) 1000) (<= 1 (nth counts 2) 10000))
                    (throw (ex-info "Invalid bounded measurement counts" {})))
                  (apply measure! mode (concat counts [directory]))))
    (throw (ex-info "usage: bench.clj export DIR | parity | measure MODE ROWS WARMUPS SAMPLES DIR" {}))))

(apply -main *command-line-args*)
