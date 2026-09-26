(ns chez-json.bench-bb
  "Native Babashka Cheshire reference on the identical varying log rows."
  (:require [cheshire.core :as json]
            [clojure.string :as str]
            [jdbc.chdb-durable-log-fixture :as fixture])
  (:import [java.security MessageDigest]))

(defn sha256 [bytes]
  (apply str (map #(format "%02x" (bit-and 255 %))
                  (.digest (MessageDigest/getInstance "SHA-256") bytes))))

(defn summary [values]
  (let [ordered (vec (sort values))
        pct #(nth ordered (dec (int (Math/ceil (* % (count ordered))))))]
    {:count (count values) :total-ns (reduce + values)
     :p50-ns (pct 0.5) :p95-ns (pct 0.95) :p99-ns (pct 0.99)
     :max-ns (peek ordered) :p99-qualified? (>= (count values) 100)}))

(defn sample [rows]
  (let [start (System/nanoTime)
        payload (apply str (map #(str (json/generate-string %) "\n") rows))
        encoded (System/nanoTime)
        bytes (.getBytes payload "UTF-8")
        end (System/nanoTime)]
    {:payload payload :utf8 bytes :encode-and-assembly-ns (- encoded start)
     :utf8-ns (- end encoded) :total-ns (- end start)}))

(let [[n-text warmups-text samples-text directory] *command-line-args*
      n (parse-long n-text) warmups (parse-long warmups-text)
      samples (parse-long samples-text)]
  (when-not (and directory (contains? #{512 1024 5000 10000} n)
                 (<= 0 warmups 1000) (<= 1 samples 10000))
    (throw (ex-info "usage: bench_bb.clj ROWS WARMUPS SAMPLES OUTPUT_DIR" {})))
  (let [rows (mapv #(fixture/log-row % false) (range n))
        initial (sample rows) baseline (:payload initial)
        size (alength (:utf8 initial)) hash (sha256 (:utf8 initial))]
    (when-not (and (str/ends-with? baseline "\n")
                   (= rows (mapv json/parse-string (str/split-lines baseline))))
      (throw (ex-info "BB reference decoded parity failed" {})))
    (dotimes [_ warmups] (sample rows))
    (let [results (mapv (fn [_]
                          (let [result (sample rows)]
                            (when-not (and (= baseline (:payload result))
                                           (= hash (sha256 (:utf8 result))))
                              (throw (ex-info "BB measured bytes changed" {})))
                            (select-keys result [:encode-and-assembly-ns :utf8-ns :total-ns])))
                        (range samples))
          total (summary (mapv :total-ns results))
          report {:schema 1 :scope :encode-only :mode :bb-cheshire :runtime :babashka
                  :input :actual-babashka-values :oracle :decoded-values-and-row-order
                  :fixture {:shape :clickstack-otel-log-jsoneachrow :rows n
                            :start-index 0 :question-mark? false
                            :utf8-bytes size :utf8-sha256 hash :decoded-value-parity :passed}
                  :measurement {:warmups warmups :samples samples
                                :encode-and-assembly (summary (mapv :encode-and-assembly-ns results))
                                :utf8 (summary (mapv :utf8-ns results)) :total total
                                :rows-per-second (/ (* 1.0e9 n) (:p50-ns total))
                                :bytes-per-second (/ (* 1.0e9 size) (:p50-ns total))
                                :raw-samples results}}]
      (.mkdirs (java.io.File. directory))
      (when (= "1" (System/getenv "BENCH_RETAIN_PAYLOAD"))
        (spit (str directory "/bb-cheshire-" n ".jsonl") baseline))
      (spit (str directory "/bb-cheshire-" n ".edn") (str (pr-str report) "\n"))
      (println (pr-str (update report :measurement dissoc :raw-samples))))))
