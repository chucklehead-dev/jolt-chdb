(load-file "bench/chez-json/bridge.clj")
(ns chez-json.durable-probe
  "Experimental encoder injection only; unchanged Durable classification/WAL."
  (:require [clojure.edn :as edn]
            [clojure.data.json :as json]
            [clojure.string :as str]
            [db.jdbc]
            [jdbc.core :as jdbc]
            [jdbc.chdb :as chdb]
            [jdbc.chdb-durable-log-fixture :as fixture]
            [jdbc.chdb-durable-throughput :as throughput]
            [jdbc.chdb.durable :as durable]
            [jdbc.chdb.durable.digest :as digest]
            [jdbc.chdb.durable.local-posix :as local]
            [jdbc.chdb.durable.wal :as wal]
            [jdbc.chdb.json-each-row :as encoder]
            [jdbc.chdb.native :as native]
            [jolt.host :as host])
  (:import [java.io File]))

(defn rows [n index]
  (mapv #(fixture/log-row % false) (range (* n index) (* n (inc index)))))

(defn fingerprint [path]
  (digest/sha256-file (.toPath (File. path))))

(defn provenance []
  (native/ensure-loaded!)
  {:jolt-sha256 (fingerprint (System/getenv "BENCH_JOLT_BIN"))
   :native-sha256 (fingerprint (native/library-path))
   :native-version (native/chdb-version)
   :chez-version (host/scheme-version)
   :native-wal? (wal/native-line-enabled?)
   :source-sha256
   (into {} (map (fn [p] [p (fingerprint p)])
                 ["bench/chez-json/encoder.ss" "bench/chez-json/bridge.clj"
                  "bench/chez-json/durable.clj"
                  "src/jdbc/chdb/json_each_row.cljc"
                  "src/jdbc/chdb/durable/writer.clj"
                  "src/jdbc/chdb/durable/wal.cljc"]))})

(defn await! [ticket]
  (when ticket (durable/await-local-execution! ticket)))

(defn confirm! [connection]
  (let [result (durable/flush! connection)]
    (assert (contains? #{:committed :reconciled} (:status result)))
    (:status result)))

(defn write! [mode n batches output]
  (assert (.mkdir (File. output)))
  (let [objects (str output "/objects")
        _ (assert (.mkdir (File. objects)))
        warmups 2
        ;; Inputs are varied maps, prepared before the receive/encode timer.
        inputs (mapv #(rows n %) (range (+ warmups batches)))
        context (encoder/open-encoder {:parallelism 4})
        _ (chez-json.bridge/load-encoder! "bench/chez-json/encoder.ss")
        encode (case mode
                 "normal" #(encoder/encode-rows! context %)
                 "chez" (fn [rs]
                          (let [s (chez-json.bridge/encode-rows rs)
                                b (.getBytes s "UTF-8")]
                            {:payload s :utf8 b :byte-count (alength b)})))
        _ (doseq [rs inputs]
            (assert (= rs (mapv json/read-str
                               (str/split-lines (:payload (encode rs)))))))
        prefix (chdb/json-rows-insert-prefix "otel_logs" @#'throughput/log-columns)
        receipt (atom nil)
        identity (provenance)]
    (try
      (with-open [connection
                  (jdbc/connection
                   (durable/writer-dbspec
                    {:namespace-backend (local/local-backend objects)
                     :object-id "chez-probe" :scratch-parent output
                     :owner "chez-json-bounded-probe" :database "benchmark"
                     :lease-ttl-ms 300000 :heartbeat-interval-ms 100000}))]
        (jdbc/execute! connection @#'throughput/logs-ddl)
        (confirm! connection)
        (doseq [rs (take warmups inputs)]
          (durable/execute-settled! connection (str prefix (:payload (encode rs)))))
        (confirm! connection)
        (System/gc)
        (let [start (System/nanoTime)
              samples
              (loop [i warmups previous nil result []]
                (if (= i (count inputs))
                  (do (await! previous) result)
                  (let [t0 (System/nanoTime)
                        encoded (encode (nth inputs i))
                        t1 (System/nanoTime)
                        _ (await! previous)
                        admitted (durable/try-admit-buffered!
                                  connection (str prefix (:payload encoded)))
                        t2 (System/nanoTime)]
                    (assert (= :admitted (:status admitted)))
                    (recur (inc i) (:ticket admitted)
                           (conj result {:encoding-ns (- t1 t0)
                                         :caller-ns (- t2 t0)
                                         :ordinal (:ordinal admitted)})))))
              locally-done (System/nanoTime)
              publication (confirm! connection)
              confirmed (System/nanoTime)
              measured-rows (* n batches)]
          (reset! receipt
                  {:scope :prepared-varying-rows-encoding-through-confirmed-local-posix
                   :mode mode :rows-per-batch n :batches batches :warmups warmups
                   :producer :one-ticket-overlap :provenance identity
                   :publication publication :samples samples
                   :local-ns (- locally-done start)
                   :flush-ns (- confirmed locally-done)
                   :confirmed-ns (- confirmed start)
                   :local-rows-per-second (/ (* 1.0e9 measured-rows) (- locally-done start))
                   :confirmed-rows-per-second (/ (* 1.0e9 measured-rows) (- confirmed start))
                   :flush-cadence :one-barrier-after-measured-batches
                   :limitations [:prepared-input-maps :no-s3 :short-run-no-tail-qualification
                                 :benchmark-adapter-not-production-encoder-api]})))
      (finally (encoder/close! context)))
    (spit (str output "/result.edn") (str (pr-str @receipt) "\n"))
    (println (pr-str (dissoc @receipt :samples :provenance)))))

(defn readback! [output]
  (let [{:keys [rows-per-batch batches warmups]}
        (edn/read-string (slurp (str output "/result.edn")))
        expected (reduce
                  (fn [acc index]
                    (#'throughput/accumulate-expected-batch
                     acc (rows rows-per-batch index) false))
                  @#'throughput/empty-expected-aggregates
                  (range (+ warmups batches)))]
    (with-open [connection
                (jdbc/connection
                 (durable/snapshot-dbspec
                  {:namespace-backend (local/local-backend (str output "/objects"))
                   :object-id "chez-probe" :scratch-parent output}))]
      (#'throughput/verify-counts! connection expected :fresh-process-chez-json))
    (let [receipt {:status :passed :fresh-process true :rows (:n expected)
                   :oracle :independently-regenerated-production-aggregates}]
      (spit (str output "/readback.edn") (str (pr-str receipt) "\n"))
      (println (pr-str receipt)))))

(let [[action mode n batches output] *command-line-args*]
  (case action
    "write" (let [n (Long/parseLong n) batches (Long/parseLong batches)]
              (assert (contains? #{"normal" "chez"} mode))
              (assert (contains? #{512 1024 5000 10000} n))
              (assert (<= 2 batches 100))
              (write! mode n batches output))
    "read" (readback! mode)))
