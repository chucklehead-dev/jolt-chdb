(ns typed-storage-batch-lib
  (:require [clojure.string :as str]))

(def request-limit 24000)
(def upload-limit (* 6 1024 1024 1024))
(def zero-counts {:requests 0 :completed 0 :upload-bytes 0})
(def schedule [[1 10000] [2 20000] [3 50000] [4 50000] [5 20000] [6 10000]])

(defn failure-context [error]
  ;; Never retain messages, arbitrary ex-data, SQL or attribute values.
  (loop [e error left 8 out []]
    (if (or (nil? e) (zero? left)) out
      (let [data (ex-data e) label (:label data)
            stage (:jdbc.chdb.durable/startup-stage data)]
        (recur (.getCause e) (dec left)
          (conj out
            {:label (when (contains? #{:bounded-workload :schema-checkpoint
                                       :confirmed-projector :confirmed-compact-plan
                                       :owned-output-witness :witness-call :selected-prefix-path
                                       :direct-vector-witness :exact-sql :prepared-vector-sql
                                       :warmup :confirmed-export :writer-count
                                       :metadata-reader-exercised :global-reader-unmodified
                                       :stream-execution-witness :automatic-renewals
                                       :automatic-checkpoints :exporter-shutdown
                                       :fresh-full-physical-row-equality :fresh-stock-reader}
                                     label) label)
             :startup-stage (when (contains? #{:capability :read-head :acquire-lease
                                               :create-scratch :open-native :recover
                                               :renew-lease :start-writer} stage) stage)}))))))

(defn fail! []
  (throw (ex-info "Invalid batch research control or receipt" {:type ::invalid})))

(defn counts! [counts]
  (when-not (and (map? counts) (= (set (keys zero-counts)) (set (keys counts)))
                (every? #(and (integer? %) (<= 0 %)) (vals counts))
                (= (:requests counts) (:completed counts))
                (<= (:requests counts) request-limit)
                (<= (:upload-bytes counts) upload-limit))
    (fail!))
  counts)

(defn derive-source [source]
  ;; Every replacement is exact and unique; a drift is not permission to run a
  ;; different workload. Original source is also bound to a clean immutable SHA.
  (reduce
   (fn [text [before after]]
     (when-not (= 1 (count (re-seq (re-pattern (java.util.regex.Pattern/quote before)) text)))
       (fail!))
     (str/replace text before after))
   source
   [["(<= 1 n 10000)" "(<= 1 n 50000)"]
    [":checkpoint-wal-reference-threshold 128" ":checkpoint-wal-reference-threshold 16"]
    ["(>= samples 256)" "(>= samples 32)"]
    [":owned-statement-output? owned-output?})"
     ":owned-statement-output? owned-output? :max-insert-bytes 67108864})"]
    ["expected (with-bindings {#'e/*json-backend* :native-guarded-byte-batch}"
     "expected (with-bindings {#'e/*json-backend* :native-guarded-byte-batch #'e/*insert-byte-limit* 67108864}"]
    ["(System/exit 1)" "(throw error)"]
    ["report {:scope (case"
     "report {:research-only? true :max-insert-bytes 67108864 :scope (case"]]))

(defn derive-storage-source [source]
  ;; Each arm needs a distinct namespace. Keep the frozen qualification's base
  ;; prefix guard untouched; this process-local derivative accepts only the six
  ;; research children, not arbitrary suffixes or a shared base namespace.
  (let [before "#\"ci/jolt-chdb/[0-9]+-[0-9]+/typed-window\""
        after "#\"ci/jolt-chdb/[0-9]+-[0-9]+/typed-window/[1-6]\""]
    (when-not (= 1 (count (re-seq (re-pattern (java.util.regex.Pattern/quote before)) source)))
      (fail!))
    (str/replace source before after)))

(defn budget-receipt! [before receipt phase]
  (when-not (contains? #{"window" "reader"} phase) (fail!))
  (let [{:keys [after phase-counts]} receipt
        phase-request-limit (if (= phase "window") 4000 1000)
        phase-upload-limit (if (= phase "window") upload-limit 0)]
    (counts! before)
    (counts! after)
    (counts! phase-counts)
    (when-not (and (true? (:scope-returned receipt)) (= phase (:phase receipt))
                  (= before (:before receipt))
                  (= request-limit (:request-limit receipt))
                  (= upload-limit (:upload-limit receipt))
                  (= phase-request-limit (:phase-request-limit receipt))
                  (= phase-upload-limit (:phase-upload-limit receipt))
                  (<= 1 (:requests phase-counts) phase-request-limit)
                  (<= (:upload-bytes phase-counts) phase-upload-limit)
                  (= after (merge-with + before phase-counts)))
      (fail!))
    after))

(def sql-digests
  {10000 "d3afed99dac4a86aa35e34beb7933c1e84f6fdbf8ddc0cb603fc9fe2a6c25b11"
   20000 "43dc40a60e8272e00537b24e5d00cec883b88fd5665aef048e8773812bb6bd81"
   50000 "b89c21707f1a37357ddd0367a1ed50c0611548bc7e02e5dc7833f4d4452999dd"})

(defn workload-receipt! [writer reader storage n]
  (let [samples (:samples-nanos writer)
        expected-scope (case storage "local" :local-posix-typed-public-export
                              "s3" :s3-scoped-typed-public-export nil)]
    (when-not (and expected-scope (contains? sql-digests n)
                  (= :passed (:gate-status writer)) (true? (:research-only? writer))
                  (= expected-scope (:scope writer))
                  (= [n 32 3 "wide16" 1 "stream" 35 (get sql-digests n) 67108864]
                     (mapv writer [:batch-size :samples :warmups :attribute-shape
                                   :export-callers :native-execution-arm :stream-calls
                                   :sql-sha256 :max-insert-bytes]))
                  (true? (:owned-statement-output? writer))
                  (= :raw-ticks (:timestamp-wire writer))
                  (= "native" (get-in writer [:metadata-reader :mode]))
                  (= :stock (get-in writer [:metadata-reader :startup-reader]))
                  (= :stock (get-in writer [:metadata-reader :recovery-reader]))
                  (= 32 (count samples))
                  (every? #(and (integer? %) (pos? %)) samples)
                  (>= (get-in writer [:checkpoint-renewal-window :renewals] 0) 2)
                  (>= (count (get-in writer [:checkpoint-renewal-window :checkpoints])) 2)
                  (true? (:passed reader)) (true? (:full-physical-rows-equal reader))
                  (true? (:typed-values-status reader))
                  (= n (:groups reader)) (= (* n 35) (:rows reader)))
      (fail!))
    (let [ordered (vec (sort samples))
          latency (zipmap [:p50-ms :p90-ms :p95-ms :p99-ms]
                          (map (fn [p] (/ (double (nth ordered (dec (long (Math/ceil (* p 32))))))
                                         1000000.0)) [0.5 0.9 0.95 0.99]))]
      (doseq [key [:p50-ms :p95-ms :p99-ms]]
        (when-not (= (get-in writer [:latency key]) (get latency key)) (fail!)))
      {:storage storage :batch-size n :correctness true :latency latency
       :stable-tail-qualified false :samples 32 :p99-is-sample-maximum true
       :inverse-p50-rows-per-second (/ (* n 1000.0) (:p50-ms latency))
       :inverse-p99-rows-per-second (/ (* n 1000.0) (:p99-ms latency))})))
