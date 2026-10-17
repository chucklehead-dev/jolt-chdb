;; Synthetic controls for the receipt gate, not local/S3 performance evidence.
(require '[clojure.test :refer [deftest is run-tests]] '[clojure.edn :as edn])
(def checker (str (.getParent (java.io.File. *file*)) "/verify_typed_storage_receipts.clj"))
(def fixture
  {"local-writer" {:gate-status :passed :scope :local-posix-typed-public-export
                   :batch-size 10000 :samples 256 :warmups 3 :attribute-shape "wide16"
                   :export-callers 1 :native-execution-arm "stream" :stream-calls 259
                   :sql-sha256 "d3afed99dac4a86aa35e34beb7933c1e84f6fdbf8ddc0cb603fc9fe2a6c25b11"
                   :samples-nanos (vec (repeat 256 450000000))
                   :metadata-reader {:mode "native" :startup-reader :stock :recovery-reader :stock}
                   :checkpoint-renewal-window {:renewals 2 :checkpoints [{:batch 124} {:batch 252}]}
                   :latency {:p50-ms 450 :p90-ms 450 :p95-ms 450 :p99-ms 450}}
   "local-reader" {:passed true :full-physical-rows-equal true :typed-values-status true
                   :groups 10000 :rows 2590000}
   "s3-writer-budget" {:scope-returned true :phase "window" :request-limit 24000
                       :upload-limit 6442450944 :requests 2000 :completed 2000 :upload-bytes 100000}
   "s3-reader-budget" {:scope-returned true :phase "reader" :request-limit 1000
                       :upload-limit 0 :requests 5 :completed 5 :upload-bytes 0}})
(def full-fixture
  (assoc fixture "s3-writer" (assoc (get fixture "local-writer") :scope :s3-scoped-typed-public-export)
                 "s3-reader" (get fixture "local-reader")))

(defn fixture-output [reports]
  (let [root (java.io.File. (str (System/getProperty "java.io.tmpdir")
                               "/typed-receipt-control-" (java.util.UUID/randomUUID)))]
    (assert (.mkdir root))
    (try
      (doseq [[name report] reports]
        (spit (str root "/" name ".edn") (pr-str report)))
      (try
        (binding [*command-line-args* [(str root)]]
          (with-out-str (load-file checker)))
        (catch Throwable _ nil))
      (finally
        (doseq [file (.listFiles root)] (assert (.delete file)))
        (assert (.delete root))))))

(defn check-fixture [reports] (boolean (fixture-output reports)))

(deftest accepts-correct-run-even-when-performance-target-is-not-met
  (is (check-fixture full-fixture)))

(deftest derives-omitted-p90-from-real-samples
  (let [legacy (update full-fixture "local-writer" update :latency dissoc :p90-ms)
        output (fixture-output legacy)]
    (is (some? output))
    (is (== 450.0 (get-in (edn/read-string output) [:latency :p90-ms])))))

(deftest rejects-failed-or-drifted-receipts
  (doseq [[path value] [[["local-writer" :gate-status] :pending]
                       [["s3-writer" :sql-sha256] "wrong"]
                       [["s3-writer" :stream-calls] 258]
                       [["local-writer" :metadata-reader :mode] :native]
                       [["s3-writer" :checkpoint-renewal-window :renewals] 1]
                       [["s3-reader" :rows] 2580000]
                       [["s3-reader" :full-physical-rows-equal] false]
                       [["s3-reader-budget" :upload-bytes] 1]
                       [["s3-writer-budget" :scope-returned] false]
                       [["s3-writer-budget" :completed] 1999]
                       [["local-writer" :samples-nanos 0] 0]
                       [["local-writer" :samples-nanos 0] nil]
                       [["local-writer" :latency :p50-ms] 1]
                       [["local-writer" :latency :p90-ms] 1]
                       [["local-writer" :latency :p95-ms] 1]
                       [["local-writer" :latency :p99-ms] 1]]]
    (is (not (check-fixture (assoc-in full-fixture path value))) (pr-str path))))

(let [result (run-tests)]
  (System/exit (if (zero? (+ (:fail result) (:error result))) 0 1)))
