(load-file (str (.getParent (java.io.File. *file*)) "/typed_storage_batch_lib.clj"))
(load-file (str (.getParent (java.io.File. *file*)) "/typed_storage_budget_lib.clj"))
(require '[clojure.test :refer [deftest is run-tests]])

(defn rejected? [f]
  (try (f) false (catch clojure.lang.ExceptionInfo _ true)))

(deftest exact-unique-source-changes-only
  (let [source (str "(<= 1 n 10000)\n:checkpoint-wal-reference-threshold 128\n"
                    "(>= samples 256)\n:owned-statement-output? owned-output?})\n"
                    "report {:scope (case")
        derived (typed-storage-batch-lib/derive-source source)]
    (is (.contains derived "(<= 1 n 50000)"))
    (is (.contains derived ":checkpoint-wal-reference-threshold 16"))
    (is (.contains derived "(>= samples 32)"))
    (is (.contains derived ":max-insert-bytes 67108864"))
    (is (.contains derived ":research-only? true"))
    (is (rejected? #(typed-storage-batch-lib/derive-source "drift")))
    (is (rejected? #(typed-storage-batch-lib/derive-source (str source source))))))

(deftest total-budget-cannot-reset-between-processes-and-readers-cannot-upload
  (let [prior {:requests 23998 :completed 23998 :upload-bytes 6442450943}
        total (atom prior) current (atom typed-storage-batch-lib/zero-counts) calls (atom 0)
        global (typed-storage-budget-lib/requester (fn [_] (swap! calls inc)) total 24000 6442450944 #{:get :put})
        send! (typed-storage-budget-lib/requester global current 4000 6442450944 #{:get :put})]
    (send! {:method :put :request-body {:byte-count 1}})
    (is (rejected? #(send! {:method :put :request-body {:byte-count 1}})))
    (is (= 1 @calls))
    (send! {:method :get})
    (is (rejected? #(send! {:method :get})))
    (is (= 2 @calls))
    (is (= {:requests 24000 :completed 24000 :upload-bytes 6442450944} @total)))
  (let [calls (atom 0) current (atom typed-storage-batch-lib/zero-counts)
        read! (typed-storage-budget-lib/requester (fn [_] (swap! calls inc)) current 1000 0 #{:get})]
    (is (rejected? #(read! {:method :put :request-body {:byte-count 0}})))
    (is (zero? @calls)))
  (doseq [bad [nil {} {:requests 0 :completed 0 :upload-bytes 0 :secret "private"}
               {:requests 2 :completed 1 :upload-bytes 0}
               {:requests -1 :completed -1 :upload-bytes 0}
               {:requests 24001 :completed 24001 :upload-bytes 0}
               {:requests 0 :completed 0 :upload-bytes 6442450945}]]
    (is (rejected? #(typed-storage-batch-lib/counts! bad)))))

(defn budget [before phase delta]
  {:before before :after (merge-with + before delta) :phase-counts delta
   :phase phase :scope-returned true :request-limit 24000 :upload-limit 6442450944
   :phase-request-limit (if (= phase "window") 4000 1000)
   :phase-upload-limit (if (= phase "window") 6442450944 0)})

(deftest cumulative-receipts-must-chain-and-settle
  (let [prior {:requests 20 :completed 20 :upload-bytes 100}
        receipt (budget prior "window" {:requests 5 :completed 5 :upload-bytes 80})]
    (is (= (:after receipt) (typed-storage-batch-lib/budget-receipt! prior receipt "window")))
    (doseq [mutant [(assoc receipt :before typed-storage-batch-lib/zero-counts)
                    (assoc receipt :scope-returned false)
                    (assoc receipt :scope-returned "false")
                    (assoc receipt :request-limit 24001)
                    (assoc-in receipt [:after :completed] 24)
                    (assoc-in receipt [:after :upload-bytes] 179)]]
      (is (rejected? #(typed-storage-batch-lib/budget-receipt! prior mutant "window"))))
    (is (rejected? #(typed-storage-batch-lib/budget-receipt! prior receipt "unknown"))))
  (let [receipt (budget typed-storage-batch-lib/zero-counts "reader"
                        {:requests 1 :completed 1 :upload-bytes 1})]
    (is (rejected? #(typed-storage-batch-lib/budget-receipt! typed-storage-batch-lib/zero-counts receipt "reader")))))

(defn writer [n]
  {:gate-status :passed :research-only? true :scope :s3-scoped-typed-public-export
   :batch-size n :samples 32 :warmups 3 :attribute-shape "wide16" :export-callers 1
   :native-execution-arm "stream" :stream-calls 35 :sql-sha256 (get typed-storage-batch-lib/sql-digests n)
   :max-insert-bytes 67108864 :owned-statement-output? true :timestamp-wire :raw-ticks
   :metadata-reader {:mode "native" :startup-reader :stock :recovery-reader :stock}
   :samples-nanos (vec (repeat 32 1000000))
   :latency {:p50-ms 1.0 :p95-ms 1.0 :p99-ms 1.0}
   :checkpoint-renewal-window {:renewals 2 :checkpoints [{} {}]}})
(defn reader [n] {:passed true :full-physical-rows-equal true :typed-values-status true :groups n :rows (* n 35)})

(deftest non-vacuous-workload-and-percentile-controls
  (doseq [n [10000 20000 50000]]
    (is (true? (:correctness (typed-storage-batch-lib/workload-receipt! (writer n) (reader n) "s3" n)))))
  (doseq [mutant [(assoc (writer 10000) :gate-status :pending)
                  (assoc (writer 10000) :stream-calls 0)
                  (assoc (writer 10000) :research-only? false)
                  (assoc (writer 10000) :samples-nanos [1000000])
                  (assoc-in (writer 10000) [:latency :p99-ms] 2.0)
                  (assoc-in (writer 10000) [:metadata-reader :recovery-reader] :native)]]
    (is (rejected? #(typed-storage-batch-lib/workload-receipt! mutant (reader 10000) "s3" 10000))))
  (doseq [mutant [(assoc (reader 10000) :full-physical-rows-equal false)
                  (assoc (reader 10000) :rows 349999)
                  (assoc (reader 10000) :typed-values-status false)]]
    (is (rejected? #(typed-storage-batch-lib/workload-receipt! (writer 10000) mutant "s3" 10000)))))

(let [result (run-tests)]
  (System/exit (if (zero? (+ (:fail result) (:error result))) 0 1)))
