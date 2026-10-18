;; Shared total budget across the sequential sweep, plus narrower per-phase
;; caps. No URLs, identifiers, headers, bodies or credentials enter receipts.
(require '[clojure.edn :as edn] '[jdbc.chdb.durable.s3-curl :as curl])
(let [parent (.getParent (java.io.File. *file*))]
  (load-file (str parent "/typed_storage_batch_lib.clj"))
  (load-file (str parent "/typed_storage_budget_lib.clj"))
  (load-file (str parent "/typed_storage_confirmation_receipt.clj")))
(let [parent (.getParent (java.io.File. *file*))
        phase (first *command-line-args*) root (second *command-line-args*)
        ledger (System/getenv "TASK_BATCH_BUDGET_FILE")
        _ (when-not (and ledger (contains? #{"window" "reader"} phase))
            (typed-storage-batch-lib/fail!))
        before (typed-storage-batch-lib/counts! (edn/read-string (slurp ledger)))
        total (atom before) current (atom typed-storage-batch-lib/zero-counts)
        request-limit (if (= phase "window") 4000 1000)
        upload-limit (if (= phase "window") typed-storage-batch-lib/upload-limit 0)
        methods (if (= phase "window") #{:get :put} #{:get})
        global-request (typed-storage-budget-lib/requester curl/request! total
                         typed-storage-batch-lib/request-limit typed-storage-batch-lib/upload-limit methods)
        request (typed-storage-budget-lib/requester global-request current request-limit upload-limit methods)]
    (try
      (with-redefs [curl/request! request]
        (load-file (str parent "/typed_storage_batch_driver.clj")))
      (catch Throwable error
        (try
          (spit (str root "/driver-failure.edn")
                (pr-str (assoc
                  (dissoc (typed-storage-confirmation-receipt/failure-report
                            error :driver (typed-storage-confirmation-receipt/initial-counts))
                          :counts)
                  :scope :typed-batch-failure :phase phase
                  :context (typed-storage-batch-lib/failure-context error)
                  :request-progress
                  (into {} (map (fn [[key _]]
                                  (let [n (get @total key)]
                                    [key (when (and (integer? n)
                                                   (<= 0 n typed-storage-batch-lib/upload-limit)) n)]))
                                typed-storage-batch-lib/zero-counts)))))
          (catch Throwable _ nil))
        (throw error)))
    ;; Only reached after the native writer/reader and all transports close.
    (let [receipt {:phase phase :scope-returned true :before before :after @total
                   :phase-counts @current :request-limit typed-storage-batch-lib/request-limit
                   :upload-limit typed-storage-batch-lib/upload-limit
                   :phase-request-limit request-limit :phase-upload-limit upload-limit}]
      (typed-storage-batch-lib/budget-receipt! before receipt phase)
      (spit (str root "/http-budget.edn") (pr-str receipt))
      (spit ledger (pr-str @total))))
