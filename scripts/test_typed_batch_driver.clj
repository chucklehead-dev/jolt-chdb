;; Execute the real budget entry with a fake fixture/transport. No network,
;; native database, AWS credentials or stored object data are needed.
(require '[clojure.edn :as edn] '[clojure.test :refer [deftest is run-tests]]
         '[jdbc.chdb.durable.s3-curl :as curl])
(def parent (.getParent (java.io.File. *file*)))
(load-file (str parent "/typed_storage_batch_lib.clj"))
(def driver-path (str parent "/typed_storage_batch_budget_driver.clj"))
(def root (first *command-line-args*))
(def ledger (System/getenv "TASK_BATCH_BUDGET_FILE"))
(assert (= (str root "/ledger.edn") ledger))

(deftest real-entry-chains-budgets-and-does-not-complete-rejected-phases
  (let [calls (atom []) original-load load-file
        invoke! (fn [phase folder requests]
                  (.mkdirs (java.io.File. (str root "/" folder)))
                  (binding [*file* driver-path
                            *command-line-args* [phase (str root "/" folder) "10000" "32" "true" "wide16"]]
                    (with-redefs [curl/request! (fn [request] (swap! calls conj request) {:status 200})
                                  clojure.core/load-file
                                  (fn [path]
                                    (if (.endsWith path "/typed_storage_batch_driver.clj")
                                      (doseq [request requests]
                                        (if (instance? Throwable request) (throw request)
                                          (curl/request! request)))
                                      (original-load path)))]
                      (original-load driver-path))))]
    (spit ledger (pr-str typed-storage-batch-lib/zero-counts))
    (invoke! "window" "writer" [{:method :put :request-body {:byte-count 3}} {:method :get}])
    (let [receipt (edn/read-string (slurp (str root "/writer/http-budget.edn")))]
      (is (= {:requests 2 :completed 2 :upload-bytes 3} (:after receipt)))
      (is (= (:after receipt) (edn/read-string (slurp ledger)))))
    (invoke! "reader" "reader" [{:method :get}])
    (let [receipt (edn/read-string (slurp (str root "/reader/http-budget.edn")))]
      (is (= {:requests 3 :completed 3 :upload-bytes 3} (:after receipt)))
      (is (= {:requests 1 :completed 1 :upload-bytes 0} (:phase-counts receipt))))
    (is (= 3 (count @calls)))
    (is (thrown? clojure.lang.ExceptionInfo
                 (invoke! "reader" "rejected" [{:method :put :request-body {:byte-count 0}}])))
    (is (= 3 (count @calls)))
    (is (false? (.exists (java.io.File. (str root "/rejected/http-budget.edn")))))
    (is (= {:requests 3 :completed 3 :upload-bytes 3} (edn/read-string (slurp ledger))))
    (let [error (ex-info "PRIVATE-CANARY" {:label :warmup :secret "PRIVATE-CANARY"}
                        (ex-info "PRIVATE-CANARY" {:label :PRIVATE-CANARY
                                                  :type :jdbc.chdb.durable.s3/permission
                                                  :jdbc.chdb.durable/startup-stage :recover
                                                  :status 403}))
          caught (try (invoke! "window" "failed" [error]) nil (catch Throwable e e))
          _ (when-not (.exists (java.io.File. (str root "/failed/driver-failure.edn")))
              (throw caught))
          text (slurp (str root "/failed/driver-failure.edn"))
          receipt (edn/read-string text)]
      (is (identical? error caught))
      (is (false? (:scope-returned receipt)))
      (is (= :typed-batch-failure (:scope receipt)))
      (is (= [{:label :warmup :startup-stage nil} {:label nil :startup-stage :recover}]
             (:context receipt)))
      (is (= {:requests 3 :completed 3 :upload-bytes 3} (:request-progress receipt)))
      (is (false? (.contains text "PRIVATE-CANARY")))
      (is (false? (.exists (java.io.File. (str root "/failed/http-budget.edn"))))))
    (is (= {:requests 3 :completed 3 :upload-bytes 3} (edn/read-string (slurp ledger))))))

(let [result (run-tests)]
  (System/exit (if (zero? (+ (:fail result) (:error result))) 0 1)))
