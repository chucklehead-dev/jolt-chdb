;; Synthetic validator controls only; not real provider evidence.
(require '[s3-checksum-fault-receipt :as proof]
         '[clojure.test :refer [deftest is run-tests]])
(def report
  {:scope :s3-checksum-provider-controls :scope-returned true
   :throughput-qualified false :native-replay-qualified false
   :request-limit 128 :upload-limit 1048576 :requests {:requests 57 :completed 57 :upload-bytes 5200}
   :cases (mapv (fn [c]
                  (merge {:case c :passed true :scope-returned true :lease-released true}
                         (if (= c :changed-source)
                           {:provider-status 400 :mutated-before-send true :head-sequence 0 :object-absent true}
                           {:publication-status (case c :existing-readback :already-published
                                                       :injected-lost-ack :reconciled :published)
                            :head-sequence 1 :verification-completions (if (= c :new-upload) 0 1)
                            :valid-proof-observations (if (= c :new-upload) 2 0)
                            :independent-body-verified true}))) proof/case-order)})
(deftest closed-receipt-controls
  (is (= report (proof/validate! report)))
  (doseq [bad [nil {} (assoc report :scope-returned false) (assoc report :throughput-qualified true)
               (assoc report :native-replay-qualified true) (assoc report :secret "PRIVATE-CANARY")
               (assoc report :cases []) (assoc report :request-limit 129)
               (assoc-in report [:requests :completed] 56) (assoc-in report [:requests :upload-bytes] 1048577)
               (assoc-in report [:cases 0 :valid-proof-observations] 0)
               (assoc-in report [:cases 0 :verification-completions] 1)
               (assoc-in report [:cases 1 :verification-completions] 0)
               (assoc-in report [:cases 2 :verification-completions] 0)
               (assoc-in report [:cases 3 :verification-completions] 0)
               (assoc-in report [:cases 4 :provider-status] 403)
               (assoc-in report [:cases 4 :object-absent] false)
               (assoc-in report [:cases 4 :head-sequence] 1)]]
    (is (thrown? clojure.lang.ExceptionInfo (proof/validate! bad)))))
(let [r (run-tests)] (System/exit (if (zero? (+ (:fail r) (:error r))) 0 1)))
