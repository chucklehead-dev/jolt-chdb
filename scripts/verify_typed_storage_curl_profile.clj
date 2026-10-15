(require '[clojure.edn :as edn])
(let [root (first *command-line-args*)
      parent (.getParent (java.io.File. *file*))]
  ;; Never substitute transfer metadata for full row/recovery/workload checks.
  (load-file (str parent "/verify_typed_storage_receipts.clj"))
  (load-file (str parent "/typed_storage_curl_profile_receipt.clj"))
  (let [validate! (ns-resolve 'typed-storage-curl-profile-receipt 'validate!)]
    (doseq [phase ["writer" "reader"]]
      (validate! (edn/read-string (slurp (str root "/s3-" phase "-curl-profile.edn")))
                 (edn/read-string (slurp (str root "/s3-" phase "-budget.edn"))))))
  (println :native-transfer-profile-and-independent-recovery-passed))
