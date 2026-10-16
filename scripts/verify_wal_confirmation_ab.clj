;; Full independent workload/recovery oracle for BOTH arms remains required.
(require '[clojure.edn :as edn])
(let [root (first *command-line-args*)
      parent (.getParent (java.io.File. *file*))]
  (load-file (str parent "/typed_storage_confirmation_receipt.clj"))
  (let [validate! (ns-resolve 'typed-storage-confirmation-receipt 'validate!)]
    (doseq [arm ["baseline" "candidate"]]
      (binding [*command-line-args* [(str root "/" arm)]]
        (load-file (str parent "/verify_typed_storage_curl_profile.clj")))
      (doseq [[name phase] [["writer" "window"] ["reader" "reader"]]]
        (validate! (edn/read-string (slurp (str root "/" arm "/s3-" name "-confirmation.edn"))) arm phase))))
  (println :both-confirmation-arms-selection-and-independent-recovery-passed))
