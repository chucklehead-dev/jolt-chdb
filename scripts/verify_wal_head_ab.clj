;; The original recovery/workload/budget oracle remains required for EACH arm.
(let [root (first *command-line-args*)
      parent (.getParent (java.io.File. *file*))]
  (doseq [arm ["baseline" "candidate"]]
    (binding [*command-line-args* [(str root "/" arm)]]
      (load-file (str parent "/verify_typed_storage_curl_profile.clj"))))
  (println :both-fixed-wal-head-arms-and-independent-recovery-passed))
