;; Existing workload, recovery and per-arm budget checks remain mandatory.
(require '[clojure.edn :as edn])
(let [root (first *command-line-args*)
      parent (.getParent (java.io.File. *file*))
      read! (fn [wait phase]
              (edn/read-string (slurp (str root "/expect-" wait "/s3-" phase ".edn"))))]
  (doseq [wait [1000 10]]
    (let [arm-root (str root "/expect-" wait)]
      (binding [*command-line-args* [arm-root]]
        (load-file (str parent "/verify_typed_storage_receipts.clj")))
      (doseq [phase ["writer" "reader"]]
        (assert (= {:profile :expect-continue-ab :expect-100-timeout-ms wait}
                   (:storage-profile (read! wait phase)))))))
  (assert (= (:sql-sha256 (read! 1000 "writer")) (:sql-sha256 (read! 10 "writer"))))
  (prn {:comparison :expect-continue-ab :waits-ms [1000 10]
        :same-host-sequential true :correctness true
        :maximum-declared-writer-upload-bytes (* 12 1024 1024 1024)
        :not-confidence-bounds true}))
