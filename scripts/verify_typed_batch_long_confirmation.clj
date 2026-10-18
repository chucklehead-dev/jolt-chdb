(require '[clojure.edn :as edn]
         '[typed-storage-batch-lib :as batch]
         '[typed-storage-batch-confirmation :as confirmation])
(let [[root] *command-line-args*
      read! (fn [folder name] (edn/read-string (slurp (str root "/" folder "/" name ".edn"))))]
  (when-not (= #{"preflight" "local" "local-after" "s3-1"}
               (set (.list (java.io.File. root))))
    (batch/fail!))
  (doseq [folder ["local" "local-after"]]
    (prn (assoc (batch/workload-receipt! (read! folder "local-writer")
                                       (read! folder "local-reader") "local" 10000)
                :bookend folder)))
  (let [after-writer (batch/budget-receipt! batch/zero-counts (read! "s3-1" "s3-writer-budget") "window")
        after-reader (batch/budget-receipt! after-writer (read! "s3-1" "s3-reader-budget") "reader")
        selection (confirmation/selection! (read! "s3-1" "s3-writer-confirmation") "candidate" "window" 97)
        _ (confirmation/selection! (read! "s3-1" "s3-reader-confirmation") "candidate" "reader" 97)
        result (batch/workload-receipt! (read! "s3-1" "s3-writer") (read! "s3-1" "s3-reader") "s3" 50000 100)]
    (prn (assoc result :arm "candidate" :selection selection :matched-comparison false
                :target-window-passed? (and (>= (:inverse-p50-rows-per-second result) 25000)
                                            (>= (:inverse-p99-rows-per-second result) 20000))))
    (prn {:global-budget-valid true :requests (:requests after-reader)
          :upload-bytes (:upload-bytes after-reader) :not-qualification true})))
