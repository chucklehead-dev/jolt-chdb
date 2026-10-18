(require '[clojure.edn :as edn]
         '[typed-storage-batch-lib]
         '[typed-storage-batch-confirmation])
(let [[root] *command-line-args*
      read! (fn [folder name] (edn/read-string (slurp (str root "/" folder "/" name ".edn"))))]
  (when-not (= #{"preflight" "local" "local-after" "s3-1" "s3-2" "s3-3" "s3-4"}
               (set (.list (java.io.File. root))))
    (typed-storage-batch-lib/fail!))
  (doseq [folder ["local" "local-after"]]
    (prn (assoc (typed-storage-batch-lib/workload-receipt!
                  (read! folder "local-writer") (read! folder "local-reader") "local" 10000)
                :bookend folder)))
  (loop [entries typed-storage-batch-confirmation/schedule prior typed-storage-batch-lib/zero-counts]
    (if-let [[index arm] (first entries)]
      (let [folder (str "s3-" index)
            after-writer (typed-storage-batch-lib/budget-receipt!
                           prior (read! folder "s3-writer-budget") "window")
            after-reader (typed-storage-batch-lib/budget-receipt!
                           after-writer (read! folder "s3-reader-budget") "reader")
            selection (typed-storage-batch-confirmation/selection!
                        (read! folder "s3-writer-confirmation") arm "window")]
        (typed-storage-batch-confirmation/selection!
          (read! folder "s3-reader-confirmation") arm "reader")
        (prn (assoc (typed-storage-batch-lib/workload-receipt!
                      (read! folder "s3-writer") (read! folder "s3-reader") "s3" 50000)
                    :sequence index :arm arm :selection selection
                    :phase-requests (- (:requests after-reader) (:requests prior))
                    :phase-upload-bytes (- (:upload-bytes after-reader) (:upload-bytes prior))))
        (recur (next entries) after-reader))
      (prn {:global-budget-valid true :requests (:requests prior) :upload-bytes (:upload-bytes prior)
            :not-qualification true}))))
