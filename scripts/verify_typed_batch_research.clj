(require '[clojure.edn :as edn])
(load-file (str (.getParent (java.io.File. *file*)) "/typed_storage_batch_lib.clj"))
(let [[root] *command-line-args*
      read! (fn [folder name] (edn/read-string (slurp (str root "/" folder "/" name ".edn"))))]
  (prn (typed-storage-batch-lib/workload-receipt!
        (read! "local" "local-writer") (read! "local" "local-reader") "local" 10000))
  (loop [entries typed-storage-batch-lib/schedule prior typed-storage-batch-lib/zero-counts]
    (if-let [[index n] (first entries)]
      (let [folder (str "s3-" index)
            writer (read! folder "s3-writer") reader (read! folder "s3-reader")
            after-writer (typed-storage-batch-lib/budget-receipt!
                          prior (read! folder "s3-writer-budget") "window")
            after-reader (typed-storage-batch-lib/budget-receipt!
                          after-writer (read! folder "s3-reader-budget") "reader")]
        (prn (assoc (typed-storage-batch-lib/workload-receipt! writer reader "s3" n)
                    :sequence index
                    :phase-requests (- (:requests after-reader) (:requests prior))
                    :phase-upload-bytes (- (:upload-bytes after-reader) (:upload-bytes prior))))
        (recur (next entries) after-reader))
      (prn {:global-budget-valid true :requests (:requests prior) :upload-bytes (:upload-bytes prior)
            :request-limit typed-storage-batch-lib/request-limit
            :upload-limit typed-storage-batch-lib/upload-limit :not-qualification true})))
  (prn (assoc (typed-storage-batch-lib/workload-receipt!
               (read! "local-after" "local-writer") (read! "local-after" "local-reader") "local" 10000)
              :after-sweep true)))
