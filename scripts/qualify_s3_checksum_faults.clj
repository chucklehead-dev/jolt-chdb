(require '[s3-checksum-fault-lib :as faults]
         '[s3-checksum-fault-config :as config]
         '[s3-checksum-fault-receipt :as proof]
         '[typed-storage-confirmation-receipt :as receipt])
(let [[root] *command-line-args*
      _ (when-not (and (= 1 (count *command-line-args*))
                       (.isDirectory (java.io.File. root))
                       (not (.exists (java.io.File. (str root "/s3-writer-confirmation.edn")))))
          (throw (ex-info "Invalid checksum receipt destination" {})))]
  (try
    (let [options (config/options (into {} (map (fn [k] [k (System/getenv k)])) config/env-names))
          report (proof/validate! (faults/run! options root))]
      ;; Only after all native owners have closed and all checks passed.
      (spit (str root "/s3-writer-confirmation.edn") (pr-str report))
      (println :checksum-provider-controls :passed))
    (catch Throwable error
      (try
        (spit (str root "/driver-failure.edn")
              (pr-str (assoc (dissoc (receipt/failure-report error :driver (receipt/initial-counts)) :counts)
                             :scope :checksum-provider-control-failure
                             :check (let [label (:check (ex-data error))]
                                      (when (contains? faults/check-labels label) label)))))
        (catch Throwable _ nil))
      ;; Raw native errors/messages never reach console or scalar artifacts.
      (println :checksum-provider-controls :failed)
      (System/exit 1))))
