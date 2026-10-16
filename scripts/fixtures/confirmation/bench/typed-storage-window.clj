;; Real publication/provider composition probe, NOT a typed ingestion benchmark.
(require '[jdbc.chdb-typed-storage-support :as storage]
         '[jdbc.chdb.durable.backend :as backend]
         '[jdbc.chdb.durable.control :as control]
         '[jdbc.chdb.durable.s3 :as s3]
         '[jdbc.chdb.durable.s3-curl :as curl]
         '[jdbc.chdb-durable-selected-publication-test :as selected])

(defn exercise! []
  (let [[phase root] *command-line-args*
        options (storage/options root (if (= phase "window") "writer" "reader"))
        object-id (str (:object-id options) "-publication-probe")]
    (curl/with-reused-transport!
      (fn [send!]
        (let [store (backend/object-backend
                      (s3/s3-backend
                        (assoc (storage/s3-options) :max-attempts 1
                          :request! (if (= "true" (System/getenv "TEST_CONFIRMATION_DROP_PROVENANCE"))
                                      (with-meta send! nil) send!)))
                      object-id)]
          (if (= phase "window")
            (selected/with-file
              (fn [path]
                (let [token (:token (control/acquire! store selected/options))
                      publication (control/publish-wal-file-selected! store token (constantly path))]
                  (assert (= :published (:status publication)))
                  (assert (= :committed
                             (:status (control/commit-reference! store token
                                        {:kind :wal :reference (:reference publication)
                                         :verified-publication publication
                                         :verify-reference! (fn [& _] (throw (ex-info "Duplicate verification" {})))})))))))
            (let [head (control/read-head-read-only! store)
                  refs (get-in head [:head "manifest" "wal"])]
              (assert (= 1 (count refs)))
              (assert (control/verify-file-reference! store (first refs)))))))))
  (println :real-publication-probe-passed-not-ingestion-throughput))

;; The fixture alone installs its ephemeral CA. No production transport option,
;; peer/host verification, cloud credentials or persisted telemetry are changed.
(if-let [ca (System/getenv "TEST_CONFIRMATION_CA")]
  (let [original @#'curl/set-string!]
    (with-redefs-fn {#'curl/set-string!
                    (fn [handle option value stage]
                      (when (= stage :url) (original handle 10065 ca :test-ca))
                      (original handle option value stage))}
      exercise!))
  (exercise!))
