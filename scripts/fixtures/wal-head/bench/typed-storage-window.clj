;; Loopback composition fixture only: metadata identities, NOT typed workload.
(require '[jdbc.chdb-typed-storage-support :as storage]
         '[jdbc.chdb.durable.backend :as backend]
         '[jdbc.chdb.durable.control :as control]
         '[jdbc.chdb.durable.s3-curl :as curl])
(let [[phase root] *command-line-args*
      arm (System/getenv "SDK_WAL_HEAD_ARM")
      options (storage/options root (if (= phase "window") "writer" "reader"))
      _ (assert (= (str "typed-compact-prefix-wal-head-" arm) (:object-id options)))
      namespace-backend (or (:namespace-backend options)
                            (curl/s3-backend (storage/s3-options)))
      store (backend/object-backend namespace-backend (:object-id options))
      owner (str "owner-" arm)]
  (if (= phase "window")
    (control/acquire! store
                      {:owner owner :instance "fixture" :expires-at 200M :now 100M
                       :clock-skew 5M :database "default" :engine-version "26.7.3"
                       :backup-format 1 :min-reader "26.7.3"})
    (assert (= owner (get-in (control/read-head-read-only! store) [:head "lease" "owner"]))))
  (prn {:scope :loopback-head-identity-not-workload :arm arm :phase phase :passed true}))
