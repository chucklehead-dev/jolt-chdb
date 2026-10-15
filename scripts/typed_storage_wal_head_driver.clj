;; Common benchmark-only object selection for BOTH immutable source arms.
(require '[jdbc.chdb-typed-storage-support :as storage])
(let [parent (.getParent (java.io.File. *file*))]
  (load-file (str parent "/typed_storage_wal_head_profile.clj"))
  (let [wrap-options (ns-resolve 'typed-storage-wal-head-profile 'wrap-options)
        selected (wrap-options storage/options (System/getenv "SDK_WAL_HEAD_ARM"))]
    (with-redefs [storage/options selected]
      (load-file (str parent "/typed_storage_profile_driver.clj")))))
