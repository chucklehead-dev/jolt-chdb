;; Same immutable measured source for both arms; only the explicit backend
;; option differs. Readers remain stock/full-verification in BOTH arms.
(require '[jdbc.chdb-typed-storage-support :as storage])
(let [parent (.getParent (java.io.File. *file*))
      arm (System/getenv "SDK_WAL_HEAD_ARM")
      phase (first *command-line-args*)
      root (second *command-line-args*)]
  (load-file (str parent "/typed_storage_confirmation_profile.clj"))
  (let [options-wrapper (ns-resolve 'typed-storage-confirmation-profile 'options-wrapper)
        observe! (ns-resolve 'typed-storage-confirmation-profile 'with-observers!)
        initial (ns-resolve 'typed-storage-confirmation-profile 'initial-counts)
        report! (ns-resolve 'typed-storage-confirmation-profile 'report!)
        selected (options-wrapper storage/s3-options arm phase)
        counts (atom (initial))]
    (with-redefs [storage/s3-options selected]
      (observe! counts #(load-file (str parent "/typed_storage_wal_head_driver.clj"))))
    ;; Original benchmark and all transports have returned before a receipt
    ;; can claim completion. Validation rejects missing selection or fallback.
    (let [report (report! counts arm phase)]
      (spit (str root "/confirmation-selection.edn") (pr-str report))
      (prn report))))
