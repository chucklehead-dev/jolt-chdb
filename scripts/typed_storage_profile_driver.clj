;; Trusted, explicitly selected diagnostic entry point around the original
;; metered driver. No production/native transport source or options are changed.
(let [parent (.getParent (java.io.File. *file*))
      root (second *command-line-args*)
      _ (assert (contains? #{"window" "reader"} (first *command-line-args*)))
      counts (atom {})]
  (load-file (str parent "/typed_storage_curl_profile.clj"))
  (let [profile! (ns-resolve 'typed-storage-curl-profile 'with-profile!)
        report! (ns-resolve 'typed-storage-curl-profile 'report!)]
    (assert (and profile! report!))
    (profile! counts #(load-file (str parent "/typed_storage_budget.clj")))
    ;; Only reached after the original benchmark and both owned transports close.
    (spit (str root "/curl-profile.edn") (pr-str (report! counts)))))
