(ns jdbc.chdb-typed-storage-profile
  "Closed benchmark selection; not an application configuration surface.")

(defn select-profile [arm wait]
  (if (nil? wait)
    {:profile :baseline :object-id "typed-compact-prefix"}
    (do
      (when-not (and (= arm :s3) (contains? #{"1000" "10"} wait))
        (throw (ex-info "Invalid typed storage benchmark configuration"
                        {:type ::configuration :field :expect-continue-profile})))
      {:profile :expect-continue-ab
       :object-id (str "typed-compact-prefix-expect-" wait)
       :expect-100-timeout-ms (if (= wait "10") 10 1000)})))
