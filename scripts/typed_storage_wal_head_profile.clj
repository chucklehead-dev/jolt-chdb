(ns typed-storage-wal-head-profile)

(defn object-id [arm]
  (case arm
    "baseline" "typed-compact-prefix-wal-head-baseline"
    "candidate" "typed-compact-prefix-wal-head-candidate"
    (throw (ex-info "Unsupported WAL head comparison arm" {:type ::invalid-arm}))))

(defn wrap-options [original arm]
  (let [selected (object-id arm)]
    (fn [root phase]
      (let [options (original root phase)]
        (when-not (= "typed-compact-prefix" (:object-id options))
          (throw (ex-info "Unexpected benchmark object identity" {:type ::source-drift})))
        (assoc options :object-id selected)))))
