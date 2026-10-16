(ns typed-storage-confirmation-receipt)

(defn mode [arm phase]
  (when-not (and (contains? #{"baseline" "candidate"} arm)
                (contains? #{"window" "reader"} phase))
    (throw (ex-info "Invalid confirmation comparison arm" {})))
  (if (and (= arm "candidate") (= phase "window"))
    :sha256-single-put :readback))

(defn initial-counts []
  {:file-publications 0 :completed-publications 0 :publication-errors 0
   :put-completions 0 :verification-completions 0 :valid-proof-observations 0})

(defn validate! [report arm phase]
  (let [selected (mode arm phase)
        counts (select-keys report (keys (initial-counts)))
        n (:file-publications counts)]
    (assert (= (into #{:scope :mode :arm :phase :scope-returned}
                    (keys (initial-counts))) (set (keys report))))
    (assert (= :wal-upload-confirmation-selection (:scope report)))
    (assert (= [selected arm phase true]
               [(:mode report) (:arm report) (:phase report) (:scope-returned report)]))
    (assert (every? #(and (integer? %) (<= 0 %)) (vals counts)))
    (assert (zero? (:publication-errors counts)))
    (if (= phase "reader")
      (assert (every? zero? (vals counts)))
      (do
        (assert (pos? n))
        (assert (= n (:completed-publications counts) (:put-completions counts)))
        (if (= selected :sha256-single-put)
          (do (assert (>= (:valid-proof-observations counts) n) "confirmation-proof-missing")
              (assert (zero? (:verification-completions counts)) "confirmation-unexpected-readback"))
          (do (assert (zero? (:valid-proof-observations counts)))
              (assert (= n (:verification-completions counts)))))))
    report))
