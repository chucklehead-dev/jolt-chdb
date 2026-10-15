(ns typed-storage-curl-profile-receipt)

(def ^:private classes
  (into #{[:get :none]} (for [kind [:bytes :file] size [:small :medium :large]]
                         [:put kind size])))
(def ^:private required-fields
  #{:requests :request-failures :request-wall-nanos :performs-started
    :performs-returned :perform-wall-nanos :samples :perform-failures
    :curl-total-seconds :dns-complete-seconds :connect-complete-seconds
    :pretransfer-complete-seconds :first-byte-seconds :tls-complete-seconds
    :new-connections})

(defn validate! [report budget]
  (assert (= #{:scope :complete :milestones-are-cumulative :classes} (set (keys report))))
  (assert (= :aggregate-native-transfer-diagnostic (:scope report)))
  (assert (and (true? (:complete report)) (true? (:milestones-are-cumulative report))))
  (assert (seq (:classes report)))
  (doseq [[class metrics] (:classes report)]
    (assert (contains? classes class))
    (assert (every? #(contains? metrics %) required-fields))
    (assert (every? #(contains? (into required-fields [:sample-errors :perform-throws]) %) (keys metrics)))
    (assert (every? #(and (number? %) (<= 0 %) (< % Double/POSITIVE_INFINITY)) (vals metrics)))
    (assert (= (:requests metrics) (:performs-started metrics) (:performs-returned metrics) (:samples metrics)))
    (assert (every? zero? (map #(get metrics % 0) [:sample-errors :perform-throws :request-failures :perform-failures])))
    (assert (<= (:perform-wall-nanos metrics) (:request-wall-nanos metrics))))
  (assert (true? (:scope-returned budget)))
  (assert (= (:requests budget) (:completed budget)
             (reduce + (map :requests (vals (:classes report)))))))
