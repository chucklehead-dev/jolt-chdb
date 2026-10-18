(ns s3-checksum-fault-receipt)
(def case-order [:new-upload :default-readback :existing-readback :injected-lost-ack :changed-source])
(defn validate! [report]
  (let [common #{:case :passed :scope-returned :head-sequence :lease-released}
        ordinary (into common [:publication-status :verification-completions :valid-proof-observations
                               :independent-body-verified])
        rejected (into common [:provider-status :mutated-before-send :object-absent])
        counts (:requests report)
        valid?
        (and (map? report) (map? counts) (vector? (:cases report)) (every? map? (:cases report))
             (= #{:scope :scope-returned :throughput-qualified :native-replay-qualified
                  :cases :request-limit :upload-limit :requests} (set (keys report)))
             (= :s3-checksum-provider-controls (:scope report))
             (true? (:scope-returned report))
             (false? (:throughput-qualified report)) (false? (:native-replay-qualified report))
             (= 128 (:request-limit report)) (= 1048576 (:upload-limit report))
             (= #{:requests :completed :upload-bytes} (set (keys counts)))
             (every? #(and (integer? %) (<= 0 %)) (vals counts))
             (<= 1 (:requests counts) 128) (= (:requests counts) (:completed counts))
             (<= 1 (:upload-bytes counts) 1048576)
             (= case-order (mapv :case (:cases report)))
             (every?
               (fn [c]
                 (and (true? (:passed c)) (true? (:scope-returned c)) (true? (:lease-released c))
                      (if (= :changed-source (:case c))
                        (and (= rejected (set (keys c))) (= 400 (:provider-status c))
                             (= 0 (:head-sequence c)) (true? (:mutated-before-send c))
                             (true? (:object-absent c)))
                        (and (= ordinary (set (keys c))) (= 1 (:head-sequence c))
                             (true? (:independent-body-verified c))
                             (= (case (:case c) :existing-readback :already-published
                                               :injected-lost-ack :reconciled :published)
                                (:publication-status c))
                             (= (if (= :new-upload (:case c)) 0 1) (:verification-completions c))
                             (integer? (:valid-proof-observations c))
                             (<= 0 (:valid-proof-observations c) 128)
                             (case (:case c)
                               :new-upload (pos? (:valid-proof-observations c))
                               (:default-readback :existing-readback) (zero? (:valid-proof-observations c))
                               true))))) (:cases report)))]
    (when-not valid? (throw (ex-info "Invalid checksum control receipt" {:type ::invalid})))
    report))
