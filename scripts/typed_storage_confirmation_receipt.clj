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

(def ^:private failure-types
  #{:typed-storage/request-budget
    :jdbc.chdb.durable.s3/authentication :jdbc.chdb.durable.s3/permission
    :jdbc.chdb.durable.s3/throttled :jdbc.chdb.durable.s3/transport
    :jdbc.chdb.durable.s3/provider :jdbc.chdb.durable.s3/timeout
    :jdbc.chdb.durable.s3/invalid-options :jdbc.chdb.durable.s3/invalid-response
    :jdbc.chdb.durable.control/lease-held :jdbc.chdb.durable.control/lease-fenced
    :jdbc.chdb.durable.control/object-unverified
    :jdbc.chdb.durable.control/commit-ambiguous :jdbc.chdb.durable.control/timeout
    :jdbc.chdb.durable.control/backend-contract})

(defn failure-report
  "Closed diagnostic projection, never an exception/message/data dump or proof
  of settlement. Unknown categories collapse to :other; bound cause traversal."
  [error stage counts]
  (when-not (contains? #{:driver :selection-validation} stage)
    (throw (ex-info "Invalid failure diagnostic stage" {})))
  {:scope :wal-confirmation-failure :scope-returned false :stage stage
   :counts (into {} (map (fn [[k _]]
                          (let [n (get counts k)]
                            [k (if (and (integer? n) (<= 0 n 1000000)) n :invalid)]))
                        (initial-counts)))
   :causes
   (loop [e error left 8 result []]
     (if (or (nil? e) (zero? left)) result
       (let [data (ex-data e) type (:type data) status (:status data)]
         (recur (.getCause e) (dec left)
           (conj result
             {:kind (cond (instance? AssertionError e) :assertion
                          (some? data) :exception-info :else :other)
              :type (if (contains? failure-types type) type :other)
              :http-status (if (and (integer? status) (<= 100 status 599)) status nil)})))))})

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
