;; Check the fixed hosted workload and report targets separately from correctness.
(require '[clojure.edn :as edn])
(let [root (first *command-line-args*)
      read! #(edn/read-string (slurp (str root "/" % ".edn")))
      expected-sql "d3afed99dac4a86aa35e34beb7933c1e84f6fdbf8ddc0cb603fc9fe2a6c25b11"]
  (doseq [arm ["local" "s3"]]
    (let [w (read! (str arm "-writer")) r (read! (str arm "-reader"))
          times (:samples-nanos w)
          _ (assert (and (= 256 (count times))
                         (every? #(and (integer? %) (pos? %)) times)))
          ordered (vec (sort times))
          measured (zipmap [:p50-ms :p90-ms :p95-ms :p99-ms]
                           (map (fn [fraction]
                                  (/ (double (nth ordered
                                              (dec (long (Math/ceil (* fraction (count ordered)))))))
                                     1000000.0))
                                [0.50 0.90 0.95 0.99]))
          reported (:latency w)
          ;; Older benchmark envelopes omit p90. Derive it from real samples
          ;; and check every percentile the producer actually supplied.
          _ (doseq [key [:p50-ms :p95-ms :p99-ms]]
              (assert (and (number? (get reported key))
                           (== (get reported key) (get measured key)))))
          _ (when (contains? reported :p90-ms)
              (assert (and (number? (:p90-ms reported))
                           (== (:p90-ms reported) (:p90-ms measured)))))
          latency (merge reported measured)]
      (assert (= :passed (:gate-status w)))
      (assert (= (if (= arm "local") :local-posix-typed-public-export
                     :s3-scoped-typed-public-export) (:scope w)))
      (assert (= [10000 256 3 "wide16" 1 "stream" 259 expected-sql]
                 [(:batch-size w) (:samples w) (:warmups w) (:attribute-shape w)
                  (:export-callers w) (:native-execution-arm w) (:stream-calls w)
                  (:sql-sha256 w)]))
      (assert (= 256 (count (:samples-nanos w))))
      (assert (= "native" (get-in w [:metadata-reader :mode])))
      (assert (= :stock (get-in w [:metadata-reader :startup-reader])))
      (assert (= :stock (get-in w [:metadata-reader :recovery-reader])))
      (assert (>= (get-in w [:checkpoint-renewal-window :renewals]) 2))
      (assert (>= (count (get-in w [:checkpoint-renewal-window :checkpoints])) 2))
      (assert (and (:passed r) (:full-physical-rows-equal r) (:typed-values-status r)
                   (= 10000 (:groups r)) (= 2590000 (:rows r))))
      (assert (every? #(and (number? %) (pos? %))
                      (map latency [:p50-ms :p90-ms :p95-ms :p99-ms])))
      ;; These observations are not confidence bounds or a full app qualification.
      (prn {:storage arm :correctness true :latency latency
            :inverse-p50-rows-per-second (/ 10000000.0 (:p50-ms latency))
            :inverse-p99-rows-per-second (/ 10000000.0 (:p99-ms latency))
            :target-met (and (<= (:p50-ms latency) 400) (<= (:p99-ms latency) 500))})))
  (doseq [[name phase requests bytes] [["s3-writer-budget" "window" 24000 (* 6 1024 1024 1024)]
                                      ["s3-reader-budget" "reader" 1000 0]]]
    (let [b (read! name)]
      (assert (and (:scope-returned b) (= phase (:phase b))
                   (= requests (:request-limit b)) (= bytes (:upload-limit b))
                   (= (:completed b) (:requests b))
                   (<= 1 (:requests b) requests) (<= 0 (:upload-bytes b) bytes))))))
