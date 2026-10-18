(ns typed-storage-batch-progress)

(defn observe!
  "Diagnostic only: no arguments, results or errors enter the receipt. Emission
  failure must not replace the operation's result or primary exception. Counts
  describe public export returns, not transport settlement or native persistence."
  [export! expected emit! run!]
  ;; One non-mutating admission witness, three warmups and 100 measured calls.
  (assert (= 104 expected))
  (let [start (System/nanoTime)
        counts (atom {:started 0 :returned 0 :confirmed 0})
        bump! #(swap! counts update % (fn [n] (min 105 (inc n))))
        record! (fn [observation]
                  (try
                    (emit! {:scope :typed-batch-export-progress :diagnostic-only true
                            :expected-exports expected :counts @counts
                            :observation observation
                            :elapsed-ms (quot (- (System/nanoTime) start) 1000000)})
                    (catch Throwable _ nil)))
        wrapped (fn [& args]
                  (let [n (:started (bump! :started))]
                    ;; At most one small write per eight exports, plus boundaries.
                    (when (= 1 (mod n 8)) (record! :export-started)))
                  (let [result (apply export! args)]
                    (bump! :returned)
                    (when (true? result) (bump! :confirmed))
                    ;; Record this before run! can block in shutdown/cleanup.
                    ;; It includes the admission witness: NOT 104 native writes.
                    (when (= expected (:returned @counts))
                      (record! :all-exports-returned))
                    result))]
    (record! :invocation-running)
    (try
      (let [result (run! wrapped)]
        (record! :invocation-returned)
        result)
      (catch Throwable error
        (record! :invocation-failed)
        (throw error)))))
