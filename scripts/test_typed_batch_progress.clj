(require '[clojure.test :refer [deftest is run-tests]]
         '[typed-storage-batch-progress :as progress])

(deftest bounded-progress-does-not-observe-payloads
  (let [receipts (atom []) arguments (atom []) secret (Object.)
        result (progress/observe!
                 (fn [& args] (swap! arguments conj args) true)
                 104 #(swap! receipts conj %)
                 (fn [export!]
                   (dotimes [_ 104] (export! secret))
                   ;; This boundary is observable BEFORE owner cleanup/return.
                   (is (= :all-exports-returned (:observation (last @receipts))))
                   (is (= {:started 104 :returned 104 :confirmed 104}
                          (:counts (last @receipts))))
                   :original-result))]
    (is (= :original-result result))
    (is (= 104 (count @arguments)))
    (is (every? #(identical? secret (first %)) @arguments))
    (is (= 16 (count @receipts)))
    (is (= {:started 104 :returned 104 :confirmed 104} (:counts (last @receipts))))
    (is (= :invocation-returned (:observation (last @receipts))))
    (is (every? #(= #{:scope :diagnostic-only :expected-exports :counts :observation :elapsed-ms}
                     (set (keys %))) @receipts))
    (is (every? #(and (true? (:diagnostic-only %))
                     (integer? (:elapsed-ms %)) (<= 0 (:elapsed-ms %))) @receipts))))

(deftest partial-failure-and-nontrue-return-are-not-confirmation
  (let [receipts (atom []) error (ex-info "PRIVATE-CANARY" {:secret "PRIVATE-CANARY"})
        returns (atom [true false nil error])
        caught (try
                 (progress/observe!
                   (fn [] (let [value (first @returns)]
                            (swap! returns rest)
                            (if (instance? Throwable value) (throw value) value)))
                   104 #(swap! receipts conj %)
                   (fn [export!] (dotimes [_ 4] (export!))))
                 nil (catch Throwable e e))]
    (is (identical? error caught))
    (is (= {:started 4 :returned 3 :confirmed 1} (:counts (last @receipts))))
    (is (= :invocation-failed (:observation (last @receipts))))
    (is (not (.contains (pr-str @receipts) "PRIVATE-CANARY")))))

(deftest diagnostics-cannot-replace-results-or-errors
  (let [emit! (fn [_] (throw (ex-info "diagnostic write failed" {})))
        error (ex-info "primary" {})]
    (is (= :original (progress/observe! (fn [] true) 104 emit!
                                       (fn [export!] (export!) :original))))
    (is (identical? error
                   (try (progress/observe! (fn [] (throw error)) 104 emit!
                                           (fn [export!] (export!)))
                        (catch Throwable e e))))))

(let [result (run-tests)]
  (System/exit (if (zero? (+ (:fail result) (:error result))) 0 1)))
