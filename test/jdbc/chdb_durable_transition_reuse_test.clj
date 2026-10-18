(ns jdbc.chdb-durable-transition-reuse-test
  (:require [clojure.test :refer [deftest is run-tests]]
            [jdbc.chdb.durable.control :as control]
            [jdbc.chdb-durable-witness-head-test :as witness]))

(defn- counting-transitions [f]
  (let [original @#'control/reference-transition calls (atom 0)]
    (with-redefs-fn (hash-map #'control/reference-transition
                             (fn [& args] (swap! calls inc) (apply original args)))
      #(let [result (f)] {:result result :calls @calls}))))

(deftest verified-transition-is-built-once-without-skipping-cas
  (doseq [witnessed? [true false]]
    (let [f (witness/fixture :normal)
          {:keys [result calls]} (counting-transitions #(witness/commit! f witnessed?))]
      (is (= :committed (:status result)))
      (is (= (if witnessed? 1 2) calls))
      (is (= 1 @(:replacements f)))
      (is (= (if witnessed? 0 1) @(:verify-calls f)))
      (is (= [(:reference (:publication f))]
             (get-in (:head result) ["manifest" "wal"]))))))

(deftest renewal-conflict-rebuilds-from-the-new-snapshot
  (let [f (witness/fixture :renew)
        {:keys [result calls]} (counting-transitions #(witness/commit! f true))]
    (is (= :committed (:status result)))
    (is (= 2 calls))
    (is (= 2 @(:replacements f)))
    (is (= [300 1] [(get-in (:head result) ["lease" "expires_at"])
                    (get-in (:head result) ["manifest" "seq"])]))))

(deftest takeover-conflict-fences-before-another-transition
  (let [f (witness/fixture :takeover)
        {:keys [result calls]}
        (counting-transitions
          #(try (witness/commit! f true)
                (catch Throwable e (:type (ex-data e)))))]
    (is (= ::control/lease-fenced result))
    (is (= 1 calls))
    (is (= 1 @(:replacements f)))
    (let [document (:head (control/read-head! (:delegate f)))]
      (is (= 0 (get-in document ["manifest" "seq"]))))))

(deftest ambiguous-outcome-never-rebuilds-or-reissues-cas
  (doseq [mode [:ambiguous-land :ambiguous-drop]]
    (let [f (witness/fixture mode)
          {:keys [result calls]}
          (counting-transitions
            #(try (witness/commit! f true)
                  (catch Throwable e (:type (ex-data e)))))]
      (is (= 1 calls))
      (is (= 1 @(:replacements f)))
      (is (= (if (= mode :ambiguous-land) :reconciled ::control/commit-ambiguous)
             (if (map? result) (:status result) result))))))

(deftest direct-verifier-retains-fresh-transition-after-io
  (let [f (witness/fixture :normal)
        {:keys [result calls]}
        (counting-transitions
          #(control/commit-reference!
             (:store f) (:token f)
             {:kind :wal :reference (:reference (:publication f))
              :verify-reference!
              (fn [store reference]
                (control/renew! (:delegate f) (:token f) 300M)
                (control/verify-byte-reference! store reference))}))]
    (is (= :committed (:status result)))
    (is (= 2 calls))
    (is (= 300 (get-in (:head result) ["lease" "expires_at"])))
    (is (= 1 @(:replacements f)))))

(defn run-checks! []
  (let [result (run-tests 'jdbc.chdb-durable-transition-reuse-test)]
    (when-not (zero? (+ (:fail result) (:error result)))
      (throw (ex-info "WAL transition reuse controls failed" result)))
    true))

(defn -main [& _] (run-checks!))
