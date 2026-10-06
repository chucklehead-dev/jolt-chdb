(ns jdbc.chdb-durable-manifest-traversal-test
  (:require [clojure.test :refer [deftest is run-tests]]
            [jdbc.chdb.durable.head :as head]))

(defn- reference [kind generation sequence]
  {"key" (str (if (= kind :checkpoint) "checkpoints/" "wal/")
              generation "-" sequence "-aaaaaaaa"
              (if (= kind :checkpoint) ".tar.gz" ".jsonl"))
   "size" 1 "sha256" (apply str (repeat 64 "a"))})

(defn- document [base wal final-sequence]
  {"manifest" {"db" "default" "base" base "wal" wal "seq" final-sequence}})

;; Frozen pre-optimization traversal. Shared primitive checks are unchanged;
;; the independent collection/order logic is the regression oracle.
(defn- legacy-manifest! [head lease-generation]
  (let [path ["manifest"]
        manifest (#'head/object! (#'head/required head "manifest" []) path)
        db (#'head/required manifest "db" path)
        base (#'head/required manifest "base" path)
        wal (#'head/array! (#'head/required manifest "wal" path) (conj path "wal"))
        seq-number (#'head/nonnegative-safe-integer!
                    (#'head/required manifest "seq" path) (conj path "seq"))
        base-parts (when-not (nil? base)
                     (#'head/reference-parts! :checkpoint base (conj path "base")))
        wal-parts (mapv (fn [index reference]
                         (#'head/reference-parts! :wal reference (conj path "wal" index)))
                       (range (count wal)) wal)
        parts (cond-> [] base-parts (conj base-parts) true (into wal-parts))
        sequences (mapv :seq parts)
        expected-seq (if (seq sequences) (peek sequences) 0)]
    (#'head/nonblank-string! db (conj path "db"))
    (when-not (every? true? (map < sequences (rest sequences)))
      (#'head/corrupt! "head.json WAL references are not in strict replay order" (conj path "wal")))
    (when-not (= expected-seq seq-number)
      (#'head/corrupt! "head.json manifest sequence does not name its final reference" (conj path "seq")))
    (when (some #(> (:generation %) lease-generation) parts)
      (#'head/corrupt! "head.json reference generation exceeds the lease generation" path))))

(defn- outcome [validate doc]
  (try (validate doc 3) {:accepted true}
       (catch Throwable error
         {:data (ex-data error) :message (.getMessage error)})))

(deftest bounded-manifest-acceptance-and-error-parity
  (doseq [base-seq [nil 1 5]
          sequences [[] [1] [2 5] [5 2] [2 2] [1 2 5]]
          generation [1 3 4]
          final-sequence [0 1 2 5 6]]
    (let [doc (document (when base-seq (reference :checkpoint generation base-seq))
                        (mapv #(reference :wal generation %) sequences) final-sequence)]
      (is (= (outcome legacy-manifest! doc)
             (outcome #'head/validate-manifest! doc))))))

(deftest eager-reference-errors-retain-precedence-over-summary-errors
  (let [doc (document (reference :checkpoint 4 5)
                      [(reference :wal 4 2) (reference :wal 4 2)] 9)]
    (doseq [mutant [(assoc-in doc ["manifest" "db"] "")
                    (assoc-in doc ["manifest" "wal" 1 "sha256"] "bad")
                    (-> doc (assoc-in ["manifest" "db"] "")
                        (assoc-in ["manifest" "wal" 1 "key"] "bad"))
                    (assoc-in doc ["manifest" "seq"] -1)
                    (assoc-in doc ["manifest" "base" "size"] -1)
                    (assoc-in doc ["manifest" "wal"] {})]]
      (is (= (outcome legacy-manifest! mutant)
             (outcome #'head/validate-manifest! mutant))))
    ;; Explicit independent expectations keep the frozen oracle non-vacuous.
    (is (= ["manifest" "wal" 1 "key"]
           (get-in (outcome #'head/validate-manifest!
                           (assoc-in doc ["manifest" "wal" 1 "key"] "bad")) [:data :path])))
    (is (= ["manifest" "wal"]
           (get-in (outcome #'head/validate-manifest! doc) [:data :path])))))

(defn run-checks! []
  (let [result (run-tests 'jdbc.chdb-durable-manifest-traversal-test)]
    (when-not (zero? (+ (:fail result) (:error result)))
      (throw (ex-info "Manifest traversal regression checks failed" result)))
    true))
