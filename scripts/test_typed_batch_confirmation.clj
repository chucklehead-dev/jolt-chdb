(require '[typed-storage-batch-confirmation :as ab]
         '[typed-storage-confirmation-receipt :as selection]
         '[clojure.test :refer [deftest is run-tests]])
(defn report [arm phase]
  (merge (selection/initial-counts)
         {:scope :wal-upload-confirmation-selection :scope-returned true
          :arm arm :phase phase :mode (selection/mode arm phase)}
         (when (= phase "window")
           {:file-publications 33 :completed-publications 33 :put-completions 33
            :verification-completions (if (= arm "candidate") 0 33)
            :valid-proof-observations (if (= arm "candidate") 66 0)})))
(deftest exact-current-workload-and-selection
  (is (= [[1 "baseline"] [2 "candidate"] [3 "candidate"] [4 "baseline"]] ab/schedule))
  (doseq [arm ["baseline" "candidate"] phase ["window" "reader"]]
    (let [r (report arm phase)] (is (= r (ab/selection! r arm phase)))))
  (doseq [bad [(assoc (report "candidate" "window") :valid-proof-observations 0)
               (assoc (report "candidate" "window") :verification-completions 1)
               (assoc (report "candidate" "window") :file-publications 32 :completed-publications 32 :put-completions 32)
               (assoc (report "candidate" "window") :scope-returned false)
               (assoc (report "candidate" "window") :mode :readback)
               (assoc (report "candidate" "window") :publication-errors 1)]]
    (is (thrown? Throwable (ab/selection! bad "candidate" "window"))))
  (is (thrown? Throwable
        (ab/selection! (assoc (report "baseline" "window") :verification-completions 0) "baseline" "window")))
  (is (thrown? Throwable
        (ab/selection! (assoc (report "candidate" "reader") :valid-proof-observations 1) "candidate" "reader"))))
(deftest hundred-sample-selection-count-is-not-the-short-count
  (let [r (assoc (report "candidate" "window") :file-publications 97
                  :completed-publications 97 :put-completions 97 :valid-proof-observations 194)]
    (is (= r (ab/selection! r "candidate" "window" 97)))
    (is (thrown? Throwable (ab/selection! r "candidate" "window")))
    (is (thrown? Throwable (ab/selection! (report "candidate" "window") "candidate" "window" 97)))
    (is (thrown? Throwable (ab/selection! r "baseline" "window" 97)))
    (is (thrown? Throwable (ab/selection! r "candidate" "window" 96)))))
(let [r (run-tests)] (System/exit (if (zero? (+ (:fail r) (:error r))) 0 1)))
