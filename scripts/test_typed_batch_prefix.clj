;; Exercise the actual frozen configuration parser, not a separately copied
;; regular expression. Dummy option values only; no transport is opened.
(require '[clojure.test :refer [deftest is run-tests]]
         '[jdbc.chdb-typed-storage-support :as storage])
(load-file (str (.getParent (java.io.File. *file*)) "/typed_storage_batch_lib.clj"))
(def source (slurp (str (first *command-line-args*)
                       "/bench/jdbc/chdb_typed_storage_support.clj")))
(def prefix (atom "ci/jolt-chdb/123-1/typed-window/1"))
(defn options! []
  (with-redefs-fn {#'storage/required-env
                  (fn [name _] (if (= name "JOLT_CHDB_S3_PREFIX") @prefix "dummy"))}
    storage/s3-options))

(deftest actual-frozen-parser-rejects-child-before-and-accepts-only-six-after
  ;; Red reproduction: the shipped frozen parser rejects the sweep's first arm.
  (is (thrown? clojure.lang.ExceptionInfo (options!)))
  (binding [*ns* *ns*]
    (load-string (typed-storage-batch-lib/derive-storage-source source)))
  (doseq [index (range 1 7)]
    (reset! prefix (str "ci/jolt-chdb/123-1/typed-window/" index))
    (is (= @prefix (:prefix (options!)))))
  (doseq [suffix ["" "/0" "/7" "/11" "/1/extra" "/../1" "/01" "/1\n"]]
    (reset! prefix (str "ci/jolt-chdb/123-1/typed-window" suffix))
    (is (thrown? clojure.lang.ExceptionInfo (options!))))
  ;; A later fixture require must retain the checked derivative.
  (require 'jdbc.chdb-typed-storage-support)
  (reset! prefix "ci/jolt-chdb/123-1/typed-window/6")
  (is (= @prefix (:prefix (options!))))
  (is (thrown? clojure.lang.ExceptionInfo
               (typed-storage-batch-lib/derive-storage-source "drift")))
  (is (thrown? clojure.lang.ExceptionInfo
               (typed-storage-batch-lib/derive-storage-source (str source source)))))

(let [result (run-tests)]
  (System/exit (if (zero? (+ (:fail result) (:error result))) 0 1)))
