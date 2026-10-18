;; Deliberately separate from the unchanged fixed qualification entry point.
(require '[clojure.java.io :as io])
(load-file (str (.getParent (java.io.File. *file*)) "/typed_storage_batch_lib.clj"))
(let [[phase _ size samples prefix shape] *command-line-args*
      source (str (System/getenv "BENCH_EXPECT_BENCHMARK_ROOT") "/bench/typed-storage-window.clj")]
  (when-not (and (= 6 (count *command-line-args*))
                (contains? #{"window" "reader"} phase)
                (contains? #{"10000" "20000" "50000"} size)
                (= "32" samples) (= "true" prefix) (= "wide16" shape))
    (typed-storage-batch-lib/fail!))
  ;; Load the real frozen support namespace first so the fixture's later require
  ;; cannot reload it over this checked, process-local prefix derivative.
  (require 'jdbc.chdb-typed-storage-support)
  (binding [*ns* *ns*]
    (load-string
      (typed-storage-batch-lib/derive-storage-source
        (slurp (str (System/getenv "BENCH_EXPECT_BENCHMARK_ROOT")
                    "/bench/jdbc/chdb_typed_storage_support.clj")))))
  (load-string (typed-storage-batch-lib/derive-source (slurp source))))
