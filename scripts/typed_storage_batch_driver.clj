;; Deliberately separate from the unchanged fixed qualification entry point.
(require '[clojure.java.io :as io])
(load-file (str (.getParent (java.io.File. *file*)) "/typed_storage_batch_lib.clj"))
(let [[phase root size samples prefix shape] *command-line-args*
      experiment (or (System/getenv "TASK_BATCH_EXPERIMENT") "sweep")
      arm (System/getenv "TASK_CONFIRMATION_ARM")
      source (str (System/getenv "BENCH_EXPECT_BENCHMARK_ROOT") "/bench/typed-storage-window.clj")]
  (when-not (and (= 6 (count *command-line-args*))
                (contains? #{"window" "reader"} phase)
                (contains? #{"10000" "20000" "50000"} size)
                (contains? #{"32" "100"} samples) (= "true" prefix) (= "wide16" shape))
    (typed-storage-batch-lib/fail!))
  (case experiment
    "sweep" (when-not (and (= "32" samples) (not (seq arm))) (typed-storage-batch-lib/fail!))
    "confirmation"
    (when-not (and (= "32" samples) (contains? #{"baseline" "candidate"} arm)
                   (if (= "s3" (System/getenv "SDK_STORAGE_ARM"))
                     (= "50000" size)
                     (and (= "10000" size) (= "baseline" arm))))
      (typed-storage-batch-lib/fail!))
    "confirmation-long"
    (when-not (if (= "s3" (System/getenv "SDK_STORAGE_ARM"))
                (= ["50000" "100" "candidate"] [size samples arm])
                (= ["10000" "32" "baseline"] [size samples arm]))
      (typed-storage-batch-lib/fail!))
    (typed-storage-batch-lib/fail!))
  ;; Load the real frozen support namespace first so the fixture's later require
  ;; cannot reload it over this checked, process-local prefix derivative.
  (require 'jdbc.chdb-typed-storage-support)
  (binding [*ns* *ns*]
    (load-string
      (typed-storage-batch-lib/derive-storage-source
        (slurp (str (System/getenv "BENCH_EXPECT_BENCHMARK_ROOT")
                    "/bench/jdbc/chdb_typed_storage_support.clj")))))
  (let [run! #(load-string (typed-storage-batch-lib/derive-source (slurp source)))]
    (if (and (contains? #{"confirmation" "confirmation-long"} experiment)
             (= "s3" (System/getenv "SDK_STORAGE_ARM")))
      (do
        ;; Install AFTER the checked support derivative, so its redefinition
        ;; cannot discard the explicit option. No change to local bookends.
        (require 'typed-storage-confirmation-profile)
        (let [options-var (ns-resolve 'jdbc.chdb-typed-storage-support 's3-options)
              wrap (ns-resolve 'typed-storage-confirmation-profile 'options-wrapper)
              initial (ns-resolve 'typed-storage-confirmation-profile 'initial-counts)
              observe! (ns-resolve 'typed-storage-confirmation-profile 'with-observers!)
              report! (ns-resolve 'typed-storage-confirmation-profile 'report!)
              counts (atom (initial))]
          (with-redefs-fn {options-var (wrap @options-var arm phase)} #(observe! counts run!))
          ;; Full native owner settlement and selection validation precede this
          ;; receipt. Outer metering writes its shared ledger only after return.
          (spit (str root "/confirmation-selection.edn") (pr-str (report! counts arm phase)))))
      (run!))))
