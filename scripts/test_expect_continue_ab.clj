;; Feed previously validated scalar fixtures, never credentials or object data.
(require '[clojure.edn :as edn] '[clojure.java.io :as io])
(let [fixture (first *command-line-args*)
      root (.toFile (java.nio.file.Files/createTempDirectory "expect-ab-controls-"
                     (make-array java.nio.file.attribute.FileAttribute 0)))
      verifier (str (.getParent (java.io.File. *file*)) "/verify_expect_continue_ab.clj")
      verify! #(binding [*command-line-args* [(.getPath root)]] (load-file verifier))
      write! (fn [wait phase f]
               (let [file (io/file root (str "expect-" wait) (str "s3-" phase ".edn"))]
                 (spit file (pr-str (f (edn/read-string (slurp file)))))))
      reject! #(assert (try (%) false (catch Throwable _ true)))]
  (try
    (doseq [wait [1000 10]]
      (let [dir (io/file root (str "expect-" wait))]
        (.mkdirs dir)
        (doseq [name ["local-writer.edn" "local-reader.edn" "s3-writer.edn"
                     "s3-reader.edn" "s3-writer-budget.edn" "s3-reader-budget.edn"]]
          (io/copy (io/file fixture name) (io/file dir name)))
        (doseq [phase ["writer" "reader"]]
          (write! wait phase #(assoc % :storage-profile
                                     {:profile :expect-continue-ab :expect-100-timeout-ms wait})))))
    (verify!)
    (write! 10 "reader" #(assoc-in % [:storage-profile :expect-100-timeout-ms] 1000))
    (reject! verify!)
    (write! 10 "reader" #(assoc-in % [:storage-profile :expect-100-timeout-ms] 10))
    (write! 10 "reader" #(assoc % :rows 1))
    (reject! verify!)
    (write! 10 "reader" #(assoc % :rows 2590000))
    (write! 10 "writer-budget" #(assoc % :scope-returned false))
    (reject! verify!)
    (println :four-expect-ab-receipt-controls-passed)
    (finally
      (doseq [file (reverse (file-seq root))] (io/delete-file file)))))
