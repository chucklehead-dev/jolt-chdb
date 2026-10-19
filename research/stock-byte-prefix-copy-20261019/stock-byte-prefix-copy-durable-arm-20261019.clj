;; Same codec and Durable path in both arms. Ordinary selects only the prior
;; FFI prefix copy; prefix leaves the production library operation unchanged.
(require '[jdbc.chdb.byte-copy :as copy] '[jolt.ffi :as ffi] '[clojure.edn :as edn])
(let [[root arm] *command-line-args* original copy/write-prefix! exercised (volatile! false)
      ordinary (fn [pointer bytes n]
                 (vreset! exercised true)
                 (ffi/write-array pointer bytes 0 n))]
  (assert (contains? #{"ordinary" "prefix"} arm))
  (with-redefs [copy/write-prefix! (if (= arm "ordinary") ordinary original)]
    (with-bindings {#'*command-line-args* [root]}
      (load-file "/home/chuck/ai-src/evidence/stock-library-codec-durable-writer-20261019.clj")))
  (assert (= (= arm "ordinary") @exercised))
  (let [path (str root "/writer.edn") writer (edn/read-string (slurp path))]
    (spit path (pr-str (assoc writer :byte-prefix-copy-arm arm
                              :legacy-copy-exercised @exercised
                              :research-selector :byte-prefix-copy-only
                              :candidate-production-method (= arm "prefix"))))))
