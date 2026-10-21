;; Change only the output method: both arms retain the exact codec, private
;; FFI byte-copy helper, source buffers, WAL framing, flush and publication.
(require '[jdbc.chdb.byte-output :as output] '[clojure.edn :as edn])
(let [[root arm] *command-line-args*
      original output/write-prefix! calls (volatile! 0)
      selected (if (= arm "ordinary")
                 (fn [sink bytes n] (.write sink bytes 0 n)) original)
      counted (fn [sink bytes n]
                (vswap! calls inc)
                (selected sink bytes n))]
  (assert (contains? #{"ordinary" "prefix"} arm))
  (with-redefs [output/write-prefix! counted]
    (with-bindings {#'*command-line-args* [root]}
      (load-file "/home/chuck/ai-src/evidence/stock-library-codec-durable-writer-20261019.clj")))
  (assert (pos? @calls))
  (let [path (str root "/writer.edn") writer (edn/read-string (slurp path))]
    (spit path (pr-str (assoc writer :output-prefix-arm arm
                             :output-prefix-calls @calls
                             :research-selector :output-prefix-only
                             :candidate-production-method (= arm "prefix"))))))
