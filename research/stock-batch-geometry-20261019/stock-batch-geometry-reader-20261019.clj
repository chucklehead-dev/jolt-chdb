;; Independent old-stack physical reader. Geometry comes from CLI, not the
;; candidate writer receipt; retain its full per-field/multiplicity oracle.
(require '[clojure.string :as str])
(load-file "/home/chuck/ai-src/evidence/nested-sorted-sdk-fixture-20261018.clj")
(let [[root batch-text] *command-line-args* batch (parse-long batch-text)
      _ (assert (contains? #{5000 10000} batch))
      original fixture/spans
      changes [["n 5000 rounds 32" (str "n " batch " rounds 32")]
               [":rows 175000 :groups 5000 :trace-count 625"
                ":rows (* n (+ 3 rounds)) :groups n :trace-count (quot n 8)"]
               [":fresh-affinity-reader-green :rows 175000"
                ":fresh-affinity-reader-green :rows (* n (+ 3 rounds))"]]
      staged (reduce (fn [text [needle replacement]]
                       (assert (= 2 (count (str/split text (re-pattern (java.util.regex.Pattern/quote needle)) -1))))
                       (str/replace text needle replacement))
                     (slurp "/home/chuck/ai-src/evidence/nested-sorted-sdk-physical-reader-20261018.clj") changes)]
  (with-redefs [fixture/spans (fn [region] (nested-sorted-sdk-shape (original region)))]
    (with-bindings {#'*command-line-args* [root]} (load-string staged))))
