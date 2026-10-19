(require '[clojure.edn :as edn] '[clojure.string :as str])
(defn validate! [w r batch lanes]
  (assert (contains? #{5000 10000} batch))
  (assert (contains? #{1 2} lanes))
  (assert (= :batch-geometry-only (:research-selector w)))
  (assert (and (= "on" (:experimental-codec-arm w)) (:stock-runtime-gate w)
               (:candidate-product-source w) (:research-harness-overlay w)))
  ;; Reuse every existing routing, queue, ordering, retirement and readback
  ;; receipt check; change only independently supplied workload geometry.
  (let [source (slurp "/home/chuck/ai-src/evidence/validate-rich-sdk-scaling-20261019.clj")
        changes [["[160000 5000 32 lanes lanes 1 20000 625 8 \"saturated\"]"
                  (pr-str [(* batch 32) batch 32 'lanes 'lanes 1 20000 (quot batch 8) 8 "saturated"])]
                 ["(<= 5000 (:queue-capacity-per-lane w))" (str "(<= " batch " (:queue-capacity-per-lane w))")]
                 ["(assert (= 160000 (reduce +" (str "(assert (= " (* batch 32) " (reduce +")]
                 ["(<= 1 (:rows %) 5000)" (str "(<= 1 (:rows %) " batch ")")]
                 ["(= 5000 (:rows %))" (str "(= " batch " (:rows %))")]
                 ["[175000 5000 625 35]" (pr-str [(* batch 35) batch (quot batch 8) 35])]
                 [":recovered-rows 175000" (str ":recovered-rows " (* batch 35))]]
        staged (reduce (fn [text [needle replacement]]
                         (assert (= 2 (count (str/split text (re-pattern (java.util.regex.Pattern/quote needle)) -1))))
                         (str/replace text needle replacement)) source changes)
        ;; Avoid changing the original verifier or writing fake passing files.
        reader (fn [path]
                 (cond (.endsWith path ".writer.edn") (pr-str w)
                       (.endsWith path ".reader.edn") (pr-str r)
                       :else (throw (ex-info "Unexpected receipt read" {}))))]
    (let [result (edn/read-string
                  (with-out-str
                    (binding [*command-line-args* ["geometry-in-memory" (str lanes)]]
                      (with-redefs [slurp reader] (load-string staged)))))]
      (assert (:verified result))
      (prn (assoc result :source-overlay true :candidate-product-source true
                         :research-selector :batch-geometry-only :batch-size batch)))))
(when (= *file* (System/getProperty "babashka.file"))
  (let [[prefix batch lanes] *command-line-args*]
    (validate! (edn/read-string (slurp (str prefix ".writer.edn")))
               (edn/read-string (slurp (str prefix ".reader.edn")))
               (parse-long batch) (parse-long lanes))))
