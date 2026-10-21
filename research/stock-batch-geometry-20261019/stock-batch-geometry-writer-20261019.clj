;; Research geometry only: current production codec/byte/ingestion paths stay
;; unchanged. Keep total queue capacity fixed across sizes and lane counts.
(require '[clojure.string :as str] '[clojure.edn :as edn])
(load-file "/home/chuck/ai-src/evidence/nested-sorted-sdk-fixture-20261018.clj")
(defn replace-once [source needle replacement]
  (assert (= 2 (count (str/split source (re-pattern (java.util.regex.Pattern/quote needle)) -1))))
  (str/replace source needle replacement))
(let [[root batch-text lanes-text] *command-line-args*
      batch (parse-long batch-text) lanes (parse-long lanes-text)
      _ (assert (contains? #{5000 10000} batch))
      _ (assert (contains? #{1 2} lanes))
      source (slurp "/home/chuck/ai-src/evidence/rich-sdk-scaling-screen-20261019.clj")
      changes [["'[clojure.data.json.jolt-native :as json-native]" ""]
               [":operations {:json-reader (json-native/load-reader!)}" ":operations {}"]
               [":owned-statement-output? true" ":owned-statement-output? true :experimental-span-codec? true"]
               ["lanes (parse-long lanes-text) n 5000 rounds 32" (str "lanes (parse-long lanes-text) n " batch " rounds 32")]
               ["queue-capacity (quot (* 4 n) lanes)" "queue-capacity (quot 20000 lanes)"]
               ["(= 625 (count trace-groups))" "(= (quot n 8) (count trace-groups))"]]
      staged (reduce (fn [text [needle replacement]] (replace-once text needle replacement)) source changes)
      original fixture/spans]
  (spit (str root "/staged-geometry-writer.clj") staged)
  (with-redefs [fixture/spans (fn [region] (nested-sorted-sdk-shape (original region)))]
    (with-bindings {#'*command-line-args* [root lanes-text "saturated"]} (load-string staged)))
  (let [path (str root "/writer.edn") data (edn/read-string (slurp path))]
    (assert (= [batch lanes 20000] (mapv data [:batch-size :lanes :total-queue-capacity])))
    (spit path (pr-str (assoc data :experimental-codec-arm "on"
                            :candidate-product-source true :research-harness-overlay true
                            :stock-runtime-gate true :research-selector :batch-geometry-only)))))
