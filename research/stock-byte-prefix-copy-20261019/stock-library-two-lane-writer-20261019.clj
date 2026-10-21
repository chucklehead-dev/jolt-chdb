;; Research: two DISTINCT exporter instances, one serial SDK worker per owner,
;; one shared serialized Durable writer. Never concurrently call one exporter.
(require '[clojure.string :as str] '[clojure.edn :as edn])
(let [path "/home/chuck/ai-src/evidence/stock-library-codec-durable-writer-20261019.clj"
      source (slurp path)
      needle "[root \"1\" \"saturated\"]"
      _ (assert (= 2 (count (str/split source (re-pattern (java.util.regex.Pattern/quote needle)) -1))))
      two-lane-source (str/replace source needle "[root \"2\" \"saturated\"]")
      root (first *command-line-args*)
      lanes (or (second *command-line-args*) "2")
      _ (assert (contains? #{"2" "4"} lanes))
      staged (if (= lanes "2") two-lane-source
               (str/replace two-lane-source "[root \"2\" \"saturated\"]" "[root \"4\" \"saturated\"]"))]
  (spit (str root "/two-lane-overlay.clj") staged)
  (load-string staged)
  (let [path (str root "/writer.edn") writer (edn/read-string (slurp path))]
    (assert (= (parse-long lanes) (:lanes writer)))
    (spit path (pr-str (assoc writer :research-selector :distinct-exporter-lanes
                              :not-a-product-default true)))))
