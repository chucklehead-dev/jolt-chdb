(require '[clojure.edn :as edn])
(let [[prefix] *command-line-args*
      [_ arm lanes] (re-find #"-(ordinary|cached)-([24])$" prefix)
      w (edn/read-string (slurp (str prefix ".writer.edn")))]
  (assert (and arm lanes))
  (assert (= arm (:prepared-record-arm w)))
  (assert (= (parse-long lanes) (:lanes w)))
  (assert (pos? (:precompute-call-count w)))
  (assert (= (:precompute-call-count w) (:writer-view-count w) (:stream-calls w)))
  (assert (= (if (= arm "cached") (:writer-view-count w) 0) (:opaque-record-write-count w)))
  (binding [*command-line-args* [prefix lanes]]
    (load-file "/home/chuck/ai-src/evidence/validate-stock-library-lanes-20261019.bb")))
