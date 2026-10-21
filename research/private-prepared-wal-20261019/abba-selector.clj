;; Same two exporters / one serialized writer / 20k total queue / 5k rows.
;; Both arms use candidate production source; only eager private preparation
;; is disabled in ordinary. Check actual writer use, not a feature flag alone.
(require '[jdbc.chdb.owned-statement :as owned]
         '[jdbc.chdb.byte-record :as record] '[clojure.edn :as edn])
(let [[root arm] *command-line-args*
      original owned/precompute-wal writer-view owned/prepared-wal-for-write
      prepared (atom 0) written (atom 0) opaque (atom 0)]
  (assert (contains? #{"ordinary" "cached"} arm))
  (with-redefs [owned/precompute-wal
                (fn [statement]
                  (swap! prepared inc)
                  (if (= arm "ordinary") statement (original statement)))
                owned/prepared-wal-for-write
                (fn [statement]
                  (let [line (writer-view statement)]
                    (swap! written inc)
                    (when (satisfies? record/ByteRecord line) (swap! opaque inc))
                    line))]
    (with-bindings {#'*command-line-args* [root "2"]}
      (load-file "/home/chuck/ai-src/evidence/stock-library-two-lane-writer-20261019.clj")))
  (assert (pos? @prepared))
  (assert (= @prepared @written))
  (assert (= (if (= arm "cached") @written 0) @opaque))
  (let [path (str root "/writer.edn") data (edn/read-string (slurp path))]
    (spit path (pr-str (assoc data :prepared-record-arm arm
                            :precompute-call-count @prepared :writer-view-count @written
                            :opaque-record-write-count @opaque
                            :research-selector :private-precomputed-wal-only)))))
