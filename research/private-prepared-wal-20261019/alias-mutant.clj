;; Safe managed-memory negative control. Do not modify a production source or
;; run unsafe-memory mutants. Removing the private-copy boundary must fail the
;; cache-poisoning oracle, despite the SQL snapshot itself remaining unchanged.
(require '[clojure.test :as t] '[jdbc.chdb-prepared-record-test :as tests]
         '[jdbc.chdb.byte-output :as output])
(binding [t/*report-counters* (ref t/*initial-report-counters*)]
  (with-redefs [output/write-private-prefix! output/write-prefix!]
    ((:test (meta #'tests/custom-stream-mutation-and-retention-cannot-poison-private-record))))
  (let [counts @t/*report-counters*]
    (assert (= [2 1 0] (mapv counts [:pass :fail :error])))
    (prn {:safe-alias-mutant-detected true :counts counts})))
