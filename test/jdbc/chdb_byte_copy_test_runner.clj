(ns jdbc.chdb-byte-copy-test-runner
  (:require [clojure.test :as t]
            [jdbc.chdb-byte-copy-test]
            [jdbc.chdb-owned-statement-test :as owned]
            [jdbc.chdb-durable-digest-test :as digest]))

(defn -main [& _]
  (let [result (t/run-tests 'jdbc.chdb-byte-copy-test)]
    (binding [t/*report-counters* (ref t/*initial-report-counters*)]
      (t/test-vars [#'owned/copied-input-is-not-an-alias-and-output-is-opaque
                    #'owned/scoped-native-copy-frees-on-return-and-exception])
      (prn @t/*report-counters*)
      (assert (zero? (+ (:fail @t/*report-counters*) (:error @t/*report-counters*)))))
    (digest/run-checks!)
    (System/exit (if (zero? (+ (:fail result) (:error result))) 0 1))))
