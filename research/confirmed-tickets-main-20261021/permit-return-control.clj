(ns perf.permit-return-control
  "Negative control only: drop one permit return in the fake-worker fixture."
  (:require [clojure.test :as test]
            [jdbc.chdb.durable.writer :as writer]
            [jdbc.chdb-confirmed-request-test :as tickets]))

(let [original @#'writer/return-admission-permit!
      drop-once? (atom true)]
  (reset! test/counters {:test 0 :pass 0 :fail 0 :error 0})
  (with-redefs [writer/return-admission-permit!
                (fn [& args]
                  (if (compare-and-set! drop-once? true false)
                    nil
                    (apply original args)))]
    (test/test-vars [#'tickets/failed-queue-insertion-returns-permit-and-does-not-admit]))
  (let [counts @test/counters]
    (when-not (and (= 1 (:test counts)) (= 5 (:pass counts)) (= 1 (:fail counts))
                   (zero? (:error counts)) (false? @drop-once?))
      (throw (ex-info "Permit-return mutant did not produce the expected witness" {})))
    (println :permit-return-mutant-detected counts)))
