(ns jdbc.chdb-durable-policy-test
  (:require [jdbc.chdb.durable.policy :as policy]))

(def failures (atom 0))

(defn- check [label expected actual]
  (if (= expected actual)
    (println "  ok  " label)
    (do
      (swap! failures inc)
      (println "  FAIL" label "- expected" (pr-str expected)
               "got" (pr-str actual)))))

(defn- rejection [f]
  (try (f) nil (catch Throwable error (ex-data error))))

(def base-analysis
  {:query-class :read-only
   :statement-count 1
   :has-secrets false
   :writes-only-target-database true
   :changes-database-lifecycle false})

(defn- accepted? [authorize analysis]
  (try (authorize analysis) true (catch Throwable _ false)))

(defn- run-exhaustive-boundary! []
  (let [analyses
        (vec
         (for [class [:read-only :mutating :mutating-global :control :unknown]
               count [0 1 2]
               secrets [false true]
               contained [false true]
               lifecycle [false true]]
           {:query-class class
            :statement-count count
            :has-secrets secrets
            :writes-only-target-database contained
            :changes-database-lifecycle lifecycle}))
        expected-query (mapv #(and (= 1 (:statement-count %))
                                   (= :read-only (:query-class %))) analyses)
        expected-execute
        (mapv #(and (= 1 (:statement-count %))
                    (= :mutating (:query-class %))
                    (:writes-only-target-database %)
                    (not (:changes-database-lifecycle %))
                    (not (:has-secrets %))) analyses)
        actual-query (mapv #(accepted? policy/authorize-query! %) analyses)
        actual-execute (mapv #(accepted? policy/authorize-execute! %) analyses)]
    (check "query gate matches all 120 finite class/count/flag states"
           expected-query actual-query)
    (check "query gate admits exactly the eight irrelevant-flag read states"
           8 (count (filter true? actual-query)))
    (check "execute gate matches all 120 finite class/count/flag states"
           expected-execute actual-execute)
    (check "execute gate admits exactly one state" 1
           (count (filter true? actual-execute)))))

(defn run-checks! []
  (reset! failures 0)
  (println "Durable V1 query admission policy")
  (run-exhaustive-boundary!)
  (check "one read-only statement is admitted"
         base-analysis (policy/authorize-query! base-analysis))
  (check "a secret-bearing read remains admissible and must be redacted elsewhere"
         true (:has-secrets
               (policy/authorize-query! (assoc base-analysis :has-secrets true))))
  (doseq [[label analysis reason]
          [["empty query" (assoc base-analysis :statement-count 0) :statement-count]
           ["multi-statement query" (assoc base-analysis :statement-count 2) :statement-count]
           ["mutation through query" (assoc base-analysis :query-class :mutating) :query-class]
           ["global mutation through query" (assoc base-analysis :query-class :mutating-global) :query-class]
           ["control through query" (assoc base-analysis :query-class :control) :query-class]
           ["unknown through query" (assoc base-analysis :query-class :unknown) :query-class]]]
    (let [data (rejection #(policy/authorize-query! analysis))]
      (check (str label " is rejected") ::policy/rejected (:type data))
      (check (str label " identifies the boundary") reason (:reason data))))

  (println "Durable V1 execute admission policy")
  (let [mutation (assoc base-analysis :query-class :mutating)]
    (check "one contained non-secret mutation is admitted"
           mutation (policy/authorize-execute! mutation))
    (doseq [[label analysis reason]
            [["empty execute" (assoc mutation :statement-count 0) :statement-count]
             ["multi-statement execute" (assoc mutation :statement-count 2) :statement-count]
             ["read through execute" (assoc mutation :query-class :read-only) :query-class]
             ["global mutation" (assoc mutation :query-class :mutating-global) :query-class]
             ["control statement" (assoc mutation :query-class :control) :query-class]
             ["unknown statement" (assoc mutation :query-class :unknown) :query-class]
             ["cross-database mutation"
              (assoc mutation :writes-only-target-database false) :target-database]
             ["database lifecycle mutation"
              (assoc mutation :changes-database-lifecycle true) :database-lifecycle]
             ["secret-bearing mutation" (assoc mutation :has-secrets true) :secrets]]]
      (let [data (rejection #(policy/authorize-execute! analysis))]
        (check (str label " is rejected") ::policy/rejected (:type data))
        (check (str label " identifies the boundary") reason (:reason data)))))

  (doseq [[label analysis]
          [["missing analysis" nil]
           ["unknown class value" (assoc base-analysis :query-class :future-class)]
           ["negative statement count" (assoc base-analysis :statement-count -1)]
           ["non-boolean containment" (assoc base-analysis :writes-only-target-database nil)]
           ["missing secret fact" (dissoc base-analysis :has-secrets)]]]
    (doseq [[operation authorize]
            [[:query policy/authorize-query!]
             [:execute policy/authorize-execute!]]]
      (let [data (rejection #(authorize analysis))]
        (check (str label " fails closed for " (name operation))
               ::policy/rejected (:type data))
        (check (str label " is invalid analysis for " (name operation))
               :invalid-analysis (:reason data)))))
  (when-not (zero? @failures)
    (throw (ex-info (str @failures " Durable policy checks failed")
                    {:failures @failures})))
  (println "all Durable policy checks passed")
  true)

(defn -main [& _]
  (run-checks!))
