(ns jdbc.chdb-durable-head-traversal-test
  (:require [clojure.test :refer [deftest is run-tests]]
            [jdbc.chdb.durable.head :as head]
            [jdbc.chdb.durable.time-domain :as time-domain]
            [jdbc.chdb-durable-head-test :as fixture]))

(defn legacy-validate [value path schema depth]
  ;; Independent parent traversal: preserve its order and error construction.
  (cond
    (map? value)
    (do
      (when (>= depth head/max-json-depth)
        (@#'head/corrupt! "head.json exceeds the JSON nesting limit" path))
      (doseq [[key child] value]
        (when-not (string? key)
          (@#'head/corrupt! "head.json object keys must be strings" path))
        (let [known? (and (map? schema) (contains? schema key))]
          (legacy-validate child (if known? (conj path key) @#'head/redacted-object-path)
                           (when known? (get schema key)) (inc depth)))))
    (vector? value)
    (do
      (when (>= depth head/max-json-depth)
        (@#'head/corrupt! "head.json exceeds the JSON nesting limit" path))
      (let [element-schema (when (vector? schema) (first schema))]
        (doseq [[index child] (map-indexed vector value)]
          (legacy-validate child (conj path index) element-schema (inc depth)))))
    (integer? value)
    (when-not (@#'head/safe-integer? value)
      (@#'head/corrupt! "head.json integer exceeds the cross-language safe range" path))
    (number? value)
    (when-not (time-domain/finite-number? value)
      (@#'head/corrupt! "head.json number must be finite" path))
    (or (nil? value) (string? value) (boolean? value)) nil
    :else (@#'head/corrupt! "head.json contains a non-JSON value" path))
  value)

(defn outcome [f]
  (try {:value (f)}
       (catch Throwable e {:error (ex-data e) :message (.getMessage e)})))

(deftest ordered-traversal-matches-parent-values-errors-and-wire
  (let [base fixture/valid-head
        deep (reduce (fn [x _] [x]) true (range 65))
        large (into {} (map (fn [n] [(str "field-" n) n]) (range 20)))
        collision (hash-map "Aa" (Object.) "BB" Double/NaN)
        cases [base
               (assoc base "unknown" (array-map "a" [1 nil false ""] "b" {"nested" 2}))
               (assoc base "unknown" large)
               (assoc base "unknown" collision)
               (assoc base "unknown" deep)
               (assoc base "unknown" (array-map "a" Double/NaN "b" (Object.)))
               (assoc base "unknown" (array-map :not-json 1 "b" (Object.)))
               (assoc base "unknown" [1 (Object.) Double/NaN])
               (assoc-in base ["lease" "generation"] 9007199254740992)
               (assoc-in base ["manifest" "wal"] [{} {}])]]
    (doseq [mode [:writer :read-only] document cases]
      (let [expected (with-redefs [head/valid-json-value! legacy-validate]
                       (outcome #(head/validate! document mode)))
            actual (outcome #(head/validate! document mode))]
        (is (= expected actual))
        (when (:value actual)
          (let [expected-bytes (with-redefs [head/valid-json-value! legacy-validate]
                                 (vec (head/encode document)))]
            (is (= expected-bytes (vec (head/encode document))))))))))

(deftest vector-path-avoids-indexed-entry-pair-construction
  (let [calls (atom 0) old map-indexed]
    (with-redefs [clojure.core/map-indexed
                  (fn [& args] (swap! calls inc) (apply old args))]
      (legacy-validate fixture/valid-head [] @#'head/head-json-schema 0)
      (is (pos? @calls))
      (reset! calls 0)
      (@#'head/valid-json-value! fixture/valid-head [] @#'head/head-json-schema 0))
    (is (zero? @calls))))

(defn run-checks! []
  (let [r (run-tests 'jdbc.chdb-durable-head-traversal-test)]
    (when-not (zero? (+ (:fail r) (:error r)))
      (throw (ex-info "Durable head traversal checks failed" {})))
    true))

(defn -main [& _] (run-checks!))
