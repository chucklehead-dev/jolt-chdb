(ns jdbc.chdb-durable-head-character-test
  (:require [clojure.test :refer [deftest is run-tests]]
            [clojure.java.io :as io]
            [jdbc.chdb.durable.head :as head]))

(deftest exact-json-character-vocabulary
  (let [whitespace #{\space \tab \newline \return}]
    (doseq [c (concat (map char (range 256)) [\u2000 \u2028 \u3000])]
      (is (= (contains? whitespace c) (#'head/json-whitespace? c))))))

(deftest primitive-boundaries-retain-the-exact-delimiters
  (doseq [token ["123" "-1e+8" "true" "false" "null" "not-json" "😀"]
          delimiter ["" "," "]" "}" " " "\t" "\r" "\n"]]
    (let [text (str token delimiter "tail")]
      (is (= (if (empty? delimiter) (count text) (count token))
             (#'head/scan-json-primitive text 0)))))
  (doseq [c [\u2000 \u2028 \u3000]]
    (let [text (str "12" c "tail")]
      (is (= (count text) (#'head/scan-json-primitive text 0)))))
  (is (= 0 (#'head/scan-json-primitive "" 0))))

(deftest character-classification-uses-direct-cases
  ;; Jolt compiles contains? intrinsically, so redefining that var is not a
  ;; meaningful allocation oracle. Check the selected source's literal sets.
  ;; This does not constrain duplicate detection's per-object `seen` set.
  (let [selected (io/resource "jdbc/chdb/durable/head.clj")
        forms (with-open [r (java.io.PushbackReader. (io/reader selected))]
                (loop [result []]
                  (let [form (read {:eof ::eof} r)]
                    (if (= ::eof form) result (recur (conj result form))))))]
    (println "character classifier source" (str selected))
    (doseq [function-name ['json-whitespace? 'scan-json-primitive]]
      (let [form (first (filter #(and (seq? %) (= 'defn- (first %))
                                     (= function-name (second %))) forms))]
        (is (some? form))
        (is (not-any? set? (tree-seq coll? seq form)))))))

(defn run-checks! []
  (let [result (run-tests 'jdbc.chdb-durable-head-character-test)]
    (when-not (zero? (+ (:fail result) (:error result)))
      (throw (ex-info "Durable head character checks failed" {})))
    true))

(defn -main [& _] (run-checks!))
