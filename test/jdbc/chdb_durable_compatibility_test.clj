(ns jdbc.chdb-durable-compatibility-test
  (:require [clojure.data.json :as json]
            [clojure.string :as str]
            [hegel.core :as h]
            [hegel.generator :as g]
            [jdbc.chdb.durable :as durable]
            [jdbc.chdb.durable.compatibility :as compatibility]
            [jdbc.chdb.durable.control :as control]))

(def failures (atom 0))

(def ^:private corpus-path
  "test/fixtures/durable/version-ordering.json")

(defn- check [label expected actual]
  (if (= expected actual)
    (println "  ok  " label)
    (do
      (swap! failures inc)
      (println "  FAIL" label "- expected" (pr-str expected)
               "got" (pr-str actual)))))

(defn- version-lt [left right]
  (when-some [comparison
              (compatibility/compare-release-versions left right)]
    (neg? comparison)))

(defn- parse-shape [value]
  (when-let [parts (#'compatibility/version-parts value)]
    [(:release parts)
     (:prerelease-rank parts)
     (:prerelease-number parts)]))

(defn- error-type [f]
  (try (f) nil (catch Throwable error (:type (ex-data error)))))

(defn- case-for [cases left right]
  (first (filter #(and (= left (get % "left"))
                       (= right (get % "right"))) cases)))

(defn- lexical-rc-mutant [left right]
  (if (and (str/includes? left "-rc.") (str/includes? right "-rc."))
    (neg? (compare left right))
    (version-lt left right)))

(defn- prerelease-after-release-mutant [left right]
  (if (and (= left "26.7.2-rc.99") (= right "26.7.2"))
    false
    (version-lt left right)))

(defn- unrecognized-is-prerelease-mutant [left right]
  (version-lt (str/replace left "-stable" "-alpha")
              (str/replace right "-stable" "-alpha")))

(defn- fixed-three-part-mutant [left right]
  (if (and (re-matches #"[0-9]+\.[0-9]+\.[0-9]+" left)
           (re-matches #"[0-9]+\.[0-9]+\.[0-9]+" right))
    (version-lt left right)
    nil))

(defn- assert-mutant-killed! [cases label mutant left right]
  (let [case (case-for cases left right)]
    (check label true
           (and (some? case)
                (not= (get case "expected") (mutant left right))))))

(defn- version-text [release]
  (str/join "." release))

(defn- property-fail! [origin expected actual values]
  (when-not (= expected actual)
    (throw (ex-info "version ordering property failed"
                    {:hegel/origin origin
                     :expected expected :actual actual
                     :values values}))))

(defn- run-ordering-property! []
  (let [result
        (h/run-test!
         {:name "chdb/durable-version-ordering"
          :database ""
          :derandomize? true
          :verbosity :quiet
          :test-cases 80}
         (fn [_]
           (g/let [release (g/vector {:min-size 1 :max-size 6}
                                     (g/integer 0 1000))
                   rc-left (g/integer 0 1000)
                   rc-right (g/integer 0 1000)]
             (let [base (version-text release)
                   padded (str base ".0")
                   left (str base "-rc." rc-left)
                   right (str base "-rc." rc-right)]
               (property-fail! "chdb/durable-version/trailing-zero"
                               0
                               (compatibility/compare-release-versions
                                base padded)
                               [base padded])
               (property-fail! "chdb/durable-version/numeric-rc"
                               (< rc-left rc-right)
                               (version-lt left right)
                               [left right])
               (property-fail! "chdb/durable-version/release-after-rc"
                               true
                               (version-lt left base)
                               [left base])))))]
    (println "  hegel version-ordering seed" (:seed result)
             "valid" (:valid-test-cases result))
    (when-not (and (:passed? result) (not (:flaky? result)))
      (swap! failures inc)
      (println "  FAIL version-ordering property" (pr-str result)))))

(defn run-checks! []
  (reset! failures 0)
  (println "Durable upstream engine-version ordering")
  (let [corpus (json/read-str (slurp corpus-path))]
    (check "golden corpus pins the accepted upstream source"
           ["https://github.com/chdb-io/chdb"
            "66643e5030fb73c30ac5cdd31d4c7858ea040ed0"
            "chdb/durable/protocol.py"
            "1826ec56800607418764c3c03dd95633560ca78e4e43ae56f8d52ff080a5c915"]
           (mapv #(get-in corpus ["provenance" %])
                 ["repository" "commit" "path" "sha256"]))
    (doseq [{input "input" expected "expected"} (get corpus "parse_cases")]
      (check (str "upstream parse " (pr-str input))
             expected
             (parse-shape input)))
    (doseq [{left "left" right "right" :as case}
            (get corpus "compare_cases")]
      (check (str "upstream ordering " (pr-str left) " < " (pr-str right))
             (get case "expected")
             (version-lt left right)))
    (let [cases (get corpus "compare_cases")]
      (assert-mutant-killed!
       cases "golden oracle kills lexical rc ordering"
       lexical-rc-mutant "26.7.2-rc.2" "26.7.2-rc.10")
      (assert-mutant-killed!
       cases "golden oracle kills prerelease-after-release ordering"
       prerelease-after-release-mutant "26.7.2-rc.99" "26.7.2")
      (assert-mutant-killed!
       cases "golden oracle kills unrecognized-prerelease ordering"
       unrecognized-is-prerelease-mutant "26.7.2-stable" "26.7.2")
      (assert-mutant-killed!
       cases "golden oracle kills fixed-three-part parsing"
       fixed-three-part-mutant "26.7" "26.7.0"))
    (check "malformed running release fails the public gate closed"
           ::durable/engine-incompatible
           (error-type #(durable/compare-release-versions "v26.7.2" "26.7.2")))
    (check "malformed minimum reader fails the public gate closed"
           ::durable/engine-incompatible
           (error-type #(durable/compare-release-versions "26.7.2" "bad")))
    (let [current {"engine" {"version" "26.7.2"
                             "backup_format" 1
                             "min_reader" "26.7"}}]
      (check "equivalent padded minimum-reader floor is not lowered"
             "26.7.0"
             (get (#'control/checkpoint-engine
                   current {:version "26.7.3" :backup-format 1
                            :min-reader "26.7.0"})
                  "min_reader"))
      (check "numeric rc regression lowers the minimum-reader floor"
             ::control/invalid-options
             (error-type
              #(#'control/checkpoint-engine
                 (assoc-in current ["engine" "min_reader"] "26.7.2-rc.10")
                 {:version "26.7.3" :backup-format 1
                  :min-reader "26.7.2-rc.2"})))
      (check "malformed next minimum reader fails closed before publication"
             ::control/invalid-options
             (error-type
              #(#'control/checkpoint-engine
                 current {:version "26.7.3" :backup-format 1
                          :min-reader "v26.7.3"})))))
  (run-ordering-property!)
  (when-not (zero? @failures)
    (throw (ex-info (str @failures " Durable compatibility checks failed")
                    {:failures @failures})))
  (println "all Durable compatibility checks passed")
  true)

(defn -main [& _]
  (run-checks!))
