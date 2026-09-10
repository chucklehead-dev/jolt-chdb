(ns jdbc.chdb-durable-head-whitespace-test
  (:require [clojure.data.json :as json]
            [clojure.edn :as edn]
            [clojure.string :as str]
            [jdbc.chdb.durable.head :as head]))

(def failures (atom 0))

(defn- check [label expected actual]
  (if (= expected actual)
    (println "  ok  " label)
    (do
      (swap! failures inc)
      (println "  FAIL" label "- expected" (pr-str expected)
               "got" (pr-str actual)))))

(defn- error-type [f]
  (try
    (f)
    nil
    (catch Throwable error (:type (ex-data error)))))

(defn- fixture-bytes [document leading trailing]
  (byte-array (concat leading (.getBytes document "UTF-8") trailing)))

(defn- first-value-only-mutant [input]
  ;; Causal negative control: this is the tempting whitespace fix that scans
  ;; and parses the first value but forgets to reject a second value.
  (let [text (String. input "UTF-8")
        start (#'head/skip-json-whitespace text 0)
        end (#'head/scan-json-value text start)]
    (head/validate! (json/read-str (subs text start end) :bigdec true))))

(defn- padded-document [document byte-count]
  (let [prefix (subs document 0 (dec (count document)))
        field-prefix ",\"independent_padding\":\""
        suffix "\"}"
        fixed (str prefix field-prefix suffix)
        padding (- byte-count (alength (.getBytes fixed "UTF-8")))]
    (when (neg? padding)
      (throw (ex-info "fixture cannot reach requested byte count"
                      {:requested byte-count})))
    (str prefix field-prefix (apply str (repeat padding "x")) suffix)))

(defn run-checks! []
  (reset! failures 0)
  (println "Durable V1 independent JSON whitespace corpus")
  (let [{:keys [source document expected cases]}
        (edn/read-string
         (slurp "test/fixtures/durable/head-json-whitespace.edn"))]
    (check "fixture pins the normative upstream protocol"
           (select-keys head/protocol-source [:repository :commit :document])
           {:repository (:repository source)
            :commit (:commit source)
            :document (:protocol source)})
    (doseq [{:keys [id leading trailing]} cases]
      (check (str (name id) " accepts one raw-byte JSON value")
             expected
             (head/decode (fixture-bytes document leading trailing) :writer)))

    (let [duplicate-owner
          (str/replace-first document "\"owner\":null"
                             "\"owner\":null,\"owner\":\"other\"")]
      (check "UTF-8 BOM remains rejected"
             ::head/corrupt
             (error-type
              #(head/decode (fixture-bytes document [-17 -69 -65] []) :writer)))
      (check "invalid UTF-8 remains rejected"
             ::head/corrupt
             (error-type #(head/decode (byte-array [-61 40]) :writer)))
      (check "duplicate object keys remain rejected"
             ::head/corrupt
             (error-type #(head/decode duplicate-owner :writer)))
      (check "malformed JSON remains rejected"
             ::head/corrupt
             (error-type #(head/decode (subs document 0 (dec (count document)))
                                       :writer)))
      (check "whitespace without a JSON value remains rejected"
             ::head/corrupt
             (error-type #(head/decode (byte-array [32 9 13 10]) :writer)))
      (check "non-RFC Unicode whitespace remains rejected"
             ::head/corrupt
             (error-type
              #(head/decode (fixture-bytes document []
                                           (.getBytes "\u00a0" "UTF-8"))
                            :writer))))

    (let [trailing-whitespace [32 9 13 10]
          bounded-json (padded-document
                        document
                        (- head/max-head-bytes (count trailing-whitespace)))
          exact (fixture-bytes bounded-json [] trailing-whitespace)
          over (fixture-bytes bounded-json [] (conj trailing-whitespace 32))]
      (check "complete stored bytes may exactly reach the 1 MiB limit"
             head/max-head-bytes (alength exact))
      (check "whitespace inside the complete 1 MiB object remains accepted"
             true (map? (head/decode exact :writer)))
      (check "one whitespace byte beyond the complete-object limit is rejected"
             ::head/limit-exceeded (error-type #(head/decode over :writer))))

    (let [two-values (fixture-bytes document []
                                    (concat [32 9 13 10]
                                            (.getBytes "{}" "UTF-8")))]
      (check "first-value-only decoder mutant accepts the forbidden second value"
             expected (first-value-only-mutant two-values))
      (check "production decoder rejects the same second-value bytes"
             ::head/corrupt (error-type #(head/decode two-values :writer)))))
  (when-not (zero? @failures)
    (throw (ex-info (str @failures " Durable JSON whitespace checks failed")
                    {:failures @failures})))
  (println "all Durable JSON whitespace checks passed")
  true)

(defn -main [& _]
  (run-checks!))
