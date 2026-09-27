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

(defn- caught-error [f]
  (try (f) nil (catch Throwable error error)))

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

(defn- run-byte-validation-checks [document expected]
  ;; Carry forward the short-input/BOM/non-ASCII corpus from the preserved
  ;; byte-comparison experiment, with portable error and final-byte controls.
  (doseq [[label values] [["empty" []] ["one byte" [32]] ["two bytes" [32 32]]
                          ["one-byte BOM prefix" [-17]]
                          ["two-byte BOM prefix" [-17 -69]]]]
    (check (str label " rejects without an array-bounds exception")
           ::head/corrupt
           (error-type #(head/decode (byte-array values) :writer))))
  (doseq [bytes [(byte-array [-17 -69 -65])
                (fixture-bytes document [-17 -69 -65] [])]]
    (let [error (caught-error #(head/decode bytes :writer))]
      (check "complete BOM rejects as corrupt" ::head/corrupt
             (:type (ex-data error)))
      (check "BOM rejection precedes JSON parsing"
             "head.json must not contain a UTF-8 byte-order mark"
             (some-> error .getMessage))))
  (let [unicode-head (assoc expected "future-text" "λ😀")
        unicode-json (json/write-str unicode-head :escape-unicode false)]
    (check "valid multibyte and astral bytes survive validation"
           unicode-head (head/decode (.getBytes unicode-json "UTF-8") :writer))
    (check "string input retains the same Unicode value"
           unicode-head (head/decode unicode-json :writer)))
  (doseq [[label values] [["invalid continuation" [-61 40]]
                          ["truncated three-byte sequence" [-30 -126]]
                          ["truncated four-byte sequence" [-16 -97 -104]]
                          ["overlong sequence" [-64 -81]]]]
    (let [error (caught-error #(head/decode (byte-array values) :writer))]
      (check (str label " is corrupt") ::head/corrupt (:type (ex-data error)))
      (check (str label " rejects before JSON parsing")
             "head.json is not canonical UTF-8" (some-> error .getMessage))))
  (let [sentinel "head-byte-test-private-marker"
        prefix (str (subs document 0 (dec (count document)))
                    ",\"future-text\":\"" sentinel)
        malformed (byte-array (concat (.getBytes prefix "UTF-8") [-61 40]
                                      (.getBytes "\"}" "UTF-8")))
        error (caught-error #(head/decode malformed :writer))]
    (check "malformed bytes inside otherwise valid JSON are corrupt"
           ::head/corrupt (:type (ex-data error)))
    (check "malformed field rejects at the exact-byte guard"
           "head.json is not canonical UTF-8" (some-> error .getMessage))
    (check "UTF-8 diagnostics do not disclose input text"
           false (str/includes? (str error (ex-data error)) sentinel)))
  (let [bytes (fixture-bytes document [] [32])
        changed (aclone bytes)
        last-index (dec (alength bytes))]
    (check "array equality accepts an independent exact copy" true
           (java.util.Arrays/equals ^bytes bytes ^bytes changed))
    (aset-byte changed last-index (byte 9))
    (check "array equality detects only the final byte changing" false
           (java.util.Arrays/equals ^bytes bytes ^bytes changed))
    (check "different valid trailing whitespace still decodes identically"
           expected (head/decode changed :writer))
    (aset-byte changed last-index (byte -1))
    (check "malformed final byte does not bypass the round-trip guard"
           "head.json is not canonical UTF-8"
           (some-> (caught-error #(head/decode changed :writer)) .getMessage)))
  ;; Size rejection must precede both BOM detection and UTF-8 conversion.
  (doseq [bom? [false true]]
    (let [oversize (byte-array (inc head/max-head-bytes))]
      (aset-byte oversize 0 (byte -17))
      (when bom?
        (aset-byte oversize 1 (byte -69))
        (aset-byte oversize 2 (byte -65)))
      (let [error (caught-error #(head/decode oversize :writer))]
        (check "size limit precedes BOM and malformed UTF-8"
               ::head/limit-exceeded (:type (ex-data error)))
        (check "size error keeps its redacted diagnostic"
               "head.json exceeds the 1 MiB limit" (some-> error .getMessage))))))

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

    (run-byte-validation-checks document expected)

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
