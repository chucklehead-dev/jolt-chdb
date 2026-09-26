(ns chez-json.fixture
  "One varying telemetry input and an independent JSON edge corpus."
  (:require [clojure.data.json :as json]
            [clojure.string :as str]
            [jdbc.chdb-durable-log-fixture :as logs]))

(def row-counts [512 1024 5000 10000])
(def minimal-options
  {:escape-unicode false :escape-slash false :escape-js-separators false})
(def full-hash-collision-groups
  [["Aa" "BB"] ["AaAa" "BBBB" "AaBB" "BBAa"]])

(defn- full-hash-collision-map [collision-keys]
  (into (into {} (map (fn [i] [(str "padding-" i) i]) (range 100)))
        (map-indexed (fn [i key] [key (+ 1000 i)]) collision-keys)))

(defn finite-float-cases []
  ;; A fixed 128-value binary exponent/mantissa/sign grid, plus signed zero
  ;; and explicit smallest-subnormal controls. Never filter non-finite output:
  ;; parity! asserts that every generated value is finite before encoding it.
  (into [{:label "float-grid/positive-zero" :value 0.0}
         {:label "float-grid/negative-zero" :value -0.0}
         {:label "float-grid/positive-smallest-subnormal" :value 4.9e-324}
         {:label "float-grid/negative-smallest-subnormal" :value -4.9e-324}]
        (for [sign [1.0 -1.0]
              exponent [-1074 -1073 -1070 -1022 -1021 -100 -24 -10
                        -1 0 1 10 24 100 1022 1023]
              mantissa [1.0 1.125 1.5 1.9999999999999998]]
          {:label (str "float-grid/" sign "/" exponent "/" mantissa)
           :value (* sign mantissa (Math/pow 2.0 exponent))})))

(defn rows [n]
  ;; The production benchmark's varying IDs, timestamp, severity, route and
  ;; attributes; these are materialized before either encoder is timed.
  (mapv #(logs/log-row % false) (range n)))

(defn corpus []
  (into
  [{:label "empty-object" :value (array-map)}
   {:label "empty-array" :value []}
   {:label "null-and-booleans" :value [nil true false]}
   {:label "finite-numbers"
    :value [0 -1 42 9007199254740993 9223372036854775807
            0.0 -0.0 1.25 -2.5 1.0e-7 1.0e20]}
   {:label "float-format-boundaries"
    :value [1.0e-3 1.0e-4 1.0e6 1.0e7 1.2345678901234567
            2.2250738585072014e-308 4.9e-324 1.7976931348623157e308]}
   {:label "all-control-characters"
    :value (apply str (map char (range 32)))}
   {:label "escaping-and-unicode"
    :value "slash/ quote\" backslash\\ DEL\u007f é λ 中 😀 \u2028 \u2029"}
   {:label "nested-maps-vectors"
    :value (array-map "z" [nil true false (array-map "b" [1 2 3] "a" "é")]
                      "a" (array-map "empty" [] "object" (array-map)))}
   {:label "keyword-keys-and-values"
    :value (array-map :example/name :example/value "ordinary" "value")}
   {:label "hash-map-named-key-collisions"
    ;; data.json drops keyword namespaces. For a HAMT-sized map these three
    ;; distinct Clojure keys produce one JSON property name; preserve the
    ;; reference's property order so its final decoded value does not change.
    :value (assoc (into {} (map (fn [i] [(str "padding-" i) i]) (range 12)))
                  "x" 101 :a/x 202 :b/x 303)}
   {:label "hash-map-two-full-hash-collisions"
    :value (full-hash-collision-map (first full-hash-collision-groups))}
   {:label "hash-map-four-full-hash-collisions"
    :value (full-hash-collision-map (second full-hash-collision-groups))}
   {:label "ordered-object-a" :value (array-map "z" 1 "a" 2 "m" 3)}
   {:label "ordered-object-b" :value (array-map "m" 3 "a" 2 "z" 1)}
   {:label "minimal-escaping"
    :value ["/é😀\u2028\u2029" (apply str (map char (range 32)))]
    :options minimal-options}
   {:label "raw-unicode-escaped-js-separators"
    :value "/é😀\u2028\u2029"
    :options {:escape-unicode false :escape-slash false
              :escape-js-separators true}}
   {:label "escaped-unicode-raw-js-separators"
    :value "é😀\u2028\u2029"
    :options {:escape-unicode true :escape-js-separators false}}]
  (finite-float-cases)))

(defn write-json [value options]
  (apply json/write-str value (mapcat identity options)))

(defn payload [input options]
  (apply str (map #(str (write-json % options) "\n") input)))

(defn- scheme-string [s]
  (str "\""
       (apply str
              (map (fn [c]
                     (let [n (int c)]
                       (cond
                         (= n 34) "\\\""
                         (= n 92) "\\\\"
                         (or (< n 32) (> n 126))
                         (str "\\x" (Integer/toHexString n) ";")
                         :else (str c))))
                   s))
       "\""))

(declare scheme-value)

(defn scheme-value [value]
  (cond
    (nil? value) "'null"
    (true? value) "#t"
    (false? value) "#f"
    (string? value) (scheme-string value)
    (keyword? value) (str "(make-chez-json-keyword "
                          (scheme-string (subs (str value) 1)) ")")
    (number? value) (str value)
    (map? value)
    (str "(make-chez-json-object (list "
         (str/join " " (map (fn [[k v]]
                             (str "(cons " (scheme-value k) " "
                                  (scheme-value v) ")")) value)) "))")
    (sequential? value)
    (str "(vector " (str/join " " (map scheme-value value)) ")")
    :else (throw (ex-info "Unsupported fixture value" {:value value}))))

(defn- scheme-options [options]
  (str "(list "
       (str/join " " (map (fn [[k v]]
                            (str "(cons '" (name k) " "
                                 (if v "#t" "#f") ")")) options)) ")"))

(defn export-native! [directory]
  ;; This export includes no pre-encoded input for the candidate. Expected
  ;; JSON is oracle-only. Native representation construction is intentionally
  ;; outside native-ceiling timings; the Jolt bridge measures actual rows.
  (.mkdirs (java.io.File. directory))
  (let [input (rows 10000)
        edge-cases (corpus)]
    (spit (str directory "/fixture.ss")
          (str ";; Generated from the production varying log fixture.\n"
               "(define parity-cases (list\n"
               (str/join "\n"
                         (map (fn [{:keys [label value options]}]
                                (str "(list " (scheme-string label) " "
                                     (scheme-value value) " "
                                     (scheme-options options) " "
                                     (scheme-string (write-json value options)) ")"))
                              edge-cases))
               "))\n(define telemetry-rows " (scheme-value input) ")\n"))
    (doseq [n row-counts]
      (spit (str directory "/expected-" n ".jsonl")
            (payload (subvec input 0 n) {})))
    {:rows (count input) :parity-cases (count edge-cases)
     :fixture-source "bench/jdbc/chdb_durable_log_fixture.cljc"
     :native-input :preconverted-only-for-native-ceiling
     :data-json-sha "1b0716268232a79dd2b2fdb968cca171414bd589"}))
