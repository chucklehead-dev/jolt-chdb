(ns jdbc.chdb-durable-wal-encoder-test
  (:require [clojure.string :as str]
            [jdbc.chdb.durable.wal :as wal]))

(def failures (atom 0))

(defn- check [label expected actual]
  (if (= expected actual)
    (println (str "  PASS " label))
    (do
      (swap! failures inc)
      (println (str "  FAIL " label " expected=" (pr-str expected)
                    " actual=" (pr-str actual))))))

(defn- ascii-bytes [text]
  ;; The expected corpus is ASCII JSON syntax and explicit JSON escapes, so
  ;; this is a direct octet specification rather than another UTF-8 encoder.
  (when-not (every? #(<= 0 (int %) 127) text)
    (throw (ex-info "expected WAL corpus must be ASCII" {:text text})))
  (byte-array (map #(unchecked-byte (int %)) text)))

(defn- hex4 [value]
  (let [digits "0123456789abcdef"]
    (apply str
           (map (fn [shift]
                  (nth digits (bit-and 15 (bit-shift-right value shift))))
                [12 8 4 0]))))

(defn- independently-escaped-json-string [text]
  ;; This deliberately does not call data.json or any WAL encoder. Jolt strings
  ;; enumerate Unicode scalar values, so astral scalars are converted to their
  ;; JSON UTF-16 surrogate escapes explicitly.
  (apply str
         (map (fn [character]
                (let [value (int character)]
                  (cond
                    (= value 34) "\\\""
                    (= value 47) "\\/"
                    (= value 92) "\\\\"
                    (= value 8) "\\b"
                    (= value 9) "\\t"
                    (= value 10) "\\n"
                    (= value 12) "\\f"
                    (= value 13) "\\r"
                    (<= value 31) (str "\\u" (hex4 value))
                    (<= value 127) (str character)
                    (<= value 65535) (str "\\u" (hex4 value))
                    :else (let [offset (- value 65536)]
                            (str "\\u" (hex4 (+ 55296
                                                   (bit-shift-right offset 10)))
                                 "\\u" (hex4 (+ 56320
                                                   (bit-and offset 1023))))))))
              text)))

(defn- independently-encoded-line [sql]
  (ascii-bytes (str "{\"sql\":\""
                    (independently-escaped-json-string sql)
                    "\"}\n")))

(def ^:private static-corpus
  ;; These fixed JSONL byte strings document the durable on-wire record form
  ;; independently of data.json, StringWriter, or the optional native primitive.
  [["empty" "" "{\"sql\":\"\"}\n"]
   ["ASCII" "SELECT 1" "{\"sql\":\"SELECT 1\"}\n"]
   ["quote slash and backslash"
    "SELECT \"quote\" / slash \\ backslash"
    "{\"sql\":\"SELECT \\\"quote\\\" \\/ slash \\\\ backslash\"}\n"]
   ["all named controls"
    (str "controls" (char 0) (char 8) (char 9) (char 10) (char 12) (char 13))
    "{\"sql\":\"controls\\u0000\\b\\t\\n\\f\\r\"}\n"]
   ["line separators" (str (char 8232) (char 8233))
    "{\"sql\":\"\\u2028\\u2029\"}\n"]
   ["BMP" "β€" "{\"sql\":\"\\u03b2\\u20ac\"}\n"]
   ["astral" "😀" "{\"sql\":\"\\ud83d\\ude00\"}\n"]])

(defn- shared-native-probe-sql []
  ;; Keep the native admission corpus shared with the production selector.
  ;; Expected bytes still come from the independent encoder above.
  (let [probe (ns-resolve 'jdbc.chdb.durable.wal 'native-probe-sql)]
    (when-not probe
      (throw (ex-info "native WAL corpus is unavailable" {})))
    (@probe)))

(defn- bytes [value]
  (vec value))

(defn- native-line-outcome [native-line! sql]
  (try
    [:ok (bytes (native-line! sql))]
    (catch Throwable _ [:unavailable])))

(defn- output-stream-writer-range-observation []
  (let [output (java.io.ByteArrayOutputStream.)
        writer (java.io.OutputStreamWriter. output "UTF-8")]
    (.append writer "abc" 1 2)
    (.flush writer)
    (bytes (.toByteArray output))))

(defn -main [& _]
  (println "Durable V1 WAL byte encoder")
  (let [native-var (ns-resolve 'jdbc.chdb.durable.wal 'native-line-bytes)
        native-line! (when native-var @native-var)
        cases (concat
               (map (fn [[label sql expected-text]]
                      [label sql (ascii-bytes expected-text)])
                    static-corpus)
               (map-indexed (fn [index sql]
                              [(str "generated-" index) sql
                               (independently-encoded-line sql)])
                            (shared-native-probe-sql)))]
    (doseq [[label sql expected] cases]
      (check (str label " independent encoder agrees with fixed JSONL form")
             (bytes expected) (bytes (independently-encoded-line sql)))
      (check (str label " portable StringWriter bytes are exact")
             (bytes expected) (bytes (wal/portable-line-bytes sql)))
      (check (str label " selected WAL bytes are exact")
             (bytes expected) (bytes (wal/line sql)))
      (check (str label " forced managed fallback bytes are exact")
             (bytes expected)
             (bytes (with-redefs [wal/native-line-enabled? (constantly false)]
                      (wal/line sql))))
      (when native-line!
        (let [[status candidate] (native-line-outcome native-line! sql)]
          (when (= :ok status)
            (check (str label " native candidate bytes are exact")
                   (bytes expected) candidate)))))
    (let [native-complete?
          (and native-line!
               (every? (fn [[_ sql expected]]
                         (= [:ok (bytes expected)]
                            (native-line-outcome native-line! sql)))
                       cases))]
      (check "native selection requires complete independent corpus parity"
             (boolean native-complete?) (wal/native-line-enabled?)))
    (let [observed (output-stream-writer-range-observation)]
      (println "  OutputStreamWriter ranged-append observation" (pr-str observed))
      ;; Stock Jolt 0.8.10 emits [97 98 99], not the requested [98]. Do not
      ;; turn a later fixed runtime red: this negative control instead proves
      ;; the managed fallback remains exact when the bad behavior is present.
      (when (not= [98] observed)
        (check "broken OutputStreamWriter range cannot affect managed fallback"
               (bytes (independently-encoded-line "range-negative-control"))
               (bytes (wal/portable-line-bytes "range-negative-control")))))
    (check "shared native corpus retains long pre-escape boundaries"
           [0 1 2 63 64 127 128 1023]
           (mapv (fn [sql]
                   (let [quote-index (.indexOf sql (int 34))]
                     (when (and (not= -1 quote-index)
                                (= "\"/\\β😀" (subs sql quote-index)))
                       quote-index)))
                 (filter #(str/ends-with? % "\"/\\β😀")
                         (shared-native-probe-sql))))
    (when-not (zero? @failures)
      (throw (ex-info (str @failures " Durable WAL encoder checks failed")
                      {:failures @failures}))))
  (println "Durable WAL encoder checks passed"))
