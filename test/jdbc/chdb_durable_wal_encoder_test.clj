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
   ["BMP" "β€" "{\"sql\":\"\\u03b2\\u20ac\"}\n"]
   ["astral" "😀" "{\"sql\":\"\\ud83d\\ude00\"}\n"]])

(defn- generated-sqls []
  (let [boundaries [(char 0) (char 1) (char 7) (char 8) (char 9)
                    (char 10) (char 12) (char 13) (char 31) (char 32)
                    (char 34) (char 47) (char 92) (char 126) (char 127)
                    (char 128) (char 255) (char 2047) (char 2048)
                    (char 55295) (char 57344) (char 65535)]]
    (concat
     [(apply str boundaries) "β€😀" "quote\"slash/backslash\\"]
     (for [index (range 128)]
       (str "generated-" index "-"
            (apply str
                   (map (fn [offset]
                          (nth boundaries
                               (mod (+ (* 17 index) (* 11 offset))
                                    (count boundaries))))
                        (range (inc (mod index 17)))))
            (if (zero? (mod index 3)) "😀" "β"))))))

(defn- bytes [value]
  (vec value))

(defn- native-line-outcome [native-line! sql]
  (try
    [:ok (bytes (native-line! sql))]
    (catch Throwable _ [:unavailable])))

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
                            (generated-sqls)))]
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
    (check "portable fallback has no OutputStreamWriter dependency"
           false
           (str/includes? (slurp "src/jdbc/chdb/durable/wal.cljc")
                          "(OutputStreamWriter."))
    (when-not (zero? @failures)
      (throw (ex-info (str @failures " Durable WAL encoder checks failed")
                      {:failures @failures}))))
  (println "Durable WAL encoder checks passed"))
