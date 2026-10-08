(ns jdbc.chdb-owned-statement-test
  (:require [clojure.test :refer [deftest is run-tests]]
            [clojure.java.io :as io]
            [clojure.string :as string]
            [jolt.scheme :as scheme]
            [jdbc.chdb.owned-statement :as owned]
            [jdbc.chdb.durable.wal :as wal]
            [jdbc.chdb :as chdb]
            [jdbc.chdb.native :as native]
            [jdbc.chdb.json-each-row :as encoder]
            [jdbc.chdb.durable :as durable]
            [jdbc.chdb.durable.policy :as policy]
            [jdbc.chdb-placeholder-scan-test :as lexical]
            [clojure.data.json :as json]
            [jolt.ffi :as ffi]))

(defn emitted [statement]
  (let [out (java.io.ByteArrayOutputStream.)]
    (wal/write-prepared! out (owned/prepared-wal statement))
    (.toByteArray out)))

(deftest copied-input-is-not-an-alias-and-output-is-opaque
  (let [sql "INSERT INTO t VALUES ('ready?')"
        source (.getBytes sql "UTF-8") statement (owned/try-snapshot source)]
    (is (owned/statement? statement))
    (is (= (alength source) (owned/byte-count statement)))
    (aset-byte source 0 (byte 88))
    (is (= sql (owned/text statement)))
    (is (not (.contains (str statement) "ready?")))
    (is (java.util.Arrays/equals (wal/portable-line-bytes sql) (emitted statement)))
    (owned/with-query-buffer statement
      (fn [{:keys [pointer length]}]
        (is (= sql (String. (ffi/read-array pointer :byte length) "UTF-8")))))
    ;; Prepared output is independent too; mutating our emitted chunks cannot
    ;; corrupt the private input or a subsequent encoding/native copy.
    (let [prepared (owned/prepared-wal statement)
          [bytes _] (first (:chunks prepared))]
      (aset-byte bytes 0 (byte 0)))
    (is (= sql (owned/text statement)))
    (is (java.util.Arrays/equals (wal/portable-line-bytes sql) (emitted statement)))))

(deftest unsupported-inputs-decline-without-truncation
  (is (nil? (owned/try-snapshot nil)))
  (is (nil? (owned/try-snapshot "not bytes")))
  (is (nil? (owned/try-snapshot (.getBytes "λ😀" "UTF-8"))))
  (is (nil? (owned/try-snapshot (byte-array [(unchecked-byte 128)]))))
  (with-redefs [owned/max-snapshot-bytes 3]
    (is (nil? (owned/try-snapshot (.getBytes "four" "UTF-8"))))
    (is (= "abc" (owned/text (owned/try-snapshot (.getBytes "abc" "UTF-8"))))))
  (is (false? (owned/statement? {})))
  (is (thrown? clojure.lang.ExceptionInfo (owned/byte-count {}))))

(deftest word-ascii-validation-rejects-every-high-bit-position-and-tail
  (doseq [n (range 17)]
    (let [source (byte-array (repeat n (byte 127)))]
      (is (= n (owned/byte-count (owned/try-snapshot source))))
      (doseq [i (range n) high [128 129 192 254 255]]
        (aset-byte source i (unchecked-byte high))
        (is (nil? (owned/try-snapshot source)))
        (aset-byte source i (byte 127)))))
  (doseq [value (range 128)]
    (let [source (byte-array (repeat 17 (byte value)))
          statement (owned/try-snapshot source)]
      (is (owned/statement? statement))
      (is (= 17 (owned/byte-count statement))))))

(deftest caller-alias-mutant-is-detected
  ;; Deliberately replace the copy boundary locally. A marker/class check alone
  ;; would accept this broken ownership implementation; content witnesses don't.
  (let [sql "SELECT 1" source (.getBytes sql "UTF-8")
        statement (with-redefs [owned/snapshot-kernel (delay (fn [input _] input))]
                    (owned/try-snapshot source))]
    (is (owned/statement? statement))
    (aset-byte source 0 (byte 88))
    (is (not= sql (owned/text statement)))
    (is (not (java.util.Arrays/equals (wal/portable-line-bytes sql) (emitted statement))))))

(deftest exact-ascii-wal-parity-and-boundaries
  (doseq [sql ["" "SELECT ?" "SELECT '?'" "quote\"slash/\\newline\n\r\t\b\f"
               (apply str (map char (range 128)))
               (str (apply str (repeat 65512 "a")) "\"/\\\n")
               (str (apply str (repeat 65524 "a")) "\"/\\\n")
               (str (apply str (repeat 65535 "a")) "\"/\\\n")
               (str (apply str (repeat 65536 "a")) "\"/\\\n")
               (str (apply str (repeat 131071 "a")) "\"/\\\n")]]
    (let [statement (owned/try-snapshot (.getBytes sql "UTF-8"))
          expected (wal/portable-line-bytes sql) actual (emitted statement)]
      (is (owned/statement? statement))
      (is (= sql (owned/text statement)))
      (is (= (alength (.getBytes sql "UTF-8")) (owned/byte-count statement)))
      (is (= (alength expected) (wal/prepared-size (owned/prepared-wal statement))))
      (is (java.util.Arrays/equals expected actual)))))

(deftest scoped-native-copy-frees-on-return-and-exception
  (let [statement (owned/try-snapshot (.getBytes "SELECT 1" "UTF-8"))
        frees (atom []) original ffi/free]
    (with-redefs [ffi/free (fn [pointer] (swap! frees conj pointer) (original pointer))]
      (is (= :returned (owned/with-query-buffer statement (fn [_] :returned))))
      (is (= 1 (count @frees)))
      (is (thrown? clojure.lang.ExceptionInfo
                   (owned/with-query-buffer statement
                     (fn [_] (throw (ex-info "controlled native consumer failure" {:fixture true}))))))
      (is (= 2 (count @frees))))
    (is (= "SELECT 1" (owned/text statement)))))

(deftest byte-placeholder-detector-matches-lexical-oracle
  (doseq [n (range 5)
          sql (@#'lexical/words ["?" "'" "\"" "`" "\\" "-" "/" "*" "\n"] n)]
    (is (= (@#'chdb/code-placeholder? sql)
           (owned/code-placeholder? (owned/try-snapshot (.getBytes sql "UTF-8"))))))
  (doseq [sql ["SELECT -- ?\r?" "SELECT -- ?\r\n?"
               "SELECT /* outer ? /* inner ? */ outer ? */ ?"
               "SELECT 'it\\'?', ?" "SELECT 'it''s ?'"
               "SELECT 'unterminated ?" "SELECT /* unterminated ?"
               (str "SELECT " (apply str (repeat 2048 "'a?',")) "'tail\\?'" "?")]]
    (is (= (@#'chdb/code-placeholder? sql)
           (owned/code-placeholder? (owned/try-snapshot (.getBytes sql "UTF-8")))))))

(deftest word-placeholder-skip-boundaries-match-independent-scalar-oracle
  (doseq [padding (range 33)
          tail ["" "abc" "abcdefg" "?" "????????"]
          syntax ["?" "'ordinary?text'" "\"ordinary?text\"" "`ordinary?text`"
                  "'ordinary\\'?text'" "\"ordinary\\\"?text\"" "`ordinary\\`?text`"
                  "'ordinary''?text'" "\"ordinary\"\"?text\"" "`ordinary``?text`"
                  "-- ordinary?text\n" "-- ordinary?text\r"
                  "/* ordinary? /* inner? */ text */"
                  "/* ordinary? /* inner? */ text"
                  "'ordinary?text" "\"ordinary?text" "`ordinary?text"]]
    (let [sql (str (apply str (repeat padding "a")) syntax tail)]
      (is (= (@#'chdb/portable-code-placeholder? sql)
             (owned/code-placeholder? (owned/try-snapshot (.getBytes sql "UTF-8")))))
      (is (= sql (owned/text (owned/try-snapshot (.getBytes sql "UTF-8")))))))
  ;; Non-vacuity: dropping the code-mode '?' lane detector must be rejected.
  (let [source (slurp (io/resource "jdbc/chdb/owned_ascii_placeholder.ss"))
        mutant (scheme/eval-string
                 (string/replace source "(word-has? word #x3f3f3f3f)" "#f"))
        bytes (.getBytes "abc?ordinary" "UTF-8")]
    (is (true? (owned/code-placeholder? (owned/try-snapshot bytes))))
    (is (false? (mutant bytes)))))

(deftest owned-native-executor-preserves-buffer-and-does-not-materialize-text
  (let [statement (owned/try-snapshot (.getBytes "SELECT '?'" "UTF-8"))
        calls (atom [])]
    (owned/with-query-buffer statement
      (fn [buffer]
        (with-redefs [owned/text (fn [_] (throw (ex-info "unexpected text conversion" {})))
                      chdb/prepare-query (fn [& _] (throw (ex-info "unexpected text preparation" {})))
                      native/with-live-handle (fn [_ f] (f :connection))
                      native/chdb-query-with-params-n
                      (fn [& args] (swap! calls conj args) :result)
                      chdb/consume-json-result (fn [result] {:fixture result})]
          (is (= {:fixture :result}
                 (chdb/execute-owned-any-with-query-buffer :handle statement buffer))))
        (is (= 1 (count @calls)))
        (let [args (first @calls)]
          (is (= :connection (nth args 0)))
          (is (= (:pointer buffer) (nth args 1)))
          (is (= (:length buffer) (nth args 2)))
          (is (= 0 (last args))))))))

(deftest owned-native-placeholder-errors-and-invalid-buffers-stop-before-native
  (let [sql "SELECT ?" statement (owned/try-snapshot (.getBytes sql "UTF-8"))
        failure (fn [f] (try (f) nil (catch clojure.lang.ExceptionInfo e
                                     [(.getMessage e) (ex-data e)])))
        legacy (failure #(chdb/prepare-query sql []))
        calls (atom 0)]
    (is (some? legacy))
    (with-redefs [native/with-live-handle (fn [& _] (swap! calls inc))]
      (owned/with-query-buffer statement
        (fn [buffer]
          (is (= legacy (failure #(chdb/execute-owned-any-with-query-buffer :handle statement buffer))))
          (is (some? (failure #(chdb/execute-owned-any-with-query-buffer
                               :handle statement (assoc buffer :length 0)))))))
      (is (some? (failure #(chdb/execute-owned-any-with-query-buffer :handle {} {}))))
      (is (some? (failure #(chdb/execute-owned-any-with-query-buffer :handle statement nil))))
      (is (= 0 @calls)))))

(deftest default-adapter-owned-policy-and-pointer-controls
  (let [adapter (:with-native-admitted-buffer!
                 (@#'durable/default-open-operations))
        statement (owned/try-snapshot (.getBytes "INSERT INTO t VALUES ('?')" "UTF-8"))
        accepted {:query-class :mutating :statement-count 1
                  :has-secrets false :writes-only-target-database true
                  :changes-database-lifecycle false}]
    (doseq [analysis [accepted (assoc accepted :statement-count 2)
                     (assoc accepted :has-secrets true)
                     (assoc accepted :writes-only-target-database false)]]
      (let [classified (atom nil) executed (atom nil) frees (atom 0)
            original-free ffi/free]
        (with-redefs [owned/text (fn [_] (throw (ex-info "unexpected text conversion" {})))
                      native/with-query-buffer (fn [& _] (throw (ex-info "unexpected text buffer" {})))
                      native/classify-query-buffer!
                      (fn [_ buffer _] (reset! classified buffer) analysis)
                      chdb/execute-owned-any-with-query-buffer
                      (fn [_ value buffer]
                        (is (identical? statement value))
                        (reset! executed buffer) :executed)
                      ffi/free (fn [pointer] (swap! frees inc) (original-free pointer))]
          (if (= accepted analysis)
            (do
              (is (= :executed (adapter :handle statement "default"
                                       (fn [facts execute!]
                                         (policy/authorize-execute! facts) (execute!)))))
              (is (identical? @classified @executed)))
            (do
              (is (thrown? clojure.lang.ExceptionInfo
                           (adapter :handle statement "default"
                                    (fn [facts execute!]
                                      (policy/authorize-execute! facts) (execute!)))))
              (is (nil? @executed))))
          (is (some? @classified))
          (is (= 1 @frees)))))))

(deftest serial-encoder-owned-selection-and-single-pass-text-fallback
  (doseq [mode [:owned :unicode :legacy]]
    (let [context (encoder/open-encoder {:parallelism 1 :json-backend :native-guarded-byte-batch})
          visits (atom 0)
          value (reify json/JSONWriter
                  (-write [_ out _]
                    (swap! visits inc)
                    (.write out (if (= mode :unicode) "\"β\"" "42"))))
          prefix "INSERT INTO t FORMAT JSONCompactEachRow\n"]
      (try
        (let [selected (if (= mode :legacy) (dissoc context :native-prefixed-byte-writer) context)
              result (encoder/encode-limited-prefixed-statement! selected prefix [[value]] 100)]
          (is (= 1 @visits))
          (if (= mode :owned)
            (do (is (owned/statement? result)) (is (= (str prefix "[42]\n") (owned/text result))))
            (do (is (string? result))
                (is (= (str prefix (if (= mode :unicode) "[\"β\"]\n" "[42]\n")) result)))))
        (finally (encoder/close! context)))))
  (let [context (encoder/open-encoder {:parallelism 1 :json-backend :native-guarded-byte-batch})
        visits (atom [])
        rows ((fn walk [values]
                (lazy-seq (when (seq values)
                            (swap! visits conj (first values))
                            (cons [(first values)] (walk (rest values)))))) [1 2 3])]
    (try
      (is (= :jdbc.chdb.json-each-row/output-limit
             (:type (ex-data (try (encoder/encode-limited-prefixed-statement! context "prefix" rows 1)
                                 nil (catch Throwable error error))))))
      (is (= [1] @visits))
      (is (owned/statement? (encoder/encode-limited-prefixed-statement! context "prefix" [] 0)))
      (finally (encoder/close! context)))))

(defn real-native-snapshot! []
  (let [sql "INSERT INTO owned_statement_sample VALUES (1, 'ready?')"
        source (.getBytes sql "UTF-8") statement (owned/try-snapshot source)
        replay-sql (get (json/read-str (String. (emitted statement) "UTF-8")) "sql")
        handle (native/open! ":memory:")]
    (try
      (chdb/execute-any handle
        "CREATE TABLE owned_statement_sample (n UInt32, s String) ENGINE=MergeTree ORDER BY n" [])
      (aset-byte source 0 (byte 88))
      (assert (= sql replay-sql (owned/text statement)))
      (owned/with-query-buffer statement
        (fn [buffer]
          (let [analysis (native/classify-query-buffer! handle buffer "default")]
            (assert (= :mutating (:query-class analysis)))
            (assert (= 1 (:statement-count analysis)))
            ;; Conversion/preparation traps make this a non-vacuous byte-only
            ;; native execution witness, still not a wired Durable writer.
            (with-redefs [owned/text (fn [_] (throw (ex-info "unexpected text conversion" {})))
                          chdb/prepare-query (fn [& _] (throw (ex-info "unexpected text preparation" {})))]
              (chdb/execute-owned-any-with-query-buffer handle statement buffer)))))
      (let [result (chdb/execute-any handle "SELECT n, s FROM owned_statement_sample" [])]
        (assert (= [[1 "ready?"]] (:rows result))))
      (println :real-native-owned-snapshot-green :rows 1 :wal-sql-equal true)
      (finally (native/close! handle)))))

(defn -main [& [mode]]
  (let [{:keys [fail error]} (run-tests 'jdbc.chdb-owned-statement-test)]
    (when (and (= mode "native") (zero? (+ fail error)))
      (real-native-snapshot!))
    (System/exit (if (zero? (+ fail error)) 0 1))))
