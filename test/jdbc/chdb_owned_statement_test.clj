(ns jdbc.chdb-owned-statement-test
  (:require [clojure.test :refer [deftest is run-tests]]
            [jdbc.chdb.owned-statement :as owned]
            [jdbc.chdb.durable.wal :as wal]
            [jdbc.chdb :as chdb]
            [jdbc.chdb.native :as native]
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
            ;; Text is an explicit test oracle for the existing executor;
            ;; this does not claim a wired byte-only preparation path.
            (chdb/execute-any-with-query-buffer handle replay-sql buffer))))
      (let [result (chdb/execute-any handle "SELECT n, s FROM owned_statement_sample" [])]
        (assert (= [[1 "ready?"]] (:rows result))))
      (println :real-native-owned-snapshot-green :rows 1 :wal-sql-equal true)
      (finally (native/close! handle)))))

(defn -main [& [mode]]
  (let [{:keys [fail error]} (run-tests 'jdbc.chdb-owned-statement-test)]
    (when (and (= mode "native") (zero? (+ fail error)))
      (real-native-snapshot!))
    (System/exit (if (zero? (+ fail error)) 0 1))))
