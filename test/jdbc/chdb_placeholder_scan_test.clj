(ns jdbc.chdb-placeholder-scan-test
  (:require [clojure.test :refer [deftest is run-tests testing]]
            [jdbc.chdb :as chdb]))

(defn- outcome [f]
  (try {:value (f)}
       (catch Throwable error
         {:error (ex-message error) :data (ex-data error)})))

(defn- reference [sql params]
  (#'chdb/rewrite-placeholders-with-scan sql params))

(defn- candidate [sql params]
  (#'chdb/rewrite-placeholders sql params))

(defn- check-equivalent! [sql]
  (is (= (outcome #(reference sql [])) (outcome #(candidate sql [])))
      (pr-str sql)))

(deftest exact-empty-parameter-results
  (doseq [sql ["" "SELECT 1" "?" "SELECT ?" "??" "SELECT '?', ?"
               "SELECT '?'" "SELECT \"?\"" "SELECT `?`"
               "SELECT 'it\\'?'" "SELECT 'it''s ?'"
               "SELECT \"a\\\"?\"" "SELECT \"a\"\"?\""
               "SELECT `a\\`?`" "SELECT `a``?`"
               "SELECT '\\\\' ?" "SELECT '\\\\\\'?'"
               "SELECT -- ?\n1" "SELECT -- ?\n?" "SELECT -- ?\r?"
               "SELECT -- ?\r\n?" "SELECT -- ?"
               "SELECT /* ? */ 1" "SELECT /* ? */ ?"
               "SELECT /* outer ? /* inner ? */ outer ? */ 1"
               "SELECT /* outer ? /* inner ? */ outer ? */ ?"
               "SELECT /* ' \" ` \\ -- ? */ ?"
               "SELECT '?/*--', \"?/*--\", `?/*--`"
               "SELECT 'unterminated ?" "SELECT \"unterminated ?"
               "SELECT `unterminated ?" "SELECT /* unterminated ?"
               "SELECT /* outer /* inner */ ?" "SELECT '\\"
               "SELECT '\\' ?" "SELECT 'x'' ?" "SELECT \"x\"\" ?"
               "SELECT `x`` ?" "SELECT -?" "SELECT /?" "SELECT *?"
               "SELECT λ😀, 'λ😀?\\😀', \"中?\", `😀?`"
               "SELECT λ😀, 'λ😀?\\😀', \"中?\", `😀?`, ?"
               "INSERT INTO t FORMAT JSONEachRow\n?\n"
               "INSERT INTO t FORMAT JSONEachRow\n{\"body\":\"ready?\"}\nSELECT ?"]]
    (check-equivalent! sql)))

(defn- words [alphabet n]
  (if (zero? n) [""]
      (for [prefix (words alphabet (dec n)), suffix alphabet]
        (str prefix suffix))))

(deftest exhaustive-short-lexical-boundaries
  ;; Exhaustive delimiter adjacencies exercise state transitions without
  ;; sharing implementation logic with the detector under test.
  (doseq [n (range 5)
          sql (words ["?" "'" "\"" "`" "\\" "-" "/" "*" "\n"] n)]
    (check-equivalent! sql)))

(deftest long-spans-and-escape-lookahead
  (let [plain (apply str (repeat 2048 "λ😀a"))
        escaped (apply str (repeat 2048 "\\a"))
        doubled (apply str (repeat 2048 "''"))]
    (doseq [body [plain escaped doubled]
            suffix ["" ", ?"]]
      (check-equivalent! (str "SELECT '" body "?'" suffix)))
    (doseq [quote ["'" "\"" "`"]
            suffix ["" ", ?"]]
      (check-equivalent! (str "SELECT " quote plain "?\\" quote plain
                              quote suffix)))
    (check-equivalent! (str "SELECT -- " plain "?\n?"))
    (check-equivalent! (str "SELECT /* " plain "? /* " escaped "? */ ? */ ?"))
    ;; Repeated closed strings precede a distant backslash (and eventually no
    ;; backslash): a fresh suffix search per string would be quadratic.
    (check-equivalent! (str "SELECT " (apply str (repeat 2048 "'a?',"))
                            "'tail\\?'"))
    (check-equivalent! (str "SELECT " (apply str (repeat 2048 "'a?',")) "?"))))

(deftest public-identity-and-exact-errors
  (doseq [sql ["SELECT 1" "SELECT '?'" "SELECT -- ?" "SELECT /* ?"
               "INSERT INTO t FORMAT JSONEachRow\n{\"body\":\"ready?\"}\n"]]
    (is (identical? sql (chdb/prepared-sql (chdb/prepare-query sql [])))))
  (doseq [sql ["?" "SELECT '?' AS literal, ?" "SELECT -- ?\n?"
               "SELECT /* ? */ ?" "SELECT `?`, ?"]]
    (is (= {:error "more chDB placeholders than parameters"
            :data {:placeholders 1 :parameters 0 :jdbc/sql-error true}}
           (outcome #(chdb/prepare-query sql [])))))
  (testing "a true detector still reaches the established error-producing scanner"
    (let [calls (atom 0) original (deref #'chdb/rewrite-placeholders-with-scan)]
      (with-redefs-fn
        {(ns-resolve 'jdbc.chdb 'rewrite-placeholders-with-scan)
         (fn [sql params] (swap! calls inc) (original sql params))}
        #(do (outcome (fn [] (chdb/prepare-query "SELECT ?" [])))
             (is (= 1 @calls))))))
  (testing "accepted quoted payloads do not construct a rewritten SQL string"
    (let [sql "INSERT INTO t FORMAT JSONEachRow\n{\"body\":\"ready?\"}\n"]
      (with-redefs-fn
        {(ns-resolve 'jdbc.chdb 'rewrite-placeholders-with-scan)
         (fn [& _] (throw (ex-info "unexpected full rewrite" {})))}
        #(is (identical? sql (chdb/prepared-sql (chdb/prepare-query sql [])))))))
  (testing "bound requests retain the reference rewrite and error behavior"
    (doseq [[sql params] [["SELECT '?' AS literal, ?" [42]]
                         ["SELECT ? + ?" [1 2]]
                         ["SELECT '?'" [1]]
                         ["SELECT ? + ?" [1]]]]
      (is (= (outcome #(reference sql params))
             (outcome #(candidate sql params)))))))

(defn -main [& _]
  (let [{:keys [fail error]} (run-tests 'jdbc.chdb-placeholder-scan-test)]
    (System/exit (if (zero? (+ fail error)) 0 1))))
