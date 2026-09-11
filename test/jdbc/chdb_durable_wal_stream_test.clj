(ns jdbc.chdb-durable-wal-stream-test
  (:require [clojure.data.json :as json]
            [clojure.string :as str]
            [jdbc.chdb.durable :as durable]
            [jdbc.chdb.durable.backend :as backend]
            [jdbc.chdb.durable.control :as control]
            [jdbc.chdb.durable.reader :as reader]
            [jdbc.chdb.durable.writer :as writer]
            [jdbc.chdb-durable-open-test-support :as support]))

(def failures (atom 0))

(def ^:private initial-options
  {:owner "old-writer" :instance "old-instance"
   :expires-at 100M :now 0M :clock-skew 0M
   :database "default" :engine-version "26.7.2-rc.2"
   :backup-format 1 :min-reader "26.7.2-rc.2"})

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
    (catch Throwable error
      (loop [current error]
        (when current
          (or (:type (ex-data current))
              (recur (.getCause current))))))))

(defn- wal-bytes [sql-values]
  (.getBytes
   (apply str (map #(str (json/write-str {"sql" %}) "\n") sql-values))
   "UTF-8"))

(defn- concat-bytes [left right]
  (let [combined (byte-array (+ (alength left) (alength right)))]
    (System/arraycopy left 0 combined 0 (alength left))
    (System/arraycopy right 0 combined (alength left) (alength right))
    combined))

(defn- prepared-wal-store [payload]
  (let [store (backend/memory-backend)
        token (:token (control/acquire! store initial-options))
        publication (control/publish-wal-bytes! store token payload)]
    (control/commit-reference!
     store token {:kind :wal :reference (:reference publication)
                  :verify-reference! control/verify-byte-reference!})
    (control/release! store token)
    store))

(defn- attempt-open [payload f]
  (let [calls (atom [])
        close-count (atom 0)
        cleanup-count (atom 0)
        operations (support/fake-open-operations
                    calls (atom [0M]) close-count cleanup-count)]
    (f #(durable/open-reader!
         {:store (prepared-wal-store payload) :operations operations})
       calls close-count cleanup-count)))

(defn- engine-effects [calls]
  (filterv #(contains? #{:analyze-execute :execute} (first %)) calls))

(defn run-checks! []
  (reset! failures 0)
  (println "Durable bounded WAL validation and replay")

  (let [long-sql (str "SELECT '" (apply str (repeat 70000 "x")) "'")
        sql-values ["INSERT INTO t VALUES (1)" long-sql "SELECT 'β'"]
        payload (wal-bytes sql-values)
        wal-decodes (atom 0)
        original-read-str json/read-str]
    (attempt-open
     payload
     (fn [open! calls _ _]
       (with-redefs [json/read-str
                     (fn [text & options]
                       (when (str/includes? text "\"sql\"")
                         (swap! wal-decodes inc))
                       (apply original-read-str text options))]
         (let [opened (open!)]
           (try
             (check "records crossing the 64 KiB input block replay in order"
                    sql-values
                    (mapv second (filter #(= :execute (first %)) @calls)))
             (finally
               (reader/close! opened)))))))
    ;; This is a causal guard for the complete-validation pass followed by the
    ;; bounded replay pass. A one-pass execute-while-validating mutant observes
    ;; only three decodes and can partially apply a later-corrupt WAL.
    (check "every valid record is decoded once per bounded pass"
           (* 2 (count sql-values)) @wal-decodes))

  (doseq [[label suffix]
          [["malformed JSON tail" (.getBytes "{not-json}\n" "UTF-8")]
           ["noncanonical UTF-8 tail"
            (byte-array [(unchecked-byte 0xc3) 0x28 0x0a])]]]
    (attempt-open
     (concat-bytes (wal-bytes ["INSERT INTO t VALUES (1)"]) suffix)
     (fn [open! calls close-count cleanup-count]
       (check (str label " is corrupt") ::durable/corrupt
              (error-type open!))
       (check (str label " executes no validated prefix") []
              (engine-effects @calls))
       (check (str label " still closes and cleans") [1 1]
              [@close-count @cleanup-count]))))

  (doseq [[label payload]
          [["empty WAL" (byte-array 0)]
           ["unterminated WAL" (.getBytes "{\"sql\":\"SELECT 1\"}" "UTF-8")]]]
    (attempt-open
     payload
     (fn [open! calls _ _]
       (check (str label " is corrupt") ::durable/corrupt
              (error-type open!))
       (check (str label " has no engine effect") []
              (engine-effects @calls)))))

  (doseq [[label text]
          [["missing sql" "{}\n"]
           ["non-string sql" "{\"sql\":1}\n"]
           ["extra record key" "{\"sql\":\"ok\",\"extra\":1}\n"]]]
    (attempt-open
     (.getBytes text "UTF-8")
     (fn [open! calls _ _]
       (check (str label " is corrupt") ::durable/corrupt
              (error-type open!))
       (check (str label " has no engine effect") []
              (engine-effects @calls)))))

  (with-redefs [writer/max-statement-bytes 3]
    (attempt-open
     (wal-bytes ["ok" "four"])
     (fn [open! calls _ _]
       (check "an oversized tail retains limit-exceeded classification"
              ::durable/limit-exceeded (error-type open!))
       (check "an oversized tail executes no preceding statement" []
              (engine-effects @calls)))))

  (with-redefs [writer/max-statement-bytes 3]
    (attempt-open
     (concat-bytes (wal-bytes ["four"])
                   (byte-array [(unchecked-byte 0xc3) 0x28 0x0a]))
     (fn [open! calls _ _]
       (check "whole-segment UTF-8 corruption precedes an earlier record limit"
              ::durable/corrupt (error-type open!))
       (check "UTF-8 precedence has no engine effect" []
              (engine-effects @calls)))))

  (with-redefs [writer/max-statement-bytes 3]
    (attempt-open
     (concat-bytes (wal-bytes ["four"])
                   (.getBytes "{\"sql\":\"ok\"}" "UTF-8"))
     (fn [open! calls _ _]
       (check "missing final LF retains precedence over a record limit"
              ::durable/corrupt (error-type open!))
       (check "termination precedence has no engine effect" []
              (engine-effects @calls)))))

  (when-not (zero? @failures)
    (throw (ex-info (str @failures " Durable WAL streaming checks failed")
                    {:failures @failures})))
  (println "all Durable WAL streaming checks passed")
  true)

(defn -main [& _]
  (run-checks!))
