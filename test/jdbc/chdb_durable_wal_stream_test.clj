(ns jdbc.chdb-durable-wal-stream-test
  (:require [clojure.data.json :as json]
            [clojure.string :as str]
            [jdbc.chdb.durable :as durable]
            [jdbc.chdb.durable.backend :as backend]
            [jdbc.chdb.durable.control :as control]
            [jdbc.chdb.durable.reader :as reader]
            [jdbc.chdb.durable.writer :as writer]
            [jdbc.chdb-durable-open-test-support :as support])
  (:import [java.nio.file Files OpenOption]
           [java.nio.file.attribute FileAttribute]))

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

(defn- raw-bytes [values]
  (byte-array (map unchecked-byte values)))

(defn- prepared-wal-store-many [payloads]
  (let [store (backend/memory-backend)
        token (:token (control/acquire! store initial-options))]
    (doseq [payload payloads]
      (let [publication (control/publish-wal-bytes! store token payload)]
        (control/commit-reference!
         store token {:kind :wal :reference (:reference publication)
                      :verify-reference! control/verify-byte-reference!})))
    (control/release! store token)
    store))

(defn- prepared-wal-store [payload]
  (prepared-wal-store-many [payload]))

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

  (let [decoder-capability-var
        (ns-resolve 'jdbc.chdb.durable 'strict-utf8-decoder-capable-result)
        require-decoder-var
        (ns-resolve 'jdbc.chdb.durable
                    'require-strict-utf8-decoder-capability!)
        require-decoder! @require-decoder-var
        decode-var (ns-resolve 'jdbc.chdb.durable 'decode-wal-text!)
        decode! @decode-var
        source (slurp "src/jdbc/chdb/durable.clj")
        start (str/index-of source "(defn- decode-wal-text!")
        end (str/index-of source "(defn- exact-statement-bytes-exceed?" start)
        decode-source (subs source start end)]
    (check "running Jolt passes the strict decoder capability probe"
           true (require-decoder!))
    (with-redefs-fn
      {decoder-capability-var (delay false)}
      (fn []
        ;; Public opens must fail at this guard before resolving storage,
        ;; validating writer options, acquiring a lease, or touching native code.
        (check "reader fails closed before effects without strict decoding"
               ::durable/strict-utf8-decoder-unavailable
               (error-type #(durable/open-reader! {})))
        (check "writer fails closed before effects without strict decoding"
               ::durable/strict-utf8-decoder-unavailable
               (error-type #(durable/open-writer! {})))))
    (check "strict decoder preserves control, multibyte, and astral text"
           "control:\u0000\t\r latin:\u00f5 greek:\u03b2 astral:\ud83d\ude00"
           (decode! (.getBytes
                     "control:\u0000\t\r latin:\u00f5 greek:\u03b2 astral:\ud83d\ude00"
                     "UTF-8")))
    (doseq [[label bytes]
            [["isolated continuation" [0x80]]
             ["invalid continuation" [0xc3 0x28]]
             ["overlong scalar" [0xc0 0xaf]]
             ["encoded surrogate" [0xed 0xa0 0x80]]
             ["scalar above Unicode maximum" [0xf4 0x90 0x80 0x80]]
             ["truncated multibyte scalar" [0xe2 0x82]]]]
      (check (str label " is rejected by strict UTF-8 decoding")
             ::durable/corrupt
             (error-type #(decode! (raw-bytes bytes)))))
    ;; Causal source control for the allocation change: restoring the legacy
    ;; String -> getBytes -> Arrays/equals round trip turns this check red even
    ;; though valid and malformed behavior alone would still look equivalent.
    (check "WAL decoding uses one strict decoder without a UTF-8 re-encode"
           true
           (and (str/includes? decode-source
                               "(.decode decoder (ByteBuffer/wrap bytes))")
                (str/includes? decode-source "CodingErrorAction/REPORT")
                (not (str/includes? decode-source ".getBytes"))
                (not (str/includes? decode-source "Arrays/equals"))
                (not (str/includes? decode-source "(String.")))))

  (let [sql-values ["SELECT '\ud83d\ude00'"
                    "SELECT 'control \u0000\t\r'"]]
    (attempt-open
     (wal-bytes sql-values)
     (fn [open! calls _ _]
       (let [opened (open!)]
         (try
           (check "canonical astral and control UTF-8 replays exactly"
                  sql-values
                  (mapv second (filter #(= :execute (first %)) @calls)))
           (finally
             (reader/close! opened)))))))

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
    ;; This is a causal guard for the retained replay plan. Restoring the old
    ;; unconditional second parse observes twice this count.
    (check "a bounded valid segment decodes each record exactly once"
           (count sql-values) @wal-decodes))

  (let [sql-values ["SELECT 1" "SELECT 2" "SELECT 3"]
        payload (wal-bytes sql-values)
        wire-limit-var (ns-resolve 'jdbc.chdb.durable
                                   'replay-plan-wire-byte-limit)
        record-limit-var (ns-resolve 'jdbc.chdb.durable
                                     'replay-plan-record-limit)]
    (doseq [[label limit-var limit]
            [["wire-byte" wire-limit-var 1]
             ["record-count" record-limit-var 1]]]
      (let [wal-decodes (atom 0)
            original-read-str json/read-str]
        (with-redefs-fn
          {limit-var limit
           #'json/read-str
           (fn [text & options]
             (when (str/includes? text "\"sql\"")
               (swap! wal-decodes inc))
             (apply original-read-str text options))}
          #(attempt-open
            payload
            (fn [open! calls _ _]
              (let [opened (open!)]
                (try
                  (check (str label " fallback replays the exact statements")
                         sql-values
                         (mapv second (filter (fn [call]
                                                (= :execute (first call)))
                                              @calls)))
                  (finally
                    (reader/close! opened)))))))
        (check (str label " cap causally retains the two-pass fallback")
               (* 2 (count sql-values)) @wal-decodes))))

  (let [validate-var (ns-resolve 'jdbc.chdb.durable 'validate-wal!)
        replay-var (ns-resolve 'jdbc.chdb.durable 'replay-wal!)
        validate! @validate-var
        replay! @replay-var
        original-sql ["SELECT 'validated-plan-original'"]
        changed-sql ["SELECT 'mutated-after-validation'"]
        path (Files/createTempFile
              "jolt-chdb-validated-plan-" ".jsonl"
              (make-array FileAttribute 0))
        calls (atom [])
        operations {:analyze-execute!
                    (fn [_ sql database]
                      (swap! calls conj [:analyze sql database]))
                    :execute-native!
                    (fn [_ sql _]
                      (swap! calls conj [:execute sql]))}]
    (try
      (Files/write path (wal-bytes original-sql) (make-array OpenOption 0))
      (let [plan (validate! path)]
        (Files/write path (wal-bytes changed-sql) (make-array OpenOption 0))
        (replay! path plan operations :handle "default"))
      (check "a validated plan resists a later scratch-file substitution"
             [[:analyze (first original-sql) "default"]
              [:execute (first original-sql)]]
             @calls)
      (finally
        (Files/deleteIfExists path))))

  (let [validate-var (ns-resolve 'jdbc.chdb.durable 'validate-wal!)
        replay-var (ns-resolve 'jdbc.chdb.durable 'replay-wal!)
        validate! @validate-var
        replay! @replay-var
        live-plans (atom 0)
        max-live-plans (atom 0)
        calls (atom [])
        close-count (atom 0)
        cleanup-count (atom 0)
        operations (support/fake-open-operations
                    calls (atom [0M]) close-count cleanup-count)
        store (prepared-wal-store-many
               [(wal-bytes ["SELECT 'segment-one'"])
                (wal-bytes ["SELECT 'segment-two'"])])]
    (with-redefs-fn
      {validate-var
       (fn [path]
         (let [plan (validate! path)]
           (when (:statements plan)
             (let [active (swap! live-plans inc)]
               (swap! max-live-plans max active)))
           plan))
       replay-var
       (fn [path plan ops handle database]
         (try
           (replay! path plan ops handle database)
           (finally
             (when (:statements plan)
               (swap! live-plans dec)))))}
      #(let [opened (durable/open-reader! {:store store
                                           :operations operations})]
         (reader/close! opened)))
    (check "recovery validates and consumes at most one segment plan at a time"
           [1 0] [@max-live-plans @live-plans])
    (check "segment plans preserve manifest and statement order"
           ["SELECT 'segment-one'" "SELECT 'segment-two'"]
           (mapv second (filter #(= :execute (first %)) @calls))))

  ;; Put the first byte of a four-byte scalar at the final position in the
  ;; 64 KiB read block. The remaining bytes arrive in the next read, while the
  ;; record buffer and strict decoder must still reproduce the exact SQL twice
  ;; (validation, then replay).
  (let [json-prefix "{\"sql\":\""
        json-prefix-bytes (alength (.getBytes json-prefix "UTF-8"))
        ascii-count (- 65535 json-prefix-bytes)
        sql (str (apply str (repeat ascii-count "x")) "\ud83d\ude00")
        ;; Write this fixture as literal UTF-8 because data.json deliberately
        ;; escapes the astral scalar as a surrogate pair.
        payload (.getBytes (str json-prefix sql "\"}\n") "UTF-8")]
    (check "astral scalar begins on the 64 KiB read boundary"
           [0xf0 0x9f 0x98 0x80]
           (mapv #(bit-and 255 (aget payload %))
                 [65535 65536 65537 65538]))
    (attempt-open
     payload
     (fn [open! calls _ _]
       (let [opened (open!)]
         (try
           (check "split-boundary astral SQL replays exactly"
                  [sql]
                  (mapv second (filter #(= :execute (first %)) @calls)))
           (finally
             (reader/close! opened)))))))

  (let [exact-size-checks (atom 0)
        exact-size-var (ns-resolve 'jdbc.chdb.durable
                                   'exact-statement-bytes-exceed?)
        bounded-size-var (ns-resolve 'jdbc.chdb.durable
                                     'statement-bytes-exceed?)]
    (with-redefs [writer/max-statement-bytes 64]
      (with-redefs-fn
        {exact-size-var
         (fn [_ _]
           (swap! exact-size-checks inc)
           false)}
        #(attempt-open
          (wal-bytes ["SELECT 1"])
          (fn [open! _ _ _]
            (reader/close! (open!))))))
    ;; This catches an unconditional call through the exact-size seam. The
    ;; forced decision below also binds the visitor to the bounded seam, so a
    ;; mutant that restores the old direct encoding cannot bypass this check.
    (check "a wire-bounded record avoids duplicate statement materialization"
           0 @exact-size-checks)
    (with-redefs [writer/max-statement-bytes 64]
      (with-redefs-fn
        {bounded-size-var (fn [_ _ _] true)}
        #(attempt-open
          (wal-bytes ["SELECT 1"])
          (fn [open! calls _ _]
            (check "record decoding uses the bounded-size decision seam"
                   ::durable/limit-exceeded (error-type open!))
            (check "a forced size rejection has no engine effect"
                   [] (engine-effects @calls)))))))

  (doseq [[label suffix]
          [["malformed JSON tail" (.getBytes "{not-json}\n" "UTF-8")]
           ["noncanonical UTF-8 valid-JSON tail"
            (concat-bytes
             (.getBytes "{\"sql\":\"SELECT '" "UTF-8")
             (concat-bytes
              (byte-array [(unchecked-byte 0xc3) 0x28])
              (.getBytes "'\"}\n" "UTF-8")))]]]
    (attempt-open
     (concat-bytes (wal-bytes ["INSERT INTO t VALUES (1)"]) suffix)
     (fn [open! calls close-count cleanup-count]
       (check (str label " is corrupt") ::durable/corrupt
              (error-type open!))
       (check (str label " executes no validated prefix") []
              (engine-effects @calls))
       (check (str label " still closes and cleans") [1 1]
              [@close-count @cleanup-count]))))

  (let [canary "replay-plan-secret-canary-must-not-escape"
        payload (concat-bytes
                 (wal-bytes [(str "SELECT '" canary "'")])
                 (.getBytes "{malformed-tail}\n" "UTF-8"))]
    (attempt-open
     payload
     (fn [open! calls _ _]
       (let [error (try (open!) nil (catch Throwable error error))
             public-error (str (str error) " " (pr-str (ex-data error)))]
         (check "a malformed tail still prevents every planned engine effect"
                [] (engine-effects @calls))
         (check "a discarded replay plan leaks no SQL canary through calls"
                false (str/includes? (pr-str @calls) canary))
         (check "a discarded replay plan leaks no SQL canary through diagnostics"
                false (str/includes? public-error canary))))))

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

  (with-redefs [writer/max-statement-bytes 1]
    (attempt-open
     (wal-bytes ["\n"])
     (fn [open! calls _ _]
       (let [opened (open!)]
         (try
           (check "a large escaped record retains its exact decoded bound"
                  ["\n"]
                  (mapv second (filter #(= :execute (first %)) @calls)))
           (finally
             (reader/close! opened)))))))

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
