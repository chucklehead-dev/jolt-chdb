(ns jdbc.chdb-durable-wal-stream-test
  (:require [clojure.data.json :as json]
            [clojure.edn :as edn]
            [clojure.java.io :as io]
            [clojure.string :as str]
            [hegel.core :as h]
            [hegel.generator :as g]
            [jdbc.chdb.durable :as durable]
            [jdbc.chdb.durable.backend :as backend]
            [jdbc.chdb.durable.control :as control]
            [jdbc.chdb.durable.digest :as digest]
            [jdbc.chdb.durable.head :as head]
            [jdbc.chdb.durable.reader :as reader]
            [jdbc.chdb.durable.wal :as wal]
            [jdbc.chdb.durable.writer :as writer]
            [jdbc.chdb-durable-open-test-support :as support])
  (:import [java.nio ByteBuffer]
           [java.nio.charset CharacterCodingException Charset CodingErrorAction]
           [java.nio.file Files OpenOption]
           [java.nio.file.attribute FileAttribute]
           [java.util Arrays]))

(def failures (atom 0))

(def ^:private initial-options
  {:owner "old-writer" :instance "old-instance"
   :expires-at 100M :now 0M :clock-skew 0M
   :database "default" :engine-version "26.7.3"
   :backup-format 1 :min-reader "26.7.3"})

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
  ;; Exercise the managed fallback through immutable publication and reader
  ;; recovery; expected JSON syntax is independently qualified by the WAL
  ;; encoder corpus.
  (byte-array
   (map unchecked-byte (mapcat seq (map wal/line sql-values)))))

(defn- concat-bytes [left right]
  (let [combined (byte-array (+ (alength left) (alength right)))]
    (System/arraycopy left 0 combined 0 (alength left))
    (System/arraycopy right 0 combined (alength left) (alength right))
    combined))

(defn- raw-bytes [values]
  (byte-array (map unchecked-byte values)))

(defn- model-lf-offsets [values]
  (into []
        (keep-indexed (fn [index value]
                        (when (= 10 (bit-and 255 value)) index)))
        values))

(defn- scanned-lf-offsets [next-lf! chunks]
  (loop [remaining chunks base 0 result []]
    (if-let [chunk-values (first remaining)]
      (let [chunk (raw-bytes chunk-values)
            length (alength chunk)
            found (loop [start 0 offsets result]
                    (let [index (next-lf! chunk start length)]
                      (if (= index length)
                        offsets
                        (recur (inc index) (conj offsets (+ base index))))))]
        (recur (next remaining) (+ base length) found))
      result)))

(defn- run-lf-framing-property! [next-lf!]
  (println "Durable raw-LF framing Hegel property")
  (let [result
        (h/run-test!
         {:name "chdb/durable-wal-raw-lf-chunkings"
          :database "" :derandomize? true :verbosity :quiet :test-cases 80}
         (fn [_]
           (let [payload (vec (h/draw! (g/bytes {:max-size 256})))
                 chunks (h/draw! (g/chunkings payload))
                 expected (model-lf-offsets payload)
                 actual (scanned-lf-offsets next-lf! chunks)]
             (when-not (= expected actual)
               (throw (ex-info "raw LF offsets changed across chunking"
                               {:hegel/origin
                                "chdb/durable-wal/raw-lf-chunkings"
                                :expected expected :actual actual}))))))]
    (println "  hegel raw-lf-chunkings seed" (:seed result)
             "valid" (:valid-test-cases result))
    (when-not (and (:passed? result) (not (:flaky? result)))
      (swap! failures inc)
      (println "  FAIL raw-lf-chunkings" (pr-str result)))))

(def ^:private test-utf8-charset (Charset/forName "UTF-8"))

(defn- strict-decode-outcome [bytes]
  (try
    (let [decoder (doto (.newDecoder test-utf8-charset)
                    (.onMalformedInput CodingErrorAction/REPORT)
                    (.onUnmappableCharacter CodingErrorAction/REPORT))]
      [:ok (str (.decode decoder (ByteBuffer/wrap bytes)))])
    (catch CharacterCodingException _ [:error])))

(defn- decode-outcome [decode! bytes]
  (try [:ok (decode! bytes)] (catch Throwable _ [:error])))

(def ^:private differential-seed 24301)

(defn- seeded-raw-cases []
  (for [case-index (range 2048)]
    (let [length (+ 3 (mod (+ differential-seed (* 17 case-index)) 30))]
      (raw-bytes
       (for [byte-index (range length)]
         (mod (+ differential-seed
                 (* 73 case-index)
                 (* 151 byte-index)
                 (* 17 case-index byte-index))
              256))))))

(defn- seeded-valid-cases []
  (for [case-index (range 256)]
    (.getBytes
     (str "seed-" differential-seed "-" case-index "-"
          (apply str (repeat (+ 3 (mod case-index 41)) "x"))
          (case (mod case-index 4) 0 "β" 1 "😀" 2 "�" 3 "z"))
     "UTF-8")))

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

(defn- attempt-open
  ([payload f]
   (attempt-open payload {} f))
  ([payload observer-or-overrides f]
   (let [calls (atom [])
         close-count (atom 0)
         cleanup-count (atom 0)
         operation-overrides
         (if (map? observer-or-overrides)
           observer-or-overrides
           {:recovery-event! observer-or-overrides})
         operations
         (merge
          (support/fake-open-operations
           calls (atom [0M]) close-count cleanup-count)
          operation-overrides)]
     (f #(durable/open-reader!
          {:store (prepared-wal-store payload) :operations operations})
        calls close-count cleanup-count))))

(defn- engine-effects [calls]
  (filterv #(contains? #{:analyze-execute :execute} (first %)) calls))

(defn- phase-byte-total [events phase]
  (reduce + 0 (map :bytes (filter #(= phase (:phase %)) events))))

(def ^:private empty-wal-fixture-root
  "test/fixtures/durable/empty-referenced-wal")

(defn- fixture-bytes [relative]
  (Files/readAllBytes (.toPath (io/file empty-wal-fixture-root relative))))

(defn- empty-wal-fixture-store [fault]
  (let [store (backend/memory-backend)
        head-bytes (fixture-bytes "object/head.json")
        document (json/read-str (String. head-bytes "UTF-8"))
        key (get-in document ["manifest" "wal" 0 "key"])
        payload (fixture-bytes (str "object/" key))
        document
        (case fault
          :size (update-in document ["manifest" "wal" 0 "size"] inc)
          :digest (assoc-in document ["manifest" "wal" 0 "sha256"]
                            (apply str (repeat 64 "0")))
          document)
        head-bytes (if fault (head/encode document) head-bytes)]
    (backend/put-bytes-if-absent! store key payload)
    (backend/put-bytes-if-absent! store control/head-key head-bytes)
    {:store store :head-bytes (vec head-bytes) :payload payload}))

(defn- open-empty-wal-fixture
  ([fault] (open-empty-wal-fixture fault nil))
  ([fault observer]
   (let [{:keys [store head-bytes payload]} (empty-wal-fixture-store fault)
         calls (atom [])
         events (atom [])
         close-count (atom 0)
         cleanup-count (atom 0)
         operations
         (assoc (support/fake-open-operations
                 calls (atom [0M]) close-count cleanup-count)
                :recovery-event! (or observer #(swap! events conj %)))]
     {:result
      (try
        (let [opened (durable/open-reader! {:store store :operations operations})]
          (reader/close! opened)
          :opened)
        (catch Throwable error
          (loop [current error]
            (if current
              (or (:type (ex-data current)) (recur (.getCause current)))
              :unknown))))
      :calls @calls
      :events @events
      :close-count @close-count
      :cleanup-count @cleanup-count
      :head-before head-bytes
      :head-after (vec (backend/get-bytes store control/head-key))
      :payload-size (alength payload)})))

(def ^:private expected-empty-wal-recovery-events
  [{:event :durable/wal-integrity-verified :wal-index 0}
   {:event :durable/wal-validation-complete
    :wal-index 0 :record-count 0}])

(defn- valid-empty-wal-recovery-events? [events]
  (= expected-empty-wal-recovery-events events))

(defn run-checks! []
  (reset! failures 0)
  (println "Durable bounded WAL validation and replay")

  (let [next-lf-var (ns-resolve 'jdbc.chdb.durable 'next-lf-index)
        next-lf! @next-lf-var
        source (slurp "src/jdbc/chdb/durable.clj")
        start (str/index-of source "(defn- next-lf-index")
        end (str/index-of source "(defn- visit-wal!" start)
        scan-source (subs source start end)
        visit-end (str/index-of source "(defn- extend-replay-plan" end)
        visit-source (subs source end visit-end)]
    (check "typed LF finder retains the intended source shape"
           true
           (and (str/includes? scan-source
                               "[^bytes chunk ^long start ^long end]")
                (str/includes? scan-source
                               "(bit-and 255 (aget chunk index))")
                (str/includes? scan-source "(unchecked-inc index)")
                (not (str/includes? scan-source "(byte 10)"))))
    (check "WAL visitation reaches only the typed LF finder through observation"
           true
           (and (str/includes? scan-source
                               "(next-lf-index chunk start end)")
                (str/includes? visit-source
                               "(observed-next-lf-index")
                (not (str/includes? visit-source "(aget chunk"))))
    (doseq [[label values chunk-width]
            [["empty input" [] 1]
             ["empty records" [10 10] 1]
             ["missing final LF" [65 66] 1]
             ["lone CR is data" [65 13 66 10] 2]
             ["CRLF ends only at LF" [65 13 10 66 10] 2]
             ["LF at and around chunk boundaries"
              [10 65 10 66 10 10 67] 3]]]
      (let [chunks (partition-all chunk-width values)]
        (check (str label " retains exact raw LF offsets")
               (model-lf-offsets values)
               (scanned-lf-offsets next-lf! chunks))))
    (let [values (vec (concat (repeat 65535 65)
                              [10 13 66 10]
                              (repeat 9 67) [10]))]
      (check "64 KiB boundary framing remains exact"
             [65535 65538 65548]
             (scanned-lf-offsets next-lf!
                                 (partition-all 65536 values))))
    ;; The historical first-match-only mutant loses every later LF in a chunk.
    (check "all LF matches in one chunk are causally required"
           [0 2 4]
           (scanned-lf-offsets next-lf! [[10 65 10 66 10]]))
    (run-lf-framing-property! next-lf!))

  (let [decoder-capability-var
        (ns-resolve 'jdbc.chdb.durable 'strict-utf8-decoder-capable-result)
        malformed-probes-var
        (ns-resolve 'jdbc.chdb.durable 'strict-utf8-malformed-probes)
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
    (check "running Jolt passes strict and replacement-sentinel capability probes"
           true (require-decoder!))
    (check "startup capability covers every malformed UTF-8 class"
           [:stray-continuation :truncated-continuation
            :invalid-continuation-after-valid-lead :invalid-lead
            :obsolete-five-byte-lead
            :two-byte-overlong :three-byte-overlong :four-byte-overlong
            :encoded-surrogate :above-unicode-maximum]
           (mapv first @malformed-probes-var))
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
    (check "strict decoder preserves control, replacement, multibyte, and astral text"
           "control:\u0000\t\r replacement:\ufffd latin:\u00f5 greek:\u03b2 astral:\ud83d\ude00"
           (decode! (.getBytes
                     "control:\u0000\t\r replacement:\ufffd latin:\u00f5 greek:\u03b2 astral:\ud83d\ude00"
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
    (let [counts (atom {:cases 0 :mismatches 0 :rejected 0
                        :replacement-fallbacks 0})
          targeted [[0x80] [0xc3 0x28] [0xc0 0xaf]
                    [0xe0 0x80 0x80] [0xed 0xa0 0x80]
                    [0xf0 0x80 0x80 0x80] [0xf4 0x90 0x80 0x80]
                    [0xe2 0x82] [0xf0 0x9f 0x98]
                    [0xf0 0x9f 0x98 0x80] [0xef 0xbf 0xbd]]]
      (doseq [bytes
              (concat [(byte-array 0)]
                      (map #(raw-bytes [%]) (range 256))
                      (for [a (range 256) b (range 256)] (raw-bytes [a b]))
                      (map raw-bytes targeted)
                      (seeded-raw-cases)
                      (seeded-valid-cases))]
        (let [expected (strict-decode-outcome bytes)
              actual (decode-outcome decode! bytes)]
          (swap! counts update :cases inc)
          (when (= :error (first expected))
            (swap! counts update :rejected inc)
            (when (not= -1 (.indexOf (String. bytes "UTF-8") (int 0xfffd)))
              (swap! counts update :replacement-fallbacks inc)))
          (when-not (= expected actual)
            (swap! counts update :mismatches inc))))
      (check "guarded direct decoder matches exhaustive, targeted, and seeded strict decoding"
             {:seed differential-seed :cases 68108 :mismatches 0
              :all-rejections-exposed-replacement true}
             {:seed differential-seed :cases (:cases @counts)
              :mismatches (:mismatches @counts)
              :all-rejections-exposed-replacement
              (= (:rejected @counts) (:replacement-fallbacks @counts))}))
    (let [malformed (raw-bytes [0xc0 0xaf])
          replacement (.getBytes "�" "UTF-8")
          replacement-text (String. malformed "UTF-8")]
      (check "skipping the U+FFFD guard accepts malformed bytes"
             [:error :ok]
             [(first (strict-decode-outcome malformed))
              (first (decode-outcome #(String. % "UTF-8") malformed))])
      (check "accepting a mismatched round-trip is a live malformed-input mutant"
             [false ::durable/corrupt]
             [(Arrays/equals malformed (.getBytes replacement-text "UTF-8"))
              (error-type #(decode! malformed))])
      (check "rejecting every U+FFFD would reject a legitimate encoded scalar"
             [true [:ok "�"]]
             [(not= -1 (.indexOf (String. replacement "UTF-8") (int 0xfffd)))
              (decode-outcome decode! replacement)]))
    ;; Causal source control: ordinary records use the native String decoder,
    ;; while only text containing U+FFFD reaches the strict decoder. Restoring
    ;; either an unconditional strict decode or the old re-encode turns it red.
    (check "WAL decoding uses guarded native decode without UTF-8 re-encoding"
           true
           (and (str/includes? decode-source "(String. bytes \"UTF-8\")")
                (str/includes? decode-source ".indexOf text (int 0xfffd)")
                (str/includes? decode-source "(strict-decode-wal-text! bytes)")
                (not (str/includes? decode-source ".getBytes"))
                (not (str/includes? decode-source "Arrays/equals")))))

  (let [sql "INSERT INTO fallback_round_trip VALUES ('\ud83d\ude00\\u0000')"
        store (backend/memory-backend)
        acquired (control/acquire! store initial-options)
        writer-calls (atom [])
        writer-close-count (atom 0)
        writer
        (writer/start!
         {:store store :token (:token acquired) :handle :fake-writer
          :database "default"
          :operations
          (assoc (support/fake-open-operations writer-calls (atom [0M])
                                              writer-close-count (atom 0))
                 :cleanup-scratch! (fn [] nil))})]
    (try
      (writer/execute! writer sql)
      (writer/flush! writer)
      (let [reference (first (get-in (:head (control/read-head! store))
                                     ["manifest" "wal"]))]
        (check "stock fallback writer publishes its exact immutable WAL bytes"
               true
               (Arrays/equals (wal/line sql)
                              (backend/get-bytes store (get reference "key")))))
      (finally
        (writer/close! writer)))
    (let [reader-calls (atom [])
          reader-close-count (atom 0)
          reader-cleanup-count (atom 0)
          opened (durable/open-reader!
                  {:store store
                   :operations (support/fake-open-operations
                                reader-calls (atom [0M]) reader-close-count
                                reader-cleanup-count)})]
      (try
        (check "stock fallback writer WAL recovers through a fresh reader"
               [sql]
               (mapv second (filter #(= :execute (first %)) @reader-calls)))
        (finally
          (reader/close! opened)))))

  (let [sql-values ["SELECT '\ud83d\ude00'"
                    "SELECT 'control \u0000\t\r'"]
        payload (wal-bytes sql-values)
        store (prepared-wal-store payload)
        reference (first (get-in (:head (control/read-head! store))
                                 ["manifest" "wal"]))]
    (check "managed fallback bytes survive immutable reference publication"
           true
           (Arrays/equals payload
                          (backend/get-bytes store (get reference "key"))))
    (attempt-open
     payload
     (fn [open! calls _ _]
       (let [opened (open!)]
         (try
           (check "managed fallback astral and control JSONL replays exactly"
                  sql-values
                  (mapv second (filter #(= :execute (first %)) @calls)))
           (finally
             (reader/close! opened)))))))

  (let [payload (wal-bytes ["SELECT 'instrumentation-canary'"])
        record-wire-bytes (dec (alength payload))
        events (atom [])
        expected-phases
        [:wal-download :wal-hash :wal-lf-scan :wal-record-buffer
         :wal-record-copy :wal-decode :wal-json-parse
         :wal-plan-retention :wal-replay-classification :wal-replay-native]]
    (attempt-open
     payload {:recovery-phase! #(swap! events conj %)}
     (fn [open! calls _ _]
       (let [opened (open!)]
         (try
           (check "instrumented recovery preserves the executed statement"
                  ["SELECT 'instrumentation-canary'"]
                  (mapv second (filter #(= :execute (first %)) @calls)))
           (finally
             (reader/close! opened))))))
    (check "instrumentation records fixed recovery phases in operation order"
           expected-phases (mapv :phase @events))
    (check "instrumentation emits only bounded scalar event fields"
           true
           (every?
            (fn [event]
              (and (= #{:phase :status :calls :nanos :bytes}
                      (set (keys event)))
                   (contains? @#'durable/recovery-phase-labels (:phase event))
                   (= :complete (:status event))
                   (= 1 (:calls event))
                   (integer? (:nanos event))
                   (not (neg? (:nanos event)))
                   (integer? (:bytes event))
                   (not (neg? (:bytes event)))))
            @events))
    (check "instrumentation retains no path, key, SQL, payload, or error text"
           false
           (str/includes? (pr-str @events) "instrumentation-canary"))
    (check "retained-plan replay reports exact JSON bytes without the LF"
           [record-wire-bytes record-wire-bytes]
           [(phase-byte-total @events :wal-replay-classification)
            (phase-byte-total @events :wal-replay-native)]))

  (let [payload (wal-bytes ["SELECT 'same-effects'"])
        run
        (fn [operation-overrides]
          (let [result (atom nil)]
            (attempt-open
             payload operation-overrides
             (fn [open! calls _ _]
               (let [opened (open!)]
                 (try
                   (reset! result @calls)
                   (finally
                     (reader/close! opened))))))
            @result))]
    (check "disabled and enabled instrumentation preserve exact effects"
           (run {})
           (run {:recovery-phase! (fn [_])})))

  (let [payload (wal-bytes ["SELECT 'clock-free-default'"])
        nano-time-var (ns-resolve 'jdbc.chdb.durable 'recovery-nano-time)]
    (with-redefs-fn
      {nano-time-var
       (fn []
         (throw (ex-info "timer must remain disabled" {:type ::timer-read})))}
      #(attempt-open
        payload
        (fn [open! _ _ _]
          (reader/close! (open!)))))
    (check "default recovery performs no instrumentation clock read" true true))

  (let [payload (wal-bytes ["SELECT 'invalid-observer'"])]
    (attempt-open
     payload {:recovery-phase! false}
     (fn [open! calls close-count cleanup-count]
       (check "non-function instrumentation is rejected"
              ::durable/invalid-options (error-type open!))
       (check "invalid instrumentation fails before native recovery effects"
              [[] 0 0] [(engine-effects @calls)
                        @close-count @cleanup-count]))))

  (let [payload (wal-bytes ["SELECT 'throwing-observer-success'"])]
    (attempt-open
     payload {:recovery-phase! (fn [_]
                                 (throw (ex-info "observer failed" {})))}
     (fn [open! calls _ _]
       (let [opened (open!)]
         (try
           (check "throwing observer cannot mask successful recovery"
                  ["SELECT 'throwing-observer-success'"]
                  (mapv second (filter #(= :execute (first %)) @calls)))
           (finally
             (reader/close! opened)))))))

  (let [payload (wal-bytes ["SELECT 'primary-failure'"])
        primary (ex-info "primary classifier failure" {:type ::primary})]
    (attempt-open
     payload
     {:recovery-phase! (fn [_]
                         (throw (ex-info "observer failed" {})))
      :analyze-execute! (fn [& _] (throw primary))}
     (fn [open! calls _ _]
       (let [actual (try (open!) nil (catch Throwable error error))]
         (check "throwing observer preserves primary throwable identity"
                true (identical? primary actual))
         (check "failed classification reaches no native replay"
                [] (filterv #(= :execute (first %)) @calls))))))

  (let [payload
        (concat-bytes (wal-bytes ["INSERT INTO t VALUES (1)"])
                      (.getBytes "{malformed-tail}\n" "UTF-8"))
        events (atom [])]
    (attempt-open
     payload {:recovery-phase! #(swap! events conj %)}
     (fn [open! calls _ _]
       (check "instrumented malformed tail preserves corruption precedence"
              ::durable/corrupt (error-type open!))
       (check "instrumented malformed tail applies no validated prefix"
              [] (engine-effects @calls))))
    (check "failed JSON phase emits a terminal failure status"
           true
           (boolean
            (some #(and (= :wal-json-parse (:phase %))
                        (= :failed (:status %)))
                  @events)))
    (check "validation failure emits no replay phase"
           false
           (boolean
            (some #(contains? #{:wal-replay-classification :wal-replay-native}
                              (:phase %))
                  @events))))

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
        expected-record-bytes
        (reduce + 0 (map #(dec (alength (wal-bytes [%]))) sql-values))
        wire-limit-var (ns-resolve 'jdbc.chdb.durable
                                   'replay-plan-wire-byte-limit)
        record-limit-var (ns-resolve 'jdbc.chdb.durable
                                     'replay-plan-record-limit)]
    (doseq [[label limit-var limit]
            [["wire-byte" wire-limit-var 1]
             ["record-count" record-limit-var 1]]]
      (let [wal-decodes (atom 0)
            events (atom [])
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
            {:recovery-phase! #(swap! events conj %)}
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
               (* 2 (count sql-values)) @wal-decodes)
        (check (str label " fallback reports the same exact replay byte unit")
               [expected-record-bytes expected-record-bytes]
               [(phase-byte-total @events :wal-replay-classification)
                (phase-byte-total @events :wal-replay-native)]))))

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
        (check "unobserved retained plans allocate no phase byte vector"
               false (contains? plan :statement-wire-bytes))
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
       (fn [path observe!]
         (let [plan (validate! path observe!)]
           (when (:statements plan)
             (let [active (swap! live-plans inc)]
               (swap! max-live-plans max active)))
           plan))
       replay-var
       (fn [path plan ops handle database observe!]
         (try
           (replay! path plan ops handle database observe!)
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
    (let [events (atom [])]
      (attempt-open
       (concat-bytes (wal-bytes ["INSERT INTO t VALUES (1)"]) suffix)
       #(swap! events conj %)
       (fn [open! calls close-count cleanup-count]
         (check (str label " is corrupt") ::durable/corrupt
                (error-type open!))
         (check (str label " executes no validated prefix") []
                (engine-effects @calls))
         (check (str label " still closes and cleans") [1 1]
                [@close-count @cleanup-count])))
      (check (str label " trace stops at validation failure")
             [{:event :durable/wal-integrity-verified :wal-index 0}
              {:event :durable/wal-validation-failed :wal-index 0}]
             @events)))

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

  (let [provenance
        (edn/read-string
         (slurp (io/file empty-wal-fixture-root "provenance.edn")))
        result (open-empty-wal-fixture nil)]
    (check "empty-WAL fixture records tolerated noncanonical semantics"
           [:tolerated-noncanonical :zero-records :omit-reference :pending]
           [(:status provenance) (:reader-outcome provenance)
            (:writer-outcome provenance)
            (get-in provenance [:sources :protocol :clarification])])
    (check "empty-WAL fixture bytes match their pinned provenance"
           (mapv #(get-in provenance [:fixture % :sha256]) [:head :wal])
           (mapv #(digest/sha256-file
                   (.toPath
                    (io/file empty-wal-fixture-root
                             (get-in provenance [:fixture % :path]))))
                 [:head :wal]))
    (check "verified zero-byte referenced WAL opens as zero records"
           [:opened 0 []]
           [(:result result) (:payload-size result)
            (engine-effects (:calls result))])
    (check "zero-record validation trace stops before replay"
           expected-empty-wal-recovery-events
           (:events result))
    (check "trace validation rejects omission and replay-before-validation mutants"
           [false false]
           [(valid-empty-wal-recovery-events? (pop (:events result)))
            (valid-empty-wal-recovery-events?
             (into [{:event :durable/wal-replay-started
                     :wal-index 0 :record-count 0}]
                   (:events result)))])
    (check "read-only empty-WAL recovery preserves head and cleans once"
           [true 1 1]
           [(= (:head-before result) (:head-after result))
            (:close-count result) (:cleanup-count result)]))

  (let [observer-failure (ex-info "observer secret canary" {:secret "canary"})
        result (open-empty-wal-fixture
                nil (fn [_] (throw observer-failure)))]
    (check "recovery observation failure cannot replace successful recovery"
           [:opened [] 1 1]
           [(:result result) (engine-effects (:calls result))
            (:close-count result) (:cleanup-count result)]))

  (doseq [fault [:size :digest]]
    (let [result (open-empty-wal-fixture fault)]
      (check (str "empty-WAL " (name fault) " control is corrupt")
             ::durable/corrupt (:result result))
      (check (str "empty-WAL " (name fault)
                  " control reaches neither validation nor replay")
             [[] [] 1 1]
             [(:events result) (engine-effects (:calls result))
              (:close-count result) (:cleanup-count result)])))

  (attempt-open
   (.getBytes "{\"sql\":\"SELECT 1\"}" "UTF-8")
   (fn [open! calls _ _]
     (check "unterminated WAL is corrupt" ::durable/corrupt
            (error-type open!))
     (check "unterminated WAL has no engine effect" []
            (engine-effects @calls))))

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
