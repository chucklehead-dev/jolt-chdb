(ns jdbc.chdb-durable-head-test
  (:require [clojure.data.json :as json]
            [clojure.string :as str]
            [hegel.core :as h]
            [hegel.generator :as g]
            [jdbc.chdb.durable.head :as head]))

(def failures (atom 0))

(defn- check [label expected actual]
  (if (= expected actual)
    (println "  ok  " label)
    (do
      (swap! failures inc)
      (println "  FAIL" label "- expected" (pr-str expected)
               "got" (pr-str actual)))))

(defn- error-data [f]
  (try
    (f)
    nil
    (catch Throwable error (ex-data error))))

(defn- caught-error [f]
  (try
    (f)
    nil
    (catch Throwable error error)))

(def ^:private digest-a
  "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef")
(def ^:private digest-b
  "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789")

(def valid-head
  {"protocol" {"version" 1
                "reader_features" []
                "writer_features" []}
   "engine" {"name" "chdb"
             "version" "26.7.2-rc.2"
             "backup_format" 1
             "min_reader" "26.7.2-rc.2"}
   "lease" {"generation" 3
            "owner" "worker-1"
            "instance" "unique-live-instance-id"
            "expires_at" 1788230400.0M}
   "manifest" {"db" "default"
               "base" {"key" "checkpoints/3-8-acde1234.tar.gz"
                       "size" 1048576
                       "sha256" digest-a}
               "wal" [{"key" "wal/3-9-acde5678.jsonl"
                       "size" 127
                       "sha256" digest-b}]
               "seq" 9}})

(def expected-source
  {:repository "https://github.com/chdb-io/chdb.git"
   :commit "66643e5030fb73c30ac5cdd31d4c7858ea040ed0"
   :document "docs/durable/protocol-v1.mdx"
   :section "head"})

(defn- repeat-ascii [n]
  (let [chunk (apply str (repeat 1024 "x"))
        full (quot n 1024)
        remainder (mod n 1024)]
    (str (str/join (repeat full chunk)) (subs chunk 0 remainder))))

(defn- json-at-size [byte-count]
  (let [template (assoc valid-head "unknown-padding" "")
        empty-json (json/write-str template)
        padding (- byte-count (alength (.getBytes empty-json "UTF-8")))
        document (json/write-str
                  (assoc template "unknown-padding" (repeat-ascii padding)))]
    (when-not (= byte-count (alength (.getBytes document "UTF-8")))
      (throw (ex-info "test fixture did not reach its requested byte count"
                      {:requested byte-count})))
    document))

(defn- run-deterministic-checks []
  (println "Durable V1 head.json contract")
  (check "provenance pins the exact normative protocol revision"
         expected-source head/protocol-source)
  (check "valid head returns unchanged from writer validation"
         valid-head (head/validate! valid-head :writer))
  (check "valid head round-trips as UTF-8 JSON"
         valid-head (head/decode (head/encode valid-head) :writer))
  (check "known schema failures retain a diagnostic path"
         ["engine" "name"]
         (:path (error-data
                 #(head/validate! (update valid-head "engine" dissoc "name")
                                  :writer))))
  (check "known value failures retain a diagnostic path"
         ["manifest" "seq"]
         (:path (error-data
                 #(head/validate!
                   (assoc-in valid-head ["manifest" "seq"]
                             (inc head/max-safe-integer))
                   :writer))))

  (let [extended (-> valid-head
                     (assoc "future-root"
                            {"nested" [1 true "λ"]
                             "decimal" 0.12345678901234567890123456789M})
                     (assoc-in ["protocol" "future-protocol"] {"v" 7})
                     (assoc-in ["engine" "future-engine"] "opaque")
                     (assoc-in ["lease" "future-lease"] ["keep"])
                     (assoc-in ["manifest" "base" "future-ref"] 42)
                     (assoc-in ["manifest" "wal" 0 "future-wal-ref"]
                               {"keep" false}))
        decoded (head/decode (head/encode extended) :writer)
        renewed (assoc-in decoded ["lease" "expires_at"] 1788230500.0M)
        round-tripped (head/decode (head/encode renewed) :writer)]
    (check "unknown fields survive decode and encode" extended decoded)
    (check "updating a known field preserves unknown fields"
           (select-keys extended
                        ["future-root" "protocol" "engine" "manifest"])
           (select-keys round-tripped
                        ["future-root" "protocol" "engine" "manifest"]))
    (check "unknown lease field survives a known lease renewal"
           ["keep"] (get-in round-tripped ["lease" "future-lease"])))

  (let [released (assoc valid-head "lease"
                        {"generation" 3 "owner" nil "instance" nil
                         "expires_at" nil})]
    (check "released lease retains its generation and uses three nulls"
           released (head/validate! released :writer)))

  (let [checkpoint-only
        (assoc valid-head "manifest"
               {"db" "default"
                "base" {"key" "checkpoints/3-8-acde1234.tar.gz"
                        "size" 1048576
                        "sha256" digest-a}
                "wal" []
                "seq" 8})]
    (check "checkpoint-only manifest is accepted"
           checkpoint-only (head/validate! checkpoint-only :writer)))

  (let [fresh (-> valid-head
                  (assoc "lease" {"generation" 1
                                  "owner" "worker-1"
                                  "instance" "fresh-instance"
                                  "expires_at" 1788230400.0M})
                  (assoc "manifest" {"db" "default" "base" nil
                                     "wal" [] "seq" 0}))
        wal-only (assoc fresh "manifest"
                        {"db" "default" "base" nil
                         "wal" [{"key" "wal/1-1-acde5678.jsonl"
                                 "size" 1 "sha256" digest-b}]
                         "seq" 1})]
    (check "fresh manifest with no immutable references is accepted"
           fresh (head/validate! fresh :writer))
    (check "WAL-only manifest begins replay at sequence one"
           wal-only (head/validate! wal-only :writer)))

  (let [unknown-writer (assoc-in valid-head
                                 ["protocol" "writer_features"]
                                 ["future-writer"])]
    (check "unknown writer feature remains readable"
           unknown-writer (head/validate! unknown-writer :read-only))
    (check "unknown writer feature prevents writer acquisition"
           ::head/protocol-unsupported
           (:type (error-data #(head/validate! unknown-writer :writer)))))

  (doseq [[label expected-type mutant]
          [["missing known field is corrupt" ::head/corrupt
            #(head/validate! (update valid-head "engine" dissoc "name") :writer)]
           ["future protocol fails closed" ::head/protocol-unsupported
            #(head/validate! (assoc-in valid-head ["protocol" "version"] 2)
                             :read-only)]
           ["unknown reader feature fails closed" ::head/protocol-unsupported
            #(head/validate!
              (assoc-in valid-head ["protocol" "reader_features"] ["future"])
              :read-only)]
           ["unsafe integer is corrupt" ::head/corrupt
            #(head/validate!
              (assoc-in valid-head ["manifest" "seq"]
                        (inc head/max-safe-integer)) :writer)]
           ["non-string unknown key is corrupt" ::head/corrupt
            #(head/validate! (assoc valid-head :unknown true) :writer)]
           ["partially released lease is corrupt" ::head/corrupt
            #(head/validate! (assoc-in valid-head ["lease" "owner"] nil)
                             :writer)]
           ["path traversal reference is corrupt" ::head/corrupt
            #(head/validate!
              (assoc-in valid-head ["manifest" "base" "key"]
                        "checkpoints/../escape.tar.gz") :writer)]
           ["leading-zero sequence is corrupt" ::head/corrupt
            #(head/validate!
              (assoc-in valid-head ["manifest" "base" "key"]
                        "checkpoints/3-08-acde1234.tar.gz") :writer)]
           ["zero reference generation is corrupt" ::head/corrupt
            #(head/validate!
              (assoc-in valid-head ["manifest" "base" "key"]
                        "checkpoints/0-8-acde1234.tar.gz") :writer)]
           ["oversized reference sequence is corrupt" ::head/corrupt
            #(head/validate!
              (assoc-in valid-head ["manifest" "wal" 0 "key"]
                        "wal/3-9007199254740992-acde5678.jsonl") :writer)]
           ["uppercase digest is corrupt" ::head/corrupt
            #(head/validate!
              (assoc-in valid-head ["manifest" "base" "sha256"]
                        (str/upper-case digest-a)) :writer)]
           ["manifest sequence mismatch is corrupt" ::head/corrupt
            #(head/validate! (assoc-in valid-head ["manifest" "seq"] 10)
                             :writer)]
           ["future reference generation is corrupt" ::head/corrupt
            #(head/validate!
              (assoc-in valid-head ["manifest" "wal" 0 "key"]
                        "wal/4-9-acde5678.jsonl") :writer)]]]
    (check label expected-type (:type (error-data mutant))))

  (let [with-second-wal
        (-> valid-head
            (update-in ["manifest" "wal"] conj
                       {"key" "wal/3-10-acde9999.jsonl"
                        "size" 1 "sha256" digest-a})
            (assoc-in ["manifest" "seq"] 10))
        reversed (update-in with-second-wal ["manifest" "wal"]
                            #(vec (reverse %)))]
    (check "strictly ordered WAL references are accepted"
           with-second-wal (head/validate! with-second-wal :writer))
    (check "reversed WAL order mutant is corrupt"
           ::head/corrupt
           (:type (error-data #(head/validate! reversed :writer)))))

  (let [parse-error (caught-error
                     #(head/decode
                       "{\"secret_access_key\":\"hunter2\",bad"))]
    (check "malformed JSON is corrupt"
           ::head/corrupt (:type (ex-data parse-error)))
    (check "malformed JSON error does not retain a secret value"
           false (str/includes? (str parse-error (ex-data parse-error))
                                "hunter2")))
  (check "a second trailing JSON value is corrupt"
         ::head/corrupt (:type (error-data #(head/decode "{}{}"))))
  (let [document (json/write-str valid-head)
        duplicate-owner
        (str/replace-first document
                           "\"owner\":\"worker-1\""
                           "\"owner\":\"worker-1\",\"owner\":\"worker-2\"")
        escaped-duplicate-root
        (str "{\"future\":1,\"f\\u0075ture\":2," (subs document 1))]
    (check "duplicate known object key is corrupt"
           ::head/corrupt
           (:type (error-data #(head/decode duplicate-owner))))
    (check "escaped-equivalent unknown object keys are corrupt"
           ::head/corrupt
           (:type (error-data #(head/decode escaped-duplicate-root)))))
  (let [sentinel "credential-bearing-field-name-hunter2"
        secret-key-error
        (caught-error
         #(head/validate! (assoc valid-head sentinel
                                 (inc head/max-safe-integer))
                          :writer))]
    (check "invalid unknown value is corrupt"
           ::head/corrupt (:type (ex-data secret-key-error)))
    (check "unknown field name is absent from the public error"
           false (str/includes? (str secret-key-error) sentinel))
    (check "unknown field name is absent from public error data"
           false (str/includes? (str (ex-data secret-key-error)) sentinel)))
  (check "invalid UTF-8 is corrupt"
         ::head/corrupt
         (:type (error-data #(head/decode (byte-array [-61 40])))))
  (check "UTF-8 BOM is corrupt"
         ::head/corrupt (:type (error-data #(head/decode "\uFEFF{}"))))

  (let [exact (json-at-size head/max-head-bytes)
        over (json-at-size (inc head/max-head-bytes))]
    (check "the inclusive 1 MiB boundary remains reachable"
           head/max-head-bytes
           (alength (head/encode (head/decode exact :writer))))
    (check "one byte over the head limit is rejected"
           ::head/limit-exceeded
           (:type (error-data #(head/decode over :writer))))))

(defn- run-unknown-field-property! []
  (println "Durable head unknown-field Hegel property")
  (let [result
        (h/run-test!
         {:name "chdb/durable-head-unknown-field-roundtrip"
          :database ""
          :derandomize? true
          :verbosity :quiet
          :test-cases 80}
         (fn [_]
           (g/let [suffix (g/string {:max-size 20
                                     :alphabet "abcXYZ09_-λ😀"})
                   number (g/integer -1000000 1000000)
                   text (g/string {:max-size 40
                                   :alphabet "abc XYZ09_-λ😀"})
                   flag (g/boolean)]
             (let [field (str "future-" suffix)
                   value {"number" number "text" text "flag" flag}
                   extended (assoc valid-head field value)
                   round-tripped (head/decode (head/encode extended) :writer)]
               (when-not (= value (get round-tripped field))
                 (throw (ex-info "unknown head field was not preserved"
                                 {:hegel/origin
                                  "chdb/durable-head/unknown-field"})))))))]
    (println "  hegel unknown-field-roundtrip seed" (:seed result)
             "valid" (:valid-test-cases result))
    (when-not (and (:passed? result) (not (:flaky? result)))
      (swap! failures inc)
      (println "  FAIL unknown-field-roundtrip" (pr-str result)))))

(defn run-checks! []
  (reset! failures 0)
  (run-deterministic-checks)
  (run-unknown-field-property!)
  (when-not (zero? @failures)
    (throw (ex-info (str @failures " Durable head checks failed")
                    {:failures @failures})))
  (println "all Durable head checks passed")
  true)

(defn -main [& _]
  (run-checks!))
