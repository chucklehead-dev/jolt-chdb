(ns jdbc.chdb-durable-open-test
  (:require [clojure.data.json :as json]
            [clojure.edn :as edn]
            [clojure.string :as str]
            [db.jdbc]
            [db.export :as export]
            [jdbc.chdb.durable :as durable]
            [jdbc.chdb.durable.backend :as backend]
            [jdbc.chdb.durable.control :as control]
            [jdbc.chdb.durable.head :as head]
            [jdbc.chdb-durable-open-test-support :as support]
            [jdbc.chdb.durable.reader :as reader]
            [jdbc.chdb.durable.writer :as writer]
            [jdbc.core :as jdbc]
            [jolt.fibers :as fibers])
  (:import [java.nio.file Files Path]))

(def failures (atom 0))

(def ^:private initial-options
  {:owner "old-writer" :instance "old-instance"
   :expires-at 100M :now 0M :clock-skew 0M
   :database "tenant`one" :engine-version "26.7.2-rc.2"
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

(defn- prepare-raw-wal-store! [store payload]
  (let [token (:token (control/acquire! store initial-options))
        publication (control/publish-wal-bytes! store token payload)]
    (control/commit-reference!
     store token {:kind :wal :reference (:reference publication)
                  :verify-reference! control/verify-byte-reference!})
    (control/release! store token)
    store))

(defn- prepared-raw-wal-store [payload]
  (prepare-raw-wal-store! (backend/memory-backend) payload))

(defn- prepared-wal-store []
  (prepared-raw-wal-store
   (.getBytes "{\"sql\":\"INSERT INTO t VALUES (1)\"}\n" "UTF-8")))

(def ^:private abc-digest
  "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad")

(def ^:private wal-digest
  "d6b567596ae76c14b2757c09a4c80f6f2dc3580e9a65ec57ec2988586fa8dd0d")

(def ^:private zero-digest
  (apply str (repeat 64 "0")))

(defn- replace-head-bytes! [store bytes]
  (let [current (control/read-head! store)
        result (backend/replace-if-match!
                store control/head-key bytes (:etag current))]
    (when-not (= :replaced (:status result))
      (throw (ex-info "test fixture could not replace head" {:result result})))
    store))

(defn- replace-head! [store document]
  (replace-head-bytes! store (head/encode document)))

(defn- reference-fixture
  [kind fault]
  (let [store (backend/memory-backend)
        acquired (control/acquire! store initial-options)
        _ (control/release! store (:token acquired))
        payload (if (= :checkpoint kind)
                  (.getBytes "abc" "UTF-8")
                  (.getBytes "{\"sql\":\"INSERT INTO t VALUES (1)\"}\n" "UTF-8"))
        actual-size (alength payload)
        actual-digest (if (= :checkpoint kind) abc-digest wal-digest)
        key (if (= :checkpoint kind)
              "checkpoints/1-1-00000021.tar.gz"
              "wal/1-1-00000022.jsonl")
        reference {"key" key
                   "size" (if (= :size fault) (inc actual-size) actual-size)
                   "sha256" (if (= :digest fault) zero-digest actual-digest)}
        current (:head (control/read-head! store))
        manifest (if (= :checkpoint kind)
                   {"db" "tenant`one" "base" reference "wal" [] "seq" 1}
                   {"db" "tenant`one" "base" nil "wal" [reference] "seq" 1})]
    (when-not (= :missing fault)
      (backend/put-bytes-if-absent! store key payload))
    (replace-head! store (assoc current "manifest" manifest))
    {:store store :reference reference :payload payload
     :actual-size actual-size :actual-digest actual-digest}))

(defn- fixture-fault-shape
  [{:keys [store reference actual-size actual-digest]}]
  (let [stored (backend/get-bytes store (get reference "key"))]
    [(boolean stored)
     (= actual-size (get reference "size"))
     (= actual-digest (get reference "sha256"))]))

(defn- open-reference-fixture
  [mode {:keys [store]}]
  (let [calls (atom [])
        close-count (atom 0)
        cleanup-count (atom 0)
        base-operations
        (support/fake-open-operations
         calls (atom [200000M 200001M]) close-count cleanup-count)
        secondary-error
        (fn [type]
          (throw (ex-info "injected cleanup failure" {:type type})))
        operations
        (assoc base-operations
               :close-native!
               (fn [handle]
                 ((:close-native! base-operations) handle)
                 (secondary-error ::secondary-close-failure))
               :cleanup-scratch!
               (fn [path]
                 ((:cleanup-scratch! base-operations) path)
                 (secondary-error ::secondary-cleanup-failure)))
        error
        (try
          (let [opened
                (case mode
                  :reader (durable/open-reader!
                           {:store store :operations operations})
                  :writer (durable/open-writer!
                           {:store store :owner "verification-writer"
                            :instance "verification-attempt"
                            :database "ignored" :lease-ttl-ms 100000M
                            :operations operations}))]
            (case mode
              :reader (reader/close! opened)
              :writer (writer/close! opened))
            nil)
          (catch Throwable caught
            (loop [current caught]
              (when current
                (or (:type (ex-data current))
                    (recur (.getCause current)))))))]
    {:error error :calls @calls :close-count @close-count
     :cleanup-count @cleanup-count
     :head (:head (control/read-head! store))}))

(defn- reshaped-head-json [document]
  ;; Mirror Python json.dumps(sort_keys=True, indent=4) without using the
  ;; production head encoder or assuming its map iteration order.
  (letfn [(padding [depth]
            (apply str (repeat (* 4 depth) " ")))
          (render [value depth]
            (cond
              (map? value)
              (if (empty? value)
                "{}"
                (str "{\n"
                     (str/join
                      ",\n"
                      (map (fn [key]
                             (str (padding (inc depth))
                                  (json/write-str key) ": "
                                  (render (get value key) (inc depth))))
                           (sort (keys value))))
                     "\n" (padding depth) "}"))

              (vector? value)
              (if (empty? value)
                "[]"
                (str "[\n"
                     (str/join
                      ",\n"
                      (map #(str (padding (inc depth))
                                 (render % (inc depth)))
                           value))
                     "\n" (padding depth) "]"))

              :else (json/write-str value)))]
    (str (render document 0) "\n")))

(defn- recording-object-backend [delegate head-writes]
  (letfn [(record! [operation key bytes result]
            (when (and (= control/head-key key)
                       (contains? #{:created :replaced} (:status result)))
              (swap! head-writes conj
                     {:operation operation :bytes (vec bytes)}))
            result)]
    (reify backend/ObjectBackend
      (get-bytes [_ key] (backend/get-bytes delegate key))
      (get-with-etag [_ key] (backend/get-with-etag delegate key))
      (put-file-if-absent! [_ key path]
        (backend/put-file-if-absent! delegate key path))
      (put-bytes-if-absent! [_ key bytes]
        (record! :create key bytes
                 (backend/put-bytes-if-absent! delegate key bytes)))
      (replace-if-match! [_ key bytes etag]
        (record! :replace key bytes
                 (backend/replace-if-match! delegate key bytes etag)))
      (download-to-file! [_ key path]
        (backend/download-to-file! delegate key path)))))

(def ^:private python-time-fixture
  "test/fixtures/durable/python-decimal-time-oracle.json")

(def ^:private conformance-inventory
  "resources/jdbc/chdb/durable_conformance_inventory.edn")

(defn- decimal-time-fixture []
  (json/read-str (slurp python-time-fixture)))

(defn- canonical-protocol-source []
  (let [source (:source (edn/read-string (slurp conformance-inventory)))]
    [(get source :repository)
     (get source :commit)
     (get-in source [:protocol :path])
     (get-in source [:protocol :sha256])]))

(defn- raw-head-expiry [bytes]
  ;; This is intentionally independent of durable.head/decode: the oracle
  ;; observes the public adapter's stored bytes and parses only generic JSON.
  (get-in (json/read-str (String. (byte-array bytes) "UTF-8") :bigdec true)
          ["lease" "expires_at"]))

(defn- captured-heads [head-writes]
  {:operations (mapv :operation @head-writes)
   :expires-at (mapv #(raw-head-expiry (:bytes %)) @head-writes)})

(defn- capture-public-open-result []
  (let [fixture (decimal-time-fixture)
        now-ms (bigdec (get-in fixture ["inputs_ms" "now"]))
        ttl-ms (bigdec (get-in fixture ["inputs_ms" "lease_ttl"]))
        heartbeat-ms
        (bigdec (get-in fixture ["inputs_ms" "heartbeat_interval"]))
        head-writes (atom [])
        store (recording-object-backend (backend/memory-backend) head-writes)
        calls (atom [])
        close-count (atom 0)
        cleanup-count (atom 0)
        operations (support/fake-open-operations
                    calls (atom [now-ms now-ms]) close-count cleanup-count)]
    (try
      (let [opened (durable/open-writer!
                    {:store store :owner "raw-byte-oracle"
                     :instance "raw-byte-attempt" :database "default"
                     :lease-ttl-ms ttl-ms
                     :heartbeat-interval-ms heartbeat-ms
                     :operations operations})]
        (try
          {:capture (captured-heads head-writes)}
          (finally (writer/close! opened))))
      (catch Throwable error
        {:capture (captured-heads head-writes)
         :error (select-keys (ex-data error) [:type :path])}))))

(defn- prepared-checkpoint-store []
  (let [store (backend/memory-backend)
        token (:token (control/acquire! store initial-options))
        reference {"key" "checkpoints/1-1-00000011.tar.gz"
                   "size" 3
                   "sha256" "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"}]
    (backend/put-bytes-if-absent! store (get reference "key")
                                  (.getBytes "abc" "UTF-8"))
    (control/commit-reference!
     store token {:kind :checkpoint :reference reference
                  :engine-metadata {:version "26.7.2-rc.2"
                                    :backup-format 1
                                    :min-reader "26.7.2-rc.2"}
                  :verify-reference! control/verify-byte-reference!})
    (control/release! store token)
    store))

(defn- prepared-released-engine-store
  [{:keys [version backup-format min-reader]}]
  (let [store (backend/memory-backend)
        acquired
        (control/acquire!
         store (assoc initial-options
                      :engine-version version
                      :backup-format backup-format
                      :min-reader min-reader))]
    (control/release! store (:token acquired))
    store))

(defn- append-wal! [store sql]
  (let [token (:token
               (control/acquire!
                store (assoc initial-options
                             :owner "later-writer" :instance "later-instance"
                             :now 200M :expires-at 300M)))
        payload (.getBytes
                 (str "{\"sql\":\"" sql "\"}\n") "UTF-8")
        publication (control/publish-wal-bytes! store token payload)]
    (control/commit-reference!
     store token {:kind :wal :reference (:reference publication)
                  :verify-reference! control/verify-byte-reference!})
    (control/release! store token)))

(defn- run-json-shape-conformance! []
  (println "Durable public-open JSON key-order and indentation conformance")
  (let [store (prepared-wal-store)
        original (backend/get-bytes store control/head-key)
        document (head/decode original :read-only)
        reshaped-text (reshaped-head-json document)
        reshaped (.getBytes reshaped-text "UTF-8")
        calls (atom [])
        close-count (atom 0)
        cleanup-count (atom 0)
        operations
        (support/fake-open-operations
         calls (atom [0M]) close-count cleanup-count)]
    (check "sorted raw head differs from the canonical stored serialization"
           false (= (vec original) (vec reshaped)))
    (check "sorted raw head recursively reorders keys with internal indentation"
           true (and (< (.indexOf reshaped-text "\"engine\"")
                        (.indexOf reshaped-text "\"protocol\""))
                     (not= -1 (.indexOf reshaped-text "\n    \"manifest\""))
                     (not= -1 (.indexOf reshaped-text
                                              "\n        \"backup_format\""))))
    (replace-head-bytes! store reshaped)
    (let [opened (durable/open-reader! {:store store :operations operations})]
      (try
        (check "sorted and internally indented head supports public read-only recovery"
               ["INSERT INTO t VALUES (1)"]
               (mapv second (filter #(= :execute (first %)) @calls)))
        (check "reshaped public reader remains queryable after recovery"
               {:labels ["value"] :rows [[11]] :count 1}
               (reader/query! opened "SELECT ?" [11]))
        (finally (reader/close! opened))))
    (check "reshaped public reader closes and removes scratch"
           [1 1] [@close-count @cleanup-count])))

(defn- run-reference-conformance! []
  (println "Durable public-open reference verification")
  ;; Positive controls prove that the fake recovery boundary can reach the
  ;; stages which each malformed reference must prevent.
  (doseq [[kind reached-stage]
          [[:checkpoint :restore]
           [:wal :execute]]]
    (let [{:keys [store]} (reference-fixture kind :valid)
          calls (atom [])
          close-count (atom 0)
          cleanup-count (atom 0)
          opened
          (durable/open-reader!
           {:store store
            :operations (support/fake-open-operations
                         calls (atom [0M]) close-count cleanup-count)})]
      (try
        (check (str "valid " (name kind)
                    " control reaches its recovery stage")
               true (boolean (some #(= reached-stage (first %)) @calls)))
        (finally (reader/close! opened)))))

  (let [outcomes (atom [])]
    (doseq [[kind fault expected-shape]
            [[:checkpoint :missing [false true true]]
             [:wal :missing [false true true]]
             [:checkpoint :size [true false true]]
             [:checkpoint :digest [true true false]]
             [:wal :size [true false true]]
             [:wal :digest [true true false]]]
            mode [:reader :writer]]
      (let [{:keys [reference] :as fixture} (reference-fixture kind fault)
            before (:head (control/read-head! (:store fixture)))
            actual-shape (fixture-fault-shape fixture)
            schema-valid?
            (map? (head/decode
                   (backend/get-bytes (:store fixture) control/head-key)
                   :read-only))
            result (open-reference-fixture mode fixture)
            forbidden (if (= :checkpoint kind)
                        #{:restore :use :analyze-execute :execute}
                        #{:analyze-execute :execute})
            reached (filterv #(contains? forbidden (first %)) (:calls result))
            after (:head result)
            after-reference
            (if (= :checkpoint kind)
              (get-in after ["manifest" "base"])
              (first (get-in after ["manifest" "wal"])))
            expected-lease
            [nil nil (inc (get-in before ["lease" "generation"]))]
            actual-lease
            [(get-in after ["lease" "owner"])
             (get-in after ["lease" "instance"])
             (get-in after ["lease" "generation"])]
            outcome-ok?
            (and (= expected-shape actual-shape)
                 schema-valid?
                 (= ::durable/corrupt (:error result))
                 (empty? reached)
                 (= [1 1] [(:close-count result) (:cleanup-count result)])
                 (= reference after-reference)
                 (or (= :reader mode) (= expected-lease actual-lease)))]
        (check (str (name kind) " " (name fault)
                    " fixture isolates the intended verification fault")
               expected-shape actual-shape)
        (check (str (name kind) " " (name fault)
                    " fixture is a schema-valid head")
               true schema-valid?)
        (check (str (name mode) " rejects " (name fault) " " (name kind)
                    " as public corrupt despite cleanup failures")
               ::durable/corrupt (:error result))
        (check (str (name mode) " " (name fault) " " (name kind)
                    " verification prevents restore or replay")
               [] reached)
        (check (str (name mode) " " (name fault) " " (name kind)
                    " closes native state and removes scratch")
               [1 1] [(:close-count result) (:cleanup-count result)])
        (check (str (name mode) " " (name fault) " " (name kind)
                    " preserves the manifest reference")
               reference after-reference)
        (when (= :writer mode)
          (check (str "writer " (name fault) " " (name kind)
                      " failure releases its acquired lease")
                 expected-lease actual-lease))
        (swap! outcomes conj {:kind kind :fault fault :mode mode
                              :ok? outcome-ok?})))
    (doseq [[label kinds faults expected-count]
            [["public-open missing checkpoint and WAL references"
              #{:checkpoint :wal} #{:missing} 4]
             ["public-open checkpoint size and digest verification"
              #{:checkpoint} #{:size :digest} 4]
             ["public-open WAL size and digest verification"
              #{:wal} #{:size :digest} 4]]]
      (let [selected (filterv #(and (contains? kinds (:kind %))
                                    (contains? faults (:fault %)))
                              @outcomes)]
        (check label [expected-count true]
               [(count selected) (every? :ok? selected)])))))

(defn- run-deterministic-checks! []
  (println "Durable V1 public writer open and recovery")
  (run-json-shape-conformance!)
  (run-reference-conformance!)
  (check "release precedence orders release after its prerelease"
         true (pos? (durable/compare-release-versions
                     "26.7.2" "26.7.2-rc.2")))
  (check "release precedence compares numeric identifiers numerically"
         true (neg? (durable/compare-release-versions
                     "26.7.2-rc.2" "26.7.2-rc.10")))
  (check "future backup formats fail closed"
         ::durable/engine-incompatible
         (error-type
          #(durable/check-engine-compatibility!
            {"engine" {"backup_format" 2 "min_reader" "26.7.0"}}
            "26.7.2")))
  (check "an older running core fails the minimum-reader gate"
         ::durable/engine-incompatible
         (error-type
          #(durable/check-engine-compatibility!
            {"engine" {"backup_format" 1 "min_reader" "26.8.0"}}
            "26.7.2")))

  (let [fixture (decimal-time-fixture)
        expected (mapv bigdec (get fixture "expected_expires_at"))
        mutant-first (bigint (get fixture "identity_mutant_first_expires_at"))
        observed (capture-public-open-result)
        conversion-var (ns-resolve 'jdbc.chdb.durable 'epoch-ms->seconds)
        mutant
        (with-redefs-fn {conversion-var identity}
          capture-public-open-result)]
    (check "Python Decimal fixture pins the requested public millisecond inputs"
           ["1788230400125" "375" "125"]
           [(get-in fixture ["inputs_ms" "now"])
            (get-in fixture ["inputs_ms" "lease_ttl"])
            (get-in fixture ["inputs_ms" "heartbeat_interval"])])
    (check "Python Decimal fixture pins the normative protocol revision"
           (canonical-protocol-source)
           [(get-in fixture ["protocol" "repository"])
            (get-in fixture ["protocol" "commit"])
            (get-in fixture ["protocol" "document"])
            (get-in fixture ["protocol" "sha256"])])
    (check "public open records acquisition then recovery renewal head bytes"
           [:create :replace]
           (get-in observed [:capture :operations]))
    (check "independent raw JSON parsing matches Python Decimal epoch seconds"
           expected (get-in observed [:capture :expires-at]))
    (check "identity mutant exposes the exact wrong-unit first raw expiry"
           mutant-first (first (get-in mutant [:capture :expires-at])))
    (check "identity mutant differs from the Python seconds oracle"
           false (= (first expected)
                    (first (get-in mutant [:capture :expires-at]))))
    (check "identity mutant has no second active lease write"
           [mutant-first]
           (filterv some? (get-in mutant [:capture :expires-at])))
    (check "identity mutant fails at the bounded wire lease-time category"
           {:type :jdbc.chdb.durable.head/corrupt
            :path ["lease" "expires_at"]}
           (:error mutant)))

  (let [store (prepared-released-engine-store
               {:version "26.6.0" :backup-format 0 :min-reader "26.6.0"})
        calls (atom [])
        close-count (atom 0)
        cleanup-count (atom 0)
        operations (support/fake-open-operations
                    calls (atom [200000M 200001M])
                    close-count cleanup-count)
        opened (durable/open-writer!
                {:store store :owner "new-writer" :instance "new-instance"
                 :database "ignored" :lease-ttl-ms 100000M
                 :operations operations})]
    (try
      (check "existing-head acquisition records the running producer version"
             ["26.7.2-rc.2" 0 "26.6.0"]
             (let [engine (get (:head (control/read-head! store)) "engine")]
               [(get engine "version")
                (get engine "backup_format")
                (get engine "min_reader")]))
      (check "checkpoint advances producer compatibility without lowering it"
             ["26.7.2-rc.2" 1 "26.7.2-rc.2"]
             (do
               (writer/checkpoint! opened)
               (let [engine (get (:head (control/read-head! store)) "engine")]
                 [(get engine "version")
                  (get engine "backup_format")
                  (get engine "min_reader")])))
      (finally (writer/close! opened))))

  (doseq [[label metadata]
          [["future backup format"
            {:version "26.8.0" :backup-format 2 :min-reader "26.6.0"}]
           ["future minimum reader"
            {:version "26.8.0" :backup-format 1 :min-reader "26.8.0"}]]]
    (let [store (prepared-released-engine-store metadata)
          before (:head (control/read-head! store))
          calls (atom [])
          operations (support/fake-open-operations
                      calls (atom [200000M]) (atom 0) (atom 0))]
      (check (str label " rejects before writer recovery")
             ::durable/engine-incompatible
             (error-type
              #(durable/open-writer!
                {:store store :owner "new-writer" :instance "new-instance"
                 :database "ignored" :lease-ttl-ms 100000M
                 :operations operations})))
      (check (str label " rejection has no native or head side effect")
             [before []]
             [(:head (control/read-head! store)) @calls])))

  (let [store (prepared-released-engine-store
               {:version "26.8.0" :backup-format 1 :min-reader "26.6.0"})
        calls (atom [])
        operations (support/fake-open-operations
                    calls (atom [200000M]) (atom 0) (atom 0))
        opened (durable/open-reader! {:store store :operations operations})]
    (try
      (check "a newer producer version is not an exact-match reader gate"
             true
             (some? (some #(= :create (first %)) @calls)))
      (finally (reader/close! opened))))

  (let [namespace (backend/memory-backend)
        alpha-store (backend/object-backend namespace "alpha")
        _ (prepare-raw-wal-store!
           alpha-store
           (.getBytes "{\"sql\":\"INSERT INTO t VALUES (7)\"}\n" "UTF-8"))
        calls (atom [])
        clocks (atom [0M])
        close-count (atom 0)
        cleanup-count (atom 0)
        operations (support/fake-open-operations calls clocks close-count cleanup-count)]
    (let [opened (durable/open-reader!
                  {:namespace-backend namespace
                   :object-id "alpha"
                   :operations operations})]
      (check "namespace/object open recovers the selected object's WAL"
             ["INSERT INTO t VALUES (7)"]
             (mapv second (filter #(= :execute (first %)) @calls)))
      (reader/close! opened))
    (check "a sibling object remains absent"
           ::durable/not-found
           (error-type #(durable/open-reader!
                         {:namespace-backend namespace
                          :object-id "beta"
                          :operations operations})))
    (check "sibling lookup does not create a head"
           nil (backend/get-with-etag namespace "beta/head.json"))
    (check "namespace/object cannot be combined with an already-scoped store"
           ::durable/invalid-options
           (error-type #(durable/open-reader!
                         {:store alpha-store
                          :namespace-backend namespace
                          :object-id "alpha"
                          :operations operations})))
    (check "namespace and object identity are an indivisible pair"
           ::durable/invalid-options
           (error-type #(durable/open-reader!
                         {:namespace-backend namespace
                          :operations operations}))))

  (let [store (prepared-wal-store)
        calls (atom [])
        clocks (atom [200M 201M])
        close-count (atom 0)
        cleanup-count (atom 0)
        opened (durable/open-writer!
                {:store store :owner "new-writer" :instance "new-instance"
                 :database "ignored-for-existing" :lease-ttl-ms 100M
                 :operations (support/fake-open-operations calls clocks close-count
                                                           cleanup-count)})]
    (check "writer open creates, selects, and replays the stored database"
           [[:create "tenant`one"] [:use "tenant`one"]
            [:analyze-execute "INSERT INTO t VALUES (1)" "tenant`one"]
            [:execute "INSERT INTO t VALUES (1)"]]
           (filterv #(contains? #{:create :use :analyze-execute :execute}
                                (first %))
                    @calls))
    (check "writer open renews only after recovery"
           [2 0.301M]
           [(get-in (:head (control/read-head! store)) ["lease" "generation"])
            (get-in (:head (control/read-head! store)) ["lease" "expires_at"])])
    (writer/execute! opened "INSERT INTO t VALUES (2)")
    (check "queued checkpoint commits a new base and covers pending WAL"
           :committed (:status (writer/checkpoint! opened)))
    (check "checkpoint advances once, replaces base, and clears WAL buffers"
           [2 true [] 0]
           [(get-in (:head (control/read-head! store)) ["manifest" "seq"])
            (boolean (get-in (:head (control/read-head! store))
                             ["manifest" "base"]))
            (get-in (:head (control/read-head! store)) ["manifest" "wal"])
            (:pending-statements (writer/status opened))])
    (writer/close! opened)
    (check "public close flushes, releases, closes, and removes scratch"
           [2 nil 1 1 :closed]
           [(get-in (:head (control/read-head! store)) ["manifest" "seq"])
            (get-in (:head (control/read-head! store)) ["lease" "owner"])
            @close-count @cleanup-count (:lifecycle (writer/status opened))]))

  (let [store (prepared-checkpoint-store)
        calls (atom [])
        clocks (atom [200M 201M])
        close-count (atom 0)
        cleanup-count (atom 0)
        operations (assoc (support/fake-open-operations calls clocks close-count cleanup-count)
                          :restore-database!
                          (fn [_ _ _]
                            (throw (ex-info "restore failed" {}))))]
    (check "a compatible full-archive restore failure is engine-incompatible"
           ::durable/engine-incompatible
           (error-type
            #(durable/open-writer!
              {:store store :owner "new-writer" :instance "new-instance"
               :database "ignored" :lease-ttl-ms 100M
               :operations operations})))
    (check "failed recovery closes, releases, and removes scratch"
           [1 1 nil]
           [@close-count @cleanup-count
            (get-in (:head (control/read-head! store)) ["lease" "owner"])]))

  (let [namespace (backend/memory-backend)
        store (backend/object-backend namespace "jdbc-object")
        calls (atom [])
        clocks (atom [0M 1M])
        close-count (atom 0)
        cleanup-count (atom 0)
        operations
        (assoc (support/fake-open-operations calls clocks close-count cleanup-count)
               :query-native!
               (fn [_ _ params]
                 {:labels ["value"] :rows [[(first params)]] :count 1})
               :execute-native!
               (fn [_ sql params]
                 (swap! calls conj [:jdbc-execute sql (vec params)])
                 {:labels [] :rows [] :count 0}))]
    (with-open [connection
                (jdbc/connection
                 {:vendor "chdb-durable"
                  :namespace-backend namespace :object-id "jdbc-object"
                  :owner "jdbc-writer" :instance "jdbc-instance"
                  :database "default" :lease-ttl-ms 300M
                  :operations operations})]
      (check "Durable writer connection reports its role"
             :writer (durable/connection-role connection))
      (check "jdbc.core adapter preserves bound parameters for reads"
             [{:value 42}]
             (jdbc/fetch connection ["SELECT ?" 42]))
      (check "writer adapter routes query-bytes through its queue"
             [1 2 3]
             (vec (:bytes (export/query-bytes
                           connection ["SELECT ?" 42]
                           {:format :parquet}))))
      (check "jdbc.core adapter admits fully materialized mutations"
             0 (jdbc/execute! connection "INSERT INTO t VALUES (42)"))
      (check "Durable JDBC flush publishes the pending WAL"
             :committed (:status (durable/flush! connection)))
      (check "jdbc.core admits a native bound Durable mutation"
             0 (jdbc/execute! connection
                              ["INSERT INTO t VALUES (?)" 43]))
      (check "Durable JDBC flush checkpoints bound values"
             :committed (:status (durable/flush! connection)))
      (check "the native operation receives the original bound value"
             true (boolean
                   (some #{[:jdbc-execute "INSERT INTO t VALUES (?)" [43]]}
                         @calls)))
      (jdbc/execute! connection "INSERT INTO t VALUES (43)")
      (check "Durable JDBC checkpoint publishes the complete database"
             :committed (:status (durable/checkpoint! connection))))
    (check "jdbc.core close owns Durable release and cleanup"
           [nil 1 1]
           [(get-in (:head (control/read-head! store)) ["lease" "owner"])
            @close-count @cleanup-count])
    (check "checkpoint replaces earlier WAL at the JDBC extension boundary"
           [3 true []]
           (let [head (:head (control/read-head! store))]
             [(get-in head ["manifest" "seq"])
              (some? (get-in head ["manifest" "base"]))
              (get-in head ["manifest" "wal"])])))

  (let [store (backend/memory-backend)
        calls (atom [])
        clocks (atom [0M])
        close-count (atom 0)
        cleanup-count (atom 0)]
    (check "read-only open reports not-found"
           ::durable/not-found
           (error-type
            #(durable/open-reader!
              {:store store
               :operations (support/fake-open-operations
                            calls clocks close-count cleanup-count)})))
    (check "read-only not-found does not create head.json"
           nil (backend/get-with-etag store control/head-key)))

  (let [store (prepared-wal-store)
        calls (atom [])
        clocks (atom [0M])
        close-count (atom 0)
        cleanup-count (atom 0)
        base-operations (support/fake-open-operations
                         calls clocks close-count cleanup-count)
        create-scratch! (:create-scratch! base-operations)
        operations
        (assoc base-operations
               :create-scratch!
               (fn [parent]
                 ;; This happens after the reader's one head read. Recovery
                 ;; must remain pinned to the earlier manifest snapshot.
                 (append-wal! store "INSERT INTO t VALUES (2)")
                 (create-scratch! parent))
               :analyze-query!
               (fn [_ sql database]
                 (when-not (= "SELECT ?" sql)
                   (throw (ex-info "not read-only"
                                   {:type ::durable/read-only-required})))
                 (swap! calls conj [:analyze-query sql database])))]
    (with-open [connection
                (jdbc/connection
                 {:vendor "chdb-durable" :backend store :read-only? true
                  :operations operations})]
      (check "Durable reader connection reports its role"
             :reader (durable/connection-role connection))
      (check "reader restores only the first immutable manifest snapshot"
             ["INSERT INTO t VALUES (1)"]
             (mapv second (filter #(= :execute (first %)) @calls)))
      (check "reader preserves bound parameters"
             [{:value 9}] (jdbc/fetch connection ["SELECT ?" 9]))
      (check "reader rejects a mutation before native execution"
             ::durable/read-only-required
             (error-type #(jdbc/execute! connection
                                          "INSERT INTO t VALUES (3)")))
      (check "reader routes query-bytes through its serialized queue"
             [1 2 3]
             (vec (:bytes (export/query-bytes
                           connection ["SELECT ?" 7]
                           {:format :arrow}))))
      (check "read-only Durable JDBC connections cannot flush"
             ::durable/read-only-required
             (error-type #(durable/flush! connection))))
    (check "reader close closes and cleans without changing lease state"
           [1 1 nil]
           [@close-count @cleanup-count
            (get-in (:head (control/read-head! store)) ["lease" "owner"])]))

  (let [store (prepared-raw-wal-store
               (byte-array [(unchecked-byte 0xc3) 0x28 0x0a]))
        calls (atom [])
        clocks (atom [0M])
        close-count (atom 0)
        cleanup-count (atom 0)]
    (check "reader rejects noncanonical UTF-8 WAL bytes"
           ::durable/corrupt
           (error-type
            #(durable/open-reader!
              {:store store
               :operations (support/fake-open-operations
                            calls clocks close-count cleanup-count)})))
    (check "corrupt reader recovery still closes and cleans"
           [1 1]
           [@close-count @cleanup-count]))

  (let [store (prepared-wal-store)
        calls (atom [])
        clocks (atom [0M])
        close-count (atom 0)
        cleanup-count (atom 0)
        take-entered (promise)
        release-take (promise)
        operations
        (assoc (support/fake-open-operations calls clocks close-count cleanup-count)
               :take-request!
               (fn [_]
                 (deliver take-entered true)
                 @release-take
                 (throw (ex-info "reader worker failed"
                                 {:type ::reader-worker-failed}))))
        opened (durable/open-reader! {:store store :operations operations})]
    @take-entered
    (let [queued (fibers/spawn
                  #(error-type #(reader/query! opened "SELECT ?" [1])))]
      (loop [remaining 1000]
        (when (and (pos? remaining) (zero? (.size (:queue opened))))
          (Thread/yield)
          (recur (dec remaining))))
      (deliver release-take true)
      (check "terminal reader worker failure resolves a queued query"
             ::reader-worker-failed (fibers/join queued)))
    (check "terminal reader worker failure closes and cleans"
           [:closed 1 1]
           [(:lifecycle (reader/status opened))
            @close-count @cleanup-count])
    (check "reader close after terminal failure returns the same cause"
           ::reader-worker-failed
           (error-type #(reader/close! opened))))
  (with-open [connection (jdbc/connection "chdb::memory:")]
    (check "ordinary chDB connection is not a Durable role"
           true
           (try
             (durable/connection-role connection)
             false
             (catch Throwable _ true))))
  true)

(defn run-checks! []
  (reset! failures 0)
  (run-deterministic-checks!)
  (when-not (zero? @failures)
    (throw (ex-info (str @failures " Durable open checks failed")
                    {:failures @failures})))
  (println "all Durable open checks passed")
  true)

(defn -main [& _]
  (run-checks!))
