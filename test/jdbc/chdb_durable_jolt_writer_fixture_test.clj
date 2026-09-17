(ns jdbc.chdb-durable-jolt-writer-fixture-test
  "Export decoded logical protocol bytes from a real public Jolt writer."
  (:require [clojure.data.json :as json]
            [clojure.string :as str]
            [jdbc.chdb.durable :as durable]
            [jdbc.chdb.durable.backend :as backend]
            [jdbc.chdb.durable.control :as control]
            [jdbc.chdb.durable.digest :as digest]
            [jdbc.chdb.durable.local-posix :as local-posix]
            [jdbc.chdb.durable.writer :as writer])
  (:import [java.nio.file Files OpenOption Paths]
           [java.nio.file.attribute FileAttribute]))

(def ^:private empty-attributes (make-array FileAttribute 0))
(def ^:private empty-options (make-array OpenOption 0))

(defn- fail! [message]
  (throw (ex-info message {:type ::invalid-fixture})))

(defn- with-close! [body close]
  (let [outcome (try {:value (body)}
                     (catch Throwable error {:error error}))
        cleanup-error (try (close) nil (catch Throwable error error))]
    (cond
      (:error outcome) (throw (:error outcome))
      cleanup-error (throw cleanup-error)
      :else (:value outcome))))

(defn- verify-close-precedence! []
  (doseq [[body-fails? close-fails?] [[false false] [true false]
                                     [false true] [true true]]]
    (let [calls (atom [])
          primary (ex-info "body failure" {})
          cleanup (ex-info "close failure" {})
          observed (try
                     {:value
                      (with-close!
                        #(do (swap! calls conj :body)
                             (if body-fails? (throw primary) :result))
                        #(do (swap! calls conj :close)
                             (when close-fails? (throw cleanup))))}
                     (catch Throwable error {:error error}))
          expected (cond body-fails? primary close-fails? cleanup)]
      (when-not (and (= [:body :close] @calls)
                     (if expected
                       (identical? expected (:error observed))
                       (= :result (:value observed))))
        (fail! "pure close precedence control failed"))))
  (println "all four pure body/close identity and exactly-once controls passed"))

(defn- safe-key! [key]
  (when-not (and (string? key)
                (not (str/starts-with? key "/"))
                (not (str/includes? key "\\"))
                (every? #(and (not (str/blank? %))
                              (not (contains? #{"." ".."} %)))
                        (str/split key #"/" -1)))
    (fail! "unsafe referenced key"))
  key)

(defn -main [& args]
  (when-not (<= 1 (count args) 2) (fail! "usage: OUTPUT_DIR [wal|checkpoint]"))
  (verify-close-precedence!)
  (let [kind (or (second args) "wal")
        _ (when-not (contains? #{"wal" "checkpoint"} kind) (fail! "unknown fixture kind"))
        checkpoint? (= kind "checkpoint")
        base-head (atom nil)
        folded-wal (atom nil)
        root (.toAbsolutePath (Paths/get (first args) (make-array String 0)))
        provider (.resolve root "jolt-local-provider")
        scratch (.resolve root "scratch")
        object-id "jolt-writer"
        logical (.resolve root (str "fixture-store/" object-id))]
    (when (Files/exists root (make-array java.nio.file.LinkOption 0))
      (fail! "output directory must be absent"))
    (Files/createDirectories scratch empty-attributes)
    (let [namespace (local-posix/local-backend (str provider))
          store (backend/object-backend namespace object-id)
          opened (durable/open-writer!
                  {:namespace-backend namespace :object-id object-id
                   :owner "issue47-jolt-writer" :instance "fixture"
                   :database "fixture" :lease-ttl-ms 30000
                   :scratch-parent (str scratch)})]
      (with-close!
        #(do
          (writer/execute! opened
                         "CREATE TABLE events (n Int64, ok Bool, label String) ENGINE=MergeTree ORDER BY n")
          (writer/execute! opened
                         (str "INSERT INTO events VALUES (1, true, 'snowman ☃'), (2, false, 'question ?')"
                              (when-not checkpoint? ", (3, true, 'comma,quote')")))
          (when-not (= :committed (:status (writer/flush! opened)))
            (fail! "WAL flush did not commit"))
          (when checkpoint?
            (reset! folded-wal (get-in (:head (control/read-head-read-only! store)) ["manifest" "wal" 0 "key"]))
            (when-not (= :committed (:status (writer/checkpoint! opened)))
              (fail! "checkpoint did not commit"))
            (reset! base-head (:head (control/read-head-read-only! store)))
            (when-not (and (= 2 (get-in @base-head ["manifest" "seq"]))
                           (map? (get-in @base-head ["manifest" "base"]))
                           (empty? (get-in @base-head ["manifest" "wal"])))
              (fail! "checkpoint did not fold the initial WAL"))
            (writer/execute! opened "INSERT INTO events VALUES (3, true, 'comma,quote')")
            (when-not (= :committed (:status (writer/flush! opened)))
              (fail! "suffix WAL flush did not commit"))))
        #(writer/close! opened))
      (backend/put-bytes-if-absent! store "unreferenced-provider.canary"
                                    (.getBytes "must-not-be-exported" "UTF-8"))
      (let [head (:head (control/read-head-read-only! store))
            manifest (get head "manifest")
            wal (get manifest "wal")]
        (when-not (and (= "26.7.3" (get-in head ["engine" "version"]))
                       (= "26.7.3" (get-in head ["engine" "min_reader"]))
                       (nil? (get-in head ["lease" "owner"]))
                       (= "fixture" (get manifest "db"))
                       (= 1 (get-in head ["engine" "backup_format"]))
                       (if checkpoint?
                         (= (get manifest "base") (get-in @base-head ["manifest" "base"]))
                         (nil? (get manifest "base")))
                       (= (if checkpoint? 3 1) (get manifest "seq")) (= 1 (count wal)))
          (fail! "unexpected released WAL-only head"))
        (let [entries
              (mapv
               (fn [key]
                 (let [key (safe-key! key)
                       target (.resolve logical key)
                       bytes (backend/get-bytes store key)]
                   (when-not bytes (fail! "referenced object is missing"))
                   (Files/createDirectories (.getParent target) empty-attributes)
                   (Files/write target bytes empty-options)
                   {:key key :bytes (alength bytes)
                    :sha256 (digest/sha256-file target)}))
               (sort (concat ["head.json"] (map #(get % "key") wal)
                             (when checkpoint? [(get-in manifest ["base" "key"])]))))]
          (when checkpoint?
            (let [snapshot (.resolve root (str "base-only-store/" object-id))
                  key (safe-key! (get-in manifest ["base" "key"]))
                  target (.resolve snapshot key)]
              (Files/createDirectories (.getParent target) empty-attributes)
              (Files/write target (backend/get-bytes store key) empty-options)
              (spit (str (.resolve snapshot "head.json")) (json/write-str @base-head))))
          (when-not (backend/get-bytes store "unreferenced-provider.canary")
            (fail! "canary control was not established"))
          (when (and checkpoint? (not (backend/get-bytes store @folded-wal)))
            (fail! "folded WAL exclusion control was not established"))
          (spit (str (.resolve root "fixture.json"))
                (json/write-str {:schema_version 1 :object_id object-id
                                 :engine "26.7.3" :database "fixture"
                                 :inventory entries
                                 :excluded (cond-> ["unreferenced-provider.canary"] checkpoint? (conj @folded-wal))}))
          (println "exported released Jolt logical object" kind "; canary and obsolete objects excluded"))))))
