(ns jdbc.chdb-durable-local-test
  (:require [jdbc.chdb.durable.backend :as backend])
  (:import [java.io File]
           [java.nio.file Files OpenOption Path]
           [java.nio.file.attribute FileAttribute]))

(def failures (atom 0))

(defn- check [label expected actual]
  (if (= expected actual)
    (println "  ok  " label)
    (do
      (swap! failures inc)
      (println "  FAIL" label "- expected" (pr-str expected)
               "got" (pr-str actual)))))

(defn- error-type [f]
  (try (f) nil (catch Throwable error (:type (ex-data error)))))

(defn- caught [f]
  (try (f) nil (catch Throwable error error)))

(defn- delete-tree! [^File file]
  (when (.isDirectory file)
    (doseq [child (.listFiles file)]
      (delete-tree! child)))
  (.delete file))

(deftype TestDurability [lock events]
  backend/LocalDurability
  (with-exclusive-lock [_ _ f]
    (locking lock
      (swap! events conj :lock)
      (f)))
  (sync-file! [_ _]
    (swap! events conj :sync-file))
  (sync-directory! [_ _]
    (swap! events conj :sync-directory))
  (create-private-directory! [_ path]
    (Files/createDirectory path (into-array FileAttribute []))
    true)
  (create-private-temp-file! [_ parent]
    (Files/createTempFile parent ".jchdb-" ".tmp"
                          (into-array FileAttribute []))))

(defn- octets [store key]
  (some-> (backend/get-bytes store key) vec))

(defn run-checks! []
  (reset! failures 0)
  (println "Durable local shared-provider contract")
  (let [root (Files/createTempDirectory
              "jchdb-local-test-" (into-array FileAttribute []))
        events (atom [])
        store (backend/local-backend root (TestDurability. (Object.) events))]
    (try
      (reset! events [])
      (check "absent get returns nil" nil
             (backend/get-bytes store "head.json"))
      (let [input (byte-array [1 2 3])
            created (backend/put-bytes-if-absent!
                     store "wal/0001.bin" input)]
        (System/arraycopy (byte-array [99]) 0 input 0 1)
        (check "conditional create succeeds" :created (:status created))
        (check "provider owns caller bytes" [1 2 3]
               (octets store "wal/0001.bin"))
        (check "publication syncs file before parent directory"
               true
               (< (.indexOf @events :sync-file)
                  (.lastIndexOf @events :sync-directory)))
        (check "second create loses" :precondition-failed
               (:status (backend/put-bytes-if-absent!
                         store "wal/0001.bin" (byte-array [4]))))
        (let [etag (:etag (backend/get-with-etag store "wal/0001.bin"))
              winner (backend/replace-if-match!
                      store "wal/0001.bin" (byte-array [4]) etag)
              stale (backend/replace-if-match!
                     store "wal/0001.bin" (byte-array [5]) etag)]
          (check "same-snapshot first replacement wins"
                 :replaced (:status winner))
          (check "same-snapshot second replacement is stale"
                 :precondition-failed (:status stale))
          (check "stale replacement cannot overwrite" [4]
                 (octets store "wal/0001.bin"))
          (check "replacement advances opaque ETag" false
                 (= etag (:etag winner)))))

      (let [corrupt (.resolve (.resolve root "objects") "corrupt.bin")]
        (Files/write corrupt (byte-array [1 2 3]) (into-array OpenOption []))
        (check "truncated envelope fails closed" ::backend/invalid-envelope
               (error-type #(backend/get-bytes store "corrupt.bin"))))

      (let [object (.resolve (.resolve root "objects") "wal/0001.bin")
            corrupt (Files/readAllBytes object)]
        ;; Magic is eight bytes; the next 36 bytes must be a canonical UUID.
        (aset-byte corrupt 8 (byte (int \z)))
        (Files/write object corrupt (into-array OpenOption []))
        (check "invalid envelope ETag fails closed" ::backend/invalid-envelope
               (error-type #(backend/get-bytes store "wal/0001.bin"))))

      (finally
        (delete-tree! (File. (str ^Path root))))))
  (let [blocker (Files/createTempFile
                 "jchdb-local-blocker-" ".tmp"
                 (into-array FileAttribute []))
        requested (str blocker "/private-root")]
    (try
      (let [error (caught #(backend/local-backend
                            requested (TestDurability. (Object.) (atom []))))
            public (str (ex-message error) " " (pr-str (ex-data error)))]
        (check "NIO failures are sanitized" ::backend/local-io-failed
               (:type (ex-data error)))
        (check "NIO failure omits the private path" false
               (.contains public (str blocker))))
      (finally
        (Files/deleteIfExists blocker))))
  (when-not (zero? @failures)
    (throw (ex-info (str @failures " Durable local checks failed")
                    {:failures @failures})))
  (println "all Durable local shared-provider checks passed")
  true)

(defn -main [& _]
  (run-checks!))
