(ns jdbc.chdb-durable-local-test
  (:require [jdbc.chdb.durable.backend :as backend])
  (:import [java.io File]
           [java.nio.file Files OpenOption Path StandardOpenOption]
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

(defn- skip-exact! [input offset buffer]
  (loop [remaining offset]
    (when (pos? remaining)
      (let [n (.read input buffer 0 (min remaining (alength buffer)))]
        (when (not (pos? n))
          (throw (ex-info "test source is shorter than its offset" {})))
        (recur (- remaining n))))))

(defn- test-copy-file-range! [source target source-offset target-offset]
  (when-not (= target-offset (Files/size target))
    (throw (ex-info "test target offset does not match its size" {})))
  (let [buffer (byte-array 65536)
        output-options (if (zero? target-offset)
                         (into-array OpenOption
                                     [StandardOpenOption/TRUNCATE_EXISTING
                                      StandardOpenOption/WRITE])
                         (into-array OpenOption [StandardOpenOption/APPEND]))]
    (with-open [input (Files/newInputStream source (into-array OpenOption []))
                output (Files/newOutputStream target output-options)]
      (skip-exact! input source-offset buffer)
      (loop [total 0]
        (let [n (.read input buffer 0 (alength buffer))]
          (cond
            (neg? n) total
            (zero? n) (throw (ex-info "test source returned a zero-byte read" {}))
            :else (do (.write output buffer 0 n)
                      (recur (+ total n)))))))))

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
                          (into-array FileAttribute [])))
  (create-private-file! [_ path]
    (if (Files/exists path (make-array java.nio.file.LinkOption 0))
      false
      (do (Files/createFile path (into-array FileAttribute [])) true)))
  (copy-file-range! [_ source target source-offset target-offset]
    (test-copy-file-range! source target source-offset target-offset)))

(deftype FailFileSyncDurability [lock]
  backend/LocalDurability
  (with-exclusive-lock [_ _ f] (locking lock (f)))
  (sync-file! [_ _]
    (throw (ex-info "injected file sync failure" {:type ::injected-sync})))
  (sync-directory! [_ _] nil)
  (create-private-directory! [_ path]
    (Files/createDirectory path (into-array FileAttribute []))
    true)
  (create-private-temp-file! [_ parent]
    (Files/createTempFile parent ".jchdb-" ".tmp"
                          (into-array FileAttribute [])))
  (create-private-file! [_ path]
    (if (Files/exists path (make-array java.nio.file.LinkOption 0))
      false
      (do (Files/createFile path (into-array FileAttribute [])) true)))
  (copy-file-range! [_ source target source-offset target-offset]
    (test-copy-file-range! source target source-offset target-offset)))

(defn- octets [store key]
  (some-> (backend/get-bytes store key) vec))

(defn- patterned-bytes [length]
  (let [result (byte-array length)]
    (doseq [i (range length)]
      (aset-byte result i (byte (- (mod i 251) 125))))
    result))

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

      (let [source (.resolve root "checkpoint-source.bin")
            target (.resolve root "checkpoint-download.bin")
            payload (patterned-bytes (+ (* 2 65536) 17))]
        (Files/write source payload (into-array OpenOption []))
        (check "streaming file upload conditionally creates" :created
               (:status (backend/put-file-if-absent!
                         store "checkpoints/one.tar" source)))
        (Files/write source (byte-array [99]) (into-array OpenOption []))
        (check "streaming download reports exact payload bytes"
               {:status :downloaded :byte-count (alength payload)}
               (backend/download-to-file!
                store "checkpoints/one.tar" target))
        (check "uploaded file is independent of later source mutation"
               (vec payload) (vec (Files/readAllBytes target)))
        (let [error (caught #(backend/download-to-file!
                              store "checkpoints/one.tar" target))]
          (check "download never overwrites an existing path"
                 ::backend/destination-exists (:type (ex-data error)))
          (check "rejected overwrite preserves the destination"
                 (vec payload) (vec (Files/readAllBytes target))))
        (let [missing (.resolve root "missing-download.bin")]
          (check "missing object download is explicit"
                 {:status :not-found}
                 (backend/download-to-file! store "missing.bin" missing))
          (check "missing object does not create a destination" false
                 (Files/exists missing
                               (make-array java.nio.file.LinkOption 0))))
        (let [failing (backend/local-backend
                       root (FailFileSyncDurability. (Object.)))
              failed-target (.resolve root "failed-sync-download.bin")]
          (check "injected download sync failure is observable"
                 ::injected-sync
                 (error-type #(backend/download-to-file!
                               failing "checkpoints/one.tar" failed-target)))
          (check "sync-failed download removes its partial destination" false
                 (Files/exists failed-target
                               (make-array java.nio.file.LinkOption 0)))))

      (let [corrupt (.resolve (.resolve root "objects") "corrupt.bin")]
        (Files/write corrupt (byte-array [1 2 3]) (into-array OpenOption []))
        (check "truncated envelope fails closed" ::backend/invalid-envelope
               (error-type #(backend/get-bytes store "corrupt.bin")))
        (let [target (.resolve root "corrupt-download.bin")]
          (check "corrupt streaming download fails closed"
                 ::backend/invalid-envelope
                 (error-type #(backend/download-to-file!
                               store "corrupt.bin" target)))
          (check "failed download removes its partial destination" false
                 (Files/exists target
                               (make-array java.nio.file.LinkOption 0)))))

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
