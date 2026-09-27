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
      (try (f) (finally (swap! events conj :unlock)))))
  (sync-file! [_ _]
    (swap! events conj :sync-file))
  (sync-directory! [_ _]
    (swap! events conj :sync-directory))
  (create-private-directory! [_ path]
    (Files/createDirectory path (into-array FileAttribute []))
    true)
  (create-private-temp-file! [_ parent]
    (swap! events conj :create-temp)
    (Files/createTempFile parent ".jchdb-" ".tmp"
                          (into-array FileAttribute [])))
  (create-private-file! [_ path]
    (swap! events conj :create-file)
    (if (Files/exists path (make-array java.nio.file.LinkOption 0))
      false
      (do (Files/createFile path (into-array FileAttribute [])) true)))
  (copy-file-range! [_ source target source-offset target-offset]
    (swap! events conj :copy)
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

      (let [alpha (backend/object-backend store "alpha")
            beta (backend/object-backend store "beta")
            source (.resolve root "alpha-checkpoint-source.bin")
            target (.resolve root "alpha-checkpoint-download.bin")]
        (check "local namespace publishes an object-scoped head" :created
               (:status (backend/put-bytes-if-absent!
                         alpha "head.json" (byte-array [21]))))
        (check "local sibling cannot observe another object's head" nil
               (backend/get-bytes beta "head.json"))
        (check "local namespace stores the scoped physical key" [21]
               (octets store "alpha/head.json"))
        (Files/write source (byte-array [22 23]) (into-array OpenOption []))
        (check "object-scoped checkpoint upload stays streaming" :created
               (:status (backend/put-file-if-absent!
                         alpha "checkpoints/one.tar" source)))
        (check "sibling object cannot download the checkpoint"
               {:status :not-found}
               (backend/download-to-file!
                beta "checkpoints/one.tar" target))
        (check "missing sibling checkpoint creates no destination" false
               (Files/exists target
                             (make-array java.nio.file.LinkOption 0)))
        (check "selected object downloads its exact checkpoint"
               {:status :downloaded :byte-count 2}
               (backend/download-to-file!
                alpha "checkpoints/one.tar" target))
        (check "selected object checkpoint bytes remain exact" [22 23]
               (vec (Files/readAllBytes target))))

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

(defn run-digest-checks!
  "Optional digest/control checks, deliberately separate from the native-free
  backend-only runner. The focused digest test invokes this entrypoint."
  []
  (reset! failures 0)
  (let [resolve-var #'clojure.core/requiring-resolve
        resolve! @resolve-var
        hash-symbol 'jdbc.chdb.durable.digest/hash+count-input-stream
        hash-var (requiring-resolve hash-symbol)
        hash-input @hash-var
        hash-bytes (requiring-resolve 'jdbc.chdb.durable.digest/sha256-bytes)
        verify (requiring-resolve 'jdbc.chdb.durable.control/verify-file-reference!)
        root (Files/createTempDirectory "jchdb-direct-digest-"
                                        (into-array FileAttribute []))
        events (atom [])
        store (backend/local-backend root (TestDurability. (Object.) events))
        captured (atom nil)]
    (try
      (doseq [length [0 3 (+ (* 2 65536) 17)]]
        (let [payload (patterned-bytes length)
              key (str "wal/" length ".bin")
              reference {"key" key "size" length
                         "sha256" (if (zero? length)
                                    "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
                                    (hash-bytes payload))}]
          (backend/put-bytes-if-absent! store key payload)
          (reset! events [])
          (check "direct verification reads stored body without copy/create/sync"
                 reference
                 (with-redefs-fn
                   {resolve-var (fn [symbol]
                                  (when (= hash-symbol symbol)
                                    (check "digest resolution occurs before lock"
                                           [] @events)
                                    (swap! events conj :resolve))
                                  (resolve! symbol))
                    hash-var (fn [input]
                               (check "digest executes under the provider lock"
                                      :lock (last @events))
                               (swap! events conj :hash)
                               (reset! captured input)
                               (hash-input input))}
                   #(verify store reference)))
          (check "direct verification has no scratch materialization events"
                 [:resolve :lock :hash :unlock] @events)
          (check "backend closes the borrowed digest stream" true
                 (some? (caught #(.read @captured))))
          (check "direct summary has independent size and consumed count"
                 {:status :digested :size length :byte-count length
                  :sha256 (get reference "sha256")}
                 (backend/digest-object store key))))
      (let [key "wal/3.bin"
            reference {"key" key "size" 3
                       "sha256" (hash-bytes (patterned-bytes 3))}
            object (.resolve (.resolve root "objects") key)
            original (Files/readAllBytes object)
            corrupt (aclone original)]
        (aset-byte corrupt 44 (byte 17))
        (doseq [[label bytes]
                [["same-sized stored corruption" corrupt]
                 ["extra stored body byte" (byte-array (concat original [42]))]
                 ["truncated stored body" (byte-array (take 46 original))]]]
          (Files/write object bytes (into-array OpenOption []))
          (check label :jdbc.chdb.durable.control/object-unverified
                 (error-type #(verify store reference))))
        (Files/write object original (into-array OpenOption []))
        (reset! events [])
        (check "resolution failure occurs without taking the provider lock"
               [::resolve-failed []]
               [(with-redefs-fn
                  {resolve-var (fn [symbol]
                                 (if (= hash-symbol symbol)
                                   (throw (ex-info "injected resolution failure"
                                                   {:type ::resolve-failed}))
                                   (resolve! symbol)))}
                  #(error-type (fn [] (verify store reference))))
                @events])
        (let [failure (ex-info "injected digest read failure" {:type ::injected-read})]
          (reset! events [])
          (check "digest error propagates without a successful result" failure
                 (with-redefs-fn {hash-var (fn [input]
                                           (reset! captured input)
                                           (throw failure))}
                   #(caught (fn [] (verify store reference)))))
          (check "digest error releases the provider lock" :unlock (last @events))
          (check "digest error closes its input" true
                 (some? (caught #(.read @captured))))
          (check "verification works after a failed digest" reference
                 (verify store reference)))
        (let [error (with-redefs-fn
                      {hash-var (fn [_] (throw (java.io.IOException. (str root))))}
                      #(caught (fn [] (verify store reference))))]
          (check "untyped digest I/O failure is sanitized" ::backend/local-io-failed
                 (:type (ex-data error)))
          (check "digest failure does not disclose provider path" false
                 (.contains (str (ex-message error) (pr-str (ex-data error)))
                            (str root))))
        (Files/write object (byte-array [1 2 3]) (into-array OpenOption []))
        (check "direct digest rejects a truncated envelope" ::backend/invalid-envelope
               (error-type #(backend/digest-object store key)))
        (aset-byte original 8 (byte (int \z)))
        (Files/write object original (into-array OpenOption []))
        (check "direct digest validates envelope ETag" ::backend/invalid-envelope
               (error-type #(backend/digest-object store key))))
      (let [alpha (backend/object-backend store "alpha")
            beta (backend/object-backend store "beta")]
        (backend/put-bytes-if-absent! alpha "body" (byte-array [5]))
        (check "scoped direct digest uses the prefixed object" 1
               (:byte-count (backend/digest-object alpha "body")))
        (check "scoped digest cannot observe its sibling" {:status :not-found}
               (backend/digest-object beta "body"))
        (check "scoped unsupported backend stays explicit" {:status :unsupported}
               (backend/digest-object
                (backend/object-backend (backend/memory-backend) "a") "body")))
      (check "direct digest missing object is explicit" {:status :not-found}
             (backend/digest-object store "missing"))
      (check "direct digest rejects unsafe keys" ::backend/invalid-key
             (error-type #(backend/digest-object store "../outside")))
      (let [target (.resolve root "outside")
            link (.resolve (.resolve root "objects") "linked")]
        (Files/write target (byte-array [1]) (into-array OpenOption []))
        (Files/createSymbolicLink link target (into-array FileAttribute []))
        (try
          (check "direct digest rejects symbolic links" ::backend/unsafe-local-root
                 (error-type #(backend/digest-object store "linked")))
          (finally (Files/deleteIfExists link))))
      (finally (delete-tree! (File. (str root))))))
  (when-not (zero? @failures)
    (throw (ex-info "Durable direct digest checks failed" {:failures @failures})))
  (println "all Durable direct digest checks passed")
  true)

(defn -main [& _]
  (run-checks!))
