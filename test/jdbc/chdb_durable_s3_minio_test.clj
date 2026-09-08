(ns jdbc.chdb-durable-s3-minio-test
  (:require [jdbc.chdb.durable.backend :as backend]
            [jdbc.chdb.durable.control :as control]
            [jdbc.chdb.durable.s3 :as s3]
            [jdbc.chdb.durable.s3-curl :as s3-curl])
  (:import [java.nio.file Files OpenOption]
           [java.nio.file.attribute FileAttribute]
           [java.util.concurrent CountDownLatch]))

(def failures (atom 0))

(defn- check [label expected actual]
  (if (= expected actual)
    (println "  ok  " label)
    (do (swap! failures inc)
        (println "  FAIL" label "- expected" (pr-str expected)
                 "got" (pr-str actual)))))

(defn- error-type [f]
  (try (f) nil (catch Throwable error (:type (ex-data error)))))

(defn -main [endpoint]
  (reset! failures 0)
  (println "Durable V1 MinIO S3 conformance")
  (let [auth {:access-key "MINIOACCESS" :secret-key "MINIOSECRET"}
        create-bucket
        (s3-curl/request!
         {:method :put :url (str endpoint "/durable-conformance")
          :headers {}
          :request-body {:bytes (byte-array 0) :byte-count 0}
          :auth auth :region "us-east-1"
          :connect-timeout-ms 5000 :timeout-ms 30000})
        namespace
        (s3/s3-backend
         {:endpoint endpoint :bucket "durable-conformance"
          :prefix "qualification" :region "us-east-1"
          :access-key (:access-key auth) :secret-key (:secret-key auth)
          :max-attempts 1 :connect-timeout-ms 5000 :timeout-ms 30000})
        store (backend/object-backend namespace "object-a")]
    (check "signed CreateBucket succeeds against MinIO" 200
           (:status create-bucket))
    (check "missing object remains absent" nil
           (backend/get-bytes store "head.json"))
    (check "native immutable create wins exactly once"
           [:created :precondition-failed]
           [(:status (backend/put-bytes-if-absent!
                      store "cas/value" (byte-array [1])))
            (:status (backend/put-bytes-if-absent!
                      store "cas/value" (byte-array [2])))])
    (let [start (CountDownLatch. 1)
          contender (fn [value]
                      (future
                        (.await start)
                        (:status (backend/put-bytes-if-absent!
                                  store "cas/concurrent"
                                  (byte-array [value])))))
          first (contender 1)
          second (contender 2)]
      (.countDown start)
      (check "concurrent native creates have exactly one winner"
             {:created 1 :precondition-failed 1}
             (frequencies [@first @second])))
    (let [{stale-etag :etag} (backend/get-with-etag store "cas/value")]
      (check "native If-Match replacement succeeds"
             :replaced
             (:status (backend/replace-if-match!
                       store "cas/value" (byte-array [3]) stale-etag)))
      (check "stale native If-Match loses"
             :precondition-failed
             (:status (backend/replace-if-match!
                       store "cas/value" (byte-array [4]) stale-etag))))

    (let [acquire-options
          {:owner "writer-a" :instance "instance-a"
           :now 100M :expires-at 300M
           :database "default" :engine-version "26.7.2-rc.2"
           :backup-format 1 :min-reader "26.7.2-rc.2"}
          acquired (control/acquire! store acquire-options)
          token (:token acquired)]
      (check "real S3 head create acquires generation one"
             1 (:generation token))
      (check "real S3 live competing lease is rejected"
             ::control/lease-held
             (error-type #(control/acquire!
                           store (assoc acquire-options
                                        :owner "writer-b"
                                        :instance "instance-b"))))
      (let [publication
            (control/publish-wal-bytes!
             store token (.getBytes "INSERT INTO t VALUES (1)\n" "UTF-8"))
            committed
            (control/commit-reference!
             store token {:kind :wal :reference (:reference publication)
                          :verify-reference! control/verify-byte-reference!})]
        (check "real immutable WAL publication is verified" :published
               (:status publication))
        (check "real head CAS commits exactly one WAL reference"
               [1 1]
               [(get-in (:head committed) ["manifest" "seq"])
                (count (get-in (:head committed) ["manifest" "wal"]))])))

    (let [source (Files/createTempFile
                  "jchdb-minio-upload-" ".bin" (make-array FileAttribute 0))
          target (Files/createTempFile
                  "jchdb-minio-download-" ".bin" (make-array FileAttribute 0))
          payload (byte-array (* 2 1024 1024))]
      (try
        (dotimes [i (alength payload)]
          (aset-byte payload i (byte (- (mod i 251) 125))))
        (Files/write source payload (make-array OpenOption 0))
        (Files/deleteIfExists target)
        (check "file uses real streaming conditional upload"
               :created
               (:status (backend/put-file-if-absent!
                         store "checkpoints/stream.bin" source)))
        (check "real download returns exact streamed byte count"
               {:status :downloaded :byte-count (alength payload)}
               (backend/download-to-file!
                store "checkpoints/stream.bin" target))
        (check "real streamed payload remains byte exact"
               true
               (java.util.Arrays/equals payload (Files/readAllBytes target)))
        (finally
          (Files/deleteIfExists source)
          (Files/deleteIfExists target)))))

  (when-not (zero? @failures)
    (throw (ex-info (str @failures " MinIO conformance checks failed")
                    {:failures @failures})))
  (println "all Durable MinIO conformance checks passed"))
