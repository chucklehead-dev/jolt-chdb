(ns jdbc.chdb-durable-s3-curl-test
  (:require [clojure.string :as str]
            [jdbc.chdb.durable.backend :as backend]
            [jdbc.chdb.durable.control :as control]
            [jdbc.chdb.durable.s3 :as s3]
            [jdbc.chdb.durable.s3-curl :as s3-curl])
  (:import [java.nio.file Files OpenOption StandardOpenOption]
           [java.nio.file.attribute FileAttribute]))

(def failures (atom 0))

(defn- check [label expected actual]
  (if (= expected actual)
    (println "  ok  " label)
    (do (swap! failures inc)
        (println "  FAIL" label "- expected" (pr-str expected)
                 "got" (pr-str actual)))))

(defn- error-type [f]
  (try (f) nil (catch Throwable error (:type (ex-data error)))))

(def acquisition-options
  {:owner "writer-timeout"
   :instance "instance-timeout"
   :expires-at 200M
   :now 100M
   :clock-skew 5M
   :database "default"
   :engine-version "26.7.2-rc.2"
   :backup-format 1
   :min-reader "26.7.2-rc.2"})

(defn -main [endpoint]
  (reset! failures 0)
  (println "Durable V1 libcurl SigV4 loopback transport")
  (let [probe (s3-curl/request!
               {:method :put
                :url (str endpoint "/bucket/transport-probe")
                :headers {"if-none-match" "*"}
                :request-body {:bytes (byte-array [7]) :byte-count 1}
                :response-body :bytes
                :auth {:access-key "ACCESS" :secret-key "SECRET"
                       :session-token "SESSION"}
                :region "us-east-1"
                :connect-timeout-ms 2000 :timeout-ms 10000})
        _ (check "direct transport signs and streams a request" 200
                 (:status probe))
        namespace (s3-curl/s3-backend
                   {:endpoint endpoint
                    :bucket "bucket"
                    :prefix "tenant"
                    :region "us-east-1"
                    :access-key "ACCESS"
                    :secret-key "SECRET"
                    :session-token "SESSION"
                    :max-attempts 1
                    :connect-timeout-ms 2000
                    :timeout-ms 10000})
        store (backend/object-backend namespace "object")
        first-bytes (.getBytes "first" "UTF-8")
        second-bytes (.getBytes "second" "UTF-8")]
    (check "default transport performs signed conditional create"
           :created
           (:status (backend/put-bytes-if-absent!
                     store "head.json" first-bytes)))
    (check "conditional create receives provider 412"
           :precondition-failed
           (:status (backend/put-bytes-if-absent!
                     store "head.json" second-bytes)))
    (let [direct-get
          (s3-curl/request!
           {:method :get
            :url (str endpoint "/bucket/tenant/object/head.json")
            :headers {}
            :response-body :bytes
            :auth {:access-key "ACCESS" :secret-key "SECRET"
                   :session-token "SESSION"}
            :region "us-east-1"
            :connect-timeout-ms 2000 :timeout-ms 10000})]
      (check "direct transport receives response bytes" "first"
             (String. (:body direct-get) "UTF-8")))
    (let [{:keys [bytes etag]} (backend/get-with-etag store "head.json")]
      (check "GET response body crosses the write callback exactly"
             "first" (String. bytes "UTF-8"))
      (check "header callback retains opaque ETag" true (string? etag))
      (check "conditional replace uses provider ETag"
             :replaced
             (:status (backend/replace-if-match!
                       store "head.json" second-bytes etag)))
      (check "stale provider ETag receives 412"
             :precondition-failed
             (:status (backend/replace-if-match!
                       store "head.json" first-bytes etag))))
    (check "replaced bytes remain exact"
           "second" (String. (backend/get-bytes store "head.json") "UTF-8"))
    (let [transport-errors (atom [])
          timed-namespace
          (s3/s3-backend
           {:endpoint endpoint
            :bucket "bucket"
            :prefix "timeout-after-commit"
            :region "us-east-1"
            :access-key "ACCESS"
            :secret-key "PRIVATE-SECRET"
            :session-token "SESSION"
            :max-attempts 1
            :request!
            (fn [request]
              (try
                (s3-curl/request!
                 (assoc request
                        :connect-timeout-ms 2000
                        :timeout-ms 100))
                (catch Throwable error
                  (swap! transport-errors conj
                         {:message (.getMessage error)
                          :data (ex-data error)})
                  (throw error))))})
          timed-store (backend/object-backend timed-namespace "object")
          token (:token (control/acquire! timed-store acquisition-options))
          wal-bytes (.getBytes "timeout-recovery" "UTF-8")
          started (System/nanoTime)
          publication (control/publish-wal-bytes!
                       timed-store token wal-bytes)
          elapsed-ms (/ (- (System/nanoTime) started) 1000000.0)
          reference (:reference publication)
          committed (control/commit-reference!
                     timed-store token
                     {:kind :wal
                      :reference reference
                      :verify-reference! control/verify-byte-reference!})
          captured (first @transport-errors)
          rendered (pr-str @transport-errors)]
      (check "real libcurl timeout after immutable create is observed"
             [28 :transport false]
             [(get-in captured [:data :curl-code])
              (get-in captured [:data :category])
              (get-in captured [:data :definitely-not-sent?])])
      (check "ambiguous immutable create reconciles through an exact reread"
             :reconciled (:status publication))
      (check "reconciled transport state remains usable for head commit"
             [:committed 1 [reference]]
             [(:status committed)
              (get-in (:head committed) ["manifest" "seq"])
              (get-in (:head committed) ["manifest" "wal"])])
      (check "post-timeout read returns the exact immutable payload"
             "timeout-recovery"
             (String. (backend/get-bytes timed-store (get reference "key"))
                      "UTF-8"))
      (check "timeout and reconciliation complete within a bounded interval"
             true (< elapsed-ms 3000.0))
      (check "native timeout diagnostics retain no credential, URL, or key"
             false
             (boolean
              (some #(str/includes? rendered %)
                    ["PRIVATE-SECRET" endpoint "timeout-after-commit"
                     (get reference "key")])))
      (check "exactly one native transfer failure required reconciliation"
             1 (count @transport-errors)))
    (let [source (Files/createTempFile
                  "jchdb-curl-upload-" ".bin" (make-array FileAttribute 0))
          target (Files/createTempFile
                  "jchdb-curl-download-" ".bin" (make-array FileAttribute 0))
          missing (Files/createTempFile
                   "jchdb-curl-missing-" ".bin" (make-array FileAttribute 0))
          truncated (Files/createTempFile
                     "jchdb-curl-truncated-" ".bin"
                     (make-array FileAttribute 0))]
      (try
        (Files/write source (byte-array [1 2 3 4 5])
                     (make-array OpenOption 0))
        (Files/deleteIfExists target)
        (Files/deleteIfExists missing)
        (Files/deleteIfExists truncated)
        (check "file upload streams through the read callback"
               :created
               (:status (backend/put-file-if-absent!
                         store "checkpoints/test.tar" source)))
        (check "file download streams through the write callback"
               {:status :downloaded :byte-count 5}
               (backend/download-to-file!
                store "checkpoints/test.tar" target))
        (check "streamed file bytes remain exact"
               [1 2 3 4 5] (vec (Files/readAllBytes target)))
        (check "404 download is reported without a destination artifact"
               [{:status :not-found} false]
               [(backend/download-to-file! store "missing" missing)
                (Files/exists missing (make-array java.nio.file.LinkOption 0))])
        (check "truncated transfer fails and removes its partial destination"
               [::s3/transport false]
               [(error-type #(backend/download-to-file!
                              store "truncated" truncated))
                (Files/exists truncated
                              (make-array java.nio.file.LinkOption 0))])
        (finally
          (Files/deleteIfExists source)
          (Files/deleteIfExists target)
          (Files/deleteIfExists missing)
          (Files/deleteIfExists truncated)))))
  (when-not (zero? @failures)
    (throw (ex-info (str @failures " libcurl transport checks failed")
                    {:failures @failures})))
  (println "all Durable libcurl transport checks passed"))
