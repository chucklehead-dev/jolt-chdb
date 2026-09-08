(ns jdbc.chdb-durable-s3-curl-test
  (:require [clojure.string :as str]
            [jdbc.chdb.durable.backend :as backend]
            [jdbc.chdb.durable.control :as control]
            [jdbc.chdb.durable.s3 :as s3]
            [jdbc.chdb.durable.s3-curl :as s3-curl])
  (:import [java.nio.file Files OpenOption StandardOpenOption]
           [java.nio.file.attribute FileAttribute PosixFilePermissions]))

(def failures (atom 0))

(def ^:private timeout-access-key "JOLT-ACCESS-KEY-CANARY-9A4C")
(def ^:private timeout-secret-key "JOLT-SECRET-KEY-CANARY-72ED")
(def ^:private timeout-session-token "JOLT-SESSION-TOKEN-CANARY-B813")

(defn- check [label expected actual]
  (if (= expected actual)
    (println "  ok  " label)
    (do (swap! failures inc)
        (println "  FAIL" label "- expected" (pr-str expected)
                 "got" (pr-str actual)))))

(defn- error-type [f]
  (try (f) nil (catch Throwable error (:type (ex-data error)))))

(defn- recording-timeout-store [endpoint prefix transport-errors]
  (backend/object-backend
   (s3/s3-backend
    {:endpoint endpoint
     :bucket "bucket"
     :prefix prefix
     :region "us-east-1"
     :access-key timeout-access-key
     :secret-key timeout-secret-key
     :session-token timeout-session-token
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
   "object"))

(defn- leaks-private-data?
  [errors forbidden]
  (let [rendered (pr-str errors)]
    (boolean (some #(str/includes? rendered %) forbidden))))

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
          timed-store (recording-timeout-store
                       endpoint "timeout-after-commit" transport-errors)
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
          captured (first @transport-errors)]
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
             (leaks-private-data?
              @transport-errors
              [timeout-access-key timeout-secret-key timeout-session-token
               endpoint "timeout-after-commit" "timeout-recovery"
               (get reference "key")]))
      (check "exactly one native transfer failure required reconciliation"
             1 (count @transport-errors)))
    (let [transport-errors (atom [])
          timed-store (recording-timeout-store
                       endpoint "timeout-after-cas" transport-errors)
          token1 (:token (control/acquire! timed-store acquisition-options))
          wal-bytes (.getBytes "head-cas-timeout" "UTF-8")
          publication (control/publish-wal-bytes!
                       timed-store token1 wal-bytes)
          reference (:reference publication)
          started (System/nanoTime)
          committed (control/commit-reference!
                     timed-store token1
                     {:kind :wal
                      :reference reference
                      :verify-reference! control/verify-byte-reference!})
          elapsed-ms (/ (- (System/nanoTime) started) 1000000.0)
          recovery-head (:head (control/read-head-read-only! timed-store))
          _ (control/release! timed-store token1)
          acquired2
          (control/acquire!
           timed-store
           (assoc acquisition-options
                  :owner "writer-after-timeout"
                  :instance "instance-after-timeout"
                  :now 101M
                  :expires-at 300M))
          token2 (:token acquired2)
          before-stale (:head (control/read-head! timed-store))
          stale-result (error-type #(control/renew! timed-store token1 350M))
          after-stale (:head (control/read-head! timed-store))
          captured (first @transport-errors)]
      (check "real libcurl timeout after head CAS is observed"
             [28 :transport false]
             [(get-in captured [:data :curl-code])
              (get-in captured [:data :category])
              (get-in captured [:data :definitely-not-sent?])])
      (check "ambiguous head CAS reconciles the exact intended transition"
             [:reconciled 1 [reference]]
             [(:status committed)
              (get-in (:head committed) ["manifest" "seq"])
              (get-in (:head committed) ["manifest" "wal"])])
      (check "read-only recovery sees the reconciled sequence and reference"
             [1 [reference]]
             [(get-in recovery-head ["manifest" "seq"])
              (get-in recovery-head ["manifest" "wal"])])
      (check "a subsequent writer retains the committed manifest exactly once"
             [:acquired 2 1 [reference]]
             [(:status acquired2)
              (:generation token2)
              (get-in (:head acquired2) ["manifest" "seq"])
              (get-in (:head acquired2) ["manifest" "wal"])])
      (check "the stale owner is fenced without changing the recovered head"
             [::control/lease-fenced before-stale]
             [stale-result after-stale])
      (check "the post-CAS-timeout transport remains reusable"
             "head-cas-timeout"
             (String. (backend/get-bytes timed-store (get reference "key"))
                      "UTF-8"))
      (check "head-CAS timeout reconciliation has a bounded runtime"
             true (< elapsed-ms 3000.0))
      (check "head-CAS diagnostics retain no credential, URL, or key"
             false
             (leaks-private-data?
              @transport-errors
              [timeout-access-key timeout-secret-key timeout-session-token
               endpoint "timeout-after-cas" "head-cas-timeout"
               (get reference "key")]))
      (check "exactly one head-CAS transfer failure required reconciliation"
             1 (count @transport-errors)))
    (let [transport-errors (atom [])
          timed-store (recording-timeout-store
                       endpoint "timeout-before-cas" transport-errors)
          token (:token (control/acquire! timed-store acquisition-options))
          wal-bytes (.getBytes "head-cas-not-applied" "UTF-8")
          reference (:reference
                     (control/publish-wal-bytes! timed-store token wal-bytes))
          commit-result
          (error-type
           #(control/commit-reference!
             timed-store token
             {:kind :wal
              :reference reference
              :verify-reference! control/verify-byte-reference!}))
          unchanged (:head (control/read-head! timed-store))
          captured (first @transport-errors)]
      (check "real timeout before head CAS remains explicitly uncommitted"
             [::control/commit-ambiguous 0 []]
             [commit-result
              (get-in unchanged ["manifest" "seq"])
              (get-in unchanged ["manifest" "wal"])])
      (check "before-CAS negative control is a native ambiguous timeout"
             [28 :transport false]
             [(get-in captured [:data :curl-code])
              (get-in captured [:data :category])
              (get-in captured [:data :definitely-not-sent?])])
      (check "before-CAS negative control reached its provider fault branch"
             "request-reached-before-cas"
             (String.
              (backend/get-bytes timed-store
                                 "fixture-timeout-before-cas-hit")
              "UTF-8"))
      (check "the dropped-CAS transport remains readable without poisoning"
             "head-cas-not-applied"
             (String. (backend/get-bytes timed-store (get reference "key"))
                      "UTF-8"))
      (check "before-CAS diagnostics retain no credential, URL, or key"
             false
             (leaks-private-data?
              @transport-errors
              [timeout-access-key timeout-secret-key timeout-session-token
               endpoint "timeout-before-cas" "head-cas-not-applied"
               (get reference "key")]))
      (check "negative control visits exactly one native timeout"
             1 (count @transport-errors)))
    (let [directory (Files/createTempDirectory
                     "jchdb-curl-files-" (make-array FileAttribute 0))
          _ (Files/setPosixFilePermissions
             directory (PosixFilePermissions/fromString "rwx------"))
          unsafe-directory (Files/createTempDirectory
                            "jchdb-curl-unsafe-"
                            (make-array FileAttribute 0))
          _ (Files/setPosixFilePermissions
             unsafe-directory (PosixFilePermissions/fromString "rwxrwx---"))
          source (.resolve directory "upload.bin")
          target (.resolve directory "download.bin")
          missing (.resolve directory "missing.bin")
          truncated (.resolve directory "truncated.bin")
          unsupported (.resolve directory "unsupported.bin")
          unsafe-target (.resolve unsafe-directory "download.bin")]
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
        (check "download collision preserves the existing destination"
               [::s3/transport [1 2 3 4 5]]
               [(error-type #(backend/download-to-file!
                              store "checkpoints/test.tar" target))
                (vec (Files/readAllBytes target))])
        (check "streamed download rejects a writable shared parent"
               [::s3/transport false]
               [(error-type #(backend/download-to-file!
                              store "checkpoints/test.tar" unsafe-target))
                (Files/exists unsafe-target
                              (make-array java.nio.file.LinkOption 0))])
        (let [original-os-name (System/getProperty "os.name")]
          (try
            (System/setProperty "os.name" "Windows")
            (check "unsupported streamed destination fails sanitized and clean"
                   [::s3/transport false]
                   [(error-type #(backend/download-to-file!
                                  store "checkpoints/test.tar" unsupported))
                    (Files/exists unsupported
                                  (make-array java.nio.file.LinkOption 0))])
            (finally
              (System/setProperty "os.name" original-os-name))))
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
          (Files/deleteIfExists truncated)
          (Files/deleteIfExists unsupported)
          (Files/deleteIfExists directory)
          (Files/setPosixFilePermissions
           unsafe-directory (PosixFilePermissions/fromString "rwx------"))
          (Files/deleteIfExists unsafe-target)
          (Files/deleteIfExists unsafe-directory)))))
  (when-not (zero? @failures)
    (throw (ex-info (str @failures " libcurl transport checks failed")
                    {:failures @failures})))
  (println "all Durable libcurl transport checks passed"))
