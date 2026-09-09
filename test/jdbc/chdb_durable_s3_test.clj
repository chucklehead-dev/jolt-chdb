(ns jdbc.chdb-durable-s3-test
  (:require [clojure.string :as str]
            [jdbc.chdb.durable.backend :as backend]
            [jdbc.chdb.durable.control :as control]
            [jdbc.chdb.durable.s3 :as s3])
  (:import [java.nio.file Files OpenOption StandardOpenOption]
           [java.nio.file.attribute FileAttribute]))

(def failures (atom 0))

(defn- check [label expected actual]
  (if (= expected actual)
    (println "  ok  " label)
    (do (swap! failures inc)
        (println "  FAIL" label "- expected" (pr-str expected)
                 "got" (pr-str actual)))))

(defn- caught [f]
  (try (f) nil (catch Throwable error error)))

(defn- copy-bytes [bytes]
  (java.util.Arrays/copyOf bytes (alength bytes)))

(defn- corrupt-copy [bytes]
  (let [result (copy-bytes bytes)]
    (when (pos? (alength result))
      (aset-byte result 0 (unchecked-byte (bit-xor 255 (aget result 0)))))
    result))

(defn- fake-transport []
  (let [objects (atom {})
        calls (atom [])
        next-etag (atom 0)
        fault (atom nil)]
    {:objects objects
     :calls calls
     :fault fault
     :request!
     (fn [{:keys [operation method url headers request-body response-body]
           :as request}]
       (swap! calls conj (dissoc request :auth))
       (let [key (second (str/split url #"/bucket/" 2))
             selected @fault
             inject? (= operation (:operation selected))
             before? (and inject? (= :before (:phase selected)))
             after? (and inject? (= :after (:phase selected)))
             corrupt? (and inject? (= :corrupt-response (:phase selected)))
             transport-error
             #(throw (ex-info "transport contained PRIVATE-SECRET"
                              {:category :transport
                               :definitely-not-sent? %}))]
         (when before? (reset! fault nil) (transport-error true))
         (let [response
               (if (= :get method)
                 (if-let [{:keys [bytes etag]} (get @objects key)]
                   (if (= :bytes response-body)
                     {:status 200 :headers {"etag" etag}
                      :body (copy-bytes bytes)}
                     (let [path (get-in response-body [:file])
                           response-bytes (if corrupt?
                                            (corrupt-copy bytes)
                                            bytes)]
                       (when corrupt? (reset! fault nil))
                       (Files/write path response-bytes
                                    (into-array OpenOption
                                                [StandardOpenOption/CREATE_NEW
                                                 StandardOpenOption/WRITE]))
                       {:status 200 :headers {"etag" etag}
                        :byte-count (alength response-bytes)}))
                   {:status 404})
                 (let [current (get @objects key)
                       if-none (get headers "if-none-match")
                       if-match (get headers "if-match")]
                   (if (or (and (= "*" if-none) current)
                           (and if-match (not= if-match (:etag current))))
                     {:status 412}
                     (let [bytes (or (:bytes request-body)
                                     (Files/readAllBytes (:file request-body)))
                           etag (str "\"etag-" (swap! next-etag inc) "\"")]
                       (swap! objects assoc key {:bytes (copy-bytes bytes)
                                                 :etag etag})
                       {:status 200 :headers {"etag" etag}}))))]
           (when after? (reset! fault nil) (transport-error false))
           response)))}))

(def base-options
  {:endpoint "http://127.0.0.1:9000/"
   :bucket "bucket"
   :prefix "tenant data"
   :region "us-east-1"
   :access-key "PRIVATE-ACCESS"
   :secret-key "PRIVATE-SECRET"
   :max-attempts 1})

(def acquire-options
  {:owner "writer" :instance "instance" :expires-at 200M :now 100M
   :database "default" :engine-version "26.7.2-rc.2"
   :backup-format 1 :min-reader "26.7.2-rc.2"})

(defn run-checks! []
  (reset! failures 0)
  (println "Durable V1 S3-compatible backend semantics")
  (let [{:keys [request! calls] :as fake} (fake-transport)
        namespace (s3/s3-backend (assoc base-options :request! request!))
        store (backend/object-backend namespace "object one")
        acquired (control/acquire! store acquire-options)
        token (:token acquired)]
    (check "fresh head uses native atomic create" "*"
           (get-in (first (filter #(= :put (:method %)) @calls))
                   [:headers "if-none-match"]))
    (check "namespace, object, and protocol keys are path encoded"
           true (.contains (:url (first @calls))
                           "/bucket/tenant%20data/object%20one/head.json"))
    (check "transport request log need not retain credentials" false
           (contains? (first @calls) :auth))
    (let [returned (backend/get-bytes store "head.json")]
      (aset-byte returned 0 (byte 0))
      (check "S3 backend owns returned response bytes" false
             (= 0 (first (backend/get-bytes store "head.json")))))

    (reset! (:fault fake) {:operation :put-bytes-if-absent :phase :after})
    (let [publication (control/publish-wal-bytes!
                       store token (.getBytes "abc" "UTF-8"))]
      (check "timeout after immutable create reconciles exact bytes"
             :reconciled (:status publication))
      (reset! (:fault fake) {:operation :replace-if-match :phase :after})
      (check "timeout after head CAS reconciles exact manifest transition"
             :reconciled
             (:status
              (control/commit-reference!
               store token {:kind :wal :reference (:reference publication)
                            :verify-reference!
                            control/verify-byte-reference!}))))

    (let [last-replace (last (filter #(= :replace-if-match (:operation %))
                                     @calls))]
      (check "head replacement uses the opaque ETag as If-Match"
             true (string? (get-in last-replace [:headers "if-match"]))))

    (let [source (Files/createTempFile
                  "jchdb-s3-upload-" ".bin" (make-array FileAttribute 0))
          target (Files/createTempFile
                  "jchdb-s3-download-" ".bin" (make-array FileAttribute 0))]
      (try
        (Files/write source (byte-array [4 5 6]) (make-array OpenOption 0))
        (Files/deleteIfExists target)
        (check "file publication passes a streaming file descriptor" :created
               (:status (backend/put-file-if-absent!
                         store "checkpoints/test.tar.gz" source)))
        (check "file request reports the exact bounded byte count" 3
               (get-in (last @calls) [:request-body :byte-count]))
        (check "download delegates to a create-new streaming destination"
               {:status :downloaded :byte-count 3}
               (backend/download-to-file!
                store "checkpoints/test.tar.gz" target))
        (check "streamed download bytes remain exact" [4 5 6]
               (vec (Files/readAllBytes target)))
        (let [publication (control/publish-checkpoint-file!
                           store token source)
              reference (:reference publication)]
          (reset! (:fault fake)
                  {:operation :download-to-file
                   :phase :corrupt-response})
          (let [error (caught #(control/verify-file-reference!
                                store reference))]
            (check "corrupt streamed checkpoint response fails recovery verification"
                   [::control/object-unverified :integrity]
                   [(:type (ex-data error)) (:reason (ex-data error))]))
          (check "a later byte-exact checkpoint download still verifies"
                 reference
                 (control/verify-file-reference! store reference)))
        (finally
          (Files/deleteIfExists source)
          (Files/deleteIfExists target))))

    (reset! (:fault fake) {:operation :put-bytes-if-absent :phase :before})
    (let [error (caught #(backend/put-bytes-if-absent!
                          store "wal/not-sent.jsonl" (byte-array [1])))
          public (str (ex-message error) " " (pr-str (ex-data error)))]
      (check "proved-not-sent transport failure is not ambiguous"
             ::s3/transport (:type (ex-data error)))
      (check "transport failure strips credential-bearing causes" false
             (.contains public "PRIVATE-SECRET"))))

  (let [{:keys [request!]} (fake-transport)
        store (s3/s3-backend (assoc base-options :request! request!))]
    (check "immutable S3 create exposes native precondition failure"
           [:created :precondition-failed]
           [(:status (backend/put-bytes-if-absent!
                      store "fixed/key" (byte-array [1])))
            (:status (backend/put-bytes-if-absent!
                      store "fixed/key" (byte-array [2])))]))

  (let [attempts (atom 0)
        retrying (s3/s3-backend
                  (assoc base-options :max-attempts 3
                         :request! (fn [_]
                                     (if (< (swap! attempts inc) 3)
                                       {:status 503}
                                       {:status 404}))))]
    (check "read retries are bounded and can recover" [nil 3]
           [(backend/get-bytes retrying "head.json") @attempts]))

  (let [now (atom 0)
        waits (atom [])
        requests (atom [])
        store
        (s3/s3-backend
         (assoc base-options
                :max-attempts 4
                :retry-deadline-ms 25
                :retry-initial-backoff-ms 10
                :retry-max-backoff-ms 20
                :connect-timeout-ms 50
                :timeout-ms 100
                :monotonic-ms! (fn [] @now)
                :await-backoff! (fn [milliseconds]
                                  (swap! waits conj milliseconds)
                                  (swap! now + milliseconds))
                :request! (fn [request]
                            (swap! requests conj
                                   [(:connect-timeout-ms request)
                                    (:timeout-ms request)])
                            {:status 503})))
        error (caught #(backend/get-bytes store "head.json"))]
    (check "read retry deadline has a distinct timeout category"
           ::s3/timeout (:type (ex-data error)))
    (check "S3 backoff and request timeouts are clipped to the shared deadline"
           [[10 15] [[25 25] [15 15]]]
           [@waits @requests]))

  (let [calls (atom 0)
        waits (atom [])
        store
        (s3/s3-backend
         (assoc base-options :max-attempts 4
                :await-backoff! #(swap! waits conj %)
                :request! (fn [_]
                            (swap! calls inc)
                            (throw (ex-info "PRIVATE-SECRET"
                                            {:category :transport
                                             :definitely-not-sent? false})))))
        result (backend/put-bytes-if-absent!
                store "wal/uncertain.jsonl" (byte-array [1]))]
    (check "uncertain writes are never reissued"
           [:ambiguous 1 []]
           [(:status result) @calls @waits]))

  (let [throttled (s3/s3-backend
                   (assoc base-options :request! (constantly {:status 429})))
        error (caught #(backend/get-bytes throttled "head.json"))]
    (check "exhausted throttling has a distinct category"
           ::s3/throttled (:type (ex-data error))))

  (let [store (s3/s3-backend
               (assoc base-options :request!
                      (fn [_]
                        (throw (ex-info "PRIVATE-SECRET"
                                        {:category :authentication})))))
        request-error (caught #(backend/get-bytes store "head.json"))]
    (check "authentication failures have a distinct category"
           ::s3/authentication (:type (ex-data request-error)))
    (check "authentication errors omit credentials" false
           (.contains (str (ex-message request-error) " "
                           (pr-str (ex-data request-error)))
                      "PRIVATE-SECRET")))

  (doseq [options [(assoc base-options :endpoint
                          "https://PRIVATE-SECRET@example.com")
                   (assoc base-options :bucket "bad/bucket")
                   (assoc base-options :session-token "")
                   (assoc base-options :timeout-ms 0)
                   (assoc base-options :max-response-bytes -1)]]
    (check "unsafe S3 configuration fails before transport"
           ::s3/invalid-options
           (:type (ex-data
                   (caught #(s3/s3-backend
                             (assoc options :request! (constantly nil))))))))

  (when-not (zero? @failures)
    (throw (ex-info (str @failures " Durable S3 checks failed")
                    {:failures @failures})))
  (println "all Durable S3 backend semantic checks passed")
  true)

(defn -main [& _]
  (run-checks!))
