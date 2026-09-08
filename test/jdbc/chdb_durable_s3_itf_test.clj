(ns jdbc.chdb-durable-s3-itf-test
  (:require [clojure.data.json :as json]
            [jdbc.chdb-durable-itf-test :as itf]
            [jdbc.chdb.durable.backend :as backend]
            [jdbc.chdb.durable.s3 :as s3]
            [jdbc.chdb.durable.s3-curl :as s3-curl]))

(def ^:private max-traces 256)

(def ^:private auth
  {:access-key "ACCESS"
   :secret-key "SECRET"
   :session-token "SESSION"})

(defn- fail! [message data]
  (throw (ex-info message
                  (assoc data :hegel/origin "chdb/durable-s3-itf/replay"))))

(defn- request! [evidence request]
  (swap! evidence update :attempts inc)
  (let [response
        (s3-curl/request!
         (assoc request
                :connect-timeout-ms 2000
                :timeout-ms 10000))]
    (swap! evidence
           (fn [current]
             (-> current
                 (update :responses inc)
                 (update-in [:statuses (:status response)] (fnil inc 0)))))
    response))

(defn- fresh-store-factory [endpoint evidence]
  (let [next-trace (atom -1)]
    (fn []
      (let [trace-index (swap! next-trace inc)]
        (when-not (< trace-index max-traces)
          (fail! "S3 ITF replay exceeded its namespace bound"
                 {:max-traces max-traces}))
        (backend/object-backend
         (s3/s3-backend
          {:endpoint endpoint
           :bucket "bucket"
           :prefix (format "durable-itf/trace-%03d" trace-index)
           :region "us-east-1"
           :access-key (:access-key auth)
           :secret-key (:secret-key auth)
           :session-token (:session-token auth)
           :max-attempts 1
           :request! #(request! evidence %)})
         "object")))))

(defn- provider-evidence [endpoint]
  (let [response
        (s3-curl/request!
         {:method :get
          :url (str endpoint "/__fixture__/stats")
          :headers {}
          :response-body :bytes
          :auth auth
          :region "us-east-1"
          :connect-timeout-ms 2000
          :timeout-ms 10000})]
    (when-not (= 200 (:status response))
      (fail! "S3 ITF fixture did not return provider evidence"
             {:status (:status response)}))
    (json/read-str (String. (:body response) "UTF-8") :key-fn keyword)))

(defn -main [endpoint & traces]
  (when-not (and (string? endpoint) (seq traces) (<= (count traces) max-traces))
    (fail! "S3 ITF replay needs an endpoint and one through 256 traces"
           {:trace-count (count traces)}))
  (println "Durable Quint ITF replay through signed libcurl S3 loopback")
  (let [evidence (atom {:attempts 0 :responses 0 :statuses {}})
        fresh-store (fresh-store-factory endpoint evidence)]
    (doseq [trace traces]
      (itf/replay! trace fresh-store))
    (let [provider (provider-evidence endpoint)
          expected-traces (count traces)
          client @evidence]
      (when-not (and (pos? (:attempts client))
                     (= (:attempts client) (:responses client))
                     (pos? (get (:statuses client) 200 0))
                     (pos? (get (:statuses client) 404 0))
                     (= expected-traces (:namespaces provider))
                     (pos? (:gets provider))
                     (pos? (:puts provider))
                     (= (:attempts client)
                        (:authenticated-requests provider))
                     (= (+ (:gets provider) (:puts provider))
                        (:authenticated-requests provider)))
        (fail! "S3 ITF replay evidence was vacuous or inconsistent"
               {:trace-count expected-traces
                :client client
                :provider provider}))
      (println "Durable S3 ITF replay passed:"
               expected-traces "trace(s),"
               (:attempts client) "libcurl request(s),"
               (:authenticated-requests provider) "provider request(s),"
               (:namespaces provider) "fresh namespace(s)"))))
