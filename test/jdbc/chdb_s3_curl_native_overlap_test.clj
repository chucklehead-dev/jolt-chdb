(ns jdbc.chdb-s3-curl-native-overlap-test
  (:require [jdbc.chdb.durable :as durable]
            [jdbc.chdb.durable.backend :as backend]
            [jdbc.chdb.durable.control :as control]
            [jdbc.chdb.durable.owned-thread :as owned-thread]
            [jdbc.chdb.durable.reader :as reader]
            [jdbc.chdb.durable.s3 :as s3]
            [jdbc.chdb.durable.s3-curl :as curl]
            [jdbc.chdb.durable.s3-writer :as s3-writer]
            [jdbc.chdb.durable.writer :as writer]))

(defn- options [endpoint]
  {:endpoint endpoint :bucket "bucket" :prefix "writer-native-overlap"
   :region "us-east-1" :access-key "ACCESS" :secret-key "SECRET"
   :session-token "SESSION" :max-attempts 1
   :connect-timeout-ms 2000 :timeout-ms 15000})

(defn- fixture! [endpoint operation]
  (curl/request!
    {:method :get :url (str endpoint "/__fixture__/writer-overlap/native/" operation)
     :region "us-east-1"
     :auth {:access-key "ACCESS" :secret-key "SECRET" :session-token "SESSION"}
     :response-body :bytes :connect-timeout-ms 2000 :timeout-ms 15000}))

(defn- await-entry! [endpoint]
  (loop [remaining 200]
    (when-not (= 200 (:status (fixture! endpoint "entered")))
      (assert (pos? remaining) "Native writer WAL request did not reach server")
      (Thread/sleep 10)
      (recur (dec remaining)))))

(defn- await-renewal! [store before]
  (loop [remaining 100]
    (let [head (:head (control/read-head! store))]
      (if (> (get-in head ["lease" "expires_at"])
             (get-in before ["lease" "expires_at"]))
        head
        (do
          (assert (pos? remaining) "Real native writer heartbeat renewal failed")
          (Thread/sleep 10)
          (recur (dec remaining)))))))

(defn- write! [endpoint scratch]
  (let [tick (promise) waits (atom 0) observed (atom nil)
        independent (backend/object-backend (curl/s3-backend (options endpoint)) "object")]
    (s3-writer/with-writer!
      (options endpoint)
      {:object-id "object" :owner "native-overlap" :instance "writer"
       :database "otel" :scratch-parent scratch :lease-ttl-ms 900000
       :heartbeat-interval-ms 1000
       :operations {:await-heartbeat!
                    (fn [stop _]
                      (if (= 1 (swap! waits inc))
                        (deref tick 10000 :stop)
                        (do @stop :stop)))}}
      (fn [w]
        (reset! observed w)
        (let [store (:store w) caller (owned-thread/completion)]
          (writer/execute-and-flush! w "CREATE TABLE samples (n UInt64) ENGINE=MergeTree ORDER BY n")
          (let [before (:head (control/read-head! store))]
            (assert (= 200 (:status (fixture! endpoint "arm"))))
            (owned-thread/start! caller
                                #(writer/execute-and-flush!
                                   w "INSERT INTO samples SELECT number FROM numbers(512)"))
            (try
              (await-entry! endpoint)
              (deliver tick :tick)
              (let [during (await-renewal! independent before)]
                (assert (not (realized? (:outcome caller))))
                (assert (= (get-in before ["manifest" "seq"])
                           (get-in during ["manifest" "seq"])))
                (assert (= (get-in before ["lease" "generation"])
                           (get-in during ["lease" "generation"]))))
              (finally
                (deliver tick :stop)
                (assert (= 200 (:status (fixture! endpoint "release"))))
                (owned-thread/join! caller)))
            (let [after (:head (control/read-head! store))
                  observation (writer/persistence-observation w)]
              (assert (= (inc (get-in before ["manifest" "seq"]))
                         (get-in after ["manifest" "seq"])))
              (assert (= :confirmed (:state observation)))
              (assert (= (get-in after ["manifest" "seq"])
                         (:confirmed-sequence observation))))))))
    (assert (nil? (get-in (:head (control/read-head! independent)) ["lease" "owner"])))
    (assert (= :closed (:lifecycle (writer/status @observed))))
    (assert (realized? (:outcome (:worker @observed))))
    (assert (realized? (:outcome (:heartbeat @observed))))
    (prn {:gate :real-native-writer-renewal-overlap :passed true
          :owner :experimental-library :rows 512 :ack :confirmed
          :heartbeat :controlled-tick})))

(defn- read! [endpoint scratch]
  ;; Invoked in a fresh OS process with stock parsing and fresh curl handles.
  (let [r (durable/open-reader!
            {:namespace-backend (curl/s3-backend (options endpoint))
             :object-id "object" :scratch-parent scratch})]
    (try
      (let [rows (:rows (reader/query! r "SELECT n FROM samples ORDER BY n" []))]
        (assert (= (mapv #(vector (str %)) (range 512))
                   (mapv #(mapv str %) rows))))
      (prn {:gate :fresh-process-stock-recovery :passed true :rows 512})
      (finally (reader/close! r)))))

(defn -main [phase endpoint scratch]
  (case phase "writer" (write! endpoint scratch) "reader" (read! endpoint scratch)))
