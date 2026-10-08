(ns jdbc.chdb-s3-curl-native-overlap-test
  (:require [jdbc.chdb.durable :as durable]
            [jdbc.chdb.durable.backend :as backend]
            [jdbc.chdb.durable.control :as control]
            [jdbc.chdb.durable.owned-thread :as owned-thread]
            [jdbc.chdb.durable.reader :as reader]
            [jdbc.chdb.durable.s3 :as s3]
            [jdbc.chdb.durable.s3-curl :as curl]
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

(defn- write! [endpoint scratch]
  (curl/with-reused-transport!
    (fn [send-data]
      (curl/with-reused-transport!
        (fn [send-renewal]
          (let [namespace (s3/s3-backend (assoc (options endpoint) :request! send-data))
                store (backend/object-backend namespace "object")
                renewal-store (backend/object-backend
                                (s3/s3-backend (assoc (options endpoint) :request! send-renewal))
                                "object")
                start @#'writer/start! tick (promise) renewed (promise) waits (atom 0)
                w (with-redefs
                    [writer/start!
                     (fn [opts]
                       (let [renew! (get-in opts [:operations :renew!])]
                         (start
                           (update opts :operations assoc
                                   :await-heartbeat!
                                   (fn [stop _]
                                     (if (= 1 (swap! waits inc))
                                       (deref tick 10000 :stop)
                                       (do @stop :stop)))
                                   :renew!
                                   (fn [_ token expiry retry]
                                     (try
                                       ;; Preserve the public open layer's exact
                                       ;; time conversion and retry semantics.
                                       (let [r (renew! renewal-store token expiry retry)]
                                         (deliver renewed {:value r})
                                         r)
                                       (catch Throwable e
                                         (deliver renewed {:error e})
                                         (throw e))))))))]
                    (durable/open-writer!
                      {:namespace-backend namespace :object-id "object"
                       :owner "native-overlap" :instance "writer" :database "otel"
                       :scratch-parent scratch :lease-ttl-ms 900000
                       :heartbeat-interval-ms 1000}))
                caller (owned-thread/completion)]
            (try
              (writer/execute-and-flush! w "CREATE TABLE samples (n UInt64) ENGINE=MergeTree ORDER BY n")
              (let [before (:head (control/read-head! store))]
                (assert (= 200 (:status (fixture! endpoint "arm"))))
                (owned-thread/start!
                  caller #(writer/execute-and-flush!
                            w "INSERT INTO samples SELECT number FROM numbers(512)"))
                (try
                  (await-entry! endpoint)
                  (deliver tick :tick)
                  (let [r (deref renewed 5000 :timeout)]
                    (assert (and (map? r) (contains? r :value))
                            "Real native writer heartbeat renewal failed")
                    (assert (not (realized? (:outcome caller))))
                    (let [during (:head (control/read-head!
                                          (backend/object-backend
                                            (curl/s3-backend (options endpoint)) "object")))]
                      (assert (= (get-in before ["manifest" "seq"])
                                 (get-in during ["manifest" "seq"])))
                      (assert (= (get-in before ["lease" "generation"])
                                 (get-in during ["lease" "generation"])))
                      (assert (> (get-in during ["lease" "expires_at"])
                                 (get-in before ["lease" "expires_at"])))))
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
                             (:confirmed-sequence observation)))))
              (finally
                (deliver tick :stop)
                (writer/close! w)))
            (assert (nil? (get-in (:head (control/read-head! store)) ["lease" "owner"])))
            (assert (realized? (:outcome (:worker w))))
            (assert (realized? (:outcome (:heartbeat w))))
            (prn {:gate :real-native-writer-renewal-overlap :passed true
                  :rows 512 :ack :confirmed :heartbeat :controlled-tick})))))))

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
