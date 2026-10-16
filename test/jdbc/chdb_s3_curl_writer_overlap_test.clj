(ns jdbc.chdb-s3-curl-writer-overlap-test
  (:require [clojure.test :as test :refer [deftest is]]
            [jdbc.chdb.durable.backend :as backend]
            [jdbc.chdb.durable.control :as control]
            [jdbc.chdb.durable.owned-thread :as owned-thread]
            [jdbc.chdb.durable.s3 :as s3]
            [jdbc.chdb.durable.s3-curl :as curl]
            [jdbc.chdb.durable.writer :as writer]))

(def ^:dynamic *endpoint* nil)

(defn- fixture-request [endpoint mode operation]
  {:method (if (= operation "data") :put :get)
   :url (str endpoint "/__fixture__/writer-overlap/" mode "/" operation)
   :region "us-east-1"
   :auth {:access-key "ACCESS" :secret-key "SECRET" :session-token "SESSION"}
   :request-body (when (= operation "data") {:bytes (byte-array [7]) :byte-count 1})
   :response-body :bytes :connect-timeout-ms 2000 :timeout-ms 15000})

(defn- await-server-entry! [request]
  (loop [remaining 200]
    (if (= 200 (:status (curl/request! request)))
      true
      (if (pos? remaining)
        (do (Thread/sleep 10) (recur (dec remaining)))
        (throw (ex-info "Server did not observe writer transfer" {}))))))

(defn- exercise-heartbeat [shared?]
  (let [endpoint *endpoint* mode (if shared? "shared" "split")
        tick (promise) renewed (promise) waits (atom 0)
        inits (atom 0) cleanups (atom 0) native-closes (atom 0)
        handles (atom []) renewal-performs (atom 0) held? (atom false)
        original-init @#'curl/curl-easy-init
        original-cleanup @#'curl/curl-easy-cleanup
        original-perform @#'curl/curl-easy-perform
        result (atom nil)
        exercise
        (fn [send-data send-renewal]
          (let [options {:endpoint endpoint :bucket "bucket"
                         :prefix (str "writer-heartbeat-" mode) :region "us-east-1"
                         :access-key "ACCESS" :secret-key "SECRET"
                         :session-token "SESSION" :max-attempts 1}
                data-store (backend/object-backend
                             (s3/s3-backend (assoc options :request! send-data)) "object")
                renewal-store (backend/object-backend
                                (s3/s3-backend (assoc options :request! send-renewal)) "object")
                acquired (control/acquire!
                           data-store {:owner "heartbeat-writer" :instance "heartbeat-instance"
                                       :expires-at 200M :now 100M :clock-skew 5M
                                       :database "default" :engine-version "26.7.3"
                                       :backup-format 1 :min-reader "26.7.3"})
                data-request (fixture-request endpoint mode "data")
                w (writer/start!
                    {:store data-store :token (:token acquired) :handle :fake-native
                     :database "default" :lease-expiry 200000 :lease-ttl-ms 150000
                     :heartbeat-interval-ms 1000
                     :operations
                     {:now-ms (constantly 100000)
                      :analyze-query! (fn [& _] nil)
                      :query-native! (fn [& _] (send-data data-request))
                      :close-native! (fn [_] (swap! native-closes inc))
                      ;; Drive exactly one tick through the real heartbeat loop;
                      ;; subsequent iterations wait for normal writer shutdown.
                      :await-heartbeat!
                      (fn [stop _]
                        (if (= 1 (swap! waits inc))
                          (deref tick 5000 :stop)
                          (do @stop :stop)))
                      ;; Raw writer milliseconds -> Protocol V1 wire seconds,
                      ;; then the reverse conversion required by heartbeat state.
                      :renew!
                      (fn [_ token expiry retry]
                        (try
                          (let [r (control/renew! renewal-store token
                                                 (/ (bigdec expiry) 1000M) retry)]
                            (deliver renewed
                                     {:renewed? true :head (:head r)
                                      :generation-unchanged?
                                      (= (:generation token)
                                         (get-in r [:head "lease" "generation"]))})
                            (update-in r [:head "lease" "expires_at"] #(* % 1000)))
                          (catch Throwable error
                            (deliver renewed {:renewed? false :type (:type (ex-data error))})
                            (throw error))))}})
                caller (owned-thread/completion)]
            (reset! held? true)
            (owned-thread/start! caller #(writer/query! w "SELECT 1"))
            (try
              (is (await-server-entry! (fixture-request endpoint mode "entered")))
              (deliver tick :tick)
              (let [r (deref renewed 5000 :timeout)]
                (is (map? r) "heartbeat must attempt renewal while server holds the data response")
                (is (not (realized? (:outcome caller))))
                (reset! result r))
              (finally
                (deliver tick :stop)
                (is (= 200 (:status (curl/request! (fixture-request endpoint mode "release")))))
                (try
                  (is (= 200 (:status (owned-thread/join! caller))))
                  (finally
                    (reset! held? false)
                    (writer/close! w)))))
            (is (= :closed (:lifecycle (writer/status w))))
            (is (realized? (:outcome (:worker w))))
            (is (realized? (:outcome (:heartbeat w))))
            (is (= (if shared? 200000 250000) (:expires-at @(:lease-state w))))
            (is (= {"generation" (:generation (:token acquired))
                    "owner" nil "instance" nil "expires_at" nil}
                   (get-in (:head (control/read-head! data-store)) ["lease"])))))]
    (with-redefs-fn
      {#'curl/curl-easy-init
       (fn [] (let [h (original-init)] (swap! inits inc) (swap! handles conj h) h))
       #'curl/curl-easy-cleanup (fn [h] (swap! cleanups inc) (original-cleanup h))
       #'curl/curl-easy-perform
       (fn [h]
         (when (and @held? (identical? h (second @handles)))
           (swap! renewal-performs inc))
         (original-perform h))}
      #(curl/with-reused-transport!
         (fn [send-data]
           (if shared?
             (exercise send-data send-data)
             (curl/with-reused-transport!
               (fn [send-renewal] (exercise send-data send-renewal)))))))
    (is (= @inits @cleanups))
    (is (= 1 @native-closes))
    ;; Fresh fixture-control handles also exist, so count renewal performs only
    ;; for the independently owned second persistent handle in the split case.
    (when-not shared? (is (= 2 @renewal-performs)))
    @result))

(deftest writer-heartbeat-renews-during-on-wire-data-transfer
  (is (= {:renewed? false :type :jdbc.chdb.durable.s3/transport}
         (exercise-heartbeat true)))
  (let [r (exercise-heartbeat false)]
    (is (true? (:renewed? r)))
    (is (= 250 (get-in r [:head "lease" "expires_at"])))
    (is (true? (:generation-unchanged? r)))))

(defn run-tests! [endpoint]
  (binding [*endpoint* endpoint]
    (let [r (test/run-tests 'jdbc.chdb-s3-curl-writer-overlap-test)]
      (when-not (zero? (+ (:fail r) (:error r)))
        (throw (ex-info "Writer renewal overlap checks failed"
                        {:failures (:fail r) :errors (:error r)}))))))
