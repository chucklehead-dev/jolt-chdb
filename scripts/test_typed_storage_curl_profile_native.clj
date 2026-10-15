(ns test-typed-storage-curl-profile-native
  (:require [typed-storage-curl-profile :as profile]
            [jdbc.chdb.durable.s3-curl :as curl]))

(defn -main [endpoint]
  (let [request {:method :get :url (str endpoint "/bucket/missing-profile-object")
                 :auth {:access-key "ACCESS" :secret-key "SECRET" :session-token "SESSION"}
                 :region "us-east-1" :timeout-ms 5000}
        fresh (atom {}) reused (atom {})
        check! (fn [send] (dotimes [_ 3] (assert (= 404 (:status (send request))))))]
    (profile/with-profile! fresh #(check! curl/request!))
    (profile/with-profile! reused #(curl/with-reused-transport! check!))
    (doseq [counts [fresh reused]]
      (let [r (profile/report! counts) c (get-in r [:classes [:get :none]])]
        (assert (= 3 (:requests c) (:performs-started c) (:samples c)))
        (assert (pos? (:curl-total-seconds c)))
        (assert (<= (:perform-wall-nanos c) (:request-wall-nanos c)))
        (assert (zero? (:tls-complete-seconds c)))
        (assert (not (.contains (pr-str r) "missing-profile-object")))))
    (assert (= 3 (get-in @fresh [[:get :none] :new-connections])))
    (assert (= 1 (get-in @reused [[:get :none] :new-connections])))
    (prn {:scope :loopback-native-profile-not-aws :fresh (profile/report! fresh)
          :reused (profile/report! reused)})))
