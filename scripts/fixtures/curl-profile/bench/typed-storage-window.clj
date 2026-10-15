;; Local composition fixture, not a workload/recovery qualification.
(require '[jdbc.chdb.durable.s3-curl :as curl])
(assert (= "reader" (first *command-line-args*)))
(curl/with-reused-transport!
  (fn [send]
    (dotimes [_ 3]
      (assert (= 404 (:status (send {:method :get
                                     :url (str (System/getenv "TEST_PROFILE_ENDPOINT") "/bucket/missing-profile-object")
                                     :auth {:access-key "ACCESS" :secret-key "SECRET" :session-token "SESSION"}
                                     :region "us-east-1" :timeout-ms 5000})))))))
