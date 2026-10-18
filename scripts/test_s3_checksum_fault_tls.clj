;; Actual shared provider-control entry against explicit ephemeral TLS trust.
(require '[s3-checksum-fault-lib :as faults]
         '[s3-checksum-fault-config :as config]
         '[jdbc.chdb.durable.s3-curl :as curl]
         '[clojure.edn :as edn]
         '[clojure.data.json :as json])
(let [[url ca-path root] *command-line-args*
      _ (assert (re-matches #"https://127\.0\.0\.1:[0-9]+" url))
      probe {:method :get :url (str url "/bucket/probe") :response-body :bytes
             :auth {:access-key "ACCESS" :secret-key "SECRET" :session-token "SESSION"}
             :region "us-east-1" :connect-timeout-ms 2000 :timeout-ms 10000}
      code (try (curl/request! probe) nil (catch Throwable e (:curl-code (ex-data e))))]
  (assert (= 60 code) "Test CA must be rejected before explicit fixture trust")
  (let [original @#'curl/set-string!
        stats-response
        (with-redefs-fn
          {#'curl/set-string!
           (fn [handle option value stage]
             (when (= stage :url) (original handle 10065 ca-path :test-ca))
             (original handle option value stage))
           #'config/options
           (fn [_] {:endpoint url :bucket "bucket" :prefix "checksum-provider-controls"
                         :region "us-east-1" :access-key "ACCESS" :secret-key "SECRET"
                         :session-token "SESSION" :connect-timeout-ms 2000
                         :timeout-ms 10000 :max-response-bytes 65536})}
          ;; Same CLI file and classpath as cloud; only test transport settings
          ;; and explicit ephemeral CA trust differ. Production never loads this.
          #(do
             (binding [*command-line-args* [root]]
               (load-file "scripts/qualify_s3_checksum_faults.clj"))
             (curl/request! (assoc probe :url (str url "/__fixture__/stats")))))
        report (edn/read-string (slurp (str root "/s3-writer-confirmation.edn")))
        stats (json/read-str (String. (:body stats-response) "UTF-8"))]
    (assert (= faults/cases (mapv :case (:cases report))))
    (assert (every? :passed (:cases report)))
    (assert (true? (:scope-returned report)))
    (assert (= (get-in report [:requests :requests]) (get-in report [:requests :completed])
               (get stats "authenticated-requests")))
    (assert (and (pos? (get stats "gets")) (pos? (get stats "puts"))))
    (prn report)))
