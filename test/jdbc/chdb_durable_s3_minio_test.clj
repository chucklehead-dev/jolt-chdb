(ns jdbc.chdb-durable-s3-minio-test
  (:require [jdbc.chdb-durable-s3-provider-test :as provider]))

(defn -main [endpoint]
  (provider/run!
   {:label "MinIO"
    :endpoint endpoint
    :bucket "durable-conformance"
    :prefix "qualification"
    :region "us-east-1"
    :auth {:access-key "MINIOACCESS" :secret-key "MINIOSECRET"}
    :create-bucket? true}))
