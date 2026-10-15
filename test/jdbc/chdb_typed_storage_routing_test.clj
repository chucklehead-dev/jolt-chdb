(ns jdbc.chdb-typed-storage-routing-test
  (:require [clojure.test :as t :refer [deftest is]]
            [jdbc.chdb-typed-storage-support :as storage]
            [jdbc.chdb-typed-storage-profile :as profile]
            [jdbc.chdb.durable.backend :as backend]
            [jdbc.chdb.durable.s3-curl :as curl]))

(deftest snapshot-selection-reaches-real-request-factory
  (doseq [wait ["1000" "10"]]
    (let [root (java.io.File/createTempFile "expect-profile-" ".tmp")
          selected (profile/select-profile :s3 wait)
          requests (atom [])]
      (.delete root)
      (try
        (with-redefs [storage/arm (constantly :s3)
                      storage/selected-profile (constantly selected)
                      storage/s3-options (constantly
                        {:endpoint "http://127.0.0.1:1" :bucket "fixture" :prefix "fixture"
                         :region "us-east-1" :access-key "fixture" :secret-key "fixture"
                         :expect-100-timeout-ms (:expect-100-timeout-ms selected)})
                      curl/request! (fn [request]
                                      (swap! requests conj request)
                                      {:status 200 :headers {"etag" "\"fixture\""}
                                       :body (.getBytes "fixture" "UTF-8")})]
          (let [options (storage/options (.getPath root) "reader")
                store (backend/object-backend (:namespace-backend options) (:object-id options))]
            (is (= "fixture" (String. (:bytes (backend/get-with-etag store "object")) "UTF-8")))
            (is (= [(:expect-100-timeout-ms selected)] (mapv :expect-100-timeout-ms @requests)))
            (is (= [:get] (mapv :method @requests)))
            (is (.endsWith (:url (first @requests)) (str (:object-id selected) "/object")))))
        (finally (doseq [file (reverse (file-seq root))] (.delete file)))))))

(defn -main [& _]
  (let [r (t/run-tests 'jdbc.chdb-typed-storage-routing-test)]
    (System/exit (if (zero? (+ (:fail r) (:error r))) 0 1))))
