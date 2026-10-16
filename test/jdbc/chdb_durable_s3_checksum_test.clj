(ns jdbc.chdb-durable-s3-checksum-test
  (:require [clojure.test :refer [deftest is run-tests]]
            [jdbc.chdb.durable.backend :as backend]
            [jdbc.chdb.durable.s3 :as s3]
            [jdbc.chdb.durable.s3-checksum :as checksum]))

(def empty-digest "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855")
(def empty-base64 "47DEQpj8HBSa+/TImW+5JCeuQeRkm5NMpJWZG3hSuFU=")

(defn- invalid? [f]
  (try (f) false (catch Throwable e (= ::checksum/invalid-confirmation (:type (ex-data e))))))

(deftest canonical-single-put-checksum
  (is (= empty-base64 (checksum/request-checksum empty-digest)))
  (is (checksum/confirmed? empty-digest {"x-amz-checksum-sha256" empty-base64}))
  (is (checksum/confirmed? empty-digest {"x-amz-checksum-sha256" empty-base64
                                       "x-amz-checksum-type" "FULL_OBJECT"})))

(deftest absent-and-composite-never-confirm
  (is (false? (checksum/confirmed? empty-digest {})))
  (is (false? (checksum/confirmed? empty-digest {"etag" empty-digest})))
  (is (false? (checksum/confirmed? empty-digest {"x-amz-checksum-sha256" empty-base64
                                               "x-amz-checksum-type" "COMPOSITE"}))))

(deftest malformed-and-conflicting-never-confirm
  (doseq [value ["" "not-base64" (str empty-base64 "-2") 42]]
    (is (invalid? #(checksum/confirmed? empty-digest {"x-amz-checksum-sha256" value}))))
  (is (invalid? #(checksum/confirmed? empty-digest {"x-amz-checksum-sha256" empty-base64
                                                   "x-amz-checksum-type" "unknown"})))
  (doseq [value [nil "bad" (apply str (repeat 64 "G"))]]
    (is (invalid? #(checksum/request-checksum value)))))

(deftest confirmation-is-exact-and-not-an-ordinary-map
  (let [owner (Object.) other (Object.)
        ordinary {:status :created :etag "opaque" :verified true}
        result (backend/confirmed-upload-result owner "wal/key" 7 empty-digest ordinary)]
    (is (not (backend/confirmed-upload? ordinary owner "wal/key" 7 empty-digest)))
    (is (backend/confirmed-upload? result owner "wal/key" 7 empty-digest))
    (is (not (backend/confirmed-upload? result other "wal/key" 7 empty-digest)))
    (is (not (backend/confirmed-upload? result owner "wal/other" 7 empty-digest)))
    (is (not (backend/confirmed-upload? result owner "wal/key" 8 empty-digest)))
    (is (not (backend/confirmed-upload? result owner "wal/key" 7 "wrong")))
    (is (not (backend/confirmed-upload? (assoc result :status :ambiguous)
                                      owner "wal/key" 7 empty-digest)))))

(deftest custom-transport-cannot-opt-in-with-flags
  (let [calls (atom 0)
        f (with-meta (fn [_] (swap! calls inc) {:status 200})
                     {:verified true :native true})
        store (s3/s3-backend
                {:endpoint "https://s3.us-east-2.amazonaws.com" :bucket "bucket"
                 :region "us-east-2" :access-key "ACCESS" :secret-key "SECRET"
                 :wal-upload-confirmation :sha256-single-put :request! f})]
    (is (= {:status :unsupported}
           (backend/put-file-with-checksum-if-absent! store "wal/key" "not-opened" 0 empty-digest)))
    (is (zero? @calls))))

(defn run-checks! []
  (let [result (run-tests 'jdbc.chdb-durable-s3-checksum-test)]
    (when (pos? (+ (:fail result) (:error result)))
      (throw (ex-info "S3 checksum checks failed" {})))
    result))

(defn -main [& _] (run-checks!))
