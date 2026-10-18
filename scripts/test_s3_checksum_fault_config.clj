(require '[s3-checksum-fault-config :as config]
         '[clojure.test :refer [deftest is run-tests]])
(def env {"AWS_REGION" "us-east-2" "AWS_ACCESS_KEY_ID" "PRIVATE-KEY"
          "AWS_SECRET_ACCESS_KEY" "PRIVATE-SECRET" "AWS_SESSION_TOKEN" "PRIVATE-SESSION"
          "TASK_BUCKET" "private-bucket" "TASK_CHECKSUM_PREFIX" "ci/jolt-chdb/123-1/typed-window/checksum-faults"
          "GITHUB_RUN_ID" "123" "GITHUB_RUN_ATTEMPT" "1"})
(deftest closed-provider-location-and-run-binding
  (is (= "https://s3.us-east-2.amazonaws.com" (:endpoint (config/options env))))
  (is (= "PRIVATE-SECRET" (:secret-key (config/options env))))
  ;; User-supplied endpoint-like values never become destinations.
  (is (= "https://s3.us-east-2.amazonaws.com"
         (:endpoint (config/options (assoc env "JOLT_CHDB_S3_ENDPOINT" "https://evil.invalid")))))
  (doseq [bad (concat (map #(dissoc env %) config/env-names)
                     [(assoc env "AWS_REGION" "us-east-2/evil")
                      (assoc env "GITHUB_RUN_ID" "124")
                      (assoc env "GITHUB_RUN_ATTEMPT" "01")
                      (assoc env "TASK_CHECKSUM_PREFIX" "ci/other")
                      (assoc env "TASK_CHECKSUM_PREFIX" "ci/jolt-chdb/123-1/typed-window/checksum-faults/../other")])]
    (let [error (try (config/options bad) nil (catch Throwable e e))]
      (is (= :s3-checksum-fault-config/invalid (:type (ex-data error))))
      (is (not (.contains (str (ex-data error)) "PRIVATE"))))))
(let [r (run-tests)] (System/exit (if (zero? (+ (:fail r) (:error r))) 0 1)))
