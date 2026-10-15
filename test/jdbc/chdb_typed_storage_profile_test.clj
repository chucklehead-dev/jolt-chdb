(ns jdbc.chdb-typed-storage-profile-test
  (:require [clojure.test :as t :refer [deftest is]]
            [jdbc.chdb-typed-storage-profile :as profile]))

(deftest unchanged-default-and-separated-fixed-arms
  (doseq [arm [:local :s3]]
    (is (= {:profile :baseline :object-id "typed-compact-prefix"}
           (profile/select-profile arm nil))))
  (let [baseline (profile/select-profile :s3 "1000")
        candidate (profile/select-profile :s3 "10")]
    (is (= 1000 (:expect-100-timeout-ms baseline)))
    (is (= 10 (:expect-100-timeout-ms candidate)))
    (is (= :expect-continue-ab (:profile baseline) (:profile candidate)))
    (is (not= (:object-id baseline) (:object-id candidate)))
    (is (every? #(not= "typed-compact-prefix" (:object-id %)) [baseline candidate]))))

(deftest reject-open-ended-or-local-selection
  (doseq [[arm value] [[:s3 ""] [:s3 "0"] [:s3 "1"] [:s3 "1001"]
                      [:s3 "10.0"] [:s3 10] [:local "10"] [:local "1000"]]]
    (let [error (try (profile/select-profile arm value) nil
                     (catch Throwable e e))]
      (is (= ::profile/configuration (:type (ex-data error))))
      (is (= :expect-continue-profile (:field (ex-data error)))))))

(defn -main [& _]
  (let [r (t/run-tests 'jdbc.chdb-typed-storage-profile-test)]
    (System/exit (if (zero? (+ (:fail r) (:error r))) 0 1))))
