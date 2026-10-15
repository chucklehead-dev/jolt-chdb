(ns test-typed-storage-wal-head-profile
  (:require [clojure.test :refer [deftest is run-tests]]
            [typed-storage-wal-head-profile :as profile]))

(deftest only-closed-distinct-identities-with-unchanged-options
  (is (not= (profile/object-id "baseline") (profile/object-id "candidate")))
  (doseq [arm ["baseline" "candidate"] phase ["writer" "reader"]]
    (let [seen (atom []) marker (Object.)
          original (fn [root phase]
                     (swap! seen conj [root phase])
                     {:object-id "typed-compact-prefix" :marker marker})
          selected ((profile/wrap-options original arm) "root" phase)]
      (is (= (profile/object-id arm) (:object-id selected)))
      (is (identical? marker (:marker selected)))
      (is (= [["root" phase]] @seen))))
  (doseq [arm [nil "" "other" "baseline/escape"]]
    (is (thrown? Exception (profile/wrap-options (fn [& _] {}) arm))))
  (is (thrown? Exception ((profile/wrap-options
                          (fn [& _] {:object-id "changed"}) "baseline") "root" "writer"))))

(let [r (run-tests 'test-typed-storage-wal-head-profile)]
  (when-not (zero? (+ (:fail r) (:error r)))
    (throw (ex-info "WAL head identity controls failed" r))))
