(ns jdbc.chdb-owned-stream-fixture-test
  (:require [clojure.test :refer [deftest is]]))

(deftest native-proofs-remain-live-with-assertions-disabled
  (binding [*assert* false]
    (require 'jdbc.chdb-owned-compact-stream-native-test :reload)
    (require 'jdbc.chdb-owned-stream-option-native-test :reload))
  (doseq [namespace '[jdbc.chdb-owned-compact-stream-native-test
                     jdbc.chdb-owned-stream-option-native-test]]
    (let [check (ns-resolve namespace 'check!)]
      (is (nil? (check true)))
      (is (thrown? clojure.lang.ExceptionInfo (check false))))))
