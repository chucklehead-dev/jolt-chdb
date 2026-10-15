(ns test-typed-storage-curl-profile-receipt
  (:require [clojure.test :as t :refer [deftest is]]
            [typed-storage-curl-profile-receipt :as receipt]))

(def budget {:requests 1 :completed 1 :scope-returned true})
(def report
  {:scope :aggregate-native-transfer-diagnostic :complete true :milestones-are-cumulative true
   :classes {[:get :none]
             {:requests 1 :request-failures 0 :request-wall-nanos 1000
              :performs-started 1 :performs-returned 1 :perform-wall-nanos 100
              :samples 1 :perform-failures 0 :new-connections 1
              :curl-total-seconds 0.0000001 :dns-complete-seconds 0.0
              :connect-complete-seconds 0.0 :pretransfer-complete-seconds 0.0
              :first-byte-seconds 0.0 :tls-complete-seconds 0.0}}})

(deftest fixed-shape-and-budget-bound-profile
  (is (nil? (receipt/validate! report budget)))
  (doseq [mutate [#(assoc % :url "secret")
                  #(assoc % :classes {[:get :secret] (get-in % [:classes [:get :none]])})
                  #(assoc-in % [:classes [:get :none] :curl-total-seconds] Double/NaN)
                  #(assoc-in % [:classes [:get :none] :sample-errors] 1)
                  #(update-in % [:classes [:get :none]] dissoc :tls-complete-seconds)
                  #(assoc-in % [:classes [:get :none] :samples] 0)
                  #(assoc-in % [:classes [:get :none] :new-connections] "secret")]]
    (is (try (receipt/validate! (mutate report) budget) false (catch Throwable _ true))))
  (is (try (receipt/validate! report (assoc budget :requests 2)) false (catch Throwable _ true)))
  (is (try (receipt/validate! report (assoc budget :scope-returned false)) false (catch Throwable _ true))))

(defn -main [& _]
  (let [r (t/run-tests 'test-typed-storage-curl-profile-receipt)]
    (System/exit (if (zero? (+ (:fail r) (:error r))) 0 1))))
