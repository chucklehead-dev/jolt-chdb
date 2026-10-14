;; Pure request-budget controls: no network, credentials or native libraries.
(load-file (str (.getParent (java.io.File. *file*)) "/typed_storage_budget_lib.clj"))
(require '[clojure.test :refer [deftest is run-tests]])

(defn counters [] (atom {:requests 0 :upload-bytes 0 :completed 0}))
(defn rejected? [f]
  (try (f) false
       (catch clojure.lang.ExceptionInfo e
         (= :typed-storage/request-budget (:type (ex-data e))))))

(deftest delegation-and-limits
  (let [counts (counters) calls (atom []) result (Object.)
        send! (typed-storage-budget-lib/requester
                (fn [r] (swap! calls conj r) result) counts 2 3 #{:get :put})
        put {:method :put :request-body {:byte-count 3}}]
    (is (identical? result (send! put)))
    (is (identical? result (send! {:method :get})))
    (is (= [put {:method :get}] @calls))
    (is (= {:requests 2 :upload-bytes 3 :completed 2} @counts))
    (is (rejected? #(send! {:method :get})))
    (is (= 2 (count @calls)))
    (is (= 2 (:requests @counts))))
  (let [counts (counters) calls (atom 0)
        send! (typed-storage-budget-lib/requester
                (fn [_] (swap! calls inc)) counts 10 3 #{:get :put})]
    (send! {:method :put :request-body {:byte-count 2}})
    (is (rejected? #(send! {:method :put :request-body {:byte-count 2}})))
    (is (= 1 @calls))
    (is (= 2 (:upload-bytes @counts)))))

(deftest malformed-and-read-only-requests
  (doseq [request [{:method :delete} {:method :put}
                   {:method :put :request-body {}}
                   {:method :put :request-body {:byte-count -1}}
                   {:method :put :request-body {:byte-count 1.5}}
                   {:method :get :request-body {:byte-count 0}}]]
    (let [counts (counters) calls (atom 0)
          send! (typed-storage-budget-lib/requester
                  (fn [_] (swap! calls inc)) counts 10 10 #{:get :put})]
      (is (rejected? #(send! request)))
      (is (zero? @calls))
      (is (= (deref (counters)) @counts))))
  (let [counts (counters) calls (atom 0)
        read! (typed-storage-budget-lib/requester
                (fn [_] (swap! calls inc)) counts 10 0 #{:get})]
    (is (rejected? #(read! {:method :put :request-body {:byte-count 0}})))
    (is (zero? @calls))
    (read! {:method :get})
    (is (= 1 @calls))))

(deftest preserves-transport-failure
  (let [counts (counters) failure (ex-info "fixture failure" {})
        send! (typed-storage-budget-lib/requester
                (fn [_] (throw failure)) counts 1 0 #{:get})]
    (is (identical? failure (try (send! {:method :get})
                                (catch Throwable e e))))
    (is (= {:requests 1 :upload-bytes 0 :completed 0} @counts))
    (is (rejected? #(send! {:method :get})))))

(deftest concurrent-reservations-stay-bounded
  (let [counts (counters) calls (atom 0) start (promise)
        send! (typed-storage-budget-lib/requester
                (fn [_] (swap! calls inc)) counts 10 10 #{:get :put})
        workers (mapv (fn [_] (future @start
                               (try (send! {:method :put :request-body {:byte-count 1}})
                                    :accepted
                                    (catch clojure.lang.ExceptionInfo _ :rejected))))
                      (range 30))]
    (deliver start true)
    (let [outcomes (mapv deref workers)]
      (is (= 10 (count (filter #{:accepted} outcomes))))
      (is (= 20 (count (filter #{:rejected} outcomes))))
      (is (= 10 @calls))
      (is (= {:requests 10 :upload-bytes 10 :completed 10} @counts)))))

(let [result (run-tests)]
  (System/exit (if (zero? (+ (:fail result) (:error result))) 0 1)))
