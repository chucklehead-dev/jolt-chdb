;; Trusted workflow wrapper around the immutable typed driver. It observes only
;; method/byte counts; no URLs, headers, credentials, payloads or object names.
(require '[jdbc.chdb.durable.s3-curl :as curl])
(load-file (str (.getParent (java.io.File. *file*)) "/typed_storage_budget_lib.clj"))

(let [phase (first *command-line-args*)
      root (second *command-line-args*)
      driver (System/getenv "TASK_ORIGINAL_TYPED_DRIVER")
      _ (assert (contains? #{"window" "reader"} phase))
      _ (assert (and driver (.endsWith driver "/bench/typed-storage-window.clj")))
      request-limit (if (= phase "window") 24000 1000)
      upload-limit (if (= phase "window") (* 6 1024 1024 1024) 0)
      counts (atom {:requests 0 :upload-bytes 0 :completed 0})
      original curl/request!]
  (with-redefs [curl/request! (typed-storage-budget-lib/requester
                              original counts request-limit upload-limit
                              (if (= phase "window") #{:get :put} #{:get}))]
    (load-file driver))
  ;; Only reached after the original driver and its resource scope return.
  ;; The shell also requires process exit0; errors do not fabricate completion.
  (spit (str root "/http-budget.edn")
        (pr-str (assoc @counts :scope-returned true :request-limit request-limit
                              :upload-limit upload-limit :phase phase))))
