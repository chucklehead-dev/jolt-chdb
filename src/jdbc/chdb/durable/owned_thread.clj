(ns jdbc.chdb.durable.owned-thread
  "Small result-preserving wrapper around Jolt's public owned OS-thread API."
  (:require [clojure.core.async :as async]))

(defn completion []
  (promise))

(defn start!
  "Run `f` on a real OS thread and settle `completion` with its exact outcome."
  [completion f]
  (async/thread-call
   (fn []
     (try
       (deliver completion {:value (f)})
       (catch Throwable error
         ;; Keep the object itself: lifecycle callers depend on Throwable
         ;; identity, not only on its public category or printed form.
         (deliver completion {:error error})))
     nil)
   :mixed)
  completion)

(defn join!
  "Wait for an owned thread and return its value or rethrow its exact error."
  [completion]
  (let [{:keys [value error]} @completion]
    (if error (throw error) value)))
