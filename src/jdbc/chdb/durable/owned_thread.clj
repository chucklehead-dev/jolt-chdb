(ns jdbc.chdb.durable.owned-thread
  "Small result-preserving wrapper around Jolt's public owned OS-thread API.")

(defrecord Completion [outcome thread])

(def ^:private current-completion (ThreadLocal.))

(defn completion []
  (->Completion (promise) (atom nil)))

(defn- fail-self-join! [completion]
  ;; Jolt materializes a fresh Java-facing wrapper for currentThread, so a
  ;; completion marker in the actual thread-local context is the reliable
  ;; same-thread identity check.
  (when (identical? completion (.get current-completion))
    (throw (ex-info "An owned thread cannot join itself"
                    {:type ::self-join}))))

(defn start!
  "Run `f` on a real OS thread and settle `completion` with its exact outcome."
  [completion f]
  (let [thread
        (Thread.
         (fn []
           (.set current-completion completion)
           (try
             (try
               (deliver (:outcome completion) {:value (f)})
               (catch Throwable error
                 ;; Keep the object itself: lifecycle callers depend on Throwable
                 ;; identity, not only on its public category or printed form.
                 (deliver (:outcome completion) {:error error})))
             (finally
               (.remove current-completion)))))]
    (when-not (compare-and-set! (:thread completion) nil thread)
      (throw (IllegalStateException. "Owned thread was already started")))
    (.start thread)
    completion))

(defn join!
  "Wait for an owned thread and return its value or rethrow its exact error."
  [completion]
  (let [thread @(:thread completion)]
    (when-not thread
      (throw (IllegalStateException. "Owned thread was not started")))
    (fail-self-join! completion)
    ;; Waiting on the outcome alone leaves a window in which the worker has
    ;; published its result but its OS thread is still live. An interrupt is
    ;; retained by identity, but it cannot turn this ownership boundary back
    ;; into a best-effort wait.
    (let [interrupted
          (loop [first-interrupt nil]
            (let [result (try
                           (.join ^Thread thread)
                           :joined
                           (catch InterruptedException error error))]
              (if (= :joined result)
                first-interrupt
                (recur (or first-interrupt result)))))]
      (when interrupted
        (.interrupt (Thread/currentThread))
        (throw interrupted)))
    (let [{:keys [value error]} @(:outcome completion)]
      (if error (throw error) value))))

(defn join-after!
  "Run `f`, then positively join `completion` before returning or rethrowing.

  If both operations fail, `f` remains the primary caller-visible error."
  [completion f]
  ;; `f` may await the worker's terminal result. Reject reentrant close before
  ;; that wait, rather than relying only on join!'s later self-join check.
  (fail-self-join! completion)
  (let [primary (try {:value (f)}
                     (catch Throwable error {:error error}))
        joined (try {:value (join! completion)}
                    (catch Throwable error {:error error}))]
    (if-let [error (:error primary)]
      (throw error)
      (if-let [error (:error joined)]
        (throw error)
        (:value primary)))))
