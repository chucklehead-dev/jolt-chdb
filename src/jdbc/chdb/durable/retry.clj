(ns jdbc.chdb.durable.retry
  "Portable operation-scoped retry budgets for Durable V1.")

(def default-deadline-ms 5000)
(def default-initial-backoff-ms 10)
(def default-max-backoff-ms 250)

(defn monotonic-ms []
  (quot (System/nanoTime) 1000000))

(defn await-backoff! [milliseconds]
  (deref (promise) milliseconds ::elapsed)
  true)

(defn- positive-integer! [value label]
  (when-not (and (integer? value) (pos? value))
    (throw (ex-info (str label " must be a positive integer")
                    {:type ::invalid-options})))
  value)

(defn start
  "Validate and start one retry budget.

  `:stopped?` is checked before and after every wait. Test-only clock and await
  seams make deadline behavior deterministic without sleeping."
  [{:keys [max-attempts retry-deadline-ms retry-initial-backoff-ms
           retry-max-backoff-ms monotonic-ms! await-backoff! stopped?]
    :or {max-attempts 4
         retry-deadline-ms default-deadline-ms
         retry-initial-backoff-ms default-initial-backoff-ms
         retry-max-backoff-ms default-max-backoff-ms
         monotonic-ms! monotonic-ms
         await-backoff! await-backoff!
         stopped? (constantly false)}}]
  (positive-integer! max-attempts "max-attempts")
  (positive-integer! retry-deadline-ms "retry-deadline-ms")
  (positive-integer! retry-initial-backoff-ms
                     "retry-initial-backoff-ms")
  (positive-integer! retry-max-backoff-ms "retry-max-backoff-ms")
  (when (> retry-initial-backoff-ms retry-max-backoff-ms)
    (throw (ex-info "retry-initial-backoff-ms must not exceed retry-max-backoff-ms"
                    {:type ::invalid-options})))
  (doseq [[label operation]
          [["monotonic-ms!" monotonic-ms!]
           ["await-backoff!" await-backoff!]
           ["stopped?" stopped?]]]
    (when-not (fn? operation)
      (throw (ex-info (str label " must be callable")
                      {:type ::invalid-options}))))
  {:max-attempts max-attempts
   :deadline-ms retry-deadline-ms
   :initial-backoff-ms retry-initial-backoff-ms
   :max-backoff-ms retry-max-backoff-ms
   :monotonic-ms! monotonic-ms!
   :await-backoff! await-backoff!
   :stopped? stopped?
   :started-at-ms (monotonic-ms!)})

(defn remaining-ms
  "Return the nonnegative time remaining in a started budget."
  [{:keys [deadline-ms monotonic-ms! started-at-ms]}]
  (max 0 (- deadline-ms (- (monotonic-ms!) started-at-ms))))

(defn- backoff-ms [{:keys [initial-backoff-ms max-backoff-ms]} attempt]
  (loop [delay initial-backoff-ms
         remaining (dec attempt)]
    (if (or (zero? remaining) (= delay max-backoff-ms))
      delay
      (recur (min max-backoff-ms (* 2 delay)) (dec remaining)))))

(defn await-next!
  "Wait before the attempt after `attempt`, returning one decision keyword.

  Results are `:retry`, `:attempt-limit`, `:deadline`, or `:stopped`. The
  caller retains operation-specific certainty and error classification."
  [budget attempt]
  (cond
    ((:stopped? budget)) :stopped
    (>= attempt (:max-attempts budget)) :attempt-limit
    :else
    (let [remaining (remaining-ms budget)]
      (if (zero? remaining)
        :deadline
        (let [delay (min remaining (backoff-ms budget attempt))]
          ((:await-backoff! budget) delay)
          (cond
            ((:stopped? budget)) :stopped
            (zero? (remaining-ms budget)) :deadline
            :else :retry))))))

(defn run!
  "Run retryable attempt callbacks without exposing host-specific tail calls.

  `attempt!` receives the one-based attempt number and caller state. It returns
  `{:status :done :value value}` or `{:status :retry :state next-state}`.
  Exhaustion returns the same map shape with status `:attempt-limit`,
  `:deadline`, or `:stopped`; callers retain certainty-specific error mapping."
  [budget initial-state attempt!]
  (when-not (fn? attempt!)
    (throw (ex-info "attempt! must be callable" {:type ::invalid-options})))
  (loop [attempt 1
         state initial-state]
    (if ((:stopped? budget))
      {:status :stopped :attempt (dec attempt) :state state}
      (let [result (attempt! attempt state)]
        (case (:status result)
          :done result
          :retry
          (let [decision (await-next! budget attempt)]
            (if (= :retry decision)
              (recur (inc attempt) (:state result))
              {:status decision :attempt attempt :state (:state result)}))
          (throw (ex-info "attempt! returned an unsupported retry status"
                          {:type ::invalid-result})))))))
