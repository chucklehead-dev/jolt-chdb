(ns jdbc.chdb-durable-epoch-seconds-test
  (:require [clojure.string :as str]
            [jdbc.chdb.durable :as durable]
            [jdbc.chdb.durable.backend :as backend]
            [jdbc.chdb.durable.control :as control]
            [jdbc.chdb-durable-open-test-support :as support]
            [jdbc.chdb.durable.writer :as writer]))

(def failures (atom 0))

(def ^:private python-fixture
  "test/fixtures/durable/python-live-fractional-seconds.json")
(def ^:private legacy-millisecond-fixture
  "test/fixtures/durable/legacy-jolt-millisecond-expiry.json")
(def ^:private python-now-ms 1788230400000M)

(defn- check [label expected actual]
  (if (= expected actual)
    (println "  ok  " label)
    (do
      (swap! failures inc)
      (println "  FAIL" label "- expected" (pr-str expected)
               "got" (pr-str actual)))))

(defn- error-type [f]
  (try (f) nil (catch Throwable error (:type (ex-data error)))))

(defn- fixture-store [path]
  (let [store (backend/memory-backend)]
    (backend/put-bytes-if-absent!
     store control/head-key (.getBytes (str/trim (slurp path)) "UTF-8"))
    store))

(defn- fail-one-replace-backend [delegate failure-at replace-count]
  (reify backend/ObjectBackend
    (get-bytes [_ key] (backend/get-bytes delegate key))
    (get-with-etag [_ key] (backend/get-with-etag delegate key))
    (put-file-if-absent! [_ key path]
      (backend/put-file-if-absent! delegate key path))
    (put-bytes-if-absent! [_ key bytes]
      (backend/put-bytes-if-absent! delegate key bytes))
    (replace-if-match! [_ key bytes etag]
      (if (= failure-at (swap! replace-count inc))
        {:status :precondition-failed}
        (backend/replace-if-match! delegate key bytes etag)))
    (download-to-file! [_ key path]
      (backend/download-to-file! delegate key path))))

(defn- await-fenced! [opened]
  (loop [remaining 100000]
    (if (or (not (:writable? (writer/status opened)))
            (zero? remaining))
      (not (:writable? (writer/status opened)))
      (do (Thread/yield) (recur (dec remaining))))))

(defn- operations [clocks calls close-count cleanup-count]
  (support/fake-open-operations calls clocks close-count cleanup-count))

(defn- open! [store now-ms opts]
  (let [calls (atom [])
        close-count (atom 0)
        cleanup-count (atom 0)]
    (durable/open-writer!
     (merge {:store store
             :owner "jolt-writer"
             :instance "jolt-instance"
             :database "default"
             :lease-ttl-ms 375M
             :heartbeat-interval-ms 125M
             :operations (operations (atom [now-ms now-ms])
                                     calls close-count cleanup-count)}
            opts))))

(defn run-checks! []
  (reset! failures 0)
  (println "Durable V1 epoch-seconds binding boundary")

  (let [mutant-store (fixture-store python-fixture)
        result
        (control/acquire!
         mutant-store
         {:owner "old-millisecond-boundary"
          :instance "old-instance"
          :expires-at (+ python-now-ms 375M)
          :now python-now-ms
          :clock-skew 0M
          :database "default"
          :engine-version "26.7.2-rc.2"
          :backup-format 1
          :min-reader "26.7.2-rc.2"})]
    (check "old millisecond boundary steals a live Python lease (red control)"
           [:acquired 2]
           [(:status result)
            (get-in (:head result) ["lease" "generation"])]))

  (let [store (fixture-store python-fixture)]
    (check "public open preserves a live fractional Python lease"
           ::control/lease-held
           (error-type #(open! store python-now-ms {})))
    (check "rejected cross-binding takeover does not touch the head"
           [1 "python-writer" 1788230400.125M]
           (let [lease (get-in (:head (control/read-head! store)) ["lease"])]
             [(get lease "generation")
              (get lease "owner")
              (get lease "expires_at")])))

  (let [held-store (fixture-store python-fixture)
        takeover-store (fixture-store python-fixture)]
    (check "clock skew remains milliseconds at the public boundary"
           ::control/lease-held
           (error-type #(open! held-store (+ python-now-ms 624M)
                               {:clock-skew-ms 500M})))
    (let [opened (open! takeover-store (+ python-now-ms 625M)
                        {:clock-skew-ms 500M})]
      (try
        (check "fractional-seconds takeover increments fencing generation once"
               [2 "jolt-writer" 1788230401.001M]
               (let [lease (get-in (:head (control/read-head! takeover-store))
                                   ["lease"])]
                 [(get lease "generation")
                  (get lease "owner")
                  (get lease "expires_at")]))
        (finally (writer/close! opened)))))

  (let [store (backend/memory-backend)
        opened (open! store (+ python-now-ms 125M) {})]
    (try
      (check "Jolt writes Python-shaped epoch seconds with millisecond precision"
             [1 "jolt-writer" 1788230400.501M]
             (let [lease (get-in (:head (control/read-head! store)) ["lease"])]
               [(get lease "generation")
                (get lease "owner")
                (get lease "expires_at")]))
      (finally (writer/close! opened))))

  (let [store (backend/memory-backend)
        calls (atom [])
        close-count (atom 0)
        cleanup-count (atom 0)
        now-ms (long 1788230400000)
        opened
        (durable/open-writer!
         {:store store
          :owner "plain-long-writer"
          :instance "plain-long-instance"
          :database "default"
          :lease-ttl-ms 30000
          :heartbeat-interval-ms 10000
          :operations
          (operations (atom [now-ms now-ms])
                      calls close-count cleanup-count)})]
    (try
      (check "plain long wall clock and integer TTL write epoch seconds"
             [1 1788230430.001M]
             [(get-in (:head (control/read-head! store))
                      ["lease" "generation"])
              (get-in (:head (control/read-head! store))
                      ["lease" "expires_at"])])
      (finally (writer/close! opened))))

  (let [store (backend/memory-backend)
        calls (atom [])
        close-count (atom 0)
        cleanup-count (atom 0)
        heartbeat-cycle-finished (promise)
        waits (atom 0)
        now-ms (+ python-now-ms 125M)
        base-operations
        (operations (atom [now-ms now-ms (+ now-ms 100M)])
                    calls close-count cleanup-count)
        opened
        (durable/open-writer!
         {:store store
          :owner "jolt-heartbeat-writer"
          :instance "jolt-heartbeat-instance"
          :database "default"
          :lease-ttl-ms 375M
          :heartbeat-interval-ms 125M
          :operations
          (assoc base-operations
                 :await-heartbeat!
                 (fn [stop _]
                   (if (= 1 (swap! waits inc))
                     :tick
                     (do
                       (deliver heartbeat-cycle-finished true)
                       @stop
                       :stop))))})]
    (try
      @heartbeat-cycle-finished
      (check "heartbeat renews in seconds without corrupting local ms expiry"
             [1 1788230400.6M :executed]
             [(get-in (:head (control/read-head! store))
                      ["lease" "generation"])
              (get-in (:head (control/read-head! store))
                      ["lease" "expires_at"])
              (do (writer/execute! opened "INSERT INTO t VALUES (1)")
                  :executed)])
      (finally (writer/close! opened))))

  (let [delegate (backend/memory-backend)
        acquired
        (control/acquire!
         delegate
         {:owner "red-control" :instance "red-instance"
          :expires-at 10M :now 0M :clock-skew 0M
          :database "default" :engine-version "26.7.2-rc.2"
          :backup-format 1 :min-reader "26.7.2-rc.2"})
        replace-count (atom 0)
        local-now (atom 0M)
        store (fail-one-replace-backend delegate 1 replace-count)
        result
        (control/renew!
         store (:token acquired) 20M
         {:max-attempts 4
          :retry-deadline-ms 1000
          :retry-initial-backoff-ms 1
          :retry-max-backoff-ms 1
          :monotonic-ms! (constantly 0)
          :await-backoff! (fn [_] (reset! local-now 10M))})]
    (check "dropping writer-local stopped? retries after expiry (red control)"
           [:committed 2 10M]
           [(:status result) @replace-count @local-now]))

  (let [delegate (backend/memory-backend)
        replace-count (atom 0)
        store (fail-one-replace-backend delegate 2 replace-count)
        calls (atom [])
        close-count (atom 0)
        cleanup-count (atom 0)
        now-ms (atom (+ python-now-ms 125M))
        backoffs (atom 0)
        heartbeat-ticks (atom 0)
        base-operations
        (operations (atom [@now-ms]) calls close-count cleanup-count)
        opened
        (durable/open-writer!
         {:store store
          :owner "stopped-aware-writer"
          :instance "stopped-aware-instance"
          :database "default"
          :lease-ttl-ms 375M
          :heartbeat-interval-ms 125M
          :retry-deadline-ms 1000
          :retry-initial-backoff-ms 1
          :retry-max-backoff-ms 1
          :operations
          (assoc base-operations
                 :now-ms #(deref now-ms)
                 :monotonic-ms! (constantly 0)
                 :await-backoff!
                 (fn [_]
                   (swap! backoffs inc)
                   ;; Recovery extended the original lease by one ms.
                   (reset! now-ms (+ python-now-ms 501M)))
                 :await-heartbeat!
                 (fn [stop _]
                   (if (= 1 (swap! heartbeat-ticks inc))
                     :tick
                     (do @stop :stop))))})]
    (try
      (check "heartbeat retry stops at locally proved expiry before another CAS"
             [true false 2 1 ::control/lease-fenced]
             [(await-fenced! opened)
              (:writable? (writer/status opened))
              @replace-count
              @backoffs
              (error-type
               #(writer/execute! opened "INSERT INTO t VALUES (2)"))])
      (finally
        (try (writer/close! opened) (catch Throwable _)))))

  (let [store (fixture-store legacy-millisecond-fixture)]
    (check "legacy millisecond expiry is not guessed from its magnitude"
           ::control/lease-held
           (error-type #(open! store python-now-ms {})))
    (let [opened (open! store python-now-ms {:force? true})]
      (try
        (check "explicit force migration increments generation and rewrites seconds"
               [2 "jolt-writer" 1788230400.376M]
               (let [lease (get-in (:head (control/read-head! store)) ["lease"])]
                 [(get lease "generation")
                  (get lease "owner")
                  (get lease "expires_at")]))
        (finally (writer/close! opened)))))
  (when-not (zero? @failures)
    (throw (ex-info (str @failures " epoch-seconds checks failed")
                    {:failures @failures})))
  (println "Durable epoch-seconds checks passed")
  true)

(defn -main [& _]
  (run-checks!))
