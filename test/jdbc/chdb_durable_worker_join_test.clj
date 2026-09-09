(ns jdbc.chdb-durable-worker-join-test
  "Deterministic close accounting for reader and writer operation threads."
  (:require [jdbc.chdb.durable.owned-thread :as owned-thread]
            [jdbc.chdb.durable.reader :as reader]
            [jdbc.chdb.durable.writer :as writer]))

(def failures (atom 0))

(defn- check [label expected actual]
  (if (= expected actual)
    (println "  ok  " label)
    (do
      (swap! failures inc)
      (println "  FAIL" label "- expected" (pr-str expected)
               "got" (pr-str actual)))))

(defn- close-thread [label close! events started outcome]
  (Thread.
   (fn []
     (deliver started true)
     (let [result (try
                    {:value (close!)}
                    (catch Throwable error {:error error}))]
       (swap! events conj label)
       (deliver outcome result)))))

(defn- exit-barrier [events entered release entered-event returned-event]
  (fn []
    (swap! events conj entered-event)
    (deliver entered true)
    @release
    (swap! events conj returned-event)))

(defn- start-with-thread-hooks [hooks start-resource!]
  (let [real-start! owned-thread/start!
        index (atom -1)]
    (with-redefs
     [owned-thread/start!
      (fn [completion f]
        (let [hook (get hooks (swap! index inc))]
          (real-start!
           completion
           #(try
              (f)
              (finally
                (when hook (hook)))))))]
     (start-resource!))))

(defn- worker-alive? [resource]
  (.isAlive ^Thread @(-> resource :worker :thread)))

(defn- run-reader-checks! []
  (println "Durable reader operation-worker close join")
  (let [events (atom [])
        barrier-entered (promise)
        release-barrier (promise)
        closed (promise)
        started (promise)
        durable-reader
        (start-with-thread-hooks
         [(exit-barrier events barrier-entered release-barrier
                        :worker-exit-barrier :worker-function-return)]
         #(reader/start!
           {:handle :fake-handle
            :database "default"
            :operations
            {:close-native! (fn [_] (swap! events conj :native-close))
             :cleanup-scratch! (fn [] (swap! events conj :cleanup))}}))
        closing (close-thread :public-close-returned
                              #(reader/close! durable-reader)
                              events started closed)]
    (.start closing)
    @started
    (try
      (check "reader worker reaches its post-loop barrier"
             true (not= ::timeout (deref barrier-entered 5000 ::timeout)))
      (check "reader close does not return while its worker remains live"
             ::pending (deref closed 20 ::pending))
      (check "reader worker is still live at the red-control boundary"
             true (worker-alive? durable-reader))
      (finally
        (deliver release-barrier true)))
    (.join closing 5000)
    (check "reader close returns only after the worker exits"
           {:value nil} @closed)
    (check "reader worker is absent after public close"
           false (worker-alive? durable-reader))
    (check "reader resource trace closes once before worker and caller exit"
           [:native-close :cleanup :worker-exit-barrier
            :worker-function-return :public-close-returned]
           @events)
    (reader/close! durable-reader)
    (check "repeated reader close neither recreates nor rejoins live work"
           [:native-close :cleanup :worker-exit-barrier
            :worker-function-return :public-close-returned]
           @events)))

(defn- run-writer-checks! []
  (println "Durable writer operation-worker close join")
  (let [events (atom [])
        barrier-entered (promise)
        release-barrier (promise)
        original (ex-info "injected native close failure"
                          {:type ::native-close-failed})
        durable-writer
        (start-with-thread-hooks
         [(exit-barrier events barrier-entered release-barrier
                        :worker-exit-barrier :worker-function-return)]
         #(writer/start!
           {:store :fake-store
            :token {:owner "owner" :instance "instance" :generation 1}
            :handle :fake-handle
            :database "default"
            :operations
            {:release! (fn [_ _] (swap! events conj :release))
             :close-native! (fn [_]
                              (swap! events conj :native-close)
                              (throw original))
             :cleanup-scratch! (fn [] (swap! events conj :cleanup))}}))
        started-a (promise)
        started-b (promise)
        result-a (promise)
        result-b (promise)
        close-a (close-thread :public-close-a
                              #(writer/close! durable-writer)
                              events started-a result-a)
        close-b (close-thread :public-close-b
                              #(writer/close! durable-writer)
                              events started-b result-b)]
    (.start close-a)
    (.start close-b)
    @started-a
    @started-b
    (try
      (check "writer worker reaches its post-loop barrier"
             true (not= ::timeout (deref barrier-entered 5000 ::timeout)))
      (check "both concurrent writer closes await worker exit"
             [::pending ::pending]
             [(deref result-a 20 ::pending) (deref result-b 20 ::pending)])
      (check "writer worker is live at the red-control boundary"
             true (worker-alive? durable-writer))
      (finally
        (deliver release-barrier true)))
    (.join close-a 5000)
    (.join close-b 5000)
    (check "owner close rethrows the exact cleanup failure after join"
           true (identical? original (:error @result-a)))
    (check "concurrent close rethrows the exact same failure after join"
           true (identical? original (:error @result-b)))
    (check "writer worker is absent after both public closes"
           false (worker-alive? durable-writer))
    (check "writer resource cleanup and worker exit occur exactly once"
           {:release 1 :native-close 1 :cleanup 1
            :worker-exit-barrier 1 :worker-function-return 1
            :public-closes 2}
           {:release (count (filter #{:release} @events))
            :native-close (count (filter #{:native-close} @events))
            :cleanup (count (filter #{:cleanup} @events))
            :worker-exit-barrier (count (filter #{:worker-exit-barrier} @events))
            :worker-function-return (count (filter #{:worker-function-return} @events))
            :public-closes (count (filter #{:public-close-a :public-close-b}
                                          @events))})
    (let [repeated (try
                     (writer/close! durable-writer)
                     nil
                     (catch Throwable error error))]
      (check "repeated writer close retains exact failure identity"
             true (identical? original repeated)))
    (check "repeated writer close does not repeat resource cleanup"
           [1 1 1]
           [(count (filter #{:release} @events))
            (count (filter #{:native-close} @events))
            (count (filter #{:cleanup} @events))])))

(defn- run-self-join-checks! []
  (println "Durable owned-thread self-join rejection")
  (let [completion (owned-thread/completion)
        observed (promise)]
    (owned-thread/start!
     completion
     #(let [error (try
                    (owned-thread/join! completion)
                    nil
                    (catch Throwable error error))]
        (deliver observed error)
        (throw error)))
    (let [inside @observed
          outside (try
                    (owned-thread/join! completion)
                    nil
                    (catch Throwable error error))]
      (check "same-thread join fails with its exact public category"
             ::owned-thread/self-join (:type (ex-data inside)))
      (check "the owned completion retains the exact self-join exception"
             true (identical? inside outside))))
  (let [events (atom [])
        holder (atom nil)
        reentrant-error (promise)
        durable-reader
        (reader/start!
         {:handle :fake-handle
          :database "default"
          :operations
          {:close-native!
           (fn [_]
             (swap! events conj :native-close)
             (let [error (try
                           (reader/close! @holder)
                           nil
                           (catch Throwable error error))]
               (deliver reentrant-error error)
               (throw error)))
           :cleanup-scratch! (fn [] (swap! events conj :cleanup))}})]
    (reset! holder durable-reader)
    (let [external-error (try
                           (reader/close! durable-reader)
                           nil
                           (catch Throwable error error))]
      (check "native-close reentry fails fast instead of awaiting its own result"
             ::owned-thread/self-join
             (:type (ex-data @reentrant-error)))
      (check "external close observes that exact reentrant worker failure"
             true (identical? @reentrant-error external-error))
      (check "reentrant native-close failure still runs reader cleanup once"
             [:native-close :cleanup] @events)
      (check "reentrant failure still leaves no owned reader worker"
             false (worker-alive? durable-reader))))
  (let [events (atom [])
        holder (atom nil)
        reentrant-error (promise)
        durable-writer
        (writer/start!
         {:store :fake-store
          :token {:owner "owner" :instance "instance" :generation 1}
          :handle :fake-handle
          :database "default"
          :operations
          {:release! (fn [_ _] (swap! events conj :release))
           :close-native! (fn [_] (swap! events conj :native-close))
           :cleanup-scratch!
           (fn []
             (swap! events conj :cleanup)
             (let [error (try
                           (writer/close! @holder)
                           nil
                           (catch Throwable error error))]
               (deliver reentrant-error error)
               (throw error)))}})]
    (reset! holder durable-writer)
    (let [external-error (try
                           (writer/close! durable-writer)
                           nil
                           (catch Throwable error error))]
      (check "writer cleanup reentry has the self-join category"
             ::owned-thread/self-join
             (:type (ex-data @reentrant-error)))
      (check "writer close preserves the exact cleanup reentry failure"
             true (identical? @reentrant-error external-error))
      (check "writer release, native close, and reentrant cleanup run once"
             [:release :native-close :cleanup] @events)
      (check "cleanup reentry still leaves no owned writer worker"
             false (worker-alive? durable-writer))
      (let [repeated (try
                       (writer/close! durable-writer)
                       nil
                       (catch Throwable error error))]
        (check "repeated writer close retains exact reentry failure identity"
               true (identical? @reentrant-error repeated))))))

(defn- run-heartbeat-join-checks! []
  (println "Durable heartbeat OS-thread close join")
  (let [events (atom [])
        barrier-entered (promise)
        release-barrier (promise)
        close-started (promise)
        close-result (promise)
        heartbeat-hook
        (exit-barrier events barrier-entered release-barrier
                      :heartbeat-exit-barrier :heartbeat-function-return)
        durable-writer
        (start-with-thread-hooks
         [nil heartbeat-hook]
         #(writer/start!
           {:store :fake-store
            :token {:owner "owner" :instance "instance" :generation 1}
            :handle :fake-handle
            :database "default"
            :lease-expiry 10000
            :lease-ttl-ms 3000
            :heartbeat-interval-ms 1000
            :operations
            {:now-ms (constantly 0)
             :await-heartbeat! (fn [stop _] @stop :stop)
             :release! (fn [_ _] (swap! events conj :release))
             :close-native! (fn [_] (swap! events conj :native-close))
             :cleanup-scratch! (fn [] (swap! events conj :cleanup))}}))
        closing (close-thread :public-close-returned
                              #(writer/close! durable-writer)
                              events close-started close-result)]
    (.start closing)
    @close-started
    (try
      (check "heartbeat reaches its test-scoped post-loop barrier"
             true (not= ::timeout (deref barrier-entered 5000 ::timeout)))
      (check "heartbeat OS thread remains live at the barrier"
             true (.isAlive ^Thread @(-> durable-writer :heartbeat :thread)))
      (check "release waits for actual heartbeat-thread exit"
             0 (count (filter #{:release} @events)))
      (check "public close also waits for actual heartbeat-thread exit"
             ::pending (deref close-result 20 ::pending))
      (finally
        (deliver release-barrier true)))
    (.join closing 5000)
    (check "heartbeat exits before release, cleanup, and public return"
           [:heartbeat-exit-barrier :heartbeat-function-return
            :release :native-close :cleanup :public-close-returned]
           @events)
    (check "heartbeat is absent after public close"
           false (.isAlive ^Thread @(-> durable-writer :heartbeat :thread)))
    (check "operation worker is absent after heartbeat-ordered public close"
           false (worker-alive? durable-writer))))

(defn run-checks! []
  (reset! failures 0)
  (run-reader-checks!)
  (run-writer-checks!)
  (run-self-join-checks!)
  (run-heartbeat-join-checks!)
  (when-not (zero? @failures)
    (throw (ex-info (str @failures " Durable worker join checks failed")
                    {:failures @failures})))
  (println "all Durable worker join checks passed")
  true)

(defn -main [& _]
  (run-checks!))
