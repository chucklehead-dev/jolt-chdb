(ns jdbc.chdb-native-lifecycle-test
  (:require [clojure.data.json :as json]
            [jdbc.chdb.native :as native]
            [jolt.ffi :as ffi]))

(def ^:private trace-path
  "formal/quint/traces/native-process-lifecycle.itf.json")

(def failures (atom 0))

(defn- check [label expected actual]
  (if (= expected actual)
    (println "  ok  " label)
    (do
      (swap! failures inc)
      (println "  FAIL" label "- expected" (pr-str expected)
               "got" (pr-str actual)))))

(defn- rejected [f]
  (try (f) nil (catch Throwable error error)))

(defn- private-var [symbol]
  (or (ns-resolve 'jdbc.chdb.native symbol)
      (throw (ex-info "missing lifecycle test seam" {:symbol symbol}))))

(defn- fake-native [results]
  (let [remaining (atom results)
        connects (atom 0)
        closes (atom [])
        active (atom 0)
        boots (atom 0)
        signals (atom [])
        events (atom [])
        connect
        (fn [& _]
          (swap! events conj :connect)
          (let [index (swap! connects inc)
                result (or (first @remaining) :valid)]
            (swap! remaining #(if (seq %) (vec (rest %)) %))
            (case result
              :null-owner nil
              (let [owner {:owner index :result result}]
                (when (zero? @active) (swap! boots inc))
                (swap! active inc)
                owner))))
        close
        (fn [owner]
          (swap! closes conj owner)
          (swap! active dec))]
    {:connect connect
     :close close
     :read (fn [owner _]
             (when-not (= :null-connection (:result owner))
               [:connection (:owner owner)]))
     :null? nil?
     :set-signals (fn [enabled]
                    (swap! events conj [:signals enabled])
                    (swap! signals conj enabled))
     :connects connects
     :closes closes
     :active active
     :boots boots
     :signals signals
     :events events}))

(defn- with-fake-native [runtime f]
  (with-redefs-fn
    {(private-var 'driver-support) (delay {:status :supported})
     (private-var 'signals-disabled?) (atom false)
     (private-var 'storage-state)
     (atom {:phase :cold :path nil :references 0
            :bootstrap-options nil :anchor-owner nil})
     #'native/chdb-set-signal-handlers-enabled (:set-signals runtime)
     #'native/chdb-connect (:connect runtime)
     #'native/chdb-close-conn (:close runtime)
     #'ffi/read (:read runtime)
     #'ffi/null? (:null? runtime)}
    f))

(defn- legacy-last-close-cycle! [runtime]
  (dotimes [_ 2]
    (let [owner ((:connect runtime) 0 nil)]
      ((:close runtime) owner))))

(defn- trace-int [value]
  (bigint (get value "#bigint")))

(defn- model-state [state]
  (let [lifecycle (get state "lifecycle")
        anchored? (get lifecycle "anchored")]
    {:anchored? (get lifecycle "anchored")
     :path (when anchored?
             (case (get-in lifecycle ["path" "tag"])
               "Memory" ":memory:"
               "Disk" "/tmp/different-path"))
     :references (trace-int (get lifecycle "references"))
     :boots (trace-int (get lifecycle "boots"))
     :different-path-accepted? (get lifecycle "differentPathAccepted")
     :last-action (get-in lifecycle ["lastAction" "tag"])}))

(defn- runtime-state [runtime last-action different-path-accepted?]
  (merge (native/active-storage)
         {:boots (bigint @(:boots runtime))
          :different-path-accepted? different-path-accepted?
          :last-action last-action}))

(defn- replay-model-trace! [runtime trace]
  (let [states (get trace "states")
        actions (mapv #(get % "mbt::actionTaken") states)
        handle (atom nil)]
    (when-not (= ["init" "openMemory" "closePublic" "openMemory"
                  "closePublic" "openDifferent"] actions)
      (throw (ex-info "Native lifecycle ITF actions are not canonical"
                      {:type ::invalid-trace :actions actions})))
    (mapv
     (fn [state]
       (let [action (get state "mbt::actionTaken")]
         (case action
           "init" nil
           "openMemory" (reset! handle (native/open! ":memory:"))
           "closePublic" (do (native/close! @handle) (reset! handle nil))
           "openDifferent"
           (when-not (rejected #(native/open! "/tmp/different-path"))
             (throw (ex-info "different-path model step was accepted"
                             {:type ::invalid-replay}))))
         {:model (model-state state)
          :runtime (runtime-state runtime
                                  (get-in state ["lifecycle" "lastAction" "tag"])
                                  false)}))
     states)))

(defn run-checks! []
  (println "chDB process-lifetime native ownership")

  (let [legacy (fake-native [])]
    (legacy-last-close-cycle! legacy)
    (check "red control boots once per legacy last-close/reopen cycle"
           [2 2 0]
           [@(:boots legacy) @(:connects legacy) @(:active legacy)]))

  (let [runtime (fake-native [])]
    (with-fake-native
      runtime
      (fn []
        (let [first-handle (native/open! ":memory:")]
          (native/close! first-handle)
          (native/close! first-handle)
          (check "logical last close retains one hidden native anchor"
                 [1 2 1 1
                  {:phase :anchored :path ":memory:"
                   :references 0 :anchored? true}]
                 [@(:boots runtime) @(:connects runtime) @(:active runtime)
                  (count @(:closes runtime)) (native/active-storage)])

          (let [second-handle (native/open! ":memory:")]
            (check "same-path sequential reopen reuses one engine boot"
                   [1 3 2 1]
                   [@(:boots runtime) @(:connects runtime) @(:active runtime)
                    (:references (native/active-storage))])
            (native/close! second-handle))

          (let [before-connects @(:connects runtime)
                error (rejected #(native/open! "/tmp/different-path"))]
            (check "different path remains rejected after logical last close"
                   [::native/different-process-path true before-connects]
                   [(:type (ex-data error)) (:jdbc/sql-error (ex-data error))
                    @(:connects runtime)]))

          (check "signal control is disabled exactly once before bootstrap"
                 [0] @(:signals runtime))
          (check "host signal ownership is configured before native bootstrap"
                 [[:signals 0] :connect]
                 (vec (take 2 @(:events runtime))))
          (check "active storage never exposes the anchor owner pointer"
                 #{:phase :path :references :anchored?}
                 (set (keys (native/active-storage))))))))

  (let [runtime (fake-native [])]
    (with-fake-native
      runtime
      (fn []
        (let [handle (native/open! "/tmp/lifecycle-path"
                                   {:backups-allowed-path "/tmp/backups-a"})]
          (native/close! handle))
        (let [error
              (rejected
               #(native/open! "/tmp/lifecycle-path"
                              {:backups-allowed-path "/tmp/backups-b"}))]
          (check "same-path reopen cannot silently replace bootstrap options"
                 [::native/different-process-options true 2]
                 [(:type (ex-data error)) (:jdbc/sql-error (ex-data error))
                  @(:connects runtime)])))))

  (let [runtime (fake-native [:null-owner])]
    (with-fake-native
      runtime
      (fn []
        (let [error (rejected #(native/open! ":memory:"))]
          (check "failed bootstrap without an owner leaves no path claim"
                 ["chDB connection failed"
                  {:phase :cold :path nil :references 0 :anchored? false}
                  0]
                 [(ex-message error) (native/active-storage)
                  (count @(:closes runtime))])))))

  (let [runtime (fake-native [])
        trace (json/read-str (slurp trace-path))]
    (with-fake-native
      runtime
      (fn []
        (let [observations (replay-model-trace! runtime trace)]
          (doseq [[index {:keys [model runtime]}]
                  (map-indexed vector observations)]
            (check (str "ITF state " index " refines native ownership")
                   (select-keys model
                                [:anchored? :path :references :boots
                                 :different-path-accepted? :last-action])
                   (select-keys runtime
                                [:anchored? :path :references :boots
                                 :different-path-accepted? :last-action])))))))

  (let [runtime (fake-native [:null-connection])]
    (with-fake-native
      runtime
      (fn []
        (let [first-error (rejected #(native/open! ":memory:"))
              before-connects @(:connects runtime)
              retry-error (rejected #(native/open! ":memory:"))]
          (check "closed invalid bootstrap owner makes lifecycle terminal"
                 ["chDB returned a null connection"
                  ::native/terminal-native-lifecycle
                  true before-connects 1
                  {:phase :terminal :path nil
                   :references 0 :anchored? false}]
                 [(ex-message first-error) (:type (ex-data retry-error))
                  (:jdbc/sql-error (ex-data retry-error))
                  @(:connects runtime) (count @(:closes runtime))
                  (native/active-storage)])))))

  (let [runtime (fake-native [:valid :null-owner :valid])]
    (with-fake-native
      runtime
      (fn []
        (let [error (rejected #(native/open! ":memory:"))]
          (check "failed public open retains the fully published anchor"
                 ["chDB connection failed"
                  {:phase :anchored :path ":memory:"
                   :references 0 :anchored? true}
                  1 0]
                 [(ex-message error) (native/active-storage)
                  @(:active runtime) (count @(:closes runtime))]))
        (let [handle (native/open! ":memory:")]
          (native/close! handle)
          (check "same-path retry after public failure remains safe"
                 [1 3 1 1]
                 [@(:boots runtime) @(:connects runtime) @(:active runtime)
                  (count @(:closes runtime))])))))

  (when (pos? @failures)
    (throw (ex-info "chDB native lifecycle checks failed"
                    {:failures @failures})))
  (println "all chDB native lifecycle checks passed"))

(defn -main [& _]
  (reset! failures 0)
  (run-checks!))
