(ns jdbc.chdb-native-lifecycle-test
  (:require [clojure.data.json :as json]
            [jdbc.chdb.native :as native]
            [jolt.ffi :as ffi])
  (:import [java.nio.file Files]
           [java.nio.file.attribute FileAttribute]))

(def ^:private trace-path
  "formal/quint/traces/native-process-lifecycle.itf.json")
(def ^:private terminal-trace-path
  "formal/quint/traces/native-process-terminal.itf.json")
(def ^:private options-trace-path
  "formal/quint/traces/native-process-options.itf.json")

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
        shutdown-hooks (atom [])
        events (atom [])
        connect
        (fn [& _]
          (swap! events conj :connect)
          (let [index (swap! connects inc)
                result (or (first @remaining) :valid)]
            (swap! remaining #(if (seq %) (vec (rest %)) %))
            (case result
              :null-owner nil
              :connect-error (throw (ex-info "uncertain native connect" {}))
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
             (case (:result owner)
               :null-connection nil
               :read-error (throw (ex-info "uncertain owner read" {}))
               [:connection (:owner owner)]))
     :null? nil?
     :set-signals (fn [enabled]
                    (swap! events conj [:signals enabled])
                    (swap! signals conj enabled))
     :register-shutdown-hook
     (fn []
       (swap! events conj :register-shutdown-hook)
       (swap! shutdown-hooks conj
              (fn [] ((deref (private-var 'close-anchor-at-process-exit!))))))
     :connects connects
     :closes closes
     :active active
     :boots boots
     :signals signals
     :shutdown-hooks shutdown-hooks
     :events events}))

(defn- with-fake-native [runtime f]
  (with-redefs-fn
    {(private-var 'driver-support) (delay {:status :supported})
     (private-var 'signals-disabled?) (atom false)
     (private-var 'storage-state)
     (atom {:phase :cold :path nil :references 0
            :bootstrap-options nil :anchor-owner nil})
     (private-var 'register-anchor-shutdown-hook!)
     (:register-shutdown-hook runtime)
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

(defn- canonical-path-checks! []
  (let [root (Files/createTempDirectory
              "jchdb-native-path-" (make-array FileAttribute 0))
        target (.resolve root "target")
        alias (.resolve root "alias")
        lexical (.resolve target "../target/db")
        linked (.resolve alias "db")
        normalized native/canonical-storage-path]
    (try
      (Files/createDirectory target (make-array FileAttribute 0))
      (Files/createSymbolicLink alias target (make-array FileAttribute 0))
      (check "lexical path aliases share one canonical process identity"
             (normalized (str (.resolve target "db")))
             (normalized (str lexical)))
      (check "symlink path aliases share one canonical process identity"
             (normalized (str (.resolve target "db")))
             (normalized (str linked)))
      (finally
        (Files/deleteIfExists alias)
        (Files/deleteIfExists target)
        (Files/deleteIfExists root)))))

(defn- trace-int [value]
  (bigint (get value "#bigint")))

(defn- model-state [state]
  (let [lifecycle (get state "lifecycle")
        anchored? (get lifecycle "anchored")
        terminal? (get lifecycle "terminal")
        normalized native/canonical-storage-path]
    {:anchored? (get lifecycle "anchored")
     :phase (cond terminal? :terminal anchored? :anchored :else :cold)
     :path (when anchored?
             (case (get-in lifecycle ["path" "tag"])
               "Memory" ":memory:"
               "Disk" (normalized "/tmp/lifecycle-path")))
     :bootstrap-options
     (when anchored?
       {:backups-allowed-path
        (case (get-in lifecycle ["bootstrapOptions" "tag"])
          "NoBackups" nil
          "BackupsA" (normalized "/tmp/backups-a")
          "BackupsB" (normalized "/tmp/backups-b"))})
     :references (trace-int (get lifecycle "references"))
     :boots (trace-int (get lifecycle "boots"))
     :different-path-accepted? (get lifecycle "differentPathAccepted")
     :last-action (get-in lifecycle ["lastAction" "tag"])}))

(defn- runtime-state [runtime last-action different-path-accepted?]
  (let [state @@(private-var 'storage-state)]
    (merge (native/active-storage)
           {:bootstrap-options (:bootstrap-options state)}
         {:boots (bigint @(:boots runtime))
          :different-path-accepted? different-path-accepted?
          :last-action last-action})))

(def ^:private refinement-keys
  [:anchored? :phase :path :bootstrap-options :references :boots
   :different-path-accepted? :last-action])

(defn- check-refinement! [label observations]
  (doseq [[index {:keys [model runtime]}]
          (map-indexed vector observations)]
    (check (str label " ITF state " index " refines native ownership")
           (select-keys model refinement-keys)
           (select-keys runtime refinement-keys))))

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

(defn- replay-terminal-trace! [runtime trace]
  (let [states (get trace "states")
        actions (mapv #(get % "mbt::actionTaken") states)]
    (when-not (= ["init" "uncertainBootstrapFailure" "retryAfterTerminal"]
                 actions)
      (throw (ex-info "Native terminal ITF actions are not canonical"
                      {:type ::invalid-trace :actions actions})))
    (mapv
     (fn [state]
       (case (get state "mbt::actionTaken")
         "init" nil
         "uncertainBootstrapFailure"
         (when-not (rejected #(native/open! ":memory:"))
           (throw (ex-info "uncertain bootstrap model step was accepted" {})))
         "retryAfterTerminal"
         (let [error (rejected #(native/open! ":memory:"))]
           (when-not (= ::native/terminal-native-lifecycle
                        (:type (ex-data error)))
             (throw (ex-info "terminal retry model step was not rejected" {})))))
       {:model (model-state state)
        :runtime (runtime-state runtime
                                (get-in state ["lifecycle" "lastAction" "tag"])
                                false)})
     states)))

(defn- replay-options-trace! [runtime trace]
  (let [states (get trace "states")
        actions (mapv #(get % "mbt::actionTaken") states)
        handle (atom nil)]
    (when-not (= ["init" "openDiskWithBackups" "closePublic"
                  "openDifferentOptions"] actions)
      (throw (ex-info "Native options ITF actions are not canonical"
                      {:type ::invalid-trace :actions actions})))
    (mapv
     (fn [state]
       (case (get state "mbt::actionTaken")
         "init" nil
         "openDiskWithBackups"
         (reset! handle
                 (native/open! "/tmp/lifecycle-path"
                               {:backups-allowed-path "/tmp/backups-a"}))
         "closePublic" (do (native/close! @handle) (reset! handle nil))
         "openDifferentOptions"
         (when-not
          (rejected #(native/open! "/tmp/lifecycle-path"
                                   {:backups-allowed-path "/tmp/backups-b"}))
          (throw (ex-info "different-options model step was accepted" {}))))
       {:model (model-state state)
        :runtime (runtime-state runtime
                                (get-in state ["lifecycle" "lastAction" "tag"])
                                false)})
     states)))

(defn run-checks! []
  (println "chDB process-lifetime native ownership")

  (canonical-path-checks!)

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
          (check "anchor shutdown hook is registered once before public open"
                 [1 [[:signals 0] :connect :register-shutdown-hook :connect]]
                 [(count @(:shutdown-hooks runtime))
                  (vec (take 4 @(:events runtime)))])
          (check "active storage never exposes the anchor owner pointer"
                 #{:phase :path :references :anchored?}
                 (set (keys (native/active-storage))))

          ((first @(:shutdown-hooks runtime)))
          ((first @(:shutdown-hooks runtime)))
          (check "process exit closes the hidden anchor exactly once"
                 [0 3 {:phase :exit-closed :path ":memory:"
                       :references 0 :anchored? false}]
                 [@(:active runtime) (count @(:closes runtime))
                  (native/active-storage)])
          (let [before-connects @(:connects runtime)
                error (rejected #(native/open! ":memory:"))]
            (check "process-exit claim rejects every later native open"
                   [::native/process-exiting before-connects]
                   [(:type (ex-data error)) @(:connects runtime)]))))))

  (let [runtime (fake-native [])]
    (with-fake-native
      runtime
      (fn []
        (let [handle (native/open! ":memory:")
              close-error (ex-info "uncertain public close" {:stage :close})
              close-attempts (atom 0)]
          (with-redefs-fn
            {#'native/chdb-close-conn
             (fn [owner]
               ;; The first owner is the retained anchor; the second is this
               ;; public handle. A destructor error cannot establish whether
               ;; that public owner was released, so a second destructor call
               ;; would be an unsafe retry.
               (if (= 2 (:owner owner))
                 (do
                   (swap! close-attempts inc)
                   (throw close-error))
                 ((:close runtime) owner)))}
            (fn []
              (let [error (rejected #(native/close! handle))
                    second-close (native/close! handle)
                    closed-error
                    (rejected #(native/with-live-handle handle identity))]
                (check "public close failure preserves its exact throwable"
                       close-error error)
                (check "failed public close marks its handle unavailable"
                       true (:db.chdb/closed (ex-data closed-error)))
                (check "failed public close never retries its destructor"
                       [nil 1] [second-close @close-attempts])
                (check "failed public close retains an uncertain reference"
                       {:phase :anchored :path ":memory:"
                        :references 1 :anchored? true}
                       (native/active-storage)))))
          ;; The retained anchor prevents a failed public close from crossing
          ;; the unsafe last-close boundary. A later same-path public owner is
          ;; therefore still permitted, but it cannot erase the uncertain
          ;; reference from the failed owner.
          (let [retry (native/open! ":memory:")]
            (native/close! retry)
            (check "same-path retry after public close failure keeps one boot"
                   [1 3 1 {:phase :anchored :path ":memory:"
                           :references 1 :anchored? true}]
                   [@(:boots runtime) @(:connects runtime)
                    (count @(:closes runtime)) (native/active-storage)]))))))

  (let [runtime (fake-native [])]
    (with-fake-native
      runtime
      (fn []
        (let [handle (native/open! ":memory:")
              hook (first @(:shutdown-hooks runtime))
              anchor-close-attempts (atom 0)]
          (native/close! handle)
          (with-redefs-fn
            {#'native/chdb-close-conn
             (fn [_]
               (swap! anchor-close-attempts inc)
               (throw (ex-info "anchor close failed" {})))}
            (fn []
              (let [close-error (rejected hook)]
                ;; A claimed native destructor is terminal even when it throws:
                ;; retrying could double-free an owner that closed partially.
                (hook)
                (let [before-connects @(:connects runtime)
                      open-error (rejected #(native/open! ":memory:"))]
                  (check "failed exit close stays claimed and rejects reopen"
                         ["anchor close failed" 1
                          {:phase :exiting :path ":memory:"
                           :references 0 :anchored? false}
                          ::native/process-exiting before-connects]
                         [(ex-message close-error) @anchor-close-attempts
                          (native/active-storage)
                          (:type (ex-data open-error))
                          @(:connects runtime)])))))))))

  (let [runtime (fake-native [])]
    (with-fake-native
      runtime
      (fn []
        (with-redefs-fn
          {(private-var 'register-anchor-shutdown-hook!)
           (fn [] (throw (ex-info "shutdown hook rejected" {})))}
          (fn []
            (let [error (rejected #(native/open! ":memory:"))]
              (check "hook-registration failure retires owner and is terminal"
                     ["shutdown hook rejected" 1 0
                      {:phase :terminal :path nil
                       :references 0 :anchored? false}]
                     [(ex-message error) (count @(:closes runtime))
                      @(:active runtime) (native/active-storage)])))))))

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

  (let [runtime (fake-native [:null-owner :valid :valid])]
    (with-fake-native
      runtime
      (fn []
        (let [error (rejected #(native/open! ":memory:"))]
          (check "failed bootstrap without an owner leaves no path claim"
                 ["chDB connection failed"
                  {:phase :cold :path nil :references 0 :anchored? false}
                  0]
                 [(ex-message error) (native/active-storage)
                  (count @(:closes runtime))]))
        (let [handle (native/open! ":memory:")]
          (native/close! handle)
          (check "documented null-owner bootstrap is safe to retry"
                 [3 1 1 {:phase :anchored :path ":memory:"
                         :references 0 :anchored? true}]
                 [@(:connects runtime) @(:boots runtime) @(:active runtime)
                  (native/active-storage)])))))

  (let [runtime (fake-native [])
        trace (json/read-str (slurp trace-path))]
    (with-fake-native
      runtime
      (fn []
        (check-refinement! "anchor" (replay-model-trace! runtime trace)))))

  (let [runtime (fake-native [:connect-error])
        trace (json/read-str (slurp terminal-trace-path))]
    (with-fake-native
      runtime
      #(check-refinement! "terminal"
                          (replay-terminal-trace! runtime trace))))

  (let [runtime (fake-native [])
        trace (json/read-str (slurp options-trace-path))]
    (with-fake-native
      runtime
      #(check-refinement! "options"
                          (replay-options-trace! runtime trace))))

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

  (doseq [failure [:connect-error :read-error]]
    (let [runtime (fake-native [failure])]
      (with-fake-native
        runtime
        (fn []
          (let [first-error (rejected #(native/open! ":memory:"))
                before-connects @(:connects runtime)
                retry-error (rejected #(native/open! ":memory:"))]
            (check (str "uncertain bootstrap " (name failure)
                        " makes lifecycle terminal")
                   [(case failure
                      :connect-error "uncertain native connect"
                      :read-error "uncertain owner read")
                    ::native/terminal-native-lifecycle
                    before-connects
                    (if (= failure :read-error) 1 0)
                    {:phase :terminal :path nil
                     :references 0 :anchored? false}]
                   [(ex-message first-error) (:type (ex-data retry-error))
                    @(:connects runtime) (count @(:closes runtime))
                    (native/active-storage)]))))))

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
