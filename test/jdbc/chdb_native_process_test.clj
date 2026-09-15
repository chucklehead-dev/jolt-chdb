(ns jdbc.chdb-native-process-test
  (:require [jdbc.chdb :as chdb]
            [jdbc.chdb.native :as native]
            [jolt.ffi :as ffi]))

(ffi/load-library)
(ffi/defcfn c-sigaction "sigaction" [:int :pointer :pointer] :int)

(def ^:private signals
  {:ill 4 :abrt 6 :fpe 8 :segv 11
   :bus (if (= :darwin (:os (native/platform))) 10 7)})

(defn- sigaction-size []
  (case (:os (native/platform))
    :linux 152
    :darwin 16))

(defn- signal-handlers []
  (into (sorted-map)
        (map
         (fn [[signal number]]
           (let [size (sigaction-size)
                 action (ffi/alloc size)]
             (try
               (ffi/write-array action (byte-array size))
               (when-not (zero? (c-sigaction number ffi/null action))
                 (throw (ex-info "sigaction snapshot failed"
                                 {:signal signal :number number})))
               ;; Match upstream's oracle: handler identity is the first
               ;; pointer-sized field. Kernel/libc-owned padding and masks are
               ;; not stable observations of handler replacement.
               [signal (mapv #(bit-and 255 %)
                             (take (ffi/sizeof :pointer)
                                   (ffi/read-array action :byte size)))]
               (finally (ffi/free action))))))
        signals))

(defn- scalar [handle sql]
  (-> (chdb/execute-any handle sql []) :rows first first))

(defn- assert= [message expected actual]
  (when-not (= expected actual)
    (throw (ex-info message {:expected expected :actual actual}))))

(defn- within-process! []
  (let [before (signal-handlers)
        first-handle (native/open! ":memory:")
        observations (atom [["open" (signal-handlers)]])]
    (when-not (some #(some pos? %) (vals before))
      (throw (ex-info "host signal-preservation probe is vacuous"
                      {:handlers before})))
    (try
      (chdb/execute-any
       first-handle
       "CREATE TABLE process_anchor_marker (id UInt8) ENGINE = Memory" [])
      (chdb/execute-any first-handle
                        "INSERT INTO process_anchor_marker VALUES (1)" [])
      (swap! observations conj ["query" (signal-handlers)])
      (finally
        (native/close! first-handle)
        (swap! observations conj ["logical close" (signal-handlers)])))
    (let [second-handle (native/open! ":memory:")]
      (try
        (assert= "same-process :memory: reopen lost anchored state"
                 1 (scalar second-handle
                           "SELECT count() FROM process_anchor_marker"))
        (swap! observations conj ["same-path reopen" (signal-handlers)])
        (finally
          (native/close! second-handle)
          (swap! observations conj
                 ["second logical close" (signal-handlers)]))))
    (let [connect-error
          (try (native/open! "/tmp/jolt-chdb-different-process-path") nil
               (catch Throwable error error))]
      (assert= "different path was not rejected before native connect"
               ::native/different-process-path
               (:type (ex-data connect-error))))
    (doseq [[phase actual] @observations]
      (assert= (str "chDB changed host signal dispositions during " phase)
               before actual))
    (println "PASS native anchor reuse, immutable path, and signal preservation")))

(defn- fresh-process! []
  (let [before (signal-handlers)
        handle (native/open! ":memory:")]
    (try
      (assert= "a fresh process inherited another process's :memory: database"
               0
               (scalar handle
                       (str "SELECT count() FROM system.tables "
                            "WHERE database = currentDatabase() "
                            "AND name = 'process_anchor_marker'")))
      (finally (native/close! handle)))
    (assert= "fresh-process chDB lifecycle changed host signal dispositions"
             before (signal-handlers))
    (println "PASS fresh process has isolated :memory: state and clean exit")))

(defn -main [& [mode]]
  (case mode
    "within" (within-process!)
    "fresh" (fresh-process!)
    (throw (ex-info "usage: jdbc.chdb-native-process-test within|fresh"
                    {:mode mode}))))
