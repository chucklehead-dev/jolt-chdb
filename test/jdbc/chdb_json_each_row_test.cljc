(ns jdbc.chdb-json-each-row-test
  (:require #?(:bb [cheshire.core :as json]
               :jolt [clojure.data.json :as json]
               :clj [clojure.data.json :as json])
            [clojure.string :as str]
            [clojure.test :refer [deftest is run-tests testing]]
            [jdbc.chdb.json-each-row :as encoder]
            #?(:jolt [jolt.fibers :as fibers])))

(defn- failure-type [f]
  (try (f) nil
       (catch Throwable error (:type (ex-data error)))))

(defn- serial-payload [rows]
  (apply str (map #(str (#?(:bb json/generate-string
                           :jolt json/write-str
                           :clj json/write-str) %) "\n") rows)))

#?(:bb nil :jolt nil :clj
   (defn- blocking-value [entered release]
     (reify json/JSONWriter
       (-write [_ out _]
         (deliver entered :entered)
         @release
         (.append out "true")))))

#?(:bb nil
   :default
   (defn- throwing-value [error]
     (reify json/JSONWriter
       (-write [_ _ _] (throw error)))))

(defn- start-encode
  ([context rows] (start-encode encoder/encode-rows! context rows))
  ([encode context rows]
   (let [outcome (promise)
         thread (Thread.
                 (fn []
                   (deliver outcome
                            (try {:value (encode context rows)}
                                 (catch Throwable error {:error error})))))]
     (.start thread)
     {:thread thread :outcome outcome})))

(defn- result-text [result]
  (if (string? result) result (:payload result)))

(defn- finish-thread! [{:keys [thread outcome]}]
  (.join thread 5000)
  (is (not (.isAlive thread)) "encoding owner must terminate")
  (deref outcome 1000 ::timeout))

#?(:jolt
   (defn- gated-worker [chunk entered release]
     ;; Gate a child before row serialization, never inside data.json/mapv.
     (let [serialize @(ns-resolve 'jdbc.chdb.json-each-row 'serial-payload)]
       (fibers/spawn
        (fn []
          (deliver entered :entered)
          @release
          (try {:value (serialize chunk)}
               (catch Throwable error {:error error})))))))

(deftest host-default-bytes-and-order
  (let [context (encoder/open-encoder {:parallelism 4})
        corpus [[]
                [{"a" "é\n\"\\"}]
                [{"a" [nil true 42] "b" {"x" "β"}}
                 {"a" "😀" "b" "\u2028"}]
                (mapv (fn [i] {"ordinal" i "nested" {"x" (str "v" i)}})
                      (range 512))]]
    (try
      (doseq [rows corpus]
        (let [{:keys [payload utf8 byte-count]}
              (encoder/encode-rows! context rows)
              expected (serial-payload rows)]
          (is (= expected payload))
          (is (= expected (encoder/encode-text! context rows)))
          (is (= (vec (.getBytes expected "UTF-8")) (vec utf8)))
          (is (= (alength utf8) byte-count))
          #?(:bb (is (= rows
                         (mapv #(json/parse-string %)
                               (remove empty? (str/split-lines payload))))))))
      (finally (is (= :closed (encoder/close! context)))))))

#?(:bb
   (deftest babashka-native-unicode-and-finite-values
     (let [context (encoder/open-encoder {:parallelism 4})]
       (try
         (is (= 1 (:effective-parallelism context)))
         (let [payload (:payload (encoder/encode-rows! context [{"x" "é"}]))]
           (is (= {"x" "é"} (json/parse-string (str/trim payload))))
           (is (not= "{\"x\":\"\\u00e9\"}\n" payload))
           (is (str/includes? payload "é")))
         (doseq [value [Double/NaN Double/POSITIVE_INFINITY
                        Double/NEGATIVE_INFINITY (Object.)]]
           (is (= :jdbc.chdb.json-each-row/unsupported-value
                  (failure-type #(encoder/encode-rows! context [{"x" value}])))))
         (is (= "{\"x\":1}\n"
                (:payload (encoder/encode-rows! context [{"x" 1}]))))
         (finally (is (= :closed (encoder/close! context))))))))

(deftest invalid-input-and-unsupported-value-do-not-poison-context
  (let [context (encoder/open-encoder {:parallelism 4})]
    (try
      (is (= :jdbc.chdb.json-each-row/invalid-rows
             (failure-type #(encoder/encode-rows! context '(1 2)))))
      (is (thrown? Throwable
                   (encoder/encode-rows! context [{"x" (Object.)}])))
      (is (= :jdbc.chdb.json-each-row/invalid-rows
             (failure-type #(encoder/encode-text! context '(1 2)))))
      (is (thrown? Throwable
                   (encoder/encode-text! context [{"x" (Object.)}])))
      (is (= "{\"x\":1}\n"
             (encoder/encode-text! context [{"x" 1}])))
      (is (= "{\"x\":1}\n"
             (:payload (encoder/encode-rows! context [{"x" 1}]))))
      (finally (is (= :closed (encoder/close! context)))))))

(deftest text-result-skips-utf8-materialization
  (doseq [parallelism [1 4]]
    (let [context (encoder/open-encoder {:parallelism parallelism})
          materialize-var (ns-resolve 'jdbc.chdb.json-each-row 'materialize-utf8)
          original @materialize-var
          calls (atom 0)
          rows [{"message" "é😀\\\"\n"}]]
      (try
        (with-redefs-fn
          {materialize-var
           (fn [payload]
             (swap! calls inc)
             (is (some? (:active @(:state context))))
             (doseq [encode [encoder/encode-rows! encoder/encode-text!]]
               (is (= :jdbc.chdb.json-each-row/busy
                      (failure-type #(encode context [])))))
             (original payload))}
          (fn []
            (let [result (encoder/encode-rows! context rows)]
              (is (= #{:payload :utf8 :byte-count} (set (keys result))))
              (is (= 1 @calls) "canary must observe the byte-producing API")
              (is (= (:payload result) (encoder/encode-text! context rows)))
              (is (= 1 @calls) "text-only encoding must skip that materializer"))))
        (finally (encoder/close! context))))))

(deftest utf8-materialization-error-and-close-remain-inside-admission
  (let [context (encoder/open-encoder {:parallelism 4})
        materialize-var (ns-resolve 'jdbc.chdb.json-each-row 'materialize-utf8)
        original @materialize-var
        error (ex-info "materialization failure" {:canary :utf8})]
    (try
      (with-redefs-fn
        {materialize-var (fn [_]
                           (is (some? (:active @(:state context))))
                           (throw error))}
        (fn []
          (is (identical? error
                         (try (encoder/encode-rows! context []) nil
                              (catch Throwable caught caught))))))
      (is (nil? (:active @(:state context))))
      (is (= "true\n" (encoder/encode-text! context [true])))
      (with-redefs-fn
        {materialize-var (fn [payload]
                           (is (= :pending (encoder/close! context 0)))
                           (original payload))}
        (fn [] (is (= "" (:payload (encoder/encode-rows! context []))))))
      (is (= :closed (encoder/close! context 0)))
      (is (= :jdbc.chdb.json-each-row/closed
             (failure-type #(encoder/encode-text! context []))))
      (finally (encoder/close! context)))))

#?(:bb nil :jolt nil
   :clj
   (deftest bounded-close-and-busy
     (doseq [encode [encoder/encode-rows! encoder/encode-text!]]
       (let [context (encoder/open-encoder {:parallelism 4})
             entered (promise)
             release (promise)
             owned (start-encode encode context [{"x" (blocking-value entered release)}])]
         (try
           (is (= :entered (deref entered 5000 ::timeout)))
           (is (= :jdbc.chdb.json-each-row/busy
                  (failure-type #(encoder/encode-rows! context []))))
           (is (= :pending (encoder/close! context 0)))
           (is (= :jdbc.chdb.json-each-row/closed
                  (failure-type #(encoder/encode-rows! context []))))
           (finally (deliver release :release)))
         (is (= "{\"x\":true}\n" (result-text (:value (finish-thread! owned)))))
         (is (= :closed (encoder/close! context 1000)))
         (is (= :closed (encoder/close! context 0)))))))

#?(:jolt
   (deftest bounded-close-and-busy
     (doseq [encode [encoder/encode-rows! encoder/encode-text!]]
       (let [context (encoder/open-encoder {:parallelism 4})
             entered (promise)
             release (promise)
             spawn-var (ns-resolve 'jdbc.chdb.json-each-row 'spawn-chunk)
             original @spawn-var
             calls (atom 0)]
         (with-redefs-fn
           {spawn-var (fn [chunk]
                        (if (= 1 (swap! calls inc))
                          (gated-worker chunk entered release)
                          (original chunk)))}
           (fn []
             (let [owned (start-encode encode context [{"x" true}])]
               (try
                 (is (= :entered (deref entered 5000 ::timeout)))
                 (is (= :jdbc.chdb.json-each-row/busy
                        (failure-type #(encoder/encode-rows! context []))))
                 (is (= :pending (encoder/close! context 0)))
                 (is (= :jdbc.chdb.json-each-row/closed
                        (failure-type #(encoder/encode-rows! context []))))
                 (finally (deliver release :release)))
               (is (= "{\"x\":true}\n"
                      (result-text (:value (finish-thread! owned)))))
               (is (= :closed (encoder/close! context 1000)))
               (is (= :closed (encoder/close! context 0))))))))))

#?(:bb
   (deftest bounded-close-and-busy-native
     (doseq [encode [encoder/encode-rows! encoder/encode-text!]]
       (let [context (encoder/open-encoder {:parallelism 4})
             entered (promise)
             release (promise)
             row-var (ns-resolve 'jdbc.chdb.json-each-row 'row-text)
             original @row-var]
         (with-redefs-fn
           {row-var (fn [row]
                      (deliver entered :entered)
                      @release
                      (original row))}
           (fn []
             (let [owned (start-encode encode context [{"x" 1}])]
               (try
                 (is (= :entered (deref entered 5000 ::timeout)))
                 (is (= :jdbc.chdb.json-each-row/busy
                        (failure-type #(encoder/encode-rows! context []))))
                 (is (= :pending (encoder/close! context 0)))
                 (is (= :jdbc.chdb.json-each-row/closed
                        (failure-type #(encoder/encode-rows! context []))))
                 (finally (deliver release :release)))
               (is (= "{\"x\":1}\n" (result-text (:value (finish-thread! owned)))))
               (is (= :closed (encoder/close! context 1000)))
               (is (= :closed (encoder/close! context 0))))))))))

(deftest option-and-close-contract
  (is (= :jdbc.chdb.json-each-row/invalid-options
         (failure-type #(encoder/open-encoder {:parallelism 8}))))
  (is (= :jdbc.chdb.json-each-row/invalid-options
         (failure-type #(encoder/open-encoder {:queue-size 100}))))
  (let [context (encoder/open-encoder)]
    (is (= 1 (:effective-parallelism context)))
    (is (= :jdbc.chdb.json-each-row/invalid-timeout
           (failure-type #(encoder/close! context -1))))
    (is (= :closed (encoder/close! context)))
    (is (= :jdbc.chdb.json-each-row/closed
           (failure-type #(encoder/encode-rows! context []))))))

#?(:jolt
   (deftest parallel-chunks-finish-out-of-order-but-emit-in-row-order
     (doseq [encode [encoder/encode-rows! encoder/encode-text!]]
       (let [context (encoder/open-encoder {:parallelism 4})
             earlier-entered (promise)
             release-earlier (promise)
             later-completed (promise)
             later-value (reify json/JSONWriter
                           (-write [_ out _]
                             (.append out "2")
                             (deliver later-completed :completed)))
             spawn-var (ns-resolve 'jdbc.chdb.json-each-row 'spawn-chunk)
             original @spawn-var
             calls (atom 0)]
         (with-redefs-fn
           {spawn-var (fn [chunk]
                        (if (= 1 (swap! calls inc))
                          (gated-worker chunk earlier-entered release-earlier)
                          (original chunk)))}
           (fn []
             (let [owned (start-encode encode context
                                       [{"n" true} {"n" later-value}
                                        {"n" 3} {"n" 4}])]
               (try
                 (is (= :entered (deref earlier-entered 5000 ::timeout)))
                 (is (= :completed (deref later-completed 5000 ::timeout)))
                 (is (= :pending (deref (:outcome owned) 0 :pending)))
                 (finally (deliver release-earlier :release)))
               (is (= "{\"n\":true}\n{\"n\":2}\n{\"n\":3}\n{\"n\":4}\n"
                      (result-text (:value (finish-thread! owned)))))
               (is (= :closed (encoder/close! context))))))))))

#?(:jolt
   (deftest parallel-error-waits-for-siblings
     (doseq [encode [encoder/encode-rows! encoder/encode-text!]]
       (let [context (encoder/open-encoder {:parallelism 4})
             error (ex-info "row failure" {:canary :original})
             entered (promise)
             release (promise)
             spawn-var (ns-resolve 'jdbc.chdb.json-each-row 'spawn-chunk)
             original @spawn-var
             calls (atom 0)]
         (with-redefs-fn
           {spawn-var (fn [chunk]
                        (if (= 2 (swap! calls inc))
                          (gated-worker chunk entered release)
                          (original chunk)))}
           (fn []
             (let [owned (start-encode
                          encode
                          context
                          [{"x" (throwing-value error)}
                           {"x" 1} {"x" 2} {"x" 3}])]
               (try
                 (is (= :entered (deref entered 5000 ::timeout)))
                 (is (= :pending (deref (:outcome owned) 0 :pending)))
                 (is (= :jdbc.chdb.json-each-row/busy
                        (failure-type #(encoder/encode-rows! context []))))
                 (finally (deliver release :release)))
               (is (identical? error (:error (finish-thread! owned))))
               (is (= :closed (encoder/close! context))))))))))

#?(:jolt
   (deftest interrupted-join-settles-before-release
     (doseq [encode [encoder/encode-rows! encoder/encode-text!]]
       (let [context (encoder/open-encoder {:parallelism 4})
             entered (promise)
             release (promise)
             spawn-var (ns-resolve 'jdbc.chdb.json-each-row 'spawn-chunk)
             original @spawn-var
             calls (atom 0)]
         (with-redefs-fn
           {spawn-var (fn [chunk]
                        (if (= 1 (swap! calls inc))
                          (gated-worker chunk entered release)
                          (original chunk)))}
           (fn []
             (let [owned (start-encode encode context [{"x" true}])]
               (try
                 (is (= :entered (deref entered 5000 ::timeout)))
                 (.interrupt (:thread owned))
                 (is (= :pending (encoder/close! context 0)))
                 (is (= :jdbc.chdb.json-each-row/closed
                        (failure-type #(encoder/encode-rows! context []))))
                 (finally (deliver release :release)))
               (is (instance? InterruptedException
                              (:error (finish-thread! owned))))
               (is (= :closed (encoder/close! context 1000))))))))))

#?(:jolt
   (deftest partial-spawn-failure-drains-started-child
     (doseq [encode [encoder/encode-rows! encoder/encode-text!]]
       (let [context (encoder/open-encoder {:parallelism 4})
             entered (promise)
             release (promise)
             error (ex-info "spawn failure" {:canary :spawn})
             spawn-var (ns-resolve 'jdbc.chdb.json-each-row 'spawn-chunk)
             original @spawn-var
             calls (atom 0)]
         (with-redefs-fn
           {spawn-var (fn [chunk]
                        (case (swap! calls inc)
                          1 (gated-worker chunk entered release)
                          2 (throw error)
                          (original chunk)))}
           (fn []
             (let [owned (start-encode
                          encode
                          context [{"x" true}
                                   {"x" 1} {"x" 2} {"x" 3}])]
               (try
                 (is (= :entered (deref entered 5000 ::timeout)))
                 (is (= :pending (deref (:outcome owned) 0 :pending)))
                 (is (= :jdbc.chdb.json-each-row/busy
                        (failure-type #(encoder/encode-rows! context []))))
                 (finally (deliver release :release)))
               (is (identical? error (:error (finish-thread! owned))))
               (is (= :closed (encoder/close! context))))))))))

(defn -main [& _]
  (let [result (run-tests 'jdbc.chdb-json-each-row-test)]
    (when (pos? (+ (:fail result) (:error result)))
      (System/exit 1))))
