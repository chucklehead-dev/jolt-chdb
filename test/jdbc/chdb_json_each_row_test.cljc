(ns jdbc.chdb-json-each-row-test
  (:require #?(:bb [cheshire.core :as json]
               :jolt [clojure.data.json :as json]
               :clj [clojure.data.json :as json])
            [clojure.string :as str]
            [clojure.test :refer [deftest is run-tests testing]]
            [jdbc.chdb.json-each-row :as encoder]))

(defn- failure-type [f]
  (try (f) nil
       (catch Throwable error (:type (ex-data error)))))

(defn- serial-payload [rows]
  (apply str (map #(str (#?(:bb json/generate-string
                           :jolt json/write-str
                           :clj json/write-str) %) "\n") rows)))

#?(:bb nil
   :default
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

(defn- start-encode [context rows]
  (let [outcome (promise)
        thread (Thread.
                (fn []
                  (deliver outcome
                           (try {:value (encoder/encode-rows! context rows)}
                                (catch Throwable error {:error error})))))]
    (.start thread)
    {:thread thread :outcome outcome}))

(defn- finish-thread! [{:keys [thread outcome]}]
  (.join thread 5000)
  (is (not (.isAlive thread)) "encoding owner must terminate")
  (deref outcome 1000 ::timeout))

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
      (is (= "{\"x\":1}\n"
             (:payload (encoder/encode-rows! context [{"x" 1}]))))
      (finally (is (= :closed (encoder/close! context)))))))

#?(:bb nil
   :default
   (deftest bounded-close-and-busy
  (let [context (encoder/open-encoder {:parallelism 4})
        entered (promise)
        release (promise)
        owned (start-encode context [{"x" (blocking-value entered release)}])]
    (try
      (is (= :entered (deref entered 5000 ::timeout)))
      (is (= :jdbc.chdb.json-each-row/busy
             (failure-type #(encoder/encode-rows! context []))))
      (is (= :pending (encoder/close! context 0)))
      (is (= :jdbc.chdb.json-each-row/closed
             (failure-type #(encoder/encode-rows! context []))))
      (finally (deliver release :release)))
    (is (= "{\"x\":true}\n" (get-in (finish-thread! owned) [:value :payload])))
    (is (= :closed (encoder/close! context 1000)))
    (is (= :closed (encoder/close! context 0))))))

#?(:bb
   (deftest bounded-close-and-busy-native
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
           (let [owned (start-encode context [{"x" 1}])]
             (try
               (is (= :entered (deref entered 5000 ::timeout)))
               (is (= :jdbc.chdb.json-each-row/busy
                      (failure-type #(encoder/encode-rows! context []))))
               (is (= :pending (encoder/close! context 0)))
               (is (= :jdbc.chdb.json-each-row/closed
                      (failure-type #(encoder/encode-rows! context []))))
               (finally (deliver release :release)))
             (is (= "{\"x\":1}\n" (get-in (finish-thread! owned) [:value :payload])))
             (is (= :closed (encoder/close! context 1000)))
             (is (= :closed (encoder/close! context 0)))))))))

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
     (let [context (encoder/open-encoder {:parallelism 4})
           earlier-entered (promise)
           release-earlier (promise)
           later-completed (promise)
           later-value (reify json/JSONWriter
                         (-write [_ out _]
                           (.append out "2")
                           (deliver later-completed :completed)))
           owned (start-encode context
                               [{"n" (blocking-value earlier-entered release-earlier)}
                                {"n" later-value}
                                {"n" 3} {"n" 4}])]
       (try
         (is (= :entered (deref earlier-entered 5000 ::timeout)))
         (is (= :completed (deref later-completed 5000 ::timeout)))
         (is (= :pending (deref (:outcome owned) 0 :pending)))
         (finally (deliver release-earlier :release)))
       (is (= "{\"n\":true}\n{\"n\":2}\n{\"n\":3}\n{\"n\":4}\n"
              (get-in (finish-thread! owned) [:value :payload])))
       (is (= :closed (encoder/close! context))))))

#?(:jolt
   (deftest parallel-error-waits-for-siblings
     (let [context (encoder/open-encoder {:parallelism 4})
           error (ex-info "row failure" {:canary :original})
           entered (promise)
           release (promise)
           owned (start-encode
                  context
                  [{"x" (throwing-value error)}
                   {"x" (blocking-value entered release)}
                   {"x" 2} {"x" 3}])]
       (try
         (is (= :entered (deref entered 5000 ::timeout)))
         (is (= :pending (deref (:outcome owned) 0 :pending)))
         (is (= :jdbc.chdb.json-each-row/busy
                (failure-type #(encoder/encode-rows! context []))))
         (finally (deliver release :release)))
       (is (identical? error (:error (finish-thread! owned))))
       (is (= :closed (encoder/close! context))))))

#?(:jolt
   (deftest interrupted-join-settles-before-release
     (let [context (encoder/open-encoder {:parallelism 4})
           entered (promise)
           release (promise)
           owned (start-encode context
                               [{"x" (blocking-value entered release)}])]
       (try
         (is (= :entered (deref entered 5000 ::timeout)))
         (.interrupt (:thread owned))
         (is (= :pending (encoder/close! context 0)))
         (is (= :jdbc.chdb.json-each-row/closed
                (failure-type #(encoder/encode-rows! context []))))
         (finally (deliver release :release)))
       (is (instance? InterruptedException (:error (finish-thread! owned))))
       (is (= :closed (encoder/close! context 1000))))))

#?(:jolt
   (deftest partial-spawn-failure-drains-started-child
     (let [context (encoder/open-encoder {:parallelism 4})
           entered (promise)
           release (promise)
           error (ex-info "spawn failure" {:canary :spawn})
           spawn-var (ns-resolve 'jdbc.chdb.json-each-row 'spawn-chunk)
           original @spawn-var
           calls (atom 0)]
       (with-redefs-fn
         {spawn-var (fn [chunk]
                      (if (= 2 (swap! calls inc))
                        (throw error)
                        (original chunk)))}
         (fn []
           (let [owned (start-encode
                        context [{"x" (blocking-value entered release)}
                                 {"x" 1} {"x" 2} {"x" 3}])]
             (try
               (is (= :entered (deref entered 5000 ::timeout)))
               (is (= :pending (deref (:outcome owned) 0 :pending)))
               (is (= :jdbc.chdb.json-each-row/busy
                      (failure-type #(encoder/encode-rows! context []))))
               (finally (deliver release :release)))
             (is (identical? error (:error (finish-thread! owned))))
             (is (= :closed (encoder/close! context)))))))))

(defn -main [& _]
  (let [result (run-tests 'jdbc.chdb-json-each-row-test)]
    (when (pos? (+ (:fail result) (:error result)))
      (System/exit 1))))
