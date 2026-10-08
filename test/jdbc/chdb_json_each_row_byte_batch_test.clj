(ns jdbc.chdb-json-each-row-byte-batch-test
  "Source-only collector integration; no native persistence qualification."
  (:require [clojure.test :refer [deftest is]]
            [clojure.data.json :as json]
            [jdbc.chdb.json-each-row :as encoder]))

(defn- kind [operation]
  (:type (ex-data (try (operation) nil (catch Throwable error error)))))

(deftest batch-backend-preserves-wire-and-materialization
  (let [context (encoder/open-encoder {:json-backend :native-guarded-byte-batch})
        rows [[0 "é😀/\n"] {"nested" [nil true 18446744073709551615N]} []]
        expected (apply str (mapv #(str (json/write-str %) "\n") rows))]
    (try
      (is (= {:json-backend :native-guarded-byte-batch :requested-parallelism 1
              :effective-parallelism 1 :source-only? true}
             (encoder/encoder-info context)))
      (is (= expected (encoder/encode-text! context rows)))
      (is (= expected (encoder/encode-limited-text! context rows
                         (alength (.getBytes expected "UTF-8")))))
      (let [result (encoder/encode-rows! context rows)]
        (is (= expected (:payload result)))
        (is (= (vec (.getBytes expected "UTF-8")) (vec (:utf8 result)))))
      (is (= :jdbc.chdb.json-each-row/invalid-rows
             (kind #(encoder/encode-text! context '(1 2)))))
      (is (= "1\n" (encoder/encode-text! context [1])))
      (finally (is (= :closed (encoder/close! context)))))))

(deftest batch-budget-releases-admission-without-realizing-next-row
  (let [context (encoder/open-encoder {:json-backend :native-guarded-byte-batch})
        effects (atom [])
        row (fn [n] (reify json/JSONWriter
                      (-write [_ out _] (swap! effects conj n) (.append out "true"))))]
    (try
      (is (= :jdbc.chdb.json-each-row/output-limit
             (kind #(encoder/encode-limited-text! context [(row 1) (row 2) (row 3)] 5))))
      (is (= [1 2] @effects))
      (is (= "false\n" (encoder/encode-limited-text! context [false] 6)))
      (is (= :jdbc.chdb.json-each-row/invalid-limit
             (kind #(encoder/encode-limited-text! context [] -1))))
      (is (= "" (encoder/encode-limited-text! context nil 0)))
      (finally (is (= :closed (encoder/close! context)))))))

(deftest batch-custom-writer-retains-prefix-and-original-error
  (let [context (encoder/open-encoder {:json-backend :native-guarded-byte-batch})
        outputs (atom [])
        retained (atom nil)
        value (reify json/JSONWriter
                (-write [_ out _]
                  (swap! outputs conj (.toString out))
                  (reset! retained out)
                  (.append out "42")))
        error (ex-info "expected original error" {:expected true})
        bad (reify json/JSONWriter (-write [_ _ _] (throw error)))]
    (try
      (is (= "[0]\n[\"before\",42,\"after\"]\n[2]\n"
             (encoder/encode-text! context [[0] ["before" value "after"] [2]])))
      (is (= ["[\"before\","] @outputs))
      (is (= "[\"before\",42,\"after\"]\n" (.toString @retained)))
      (is (identical? error (try (encoder/encode-text! context [[bad]])
                                nil (catch Throwable observed observed))))
      (is (= "[1]\n" (encoder/encode-text! context [[1]])))
      (finally (is (= :closed (encoder/close! context)))))))

(deftest batch-uses-existing-busy-and-close-state-machine
  (let [context (encoder/open-encoder {:json-backend :native-guarded-byte-batch})
        observations (atom [])
        value (reify json/JSONWriter
                (-write [_ out _]
                  (swap! observations conj
                         (kind #(encoder/encode-text! context [0])))
                  (swap! observations conj (encoder/close! context 0))
                  (swap! observations conj
                         (kind #(encoder/encode-text! context [0])))
                  (.append out "true")))]
    (is (= "true\n" (encoder/encode-text! context [value])))
    (is (= [:jdbc.chdb.json-each-row/busy :pending :jdbc.chdb.json-each-row/closed]
           @observations))
    (is (= :closed (encoder/close! context)))
    (is (= :jdbc.chdb.json-each-row/closed
           (kind #(encoder/encode-text! context [0]))))))

(deftest batch-does-not-silently-ignore-parallelism
  (is (= :jdbc.chdb.json-each-row/serial-required
         (kind #(encoder/open-encoder {:json-backend :native-guarded-byte-batch
                                      :parallelism 4})))))

(defn -main [& _]
  (let [result (clojure.test/run-tests 'jdbc.chdb-json-each-row-byte-batch-test)]
    (System/exit (if (zero? (+ (:fail result) (:error result))) 0 1))))
