(ns jdbc.chdb-json-each-row-byte-batch-test
  "Source-only collector integration; no native persistence qualification."
  (:require [clojure.test :refer [deftest is]]
            [clojure.data.json :as json]
            [jdbc.chdb.json-each-row :as encoder]
            [jolt.scheme :as scheme]))

(deftest native-delegation-does-not-retain-the-original-lazy-row-head
  ;; A synthetic consuming backend isolates the codec's own retention from
  ;; dependency-specific serializer behavior. Full native wire/persistence
  ;; checks are separate; this is a reachability/lifetime test only.
  (let [weak (scheme/eval-string "(lambda (row) (weak-cons row #f))")
        alive? (scheme/eval-string "(lambda (pair) (not (bwp-object? (car pair))))")
        watch (atom nil) observations (atom []) visits (atom 0)
        context (assoc (encoder/open-encoder {:json-backend :configured})
                       :native-prefixed-byte-writer
                       (fn [_ rows _]
                         (dorun rows)
                         (.getBytes "[]\n" "UTF-8")))]
    (try
      (encoder/encode-limited-prefixed-statement! context "INSERT INTO probe\n"
        (map (fn [i]
               (let [row [(str "row-" i) i]]
                 (swap! visits inc)
                 (when (zero? i) (reset! watch (weak row)))
                 (when (contains? #{128 256 384} i)
                   (System/gc) (System/gc)
                   (swap! observations conj (alive? @watch)))
                 row)) (range 512)) 1048576)
      (is (= 512 @visits))
      (is (= 3 (count @observations)))
      (doseq [retained? @observations] (is (false? retained?)))
      (let [held [(str "held-row") 1] reference (weak held)]
        (System/gc) (System/gc)
        (is (true? (alive? reference)))
        (is (= "held-row" (first held))))
      (finally (encoder/close! context)))))

(defn- kind [operation]
  (:type (ex-data (try (operation) nil (catch Throwable error error)))))

(deftest prefixed-budget-and-row-observability
  (doseq [backend [:configured :native-guarded-byte-batch]]
    (let [context (encoder/open-encoder {:json-backend backend})
          prefix "INSERT β😀 FORMAT JSONCompactEachRow\n"
          seen (atom []) retained (atom nil)
          value (reify json/JSONWriter
                  (-write [_ out _]
                    (swap! seen conj (.toString out))
                    (reset! retained out)
                    (.append out "42")))
          payload "[0]\n[1,42,2]\n"]
      (try
        (is (= (str prefix payload)
               (encoder/encode-limited-prefixed-text! context prefix [[0] [1 value 2]]
                 (alength (.getBytes payload "UTF-8")))))
        (is (= ["[1,"] @seen))
        (is (= "[1,42,2]\n" (.toString @retained)))
        (is (= prefix (encoder/encode-limited-prefixed-text! context prefix nil 0)))
        (is (= :jdbc.chdb.json-each-row/invalid-prefix
               (kind #(encoder/encode-limited-prefixed-text! context nil [] 0))))
        (let [effects (atom [])
              record (fn [n] (reify json/JSONWriter
                               (-write [_ out _] (swap! effects conj n) (.append out "true"))))]
          (is (= :jdbc.chdb.json-each-row/output-limit
                 (kind #(encoder/encode-limited-prefixed-text! context prefix
                          [(record 1) (record 2) (record 3)] 5))))
          (is (= [1 2] @effects)))
        (is (= (str prefix "false\n")
               (encoder/encode-limited-prefixed-text! context prefix [false] 6)))
        (finally (encoder/close! context))))))

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
