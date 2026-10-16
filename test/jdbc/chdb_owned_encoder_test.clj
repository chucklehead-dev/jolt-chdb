(ns jdbc.chdb-owned-encoder-test
  (:require [clojure.test :refer [deftest is run-tests]]
            [clojure.data.json :as json]
            [jolt.scheme :as scheme]
            [jdbc.chdb.owned-statement :as owned]
            [jdbc.chdb.json-each-row :as encoder]))

(deftest serial-encoder-owned-selection-and-single-pass-text-fallback
  (doseq [mode [:owned :unicode :legacy]]
    (let [context (encoder/open-encoder {:parallelism 1 :json-backend :native-guarded-byte-batch})
          visits (atom 0)
          value (reify json/JSONWriter
                  (-write [_ out _]
                    (swap! visits inc)
                    (.write out (if (= mode :unicode) "\"β\"" "42"))))
          prefix "INSERT INTO t FORMAT JSONCompactEachRow\n"]
      (try
        (let [selected (if (= mode :legacy) (dissoc context :native-prefixed-byte-writer) context)
              result (encoder/encode-limited-prefixed-statement! selected prefix [[value]] 100)]
          (is (= 1 @visits))
          (if (= mode :owned)
            (do (is (owned/statement? result)) (is (= (str prefix "[42]\n") (owned/text result))))
            (do (is (string? result))
                (is (= (str prefix (if (= mode :unicode) "[\"β\"]\n" "[42]\n")) result)))))
        (finally (encoder/close! context)))))
  (let [context (encoder/open-encoder {:parallelism 1 :json-backend :native-guarded-byte-batch})
        visits (atom [])
        rows ((fn walk [values]
                (lazy-seq (when (seq values)
                            (swap! visits conj (first values))
                            (cons [(first values)] (walk (rest values)))))) [1 2 3])]
    (try
      (is (= :jdbc.chdb.json-each-row/output-limit
             (:type (ex-data (try (encoder/encode-limited-prefixed-statement! context "prefix" rows 1)
                                 nil (catch Throwable error error))))))
      (is (= [1] @visits))
      (is (owned/statement? (encoder/encode-limited-prefixed-statement! context "prefix" [] 0)))
      (finally (encoder/close! context)))))

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

(defn -main [& _]
  (let [{:keys [fail error]} (run-tests 'jdbc.chdb-owned-encoder-test)]
    (System/exit (if (zero? (+ fail error)) 0 1))))
