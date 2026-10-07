(ns jdbc.chdb-row-fragments-test
  (:require [clojure.test :refer [deftest is]]
            [clojure.data.json :as json]
            [jdbc.chdb.row-fragments :as fragments]
            [jdbc.chdb.json-each-row :as encoder]))

(deftest actual-fragment-transfer-preserves-bytes-and-independent-row-state
  (doseq [text ["" "ASCII\n" "éβ€😀" "\u0000\u007f\u0080\u07ff\u0800"
               "\ud7ff\ue000\uffff" "a\ufeffb"]
          materialize? [false true]]
    (let [row (java.io.StringWriter.) batch (StringBuilder.)
          bytes (alength (.getBytes (str text "\n") "UTF-8"))]
      (.append batch "before:")
      (.append row text)
      (when materialize? (.toString row))
      (.append row "\n")
      (is (= bytes (fragments/append-row! row batch (dec bytes))))
      (is (= "before:" (.toString batch)) "overflow must leave batch untouched")
      (is (= bytes (fragments/append-row! row batch bytes)))
      (.append row "after")
      (is (= (str "before:" text "\n") (.toString batch)))
      (is (= (str text "\nafter") (.toString row)))))
  (let [batch (StringBuilder.)]
    (is (false? (fragments/append-row! "not a writer" batch 100)))
    (is (= "" (.toString batch)))))

(deftest bounded-native-transfer-keeps-live-callbacks-and-row-boundaries
  (doseq [backend [:native-guarded :native-guarded-string-cache]]
    (let [context (encoder/open-encoder {:json-backend backend})
          retained (atom nil) effects (atom [])
          first-row (reify json/JSONWriter
                      (-write [_ sink _]
                        (swap! effects conj :first)
                        (reset! retained sink)
                        (.append sink "true")))
          second-row (reify json/JSONWriter
                       (-write [_ sink _]
                         (swap! effects conj :second)
                         (.append @retained "late")
                         (is (= "" (.toString sink)))
                         (.append sink "false")))
          third-row (reify json/JSONWriter
                      (-write [_ sink _]
                        (swap! effects conj :third)
                        (.append sink "null")))]
      (try
        (is (= "true\nfalse\n"
               (encoder/encode-limited-text! context [first-row second-row] 11)))
        (is (= [:first :second] @effects))
        (reset! effects [])
        (let [error (try (encoder/encode-limited-text!
                          context [first-row second-row third-row] 10)
                         nil (catch Throwable e e))]
          (is (= :jdbc.chdb.json-each-row/output-limit (:type (ex-data error))))
          (is (= [:first :second] @effects)))
        (is (= "false\n" (encoder/encode-limited-text! context [false] 6)))
        ;; Force decline: serialize each value once, then use the old path.
        (with-redefs [fragments/append-row! (constantly false)]
          (reset! effects [])
          (is (= "true\nfalse\n"
                 (encoder/encode-limited-text! context [first-row second-row] 11)))
          (is (= [:first :second] @effects)))
        (finally (encoder/close! context))))))
