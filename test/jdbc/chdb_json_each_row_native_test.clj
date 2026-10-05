(ns jdbc.chdb-json-each-row-native-test
  "Source-run qualification on the compiler-bearing native-writer runtime."
  (:require [clojure.test :refer [deftest is run-tests]]
            [clojure.data.json :as json]
            [jdbc.chdb.json-each-row :as encoder]))

(deftest public-native-backend-matches-portable-and-keeps-extension
  (doseq [parallelism [1 4]]
    (let [context (encoder/open-encoder {:json-backend :native-guarded
                                       :parallelism parallelism})
          calls (atom 0)
          custom (reify json/JSONWriter
                   (-write [_ sink _]
                     (swap! calls inc)
                     (.append sink "42")))
          rows (mapv (fn [n] {"ordinal" n "text" "é😀\n\"\\"
                              "nested" [nil true {"v" n}]}) (range 512))
          expected (apply str (mapv #(str (json/write-str %) "\n") rows))]
      (try
        (is (= {:json-backend :native-guarded
                :requested-parallelism parallelism
                :effective-parallelism parallelism :source-only? true}
               (encoder/encoder-info context)))
        (is (= expected (encoder/encode-text! context rows)))
        (is (= (vec (.getBytes expected "UTF-8"))
               (vec (:utf8 (encoder/encode-rows! context rows)))))
        (is (= "{\"n\":42}\n"
               (encoder/encode-text! context [{"n" custom}])))
        (is (= 1 @calls) "custom serializer effects must not be restarted")
        (let [error (ex-info "expected custom failure" {:canary :same})
              bad (reify json/JSONWriter (-write [_ _ _] (throw error)))
              observed (try (encoder/encode-text! context [{"n" bad}])
                            nil (catch Throwable e e))]
          (is (identical? error observed)))
        (is (= "{\"n\":1}\n" (encoder/encode-text! context [{"n" 1}])))
        (finally (is (= :closed (encoder/close! context))))))))

(defn -main [& _]
  (let [result (run-tests 'jdbc.chdb-json-each-row-native-test)]
    (when (pos? (+ (:fail result) (:error result))) (System/exit 1))))
