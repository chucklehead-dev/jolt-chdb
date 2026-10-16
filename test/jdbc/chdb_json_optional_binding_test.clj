(ns jdbc.chdb-json-optional-binding-test
  "Run in fresh Jolt processes with both ordinary and qualified JSON pins."
  (:require [clojure.test :refer [deftest is run-tests]]
            [jdbc.chdb.json-each-row :as encoder]))

(deftest configured-encoding-does-not-resolve-native-binding
  (let [context (encoder/open-encoder {})]
    (try
      (with-redefs [ns-resolve (fn [& _]
                                (throw (ex-info "unexpected optional lookup" {})))]
        (is (= "{\"v\":1}\n" (encoder/encode-text! context [{"v" 1}]))))
      (with-redefs [ns-resolve (fn [& _]
                                (throw (ex-info "unexpected optional lookup" {})))]
        (is (= "prefix{\"v\":1}\n"
               (encoder/encode-limited-prefixed-text! context "prefix" [{"v" 1}] 8))))
      (finally (encoder/close! context)))))

(deftest missing-binding-rejects-before-factory-or-operation
  (let [select @(ns-resolve 'jdbc.chdb.json-each-row 'with-selected-writer)
        effects (atom [])
        error (with-redefs [ns-resolve (fn [& _] nil)]
                (try
                  (select (fn [] (swap! effects conj :factory))
                          (fn [] (swap! effects conj :operation)))
                  nil
                  (catch Throwable e e)))]
    (is (= :jdbc.chdb.json-each-row/native-unavailable (:type (ex-data error))))
    (is (= [] @effects))))

(deftest optional-binding-is-scoped-and-restored
  (let [select @(ns-resolve 'jdbc.chdb.json-each-row 'with-selected-writer)
        writer-var (ns-resolve 'clojure.data.json '*experimental-native-writer*)]
    (if writer-var
      (let [outer (fn [& _] :outer)
            inner (fn [& _] :inner)
            failure (ex-info "expected operation failure" {})]
        (with-bindings {writer-var outer}
          (is (identical? inner (select (constantly inner) #(deref writer-var))))
          (is (identical? outer @writer-var))
          (is (identical? failure
                          (try (select (constantly inner) #(throw failure))
                               nil (catch Throwable e e))))
          (is (identical? outer @writer-var))))
      (let [error (try (encoder/open-encoder {:json-backend :native-guarded})
                       nil (catch Throwable e e))]
        (is (= :jdbc.chdb.json-each-row/native-unavailable (:type (ex-data error))))))))

(defn -main [& [mode]]
  (when mode
    (when-not (contains? #{"--ordinary" "--qualified"} mode)
      (throw (ex-info "unknown binding test mode" {})))
    (when-not (= (= mode "--qualified")
                 (boolean (ns-resolve 'clojure.data.json '*experimental-native-writer*)))
      (throw (ex-info "selected JSON does not match binding test mode" {}))))
  (let [result (run-tests 'jdbc.chdb-json-optional-binding-test)]
    (when (pos? (+ (:fail result) (:error result))) (System/exit 1))))
