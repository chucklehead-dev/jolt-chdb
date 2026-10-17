(ns jdbc.chdb-owned-stream-option-test
  (:require [clojure.test :refer [deftest is]] [jdbc.chdb :as chdb]
            [jdbc.chdb.native :as native] [jdbc.chdb.owned-statement :as owned]
            [jdbc.chdb.durable :as durable] [jdbc.chdb.durable.backend :as backend]
            [jdbc.chdb.durable.control :as control] [jolt.ffi :as ffi]))

(def header "insert into sample (`n`) FORMAT JSONCompactEachRow\n")
(def base {:backend (backend/memory-backend) :owner "test" :database "default"})
(defn error-type [f] (try (f) nil (catch Throwable e (:type (ex-data e)))))
(defn with-sql [sql f]
  (let [statement (owned/try-snapshot (.getBytes sql "UTF-8"))]
    (owned/with-query-buffer statement #(f statement %))))

(deftest writer-only-strict-boolean-option
  (doseq [value [true false]]
    (is (= value (:owned-compact-stream? (durable/writer-dbspec (assoc base :owned-compact-stream? value))))))
  (doseq [value [nil 0 1 "true" :true]]
    (is (= ::durable/invalid-options
           (error-type #(durable/writer-dbspec (assoc base :owned-compact-stream? value)))))
    (is (= ::durable/invalid-options
           (error-type #(durable/open-writer! (assoc base :owned-compact-stream? value))))))
  (doseq [value [true false]]
    (is (= ::durable/invalid-options
           (error-type #(durable/snapshot-dbspec {:backend (:backend base) :owned-compact-stream? value}))))
    (is (= ::durable/invalid-options
           (error-type #(durable/open-reader! {:owned-compact-stream? value}))))))

(deftest opt-in-preflight-precedes-storage-and-cannot-replace-native-contract
  (let [calls (atom [])]
    (with-redefs [native/ensure-loaded! #(swap! calls conj :load)
                  native/chdb-version (fn [] (swap! calls conj :version) "26.7.3")
                  control/read-head! (fn [& _] (swap! calls conj :storage-read) nil)]
      (is (= ::durable/unqualified-owned-stream
             (error-type #(durable/open-writer! (assoc base :owned-compact-stream? true)))))
      (is (= [:load :version] @calls))
      (reset! calls [])
      (doseq [hook [:open-native! :close-native! :classification-sql! :prepare-query!
                    :classify! :query-native! :execute-native! :execute-prepared-native! :analyze-execute!
                    :with-native-admitted-buffer! :with-native-prepared-buffer! :with-native-owned-admitted-buffer!]]
        (is (= ::durable/invalid-options
               (error-type #(durable/open-writer! (assoc base :owned-compact-stream? true
                                                       :operations {hook (fn [& _] nil)}))))))
      (is (empty? @calls)))))

(deftest selector-streams-only-the-bounded-closed-header
  (let [calls (atom [])]
    (with-redefs [chdb/execute-owned-compact-json-stream-with-query-buffer
                  (fn [_ statement buffer actual-header]
                    (swap! calls conj [:stream actual-header]) :stream)
                  chdb/execute-owned-any-with-query-buffer
                  (fn [_ statement buffer] (swap! calls conj :ordinary) :ordinary)]
      (with-sql (str header "[1]\n")
        #(is (= :stream (chdb/execute-owned-stream-or-query-with-query-buffer :handle %1 %2))))
      (is (= [[:stream header]] @calls))
      (doseq [sql ["select 1" "INSERT INTO sample FORMAT JSONCompactEachRow\n[1]\n"
                   "insert into sample FORMAT JSONEachRow\n{\"n\":1}\n"
                   header
                   "insert into sample (`n`) SETTINGS input_format_read_datetime_number_as_raw_value=0 FORMAT JSONCompactEachRow\n[1]\n"
                   (str "insert into sample (`" (apply str (repeat 8192 "x")) "`) FORMAT JSONCompactEachRow\n[1]\n")]]
        (with-sql sql #(is (= :ordinary (chdb/execute-owned-stream-or-query-with-query-buffer :handle %1 %2)))))
      (with-sql (str header "[1]\n")
        (fn [statement buffer]
          (is (thrown? clojure.lang.ExceptionInfo
                       (chdb/execute-owned-stream-or-query-with-query-buffer
                        :handle statement (update buffer :length dec)))))))))

(deftest full-buffer-classification-and-deferred-execution-stay-library-owned
  (doseq [enabled? [false true]]
    (let [events (atom []) sql (str header "[1]\n")]
      (with-redefs [native/classify-query-buffer!
                    (fn [_ buffer _]
                      (is (= sql (ffi/read-bytes (:pointer buffer) (:length buffer))))
                      (swap! events conj :classified) :analysis)
                    chdb/execute-owned-stream-or-query-with-query-buffer
                    (fn [& _] (swap! events conj :stream-selected))
                    chdb/execute-owned-any-with-query-buffer
                    (fn [& _] (swap! events conj :ordinary-selected))]
        (let [statement (owned/try-snapshot (.getBytes sql "UTF-8"))
              operation (:with-native-owned-admitted-buffer! (#'durable/default-open-operations enabled?))]
          (operation :handle statement "default"
                     (fn [analysis execute!]
                       (is (= :analysis analysis))
                       (is (= [:classified] @events))
                       (swap! events conj :admitted)
                       (execute!))))
        (is (= [:classified :admitted (if enabled? :stream-selected :ordinary-selected)] @events))))))
