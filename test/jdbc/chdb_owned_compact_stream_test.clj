(ns jdbc.chdb-owned-compact-stream-test
  (:require [clojure.test :refer [deftest is]]
            [jdbc.chdb :as chdb] [jdbc.chdb.native :as native]
            [jdbc.chdb.owned-statement :as owned] [jolt.ffi :as ffi]))

(def header "insert into sample (`n`, `label`) FORMAT JSONCompactEachRow\n")
(def payload "[1,\"ready?\"]\n")

(defn with-statement [f]
  (let [statement (owned/try-snapshot (.getBytes (str header payload) "UTF-8"))]
    (owned/with-query-buffer statement #(f statement %))))

(defn simulated [mode f]
  (let [calls (atom [])]
    (with-redefs [native/ensure-loaded! (fn [] nil)
                  native/chdb-version (fn [] (if (= mode :old) "26.7.3" "26.9.0"))
                  native/with-live-handle (fn [_ callback] (callback :connection))
                  native/chdb-stream-insert-n
                  (fn [_ pointer length format-pointer format-size]
                    (swap! calls conj [:open (ffi/read-bytes pointer length)
                                       (ffi/read-bytes format-pointer format-size)])
                    (if (= mode :null) 0 123))
                  native/chdb-stream-insert-error (fn [_] (when (= mode :init-error) "synthetic"))
                  native/chdb-stream-append
                  (fn [_ pointer length]
                    (swap! calls conj [:append (ffi/read-bytes pointer length)])
                    (if (= mode :append-error) 1 0))
                  native/chdb-stream-done (fn [_] (swap! calls conj :done) 456)
                  native/chdb-stream-cancel-insert (fn [_] (swap! calls conj :cancel))
                  native/chdb-destroy-insert-stream (fn [_] (swap! calls conj :destroy-stream))
                  native/chdb-result-error (fn [_] (when (= mode :done-error) "synthetic"))
                  native/chdb-result-length (fn [_] 0)
                  native/chdb-result-buffer (fn [_] 0)
                  native/chdb-result-rows-written (fn [_] 1)
                  native/chdb-result-elapsed (fn [_] 0.0)
                  native/chdb-result-rows-read (fn [_] 0)
                  native/chdb-result-bytes-read (fn [_] 0)
                  native/chdb-result-storage-rows-read (fn [_] 0)
                  native/chdb-result-storage-bytes-read (fn [_] 0)
                  native/chdb-result-bytes-written (fn [_] 0)
                  native/chdb-destroy-query-result (fn [_] (swap! calls conj :destroy-result))]
      (f calls))))

(deftest successful-exact-split-and-single-release
  (simulated :ok
    (fn [calls]
      (with-statement
        (fn [statement buffer]
          (is (= 1 (:count (chdb/execute-owned-compact-json-stream-with-query-buffer
                            :handle statement buffer header))))))
      (is (= [[:open "insert into sample (`n`, `label`)" "JSONCompactEachRow"]
              [:append payload] :done :destroy-result :destroy-stream] @calls)))))

(deftest rejection-precedes-native-stream
  (simulated :ok
    (fn [calls]
      (with-statement
        (fn [statement buffer]
          (doseq [bad [nil "" "insert into sample FORMAT JSONEachRow\n"
                       "insert into sample (`other`) FORMAT JSONCompactEachRow\n"
                       "insert into sample (`n`); select 1 FORMAT JSONCompactEachRow\n"
                       "insert into sample (`n`, `label`) SETTINGS input_format_read_datetime_number_as_raw_value=0 FORMAT JSONCompactEachRow\n"
                       "insert into sample (`n`, `label`) SETTINGS compatibility='26.7' FORMAT JSONCompactEachRow\n"
                       "insert into sample (`n`) FORMAT JSONCompactEachRow\n"]]
            (is (thrown? clojure.lang.ExceptionInfo
                         (chdb/execute-owned-compact-json-stream-with-query-buffer :handle statement buffer bad))))
          (is (thrown? clojure.lang.ExceptionInfo
                       (chdb/execute-owned-compact-json-stream-with-query-buffer
                        :handle statement (update buffer :length dec) header)))))
      (is (empty? @calls)))))

(deftest old-package-is-fenced
  (simulated :old
    (fn [calls]
      (with-statement
        (fn [statement buffer]
          (is (thrown? clojure.lang.ExceptionInfo
                       (chdb/execute-owned-compact-json-stream-with-query-buffer :handle statement buffer header)))))
      (is (empty? @calls)))))

(deftest failure-releases-and-cancels-only-unfinalized-stream
  (doseq [mode [:init-error :append-error :done-error]]
    (simulated mode
      (fn [calls]
        (with-statement
          (fn [statement buffer]
            (is (thrown? clojure.lang.ExceptionInfo
                         (chdb/execute-owned-compact-json-stream-with-query-buffer :handle statement buffer header)))))
        (is (= 1 (count (filter #{:destroy-stream} @calls))))
        (is (= (if (= mode :done-error) 0 1) (count (filter #{:cancel} @calls))))
        (is (= (if (= mode :done-error) 1 0) (count (filter #{:destroy-result} @calls))))))))

(deftest null-stream-is-not-destroyed-as-an-owned-handle
  (simulated :null
    (fn [calls]
      (with-statement
        (fn [statement buffer]
          (is (thrown? clojure.lang.ExceptionInfo
                       (chdb/execute-owned-compact-json-stream-with-query-buffer :handle statement buffer header)))))
      (is (= 1 (count @calls)))
      (is (= :open (ffirst @calls))))))

(deftest fixed-raw-ticks-clause-is-preserved-not-a-session-setting
  (let [query "insert into sample (`n`, `label`) SETTINGS input_format_read_datetime_number_as_raw_value=1"
        header (str query " FORMAT JSONCompactEachRow\n")
        statement (owned/try-snapshot (.getBytes (str header payload) "UTF-8"))]
    (simulated :ok
      (fn [calls]
        (owned/with-query-buffer statement
          #(is (= 1 (:count (chdb/execute-owned-compact-json-stream-with-query-buffer
                             :handle statement % header)))))
        (is (= [[:open query "JSONCompactEachRow"] [:append payload]
                :done :destroy-result :destroy-stream] @calls))))))
