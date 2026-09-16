(ns jdbc.chdb-json-rows-test
  (:require [clojure.test :refer [deftest is run-tests]]
            [db.jdbc] [jdbc.core :as jdbc]
            [db.driver :as driver] [db.jdbc-shim :as shim]
            [clojure.data.json :as json] [clojure.string :as str]
            [jdbc.chdb :as chdb] [jdbc.chdb.native :as native]
            [jolt.ffi :as ffi]))

(defn- fixture [id]
  (let [handle {:connection :mock-native :closed? (atom false) :lock (Object.)}
        drv (reify driver/Driver
              (descriptor [_] {:id id :aliases #{"row-test"} :uri-prefixes ["row-test:"]
                               :product-name "row test" :capabilities {:transactions :none :generated-keys :none}})
              (open-handle [_ _] handle)
              (close-handle [_ h] (reset! (:closed? h) true))
              (execute-handle [_ _ _ _] (throw (ex-info "unexpected JDBC execution" {}))))
        conn (with-redefs [driver/resolve-driver (fn [_] drv)] (shim/connection "row-test:memory"))]
    {:conn conn :handle handle}))

(defn- rejects? [f] (try (f) false (catch Exception _ true)))
(defn- failure-type [f] (try (f) nil (catch Exception e (:type (ex-data e)))))

(deftest row-data-is-not-placeholder-sql
  (let [{:keys [conn handle]} (fixture :chdb)
        payload "{\"message\":\"URL?x=1; 'quote' \\\\ path λ😀\",\"Events.Name\":[\"one\"]}\n"
        sql (str "INSERT INTO `events` (`message`, `Events.Name`) FORMAT JSONEachRow\n" payload)
        reference (chdb/prepared-sql (chdb/prepare-query sql []))
        executions (atom []) result {:labels [] :rows [] :count 1}]
    (try
      (with-redefs-fn
        {(ns-resolve 'jdbc.chdb 'rewrite-placeholders)
         (fn [& _] (throw (ex-info "row payload was lexed as SQL" {})))
         #'chdb/stream-insert! (fn [& _] (throw (ex-info "stream transport used" {})))
         #'chdb/execute-prepared-any (fn [h prepared]
                                     (swap! executions conj [h (chdb/prepared-sql prepared)]) result)}
        #(do (is (= result (chdb/insert-json-rows! conn "events" ["message" "Events.Name"] payload)))
             (is (= [[handle reference]] @executions))
             (is (= sql reference))))
      (finally (.close conn)))))

(deftest rejects-invalid-identifiers-and-non-data-before-execution
  (let [{:keys [conn]} (fixture :chdb) effects (atom 0)
        invalid [["events; DROP TABLE sentinel" ["message"] "{}\n"]
                 ["events`" ["message"] "{}\n"] ["db.events" ["message"] "{}\n"]
                 ["events" ["message); DROP TABLE sentinel"] "{}\n"]
                 ["events" ["message`"] "{}\n"] ["events" ["message" "message"] "{}\n"]
                 ["events" [] "{}\n"] ["events" #{"message"} "{}\n"]
                 ["events" [:message] "{}\n"] ["events" ["message"] nil]
                 ["events" ["message"] (byte-array 0)]
                 [(apply str (repeat 256 "a")) ["message"] "{}\n"]
                 ["événements" ["message"] "{}\n"] ["" ["message"] "{}\n"]
                 ["events" [(apply str (repeat 256 "a"))] "{}\n"]
                 ["events" ["λ"] "{}\n"] ["events" ["a..b"] "{}\n"]
                 ["events" [".a"] "{}\n"] ["events" ["a."] "{}\n"]
                 ["events" [""] "{}\n"]]]
    (try
      (with-redefs [chdb/execute-prepared-any (fn [& _] (swap! effects inc))]
        (doseq [[table columns payload] invalid]
          (is (= :jdbc.chdb/invalid-json-rows
                 (failure-type #(chdb/insert-json-rows! conn table columns payload)))))
        (is (zero? @effects)))
      (finally (.close conn)))))

(deftest rejects-durable-and-closed-shim-context-before-execution
  (doseq [id [:chdb-durable :chdb]]
    (let [{:keys [conn]} (fixture id) effects (atom 0)]
      (when (= id :chdb) (.close conn))
      (try
        (with-redefs [chdb/execute-prepared-any (fn [& _] (swap! effects inc))]
          (is (rejects? #(chdb/insert-json-rows! conn "events" ["message"] "{\"message\":\"?\"}\n")))
          (is (zero? @effects)))
        (finally (.close conn))))))

(deftest rejects-closed-native-handle-under-existing-lock
  (let [{:keys [conn handle]} (fixture :chdb) allocations (atom 0)]
    (reset! (:closed? handle) true)
    (try
      (with-redefs [ffi/alloc (fn [& _] (swap! allocations inc))]
        (is (rejects? #(chdb/insert-json-rows! conn "events" ["message"] "{\"message\":\"?\"}\n")))
        (is (zero? @allocations)))
      (finally (.close conn)))))

(defn run [check]
  (let [result (run-tests 'jdbc.chdb-json-rows-test)]
    (check "ordinary JSONEachRow mock boundary" 0 (+ (:fail result) (:error result)))))

(defn -main [& _]
  (let [result (run-tests 'jdbc.chdb-json-rows-test)]
    (System/exit (if (zero? (+ (:fail result) (:error result))) 0 1))))

(defn run-native!
  "Explicit ordinary-memory qualification, never part of the mock runner.
  SQL-looking malformed payload controls sample engine behavior, not a general
  adversarial-payload guarantee. Events.Name is a dotted Array column, not a
  real Nested engine type. Only bounded scalar results are printed."
  []
  (let [checks (atom 0)
        check! (fn [ok] (swap! checks inc)
                 (when-not ok (throw (ex-info "JSONEachRow native qualification failed"
                                             {:check @checks}))))
        columns ["id" "message" "Events.Name"]
        empty-result (atom nil)
        version-result (atom nil)
        rows (mapv (fn [id] {"id" id "message" (str "? 'quote' \\ slash\nλ😀\n; DROP TABLE row_sentinel " id)
                            "Events.Name" [(str "nested?" id)]}) (range 1 7))
        payload (fn [rs] (str/join "\n" (map json/write-str rs)))
        query native/chdb-query-with-params-n
        destroy native/chdb-destroy-query-result
        result-error native/chdb-result-error
        allocate ffi/alloc release ffi/free
        observed (fn [f]
                   (let [counts (atom {:queries 0 :parameters 0 :destroyed 0 :allocated 0 :freed 0
                                       :engine-error? false})
                         outcome (with-redefs
                                   [native/chdb-query-with-params-n
                                    (fn [& args] (swap! counts update :queries inc)
                                      (swap! counts update :parameters + (last args)) (apply query args))
                                    native/chdb-destroy-query-result
                                    (fn [p] (swap! counts update :destroyed inc) (destroy p))
                                    native/chdb-result-error
                                    (fn [p] (let [message (result-error p)]
                                              (when-not (str/blank? message)
                                                (swap! counts assoc :engine-error? true))
                                              message))
                                    ffi/alloc (fn [& args] (swap! counts update :allocated inc)
                                                (apply allocate args))
                                    ffi/free (fn [p] (swap! counts update :freed inc) (release p))]
                                   (try (f) :success (catch Exception _ :error)))]
                     (check! (= 1 (:queries @counts)))
                     (check! (zero? (:parameters @counts)))
                     (check! (= 1 (:destroyed @counts)))
                     (check! (= 2 (:allocated @counts) (:freed @counts)))
                     {:outcome outcome :engine-error? (:engine-error? @counts)}))]
    (check! (str/includes? (get (first rows) "message") "\n"))
    (with-open [conn (jdbc/connection "chdb::memory:")]
      (let [engine-version (:v (jdbc/fetch-one conn "select version() v"))]
        (reset! version-result {:package-version native/version :engine-version engine-version})
        (check! (= "26.7.3" native/version))
        (check! (= "26.7.2.1" engine-version)))
      (jdbc/execute! conn "create table row_fast (id Int64, message String, `Events.Name` Array(String)) engine=Memory")
      (jdbc/execute! conn "create table row_reference (id Int64, message String, `Events.Name` Array(String)) engine=Memory")
      (jdbc/execute! conn "create table row_sentinel (id Int64) engine=Memory")
      (jdbc/execute! conn "insert into row_sentinel values (71)")
      ;; Empty encoded input is passed through, not normalized or rejected by
      ;; the API. Characterize whether this pinned engine accepts it; either
      ;; way its row count must remain zero and ownership must balance.
      (let [empty-outcome (observed #(chdb/insert-json-rows! conn "row_fast" columns ""))]
        (reset! empty-result empty-outcome)
        (check! (zero? (:n (jdbc/fetch-one conn "select count() n from row_fast")))))
      (doseq [rs [(subvec rows 0 1) (subvec rows 1 3) (subvec rows 3 6)]]
        (check! (= {:outcome :success :engine-error? false}
                   (observed #(chdb/insert-json-rows! conn "row_fast" columns (payload rs)))))
        (jdbc/execute! conn (str "INSERT INTO `row_reference` (`id`, `message`, `Events.Name`) FORMAT JSONEachRow\n" (payload rs))))
      (let [readback (fn [table] (jdbc/fetch conn (str "select id, message, `Events.Name` names from " table " order by id")))
            expected (mapv (fn [row] {:id (get row "id") :message (get row "message")
                                     :names (get row "Events.Name")}) rows)]
        (check! (= expected (readback "row_fast")))
        (check! (= expected (readback "row_reference"))))
      (doseq [bad ["{not-json}" "{}\n; DROP TABLE row_sentinel"]]
        (check! (= {:outcome :error :engine-error? true}
                   (observed #(chdb/insert-json-rows! conn "row_fast" columns bad))))
        (check! (= 71 (:id (jdbc/fetch-one conn "select id from row_sentinel")))))
      (.close conn)
      (let [executions (atom 0)]
        (with-redefs [chdb/execute-prepared-any (fn [& _] (swap! executions inc))]
          (check! (rejects? #(chdb/insert-json-rows! conn "row_fast" columns (payload rows))))
          (check! (zero? @executions)))))
    ;; The real shim enforces the literal Durable driver identity before
    ;; execution; this deliberately does not claim a native Durable open.
    (let [{:keys [conn]} (fixture :chdb-durable) executions (atom 0)]
      (try
        (with-redefs [chdb/execute-prepared-any (fn [& _] (swap! executions inc))]
          (check! (rejects? #(chdb/insert-json-rows! conn "row_fast" columns (payload rows))))
          (check! (zero? @executions)))
        (finally (.close conn))))
    (println "JSONEachRow native qualification checks passed:" @checks)
    (println "Empty JSONEachRow pinned-engine outcome:" @empty-result)
    (println "Pinned package and SQL engine versions:" @version-result)
    {:checks @checks :empty-outcome @empty-result :versions @version-result}))
