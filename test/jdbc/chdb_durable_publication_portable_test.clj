(ns jdbc.chdb-durable-publication-portable-test
  "Reader/publication regression for #239: no chDB, fibers or Hegel dependency."
  (:require [clojure.test :refer [deftest is run-tests testing]]
            [jdbc.chdb.durable.backend :as backend]
            [jdbc.chdb.durable.control :as control])
  (:import [java.nio.file Files OpenOption]
           [java.nio.file.attribute FileAttribute]))

(def ^:private abc-sha256
  "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad")
(def ^:private lease-options
  {:owner "portable" :instance "one" :now 100M :expires-at 200M :clock-skew 5M
   :database "default" :engine-version "26.7.3" :backup-format 1
   :min-reader "26.7.3"})

(defn- with-source-file [f]
  (let [path (Files/createTempFile "jchdb-publication-portable-" ".jsonl"
                                   (make-array FileAttribute 0))]
    (try
      (Files/write path (.getBytes "abc" "UTF-8") (make-array OpenOption 0))
      (f path)
      (finally (Files/deleteIfExists path)))))

(defn- observed-backend [delegate reads cas-count read-error]
  (let [read! (fn []
                (swap! reads inc)
                (when-let [failure @read-error] (throw failure)))]
    (reify backend/ObjectBackend
      (get-bytes [_ key]
        (read!)
        (backend/get-bytes delegate key))
      (get-with-etag [_ key] (backend/get-with-etag delegate key))
      (put-file-if-absent! [_ key path]
        (backend/put-file-if-absent! delegate key path))
      (put-bytes-if-absent! [_ key bytes]
        (backend/put-bytes-if-absent! delegate key bytes))
      (replace-if-match! [_ key bytes etag]
        (swap! cas-count inc)
        (backend/replace-if-match! delegate key bytes etag))
      (download-to-file! [_ key path]
        (read!)
        (backend/download-to-file! delegate key path)))))

(defn- observer-options [mode events]
  (if (= :none mode)
    {}
    {:phase-observe! (fn [event]
                       (swap! events conj (select-keys event [:phase :status]))
                       (when (= :throw mode)
                         (throw (ex-info "observer failure" {:type ::observer}))))}))

(defn- publish! [kind store token path options]
  (case kind
    :bytes (control/publish-wal-bytes! store token (.getBytes "abc" "UTF-8") options)
    :file (control/publish-wal-file! store token path options)))

(defn- verifier [kind]
  (case kind :bytes control/verify-byte-reference! :file control/verify-file-reference!))

(deftest public-publication-and-witness-commit
  (with-source-file
    (fn [path]
      (doseq [kind [:bytes :file] mode [:none :record :throw]]
        (testing (str kind " with observer " mode)
          (let [delegate (backend/memory-backend)
                token (:token (control/acquire! delegate lease-options))
                reads (atom 0) cas-count (atom 0) read-error (atom nil)
                events (atom []) options (observer-options mode events)
                store (observed-backend delegate reads cas-count read-error)
                publication (publish! kind store token path options)
                reference (:reference publication)]
            (is (= :published (:status publication)))
            (is (= [3 abc-sha256] [(get reference "size") (get reference "sha256")]))
            (is (= [1 0] [@reads @cas-count]) "publication independently reads stored bytes")
            (is (= (if (= :none mode) []
                       [{:phase :wal-immutable-put :status :complete}
                        {:phase :wal-immutable-verify :status :complete}]) @events))
            ;; A redundant verifier call would now throw: witness reuse is not
            ;; inferred merely from a successful commit or a final read count.
            (reset! read-error (ex-info "unexpected second read" {:type ::second-read}))
            (let [committed (control/commit-reference!
                             store token
                             (merge options {:kind :wal :reference reference
                                             :verified-publication publication
                                             :verify-reference! (verifier kind)}))]
              (is (= [reference] (get-in (:head committed) ["manifest" "wal"])))
              (is (= [1 1] [@reads @cas-count])))
            (when-not (= :none mode)
              (is (= {:phase :wal-head-cas :status :complete} (last @events))))))))))

(deftest verification-error-is-not-replaced-by-observer-error
  (with-source-file
    (fn [path]
      (doseq [kind [:bytes :file] mode [:none :record :throw]]
        (testing (str kind " failing with observer " mode)
          (let [delegate (backend/memory-backend)
                token (:token (control/acquire! delegate lease-options))
                failure (ex-info "stored read failed" {:type ::read-failed})
                reads (atom 0) cas-count (atom 0) events (atom [])
                store (observed-backend delegate reads cas-count (atom failure))
                options (observer-options mode events)
                caught (try (publish! kind store token path options) nil
                            (catch Throwable error error))]
            (is (identical? failure caught) "the original verification error propagates")
            (is (= [1 0] [@reads @cas-count]))
            (is (= 0 (get-in (:head (control/read-head! delegate)) ["manifest" "seq"])))
            (is (= (if (= :none mode) []
                       [{:phase :wal-immutable-put :status :complete}
                        {:phase :wal-immutable-verify :status :failed}]) @events))))))))

(defn -main [& _]
  (let [result (run-tests 'jdbc.chdb-durable-publication-portable-test)]
    (when (pos? (+ (:fail result) (:error result)))
      (throw (ex-info "Durable portable publication checks failed"
                      (select-keys result [:test :pass :fail :error]))))
    (println "all Durable portable publication checks passed")))
