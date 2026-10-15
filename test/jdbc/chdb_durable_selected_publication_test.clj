(ns jdbc.chdb-durable-selected-publication-test
  (:require [clojure.test :refer [deftest is run-tests]]
            [jdbc.chdb.durable.backend :as backend]
            [jdbc.chdb.durable.control :as control]
            [jdbc.chdb.durable.head :as head]
            [jdbc.chdb.durable.digest :as digest])
  (:import [java.nio.file Files OpenOption]
           [java.nio.file.attribute FileAttribute]))

(def options
  {:owner "selected" :instance "one" :now 100M :expires-at 200M :clock-skew 5M
   :database "default" :engine-version "26.7.3" :backup-format 1
   :min-reader "26.7.3"})
(def abc-sha "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad")
(defn with-file [f]
  (let [p (Files/createTempFile "jchdb-selected-" ".jsonl" (make-array FileAttribute 0))]
    (try
      (Files/write p (.getBytes "abc" "UTF-8") (make-array OpenOption 0))
      (f p)
      (finally (Files/deleteIfExists p)))))
(defn fixture [corrupt?]
  (let [delegate (backend/memory-backend)
        token (:token (control/acquire! delegate options))
        counts (atom {:head 0 :put 0 :verify 0 :cas 0 :select 0 :digest 0})
        store (reify backend/ObjectBackend
                (get-bytes [_ k] (backend/get-bytes delegate k))
                (get-with-etag [_ k]
                  (when (= control/head-key k) (swap! counts update :head inc))
                  (backend/get-with-etag delegate k))
                (put-file-if-absent! [_ k p]
                  (swap! counts update :put inc)
                  (backend/put-file-if-absent! delegate k p))
                (put-bytes-if-absent! [_ k bytes]
                  (swap! counts update :put inc)
                  (backend/put-bytes-if-absent! delegate k bytes))
                (replace-if-match! [_ k bytes etag]
                  (swap! counts update :cas inc)
                  (backend/replace-if-match! delegate k bytes etag))
                (download-to-file! [_ k p]
                  (swap! counts update :verify inc)
                  (if corrupt?
                    (do (Files/write p (.getBytes "abd" "UTF-8")
                                     (make-array OpenOption 0))
                        {:status :downloaded :byte-count 3})
                    (backend/download-to-file! delegate k p))))]
    {:delegate delegate :store store :token token :counts counts}))
(defn selected! [f selector opts]
  (let [hash-file digest/sha256-file]
    (with-redefs [digest/sha256-file
                  (fn [path] (swap! (:counts f) update :digest inc) (hash-file path))]
      (control/publish-wal-file-selected!
       (:store f) (:token f)
       (fn [document] (swap! (:counts f) update :select inc) (selector document)) opts))))
(defn commit! [f publication]
  (control/commit-reference!
   (:store f) (:token f)
   {:kind :wal :reference (:reference publication) :verified-publication publication
    :verify-reference! (fn [& _] (throw (ex-info "witness was not accepted" {})))}))
(defn caught [f] (try (f) nil (catch Throwable e e)))

(deftest selected-valid-file-uses-one-decoded-head-and-private-witness
  (with-file
    (fn [path]
      (let [f (fixture false) seen (atom nil)
            publication (selected! f #(do (reset! seen %) path) {})]
        (is (= 1 (get-in @seen ["lease" "generation"])))
        (is (= 0 (get-in @seen ["manifest" "seq"])))
        (is (not (contains? @seen :etag)))
        (is (= {:head 1 :put 1 :verify 1 :cas 0 :select 1 :digest 2} @(:counts f)))
        (is (= :published (:status publication)))
        (is (= [3 abc-sha] ((juxt #(get % "size") #(get % "sha256")) (:reference publication))))
        (is (some? (meta publication)))
        (let [result (commit! f publication)]
          (is (= :committed (:status result)))
          (is (= [(:reference publication)] (get-in (:head result) ["manifest" "wal"])))
          (is (= {:head 2 :put 1 :verify 1 :cas 1 :select 1 :digest 2} @(:counts f))))))))

(deftest nil-selection-does-no-publication-or-digest
  (let [f (fixture false)]
    (is (nil? (selected! f (constantly nil) {})))
    (is (= {:head 1 :put 0 :verify 0 :cas 0 :select 1 :digest 0} @(:counts f)))))

(deftest callback-errors-and-invalid-paths-do-not-publish
  (with-file
    (fn [path]
      (let [failure (ex-info "selection failure" {:type ::selection})
            f (fixture false)]
        (is (identical? failure (caught #(selected! f (fn [_] (throw failure)) {}))))
        (is (= {:head 1 :put 0 :verify 0 :cas 0 :select 1 :digest 0} @(:counts f))))
      (let [f (fixture false)]
        (is (= ::control/invalid-options
               (:type (ex-data (caught #(selected! f (constantly (.resolve path "not-a-file")) {}))))))
        (is (= {:head 1 :put 0 :verify 0 :cas 0 :select 1 :digest 0} @(:counts f)))))))

(deftest invalid-inputs-precede-read-and-selection
  (doseq [[selector opts] [[nil {}] [:not-a-function {}]
                           [(fn [_] (throw (ex-info "must not run" {})))
                            {:phase-observe! :invalid}]]]
    (let [f (fixture false)]
      (is (= ::control/invalid-options
             (:type (ex-data (caught #(control/publish-wal-file-selected!
                                      (:store f) (:token f) selector opts))))))
      (is (= {:head 0 :put 0 :verify 0 :cas 0 :select 0 :digest 0} @(:counts f))))))

(deftest renewal-during-selection-does-not-change-sequence-or-clobber-expiry
  (with-file
    (fn [path]
      (let [f (fixture false)
            future {"nested" [1 true nil]}
            original (control/read-head! (:delegate f))
            _ (is (= :replaced
                     (:status (backend/replace-if-match!
                               (:delegate f) control/head-key
                               (head/encode (assoc (:head original) "future" future))
                               (:etag original)))))
            publication (selected! f (fn [document]
                                       (is (= 200 (get-in document ["lease" "expires_at"])))
                                       (is (= future (get document "future")))
                                       (control/renew! (:delegate f) (:token f) 300M)
                                       path) {})]
        (is (re-matches #"wal/1-1-[0-9a-f]{8}\.jsonl" (get-in publication [:reference "key"])))
        (is (= 1 (:head @(:counts f))))
        (let [result (commit! f publication)]
          (is (= future (get (:head result) "future")))
          (is (= [300 1] [(get-in (:head result) ["lease" "expires_at"])
                          (get-in (:head result) ["manifest" "seq"])]))
          (is (= 2 (:head @(:counts f)))))
        (is (= 1 (:cas @(:counts f))))))))

(deftest takeover-during-selection-preserves-stale-publication-but-fences-commit
  (with-file
    (fn [path]
      (let [f (fixture false)
            publication (selected! f (fn [_]
                                       (control/acquire! (:delegate f)
                                         (assoc options :owner "other" :instance "two"
                                                :now 256M :expires-at 400M))
                                       path) {})]
        (is (= :published (:status publication)))
        (is (re-matches #"wal/1-1-[0-9a-f]{8}\.jsonl" (get-in publication [:reference "key"])))
        (is (= ::control/lease-fenced (:type (ex-data (caught #(commit! f publication))))))
        (is (= 0 (:cas @(:counts f))))
        (is (= [2 0] ((juxt #(get-in % ["lease" "generation"])
                             #(get-in % ["manifest" "seq"]))
                       (:head (control/read-head! (:delegate f))))))))))

(deftest same-size-corrupt-readback-cannot-create-witness-or-cas
  (with-file
    (fn [path]
      (let [f (fixture true)]
        (is (= ::control/object-unverified
               (:type (ex-data (caught #(selected! f (constantly path) {}))))))
        (is (= {:head 1 :put 1 :verify 1 :cas 0 :select 1 :digest 2} @(:counts f)))
        (is (= 0 (get-in (:head (control/read-head! (:delegate f))) ["manifest" "seq"])))))))

(deftest intervening-manifest-advance-rejects-the-older-selected-sequence
  (with-file
    (fn [path]
      (let [f (fixture false)
            first-reference (atom nil)
            publication
            (selected! f
                       (fn [_]
                         (let [p (control/publish-wal-bytes!
                                  (:delegate f) (:token f) (.getBytes "other" "UTF-8"))]
                           (control/commit-reference!
                            (:delegate f) (:token f)
                            {:kind :wal :reference (:reference p)
                             :verified-publication p
                             :verify-reference! control/verify-byte-reference!})
                           (reset! first-reference (:reference p)))
                         path) {})]
        (is (= ::control/object-unverified
               (:type (ex-data (caught #(commit! f publication))))))
        (is (= 0 (:cas @(:counts f))))
        (let [document (:head (control/read-head! (:delegate f)))]
          (is (= 1 (get-in document ["manifest" "seq"])))
          (is (= [@first-reference] (get-in document ["manifest" "wal"]))))))))

(deftest legacy-file-entry-retains-validation-order
  (with-file
    (fn [path]
      (let [f (fixture false)
            error (caught #(control/publish-wal-file!
                             (:store f) (:token f) (.resolve path "not-a-file")
                             {:phase-observe! :invalid}))]
        (is (= "WAL publication requires a regular file" (ex-message error)))
        (is (= 0 (:head @(:counts f)))))
      (let [f (fixture false)
            error (caught #(control/publish-wal-file! (:store f) (:token f) path
                                                      {:phase-observe! :invalid}))]
        (is (= "phase-observe! must be a function" (ex-message error)))
        (is (= 0 (:head @(:counts f))))))))

(defn -main [& _]
  (let [result (run-tests 'jdbc.chdb-durable-selected-publication-test)]
    (when-not (zero? (+ (:fail result) (:error result)))
      (throw (ex-info "Selected WAL publication checks failed" result)))))
