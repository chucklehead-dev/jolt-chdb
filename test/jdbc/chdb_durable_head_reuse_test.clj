(ns jdbc.chdb-durable-head-reuse-test
  (:require [clojure.test :refer [deftest is run-tests]]
            [jdbc.chdb.durable.backend :as backend]
            [jdbc.chdb.durable.control :as control]
            [jdbc.chdb.durable.head :as head]))

(def options
  {:owner "writer" :instance "instance" :expires-at 200M :now 100M
   :clock-skew 5M :database "default" :engine-version "26.7.3"
   :backup-format 1 :min-reader "26.7.3"})

(defn read-store [value reads]
  (reify backend/ObjectBackend
    (get-with-etag [_ key]
      (is (= control/head-key key))
      (swap! reads inc)
      @value)))

(defn reuse [store previous]
  ((ns-resolve 'jdbc.chdb.durable.control 'read-commit-head!) store previous))

(deftest exact-bytes-not-etag-determine-reuse
  (let [document (head/decode (head/encode (control/fresh-head options)) :writer)
        bytes (head/encode document)
        value (atom {:bytes bytes :etag "first"})
        reads (atom 0) decodes (atom 0)
        store (read-store value reads)
        decode head/decode]
    (with-redefs [head/decode (fn [b mode]
                              (is (= :writer mode))
                              (swap! decodes inc) (decode b mode))]
      (let [first (reuse store nil)]
        (is (= document (:head first)))
        (is (not (identical? bytes (:bytes first))))
        (reset! value {:bytes (head/encode document) :etag "second"})
        (let [second (reuse store first)]
          (is (identical? (:head first) (:head second)))
          (is (= "second" (:etag second)))
          (is (= 1 @decodes)))
        ;; Same opaque ETag, different valid bytes: must decode the new head.
        (let [changed (assoc-in document ["lease" "expires_at"] 300)]
          (reset! value {:bytes (head/encode changed) :etag "first"})
          (is (= changed (:head (reuse store first))))
          (is (= 2 @decodes)))
        ;; The initial backend array must not become the retained witness.
        (aset-byte bytes 0 (byte 32))
        (reset! value {:bytes (head/encode document) :etag "third"})
        (is (= document (:head (reuse store first))))
        (is (= 2 @decodes))
        (reset! value {:bytes (.getBytes "not-json" "UTF-8") :etag "first"})
        (is (thrown? clojure.lang.ExceptionInfo (reuse store first)))
        (is (= 3 @decodes))
        (reset! value nil)
        (is (nil? (reuse store first)))
        (is (= 3 @decodes))
        (is (= 6 @reads))))))

(deftest commit-reuses-only-the-identical-second-read
  (let [delegate (backend/memory-backend)
        acquired (control/acquire! delegate options)
        reads (atom 0) decodes (atom 0)
        store (reify backend/ObjectBackend
                (get-with-etag [_ key]
                  (swap! reads inc) (backend/get-with-etag delegate key))
                (replace-if-match! [_ key bytes etag]
                  (backend/replace-if-match! delegate key bytes etag)))
        decode head/decode
        reference {"key" "wal/1-1-00000011.jsonl" "size" 3
                   "sha256" "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"}
        result (with-redefs [head/decode (fn [b mode]
                                         (swap! decodes inc) (decode b mode))]
                 (control/commit-reference!
                  store (:token acquired)
                  {:kind :wal :reference reference
                   :verify-reference! (fn [_ _] true)}))]
    (is (= :committed (:status result)))
    (is (= 2 @reads))
    ;; First read + intended wire roundtrip, not three complete decodes.
    (is (= 2 @decodes))
    (is (= (:head result) (:head (control/read-head! delegate))))))

(defn run-checks! []
  (let [r (run-tests 'jdbc.chdb-durable-head-reuse-test)]
    (when-not (zero? (+ (:fail r) (:error r)))
      (throw (ex-info "Durable head decode reuse checks failed" {})))
    true))

(defn -main [& _] (run-checks!))
