(ns jdbc.chdb-durable-control-test
  (:require [hegel.core :as h]
            [hegel.stateful :as hs]
            [hegel.trace :as ht]
            [jdbc.chdb.durable.backend :as backend]
            [jdbc.chdb.durable.control :as control]))

(def failures (atom 0))

(defn- check [label expected actual]
  (if (= expected actual)
    (println "  ok  " label)
    (do
      (swap! failures inc)
      (println "  FAIL" label "- expected" (pr-str expected)
               "got" (pr-str actual)))))

(defn- error-type [f]
  (try (f) nil (catch Throwable error (:type (ex-data error)))))

(def base-options
  {:owner "writer-1"
   :instance "instance-1"
   :expires-at 200M
   :now 100M
   :clock-skew 5M
   :database "default"
   :engine-version "26.7.2-rc.2"
   :backup-format 1
   :min-reader "26.7.2-rc.2"})

(def abc-digest
  "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad")

(defn- wal-reference [generation sequence]
  {"key" (str "wal/" generation "-" sequence "-"
              (format "%08x" (+ (* generation 16) sequence)) ".jsonl")
   "size" 3
   "sha256" abc-digest})

(defn- checkpoint-reference [generation sequence]
  {"key" (str "checkpoints/" generation "-" sequence "-"
              (format "%08x" (+ (* generation 16) sequence)) ".tar.gz")
   "size" 3
   "sha256" abc-digest})

(defn- publish-abc! [store token]
  (:reference
   (control/publish-wal-bytes! store token (.getBytes "abc" "UTF-8"))))

(defn- forwarding-backend [delegate cas-mode]
  (reify backend/ObjectBackend
    (get-bytes [_ key] (backend/get-bytes delegate key))
    (get-with-etag [_ key] (backend/get-with-etag delegate key))
    (put-file-if-absent! [_ key path]
      (backend/put-file-if-absent! delegate key path))
    (put-bytes-if-absent! [_ key bytes]
      (backend/put-bytes-if-absent! delegate key bytes))
    (replace-if-match! [_ key bytes etag]
      (case cas-mode
        :land-ambiguous
        (let [result (backend/replace-if-match! delegate key bytes etag)]
          (if (= :replaced (:status result))
            {:status :ambiguous}
            result))
        :drop-ambiguous {:status :ambiguous}
        (backend/replace-if-match! delegate key bytes etag)))
    (download-to-file! [_ key path]
      (backend/download-to-file! delegate key path))))

(defn- conflicting-publication-backend [delegate]
  (reify backend/ObjectBackend
    (get-bytes [_ key]
      (if (= control/head-key key)
        (backend/get-bytes delegate key)
        (.getBytes "different" "UTF-8")))
    (get-with-etag [_ key] (backend/get-with-etag delegate key))
    (put-file-if-absent! [_ _ _] {:status :precondition-failed})
    (put-bytes-if-absent! [_ _ _] {:status :precondition-failed})
    (replace-if-match! [_ key bytes etag]
      (backend/replace-if-match! delegate key bytes etag))
    (download-to-file! [_ key path]
      (backend/download-to-file! delegate key path))))

(defn- land-then-renew-backend [delegate token renewed-expiry]
  (reify backend/ObjectBackend
    (get-bytes [_ key] (backend/get-bytes delegate key))
    (get-with-etag [_ key] (backend/get-with-etag delegate key))
    (put-file-if-absent! [_ key path]
      (backend/put-file-if-absent! delegate key path))
    (put-bytes-if-absent! [_ key bytes]
      (backend/put-bytes-if-absent! delegate key bytes))
    (replace-if-match! [_ key bytes etag]
      (let [result (backend/replace-if-match! delegate key bytes etag)]
        (when (= :replaced (:status result))
          ;; Model the critical window: the manifest CAS landed, its response
          ;; was lost, and the heartbeat changed only lease expiry before the
          ;; commit path could reread the head.
          (control/renew! delegate token renewed-expiry))
        (if (= :replaced (:status result)) {:status :ambiguous} result)))
    (download-to-file! [_ key path]
      (backend/download-to-file! delegate key path))))

(defn- run-deterministic-checks! []
  (println "Durable V1 lease and head-CAS control")
  (let [store (backend/memory-backend)
        first-acquire (control/acquire! store base-options)
        token1 (:token first-acquire)]
    (check "fresh acquisition starts at generation one"
           1 (:generation token1))
    (check "fresh acquisition retains an empty manifest"
           0 (get-in (:head first-acquire) ["manifest" "seq"]))
    (check "fresh result is the canonical stored JSON value"
           (:head first-acquire) (:head (control/read-head! store)))
    (check "a live competing writer is rejected"
           ::control/lease-held
           (error-type
            #(control/acquire!
              store (assoc base-options
                           :owner "writer-2" :instance "instance-2"))))
    (check "fresh acquisition cannot publish an already expired lease"
           ::control/invalid-options
           (error-type
            #(control/acquire!
              (backend/memory-backend)
              (assoc base-options :expires-at 100M))))

    (let [renewed (control/renew! store token1 250M)]
      (check "heartbeat preserves the fencing generation"
             1 (get-in (:head renewed) ["lease" "generation"]))
      (check "heartbeat installs the requested expiry"
             250 (get-in (:head renewed) ["lease" "expires_at"]))
      (check "heartbeat result is the canonical stored JSON value"
             (:head renewed) (:head (control/read-head! store)))
      (check "heartbeat cannot shorten or repeat the current expiry"
             ::control/invalid-options
             (error-type #(control/renew! store token1 250M))))

    (let [takeover (control/acquire!
                    store (assoc base-options
                                 :owner "writer-2"
                                 :instance "instance-2"
                                 :now 256M
                                 :expires-at 400M))
          token2 (:token takeover)
          before (:head (control/read-head! store))
          stale-reference (wal-reference 1 1)]
      (check "normal takeover waits through clock skew and increments generation"
             2 (:generation token2))
      (check "the stale writer is fenced before object verification"
             ::control/lease-fenced
             (error-type
              #(control/commit-reference!
                store token1
                {:kind :wal
                 :reference stale-reference
                 :verify-reference!
                 (fn [_ _]
                   (throw (ex-info "stale verifier must not run" {})))})))
      (check "a stale commit leaves the head byte-semantically unchanged"
             before (:head (control/read-head! store)))
      (let [stale-publication (control/publish-wal-bytes!
                               store token1 (.getBytes "abc" "UTF-8"))]
        (check "a stale writer may publish only an unreachable old-generation object"
               {:status :published :canonical-prefix? true}
               {:status (:status stale-publication)
                :canonical-prefix?
                (boolean
                 (re-matches #"wal/1-1-[0-9a-f]{8}\.jsonl"
                             (get-in stale-publication [:reference "key"])))})
        (check "the current writer cannot commit an old-generation publication"
               ::control/object-unverified
               (error-type
                #(control/commit-reference!
                  store token2
                  {:kind :wal
                   :reference (:reference stale-publication)
                   :verify-reference! control/verify-byte-reference!})))
        (check "rejecting an old-generation reference leaves the head unchanged"
               before (:head (control/read-head! store))))

      (let [publication (control/publish-wal-bytes!
                         store token2 (.getBytes "abc" "UTF-8"))
            reference (:reference publication)
            repeated (control/publish-wal-bytes!
                      store token2 (.getBytes "abc" "UTF-8"))
            committed
            (control/commit-reference!
             store token2
             {:kind :wal :reference reference
              :verify-reference! control/verify-byte-reference!})]
        (check "writer-aware WAL publication derives the exact reference"
               {:status :published :canonical-prefix? true}
               {:status (:status publication)
                :canonical-prefix?
                (boolean
                 (re-matches #"wal/2-1-[0-9a-f]{8}\.jsonl"
                             (get reference "key")))})
        (check "every immutable publication attempt has a unique key"
               [:published true]
               [(:status repeated)
                (not= (get reference "key")
                      (get-in repeated [:reference "key"]))])
        (check "a future, never-issued generation cannot publish"
               ::control/lease-fenced
               (error-type
                #(control/publish-wal-bytes!
                  store (assoc token2 :generation 3)
                  (.getBytes "abc" "UTF-8"))))
        (check "verified immutable WAL commit advances exactly once"
               1 (get-in (:head committed) ["manifest" "seq"]))
        (check "verified immutable WAL commit names the published object"
               reference
               (first (get-in (:head committed) ["manifest" "wal"]))))

      (let [before-bad (:head (control/read-head! store))
            missing (wal-reference 2 2)]
        (check "an unverified immutable object is rejected"
               ::control/object-unverified
               (error-type
                #(control/commit-reference!
                  store token2
                  {:kind :wal :reference missing
                   :verify-reference! control/verify-byte-reference!})))
        (check "failed verification precedes and prevents head CAS"
               before-bad (:head (control/read-head! store)))
        (backend/put-bytes-if-absent!
         store (get missing "key") (.getBytes "abd" "UTF-8"))
        (check "a digest-mismatched immutable object is rejected"
               ::control/object-unverified
               (error-type
                #(control/commit-reference!
                  store token2
                  {:kind :wal :reference missing
                   :verify-reference! control/verify-byte-reference!})))
        (check "digest mismatch also precedes and prevents head CAS"
               before-bad (:head (control/read-head! store))))

      (let [reference (checkpoint-reference 2 2)]
        (backend/put-bytes-if-absent!
         store (get reference "key") (.getBytes "abc" "UTF-8"))
        (let [checkpoint
              (control/commit-reference!
               store token2
               {:kind :checkpoint :reference reference
                :verify-reference! control/verify-byte-reference!})]
          (check "checkpoint advances the manifest exactly once"
                 2 (get-in (:head checkpoint) ["manifest" "seq"]))
          (check "checkpoint replaces base and clears covered WAL"
                 [reference []]
                 [(get-in (:head checkpoint) ["manifest" "base"])
                  (get-in (:head checkpoint) ["manifest" "wal"])])))

      (control/release! store token2)
      (check "release retains generation and clears all owner fields"
             {"generation" 2 "owner" nil "instance" nil "expires_at" nil}
             (get (:head (control/read-head! store)) "lease"))
      (check "stale release cannot clear a later lease"
             ::control/lease-fenced
             (error-type #(control/release! store token1)))))

  (let [delegate (backend/memory-backend)
        store (forwarding-backend delegate :land-ambiguous)
        acquired (control/acquire! store base-options)
        token (:token acquired)
        reference (publish-abc! store token)
        committed (control/commit-reference!
                   store token
                   {:kind :wal :reference reference
                    :verify-reference! control/verify-byte-reference!})]
    (check "ambiguous CAS is acknowledged only after exact reread proof"
           :reconciled (:status committed))
    (check "reconciled ambiguous CAS has the intended sequence"
           1 (get-in (:head committed) ["manifest" "seq"])))

  (let [delegate (backend/memory-backend)
        acquired (control/acquire! delegate base-options)
        token (:token acquired)
        reference (publish-abc! delegate token)
        store (land-then-renew-backend delegate token 300M)
        committed (control/commit-reference!
                   store token
                   {:kind :wal :reference reference
                    :verify-reference! control/verify-byte-reference!})]
    (check "reference and sequence reconcile across a later heartbeat"
           :reconciled (:status committed))
    (check "heartbeat reconciliation retains the newer lease expiry"
           300 (get-in (:head committed) ["lease" "expires_at"]))
    (check "heartbeat reconciliation commits the reference exactly once"
           [1 [reference]]
           [(get-in (:head committed) ["manifest" "seq"])
            (get-in (:head committed) ["manifest" "wal"])]))

  (let [delegate (backend/memory-backend)
        acquired (control/acquire! delegate base-options)
        token (:token acquired)
        reference (publish-abc! delegate token)
        store (forwarding-backend delegate :drop-ambiguous)]
    (check "an unprovable ambiguous CAS is distinguishable"
           ::control/commit-ambiguous
           (error-type
            #(control/commit-reference!
              store token
              {:kind :wal :reference reference
               :verify-reference! control/verify-byte-reference!})))
    (check "a dropped ambiguous CAS does not advance the head"
           0 (get-in (:head (control/read-head! store)) ["manifest" "seq"])))

  (let [delegate (backend/memory-backend)
        token (:token (control/acquire! delegate base-options))
        store (conflicting-publication-backend delegate)]
    (check "an existing object that fails the exact reference is rejected"
           ::control/object-unverified
           (error-type
            #(control/publish-wal-bytes!
              store token (.getBytes "abc" "UTF-8"))))
    (check "publication rejects non-byte payloads before the backend"
           ::control/invalid-options
           (error-type #(control/publish-wal-bytes! store token "abc")))))

(def durable-event-rule
  (ht/event-model
   :durable-head-monotonic
   {:initial {:generation 0 :sequence 0 :valid? true}
    :step
    (fn [state event]
      {:generation (:generation event)
       :sequence (:sequence event)
       :valid?
       (and (:valid? state)
            (>= (:generation event) (:generation state))
            (>= (:sequence event) (:sequence state))
            (if (= :commit (:kind event)) (:published? event) true)
            (if (= :stale-rejected (:kind event))
              (not (:head-changed? event))
              true))})
    :invariant (fn [state _] (:valid? state))}))

(defn- append-event [state event]
  (update state :events conj event))

(defn- acquire-step [writer owner instance]
  (fn [state]
    (let [result
          (control/acquire!
           (:store state)
           (assoc base-options
                  :owner owner :instance instance :force? true
                  :expires-at (+ 200M (count (:events state)))))
          previous (:current state)
          document (:head result)]
      (-> state
          (assoc :current (:token result) :head document)
          (update :stale #(cond-> % previous (conj previous)))
          (append-event
           {:kind :acquire
            :writer writer
            :generation (get-in document ["lease" "generation"])
            :sequence (get-in document ["manifest" "seq"])})))))

(defn- commit-step [state]
  (let [token (:current state)
        reference (publish-abc! (:store state) token)
        result
        (control/commit-reference!
         (:store state) token
         {:kind :wal :reference reference
          :verify-reference! control/verify-byte-reference!})
        document (:head result)]
    (-> state
        (assoc :head document)
        (append-event
         {:kind :commit :published? true
          :generation (get-in document ["lease" "generation"])
          :sequence (get-in document ["manifest" "seq"])}))))

(defn- stale-step [state]
  (let [token (first (:stale state))
        before (:head (control/read-head! (:store state)))
        sequence (inc (get-in before ["manifest" "seq"]))
        reference (wal-reference (:generation token) sequence)
        rejected?
        (= ::control/lease-fenced
           (error-type
            #(control/commit-reference!
              (:store state) token
              {:kind :wal :reference reference
               :verify-reference!
               (fn [_ _]
                 (throw (ex-info "fenced writer reached verification" {})))})))
        after (:head (control/read-head! (:store state)))]
    (when-not rejected?
      (throw (ex-info "stale writer was not fenced"
                      {:hegel/origin "chdb/durable-control/stale-writer"})))
    (-> state
        (update :stale subvec 1)
        (append-event
         {:kind :stale-rejected
          :head-changed? (not= before after)
          :generation (get-in after ["lease" "generation"])
          :sequence (get-in after ["manifest" "seq"])}))))

(defn- state-valid? [state]
  (let [actual (some-> (control/read-head! (:store state)) :head)]
    (and (= (:head state) actual)
         (try
           (ht/check! (:events state) [durable-event-rule])
           true
           (catch Throwable _ false)))))

(defn- run-stateful-property! []
  (println "Durable V1 Hegel lease/head state machine")
  (let [result
        (h/run-test!
         {:name "chdb/durable-control-lease-head"
          :database ""
          :derandomize? true
          :verbosity :quiet
          :test-cases 40
          :stateful-step-count 18}
         (fn [_]
           (hs/run!
            {:initial-state {:store (backend/memory-backend)
                             :current nil :stale [] :head nil :events []}
             :rules
             [(hs/rule :acquire-writer-1
                       (acquire-step :writer-1 "writer-1" "instance-1"))
              (hs/rule :acquire-writer-2
                       (acquire-step :writer-2 "writer-2" "instance-2"))
              (hs/rule :commit-current
                       {:precondition #(some? (:current %))}
                       commit-step)
              (hs/rule :try-stale-writer
                       {:precondition #(seq (:stale %))}
                       stale-step)]
             :invariants
             [(hs/invariant :head-and-journal-match state-valid?)]})))]
    (println "  hegel lease/head seed" (:seed result)
             "valid" (:valid-test-cases result))
    (when-not (and (:passed? result) (not (:flaky? result)))
      (swap! failures inc)
      (println "  FAIL lease/head property" (pr-str result)))))

(defn run-checks! []
  (reset! failures 0)
  (run-deterministic-checks!)
  (run-stateful-property!)
  (when-not (zero? @failures)
    (throw (ex-info (str @failures " Durable control checks failed")
                    {:failures @failures})))
  (println "all Durable control checks passed")
  true)

(defn -main [& _]
  (run-checks!))
