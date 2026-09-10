(ns jdbc.chdb-durable-control-test
  (:require [hegel.core :as h]
            [hegel.stateful :as hs]
            [hegel.trace :as ht]
            [jdbc.chdb.durable.backend :as backend]
            [jdbc.chdb.durable.control :as control]
            [jolt.fibers :as fibers]))

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

(defn- stored-head-bytes [store]
  (vec (backend/get-bytes store control/head-key)))

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

(defn- delayed-ambiguous-head-backend [delegate hidden-reads replace-count]
  (let [remaining (atom 0)]
    (reify backend/ObjectBackend
      (get-bytes [_ key] (backend/get-bytes delegate key))
      (get-with-etag [_ key]
        (if (and (= control/head-key key) (pos? @remaining))
          (do (swap! remaining dec) nil)
          (backend/get-with-etag delegate key)))
      (put-file-if-absent! [_ key path]
        (backend/put-file-if-absent! delegate key path))
      (put-bytes-if-absent! [_ key bytes]
        (backend/put-bytes-if-absent! delegate key bytes))
      (replace-if-match! [_ key bytes etag]
        (swap! replace-count inc)
        (let [result (backend/replace-if-match! delegate key bytes etag)]
          (if (= :replaced (:status result))
            (do (reset! remaining hidden-reads) {:status :ambiguous})
            result)))
      (download-to-file! [_ key path]
        (backend/download-to-file! delegate key path)))))

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

(defn- ambiguous-publication-backend [delegate mode]
  (reify backend/ObjectBackend
    (get-bytes [_ key] (backend/get-bytes delegate key))
    (get-with-etag [_ key] (backend/get-with-etag delegate key))
    (put-file-if-absent! [_ key path]
      (if (= control/head-key key)
        (backend/put-file-if-absent! delegate key path)
        (case mode
          :land (do (backend/put-file-if-absent! delegate key path)
                    {:status :ambiguous})
          :conflict (do (backend/put-bytes-if-absent!
                         delegate key (.getBytes "different" "UTF-8"))
                        {:status :ambiguous})
          :drop {:status :ambiguous})))
    (put-bytes-if-absent! [_ key bytes]
      (if (= control/head-key key)
        (backend/put-bytes-if-absent! delegate key bytes)
        (case mode
          :land (do (backend/put-bytes-if-absent! delegate key bytes)
                    {:status :ambiguous})
          :conflict (do (backend/put-bytes-if-absent!
                         delegate key (.getBytes "different" "UTF-8"))
                        {:status :ambiguous})
          :drop {:status :ambiguous})))
    (replace-if-match! [_ key bytes etag]
      (backend/replace-if-match! delegate key bytes etag))
    (download-to-file! [_ key path]
      (backend/download-to-file! delegate key path))))

(defn- delayed-ambiguous-publication-backend [delegate put-count]
  (let [hidden-key (atom nil)
        hidden-reads (atom 0)]
    (reify backend/ObjectBackend
      (get-bytes [_ key]
        (if (and (= key @hidden-key) (pos? @hidden-reads))
          (do (swap! hidden-reads dec) nil)
          (backend/get-bytes delegate key)))
      (get-with-etag [_ key] (backend/get-with-etag delegate key))
      (put-file-if-absent! [_ key path]
        (backend/put-file-if-absent! delegate key path))
      (put-bytes-if-absent! [_ key bytes]
        (swap! put-count inc)
        (backend/put-bytes-if-absent! delegate key bytes)
        (reset! hidden-key key)
        (reset! hidden-reads 1)
        {:status :ambiguous})
      (replace-if-match! [_ key bytes etag]
        (backend/replace-if-match! delegate key bytes etag))
      (download-to-file! [_ key path]
        (backend/download-to-file! delegate key path)))))

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

(defn- renew-before-first-replace-backend
  [delegate token renewed-expiry replace-count]
  (reify backend/ObjectBackend
    (get-bytes [_ key] (backend/get-bytes delegate key))
    (get-with-etag [_ key] (backend/get-with-etag delegate key))
    (put-file-if-absent! [_ key path]
      (backend/put-file-if-absent! delegate key path))
    (put-bytes-if-absent! [_ key bytes]
      (backend/put-bytes-if-absent! delegate key bytes))
    (replace-if-match! [_ key bytes etag]
      (let [attempt (swap! replace-count inc)]
        (when (= 1 attempt)
          ;; The heartbeat wins after commit read but before its head CAS.
          (control/renew! delegate token renewed-expiry))
        (backend/replace-if-match! delegate key bytes etag)))
    (download-to-file! [_ key path]
      (backend/download-to-file! delegate key path))))

(defn- commit-before-first-renew-replace-backend
  [delegate token reference replace-count]
  (reify backend/ObjectBackend
    (get-bytes [_ key] (backend/get-bytes delegate key))
    (get-with-etag [_ key] (backend/get-with-etag delegate key))
    (put-file-if-absent! [_ key path]
      (backend/put-file-if-absent! delegate key path))
    (put-bytes-if-absent! [_ key bytes]
      (backend/put-bytes-if-absent! delegate key bytes))
    (replace-if-match! [_ key bytes etag]
      (let [attempt (swap! replace-count inc)]
        (when (= 1 attempt)
          ;; The manifest wins after renewal read but before its lease CAS.
          (control/commit-reference!
           delegate token
           {:kind :wal :reference reference
            :verify-reference! control/verify-byte-reference!}))
        (backend/replace-if-match! delegate key bytes etag)))
    (download-to-file! [_ key path]
      (backend/download-to-file! delegate key path))))

(defn- landed-ambiguous-renewal-backend
  [delegate landed release-response]
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
          (deliver landed true)
          @release-response)
        (if (= :replaced (:status result)) {:status :ambiguous} result)))
    (download-to-file! [_ key path]
      (backend/download-to-file! delegate key path))))

(defn- dropped-ambiguous-renewal-before-manifest-backend
  [delegate renewal-attempted release-response]
  (reify backend/ObjectBackend
    (get-bytes [_ key] (backend/get-bytes delegate key))
    (get-with-etag [_ key] (backend/get-with-etag delegate key))
    (put-file-if-absent! [_ key path]
      (backend/put-file-if-absent! delegate key path))
    (put-bytes-if-absent! [_ key bytes]
      (backend/put-bytes-if-absent! delegate key bytes))
    (replace-if-match! [_ _ _ _]
      ;; The renewal request is known not to land, but its caller cannot use
      ;; that hidden backend fact. Let a manifest commit advance before the
      ;; required reconciliation read.
      (deliver renewal-attempted true)
      @release-response
      {:status :ambiguous})
    (download-to-file! [_ key path]
      (backend/download-to-file! delegate key path))))

(defn- renew-before-every-replace-backend
  [delegate token conflicting-expiry replace-count]
  (reify backend/ObjectBackend
    (get-bytes [_ key] (backend/get-bytes delegate key))
    (get-with-etag [_ key] (backend/get-with-etag delegate key))
    (put-file-if-absent! [_ key path]
      (backend/put-file-if-absent! delegate key path))
    (put-bytes-if-absent! [_ key bytes]
      (backend/put-bytes-if-absent! delegate key bytes))
    (replace-if-match! [_ key bytes etag]
      (swap! replace-count inc)
      (control/renew! delegate token (swap! conflicting-expiry inc))
      (backend/replace-if-match! delegate key bytes etag))
    (download-to-file! [_ key path]
      (backend/download-to-file! delegate key path))))

(defn- run-deterministic-checks! []
  (println "Durable V1 lease and head-CAS control")
  (letfn [(store-at-boundary []
            (let [store (backend/memory-backend)
                  acquired (control/acquire! store base-options)]
              (control/renew! store (:token acquired) 250M)
              store))]
    (doseq [[label now] [["below" 254.999M] ["at" 255M]]]
      (let [store (store-at-boundary)
            before (stored-head-bytes store)]
        (check (str "normal takeover is rejected " label " the inclusive boundary")
               ::control/lease-held
               (error-type
                #(control/acquire!
                  store (assoc base-options
                               :owner "writer-2" :instance "instance-2"
                               :now now :expires-at 400M))))
        (check (str "rejected " label "-boundary takeover preserves head bytes")
               before
               (stored-head-bytes store))))
    (let [store (store-at-boundary)
          takeover (control/acquire!
                    store (assoc base-options
                                 :owner "writer-2" :instance "instance-2"
                                 :now 255.001M :expires-at 400M))]
      (check "normal takeover succeeds one millisecond above the boundary"
             2
             (get-in (:head takeover) ["lease" "generation"]))
      (check "expired normal takeover emits no forced-live warning"
             [] (:warnings takeover))))
  (let [store (backend/memory-backend)
        _ (control/acquire! store base-options)
        forced (control/acquire!
                store (assoc base-options
                             :owner "writer-2" :instance "instance-2"
                             :now 101M :expires-at 300M :force? true))]
    (check "successful forced live takeover returns one redacted warning"
           [{:event control/forced-live-takeover-event
             :severity :warning
             :protocol-version 1
             :lease-generation 2}]
           (:warnings forced)))
  (let [delegate (backend/memory-backend)
        store (forwarding-backend delegate :land-ambiguous)
        _ (control/acquire! store base-options)
        forced (control/acquire!
                store (assoc base-options
                             :owner "writer-2" :instance "instance-2"
                             :now 101M :expires-at 300M :force? true))]
    (check "ambiguous landed forced takeover reconciles one warning"
           [:reconciled
           [{:event control/forced-live-takeover-event
              :severity :warning
              :protocol-version 1
              :lease-generation 2}]]
           [(:status forced) (:warnings forced)]))
  (let [delegate (backend/memory-backend)
        _ (control/acquire! delegate base-options)
        store (forwarding-backend delegate :drop-ambiguous)]
    (check "dropped ambiguous forced takeover returns no false warning"
           ::control/commit-ambiguous
           (error-type
            #(control/acquire!
              store (assoc base-options
                           :owner "writer-2" :instance "instance-2"
                           :now 101M :expires-at 300M :force? true
                           :max-attempts 1)))))
  (let [store (backend/memory-backend)
        acquired (control/acquire! store base-options)
        _ (control/release! store (:token acquired))
        forced (control/acquire!
                store (assoc base-options
                             :owner "writer-2" :instance "instance-2"
                             :now 101M :expires-at 300M :force? true))]
    (check "forced acquisition of a released lease emits no live warning"
           [] (:warnings forced)))
  (let [store (backend/memory-backend)
        first-acquire (control/acquire! store base-options)
        token1 (:token first-acquire)]
    (check "fresh acquisition starts at generation one"
           1 (:generation token1))
    (check "fresh acquisition retains an empty manifest"
           0 (get-in (:head first-acquire) ["manifest" "seq"]))
    (check "fresh result is the canonical stored JSON value"
           (:head first-acquire) (:head (control/read-head! store)))
    (check "fresh acquisition emits no takeover warning"
           [] (:warnings first-acquire))
    (check "a live competing writer is rejected"
           ::control/lease-held
           (error-type
            #(control/acquire!
              store (assoc base-options
                           :owner "writer-2" :instance "instance-2"))))
    (check "omitted acquisition defaults reject the same live competitor"
           ::control/lease-held
           (error-type
            #(control/acquire!
              store (-> base-options
                        (dissoc :clock-skew)
                        (assoc :owner "writer-2" :instance "instance-2")))))
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
        token (:token (control/acquire! delegate base-options))
        reference (publish-abc! delegate token)
        replace-count (atom 0)
        waits (atom [])
        now (atom 0)
        store (delayed-ambiguous-head-backend delegate 1 replace-count)
        committed
        (control/commit-reference!
         store token
         {:kind :wal :reference reference
          :verify-reference! control/verify-byte-reference!
          :monotonic-ms! (fn [] @now)
          :await-backoff! (fn [milliseconds]
                            (swap! waits conj milliseconds)
                            (swap! now + milliseconds))})]
    (check "ambiguous commit tolerates a not-yet-visible proof read"
           [:reconciled 1 [10 20]]
           [(:status committed) @replace-count @waits])
    (check "delayed reconciliation advances the manifest exactly once"
           [1 [reference]]
           [(get-in (:head committed) ["manifest" "seq"])
            (get-in (:head committed) ["manifest" "wal"])]))

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
        replace-count (atom 0)
        store (commit-before-first-renew-replace-backend
               delegate token reference replace-count)
        renewed (control/renew! store token 300M)]
    (check "definite manifest CAS conflict retries lease renewal"
           [:committed 2]
           [(:status renewed) @replace-count])
    (check "retried renewal preserves the manifest and requested expiry"
           [300 1 [reference]]
           [(get-in (:head renewed) ["lease" "expires_at"])
            (get-in (:head renewed) ["manifest" "seq"])
            (get-in (:head renewed) ["manifest" "wal"])]))

  (let [delegate (backend/memory-backend)
        acquired (control/acquire! delegate base-options)
        token (:token acquired)
        reference (publish-abc! delegate token)
        renewal-landed (promise)
        release-response (promise)
        store (landed-ambiguous-renewal-backend
               delegate renewal-landed release-response)
        renewing (fibers/spawn #(control/renew! store token 300M))]
    @renewal-landed
    (let [committed
          (control/commit-reference!
           delegate token
           {:kind :wal :reference reference
            :verify-reference! control/verify-byte-reference!})]
      (deliver release-response true)
      (let [renewed (fibers/join renewing)]
        (check "ambiguous landed renewal survives a later manifest commit"
               :reconciled (:status renewed))
        (check "semantic renewal reconciliation preserves both transitions"
               [300 1 [reference]]
               [(get-in (:head renewed) ["lease" "expires_at"])
                (get-in (:head committed) ["manifest" "seq"])
                (get-in (:head committed) ["manifest" "wal"])]))))

  (let [delegate (backend/memory-backend)
        acquired (control/acquire! delegate base-options)
        token (:token acquired)
        reference (publish-abc! delegate token)
        renewal-attempted (promise)
        release-response (promise)
        store (dropped-ambiguous-renewal-before-manifest-backend
               delegate renewal-attempted release-response)
        renewing
        (fibers/spawn
         #(error-type (fn [] (control/renew! store token 300M))))]
    @renewal-attempted
    (control/commit-reference!
     delegate token
     {:kind :wal :reference reference
      :verify-reference! control/verify-byte-reference!})
    (deliver release-response true)
    (check "dropped ambiguous renewal is not proved by manifest advance"
           ::control/commit-ambiguous (fibers/join renewing))
    (check "failed renewal reconciliation preserves the manifest only"
           [200 1 [reference]]
           (let [latest (:head (control/read-head! delegate))]
             [(get-in latest ["lease" "expires_at"])
              (get-in latest ["manifest" "seq"])
              (get-in latest ["manifest" "wal"])])))

  (let [delegate (backend/memory-backend)
        acquired (control/acquire! delegate base-options)
        token (:token acquired)
        conflicting-expiry (atom 200M)
        replace-count (atom 0)
        now (atom 0)
        waits (atom [])
        store (renew-before-every-replace-backend
               delegate token conflicting-expiry replace-count)]
    (check "renewal reports bounded exhaustion under repeated CAS loss"
           ::control/timeout
           (error-type
            #(control/renew!
              store token 300M
              {:monotonic-ms! (fn [] @now)
               :await-backoff! (fn [milliseconds]
                                 (swap! waits conj milliseconds)
                                 (swap! now + milliseconds))})))
    (check "renewal uses capped retry backoff before timing out"
           [10 20 40] @waits)
    (check "renewal exhaustion preserves ownership and manifest"
           [4 204 0]
           (let [latest (:head (control/read-head! delegate))]
             [@replace-count
              (get-in latest ["lease" "expires_at"])
              (get-in latest ["manifest" "seq"])])))

  (let [delegate (backend/memory-backend)
        acquired (control/acquire! delegate base-options)
        token (:token acquired)
        reference (publish-abc! delegate token)
        replace-count (atom 0)
        verify-count (atom 0)
        store (renew-before-first-replace-backend
               delegate token 300M replace-count)
        committed
        (control/commit-reference!
         store token
         {:kind :wal :reference reference
          :verify-reference!
          (fn [store reference]
            (swap! verify-count inc)
            (control/verify-byte-reference! store reference))})]
    (check "definite heartbeat CAS conflict retries the reference commit"
           [:committed 2 1]
           [(:status committed) @replace-count @verify-count])
    (check "retried commit preserves renewal and advances the manifest once"
           [300 1 [reference]]
           [(get-in (:head committed) ["lease" "expires_at"])
            (get-in (:head committed) ["manifest" "seq"])
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
           (error-type #(control/publish-wal-bytes! store token "abc"))))

  (let [delegate (backend/memory-backend)
        token (:token (control/acquire! delegate base-options))
        put-count (atom 0)
        waits (atom [])
        now (atom 0)
        store (delayed-ambiguous-publication-backend delegate put-count)
        publication
        (control/publish-wal-bytes!
         store token (.getBytes "abc" "UTF-8")
         {:monotonic-ms! (fn [] @now)
          :await-backoff! (fn [milliseconds]
                            (swap! waits conj milliseconds)
                            (swap! now + milliseconds))})]
    (check "ambiguous publication retries only its delayed proof read"
           [:reconciled 1 [10]]
           [(:status publication) @put-count @waits]))

  (doseq [[label mode expected]
          [["landed ambiguous WAL publication reconciles" :land :reconciled]
           ["conflicting ambiguous WAL publication is rejected"
            :conflict ::control/object-unverified]
           ["dropped ambiguous WAL publication stays unprovable"
            :drop ::control/commit-ambiguous]]]
    (let [delegate (backend/memory-backend)
          store (ambiguous-publication-backend delegate mode)
          token (:token (control/acquire! store base-options))]
      (check label expected
             (if (#{:land} mode)
               (:status (control/publish-wal-bytes!
                         store token (.getBytes "abc" "UTF-8")))
               (error-type #(control/publish-wal-bytes!
                             store token (.getBytes "abc" "UTF-8")))))))

  (let [checkpoint (java.nio.file.Files/createTempFile
                    "jolt-chdb-ambiguous-" ".tar.gz"
                    (make-array java.nio.file.attribute.FileAttribute 0))]
    (try
      (java.nio.file.Files/write checkpoint (.getBytes "abc" "UTF-8")
                                 (make-array java.nio.file.OpenOption 0))
      (doseq [[label mode expected]
              [["landed ambiguous checkpoint publication reconciles"
                :land :reconciled]
               ["conflicting ambiguous checkpoint publication is rejected"
                :conflict ::control/object-unverified]
               ["dropped ambiguous checkpoint publication stays unprovable"
                :drop ::control/commit-ambiguous]]]
        (let [delegate (backend/memory-backend)
              store (ambiguous-publication-backend delegate mode)
              token (:token (control/acquire! store base-options))]
          (check label expected
                 (if (#{:land} mode)
                   (:status (control/publish-checkpoint-file!
                             store token checkpoint))
                   (error-type #(control/publish-checkpoint-file!
                                 store token checkpoint))))))
      (finally
        (java.nio.file.Files/deleteIfExists checkpoint)))))

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
          document (:head result)
          generation (get-in document ["lease" "generation"])
          sequence (get-in document ["manifest" "seq"])
          expected-warnings
          (if previous
            [{:event control/forced-live-takeover-event
              :severity :warning
              :protocol-version 1
              :lease-generation generation}]
            [])]
      (when-not (= expected-warnings (:warnings result))
        (throw (ex-info "forced takeover warning contract was suppressed"
                        {:hegel/origin
                         "chdb/durable-control/forced-takeover-warning"})))
      (-> state
          (assoc :current (:token result) :head document)
          (update :stale #(cond-> % previous (conj previous)))
          (cond-> previous
            (append-event
             {:kind :forced-warning
              :generation generation
              :sequence sequence}))
          (append-event
           {:kind :acquire
            :writer writer
            :generation generation
            :sequence sequence})))))

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
