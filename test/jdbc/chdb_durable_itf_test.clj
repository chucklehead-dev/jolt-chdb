(ns jdbc.chdb-durable-itf-test
  (:require [clojure.data.json :as json]
            [clojure.string :as str]
            [hegel.event-contract :as event-contract]
            [hegel.trace :as ht]
            [jdbc.chdb.durable.backend :as backend]
            [jdbc.chdb.durable.control :as control]))

(def fixture-path "formal/quint/traces/corrected-mbt.itf.json")

(def ^:private payloads
  {:object-1 {:bytes (.getBytes "abc" "UTF-8")
              :digest "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"}
   :object-2 {:bytes (.getBytes "xyz" "UTF-8")
              :digest "3608bca1e44ea6c4d268eb6db02260269892c0b42b86bbf1e77a6fa16c3c9282"}})

(def ^:private base-options
  {:expires-at 1000M
   :now 0M
   :clock-skew 0M
   :database "default"
   :engine-version "26.7.2-rc.2"
   :backup-format 1
   :min-reader "26.7.2-rc.2"})

(defn- fail! [message data]
  (throw (ex-info message
                  (assoc data :hegel/origin "chdb/durable-itf/replay"))))

(defn- read-trace [path]
  (json/read-str (slurp path)))

(defn- one-model-var [trace]
  (let [names (filter #(str/ends-with? % "::state") (get trace "vars"))]
    (when-not (= 1 (count names))
      (fail! "ITF trace must contain exactly one Durable model state"
             {:model-vars (vec names)}))
    (first names)))

(defn- itf-int [value]
  (let [text (and (map? value) (get value "#bigint"))]
    (when-not (and (string? text) (re-matches #"-?[0-9]+" text))
      (fail! "unsupported ITF integer" {:value value}))
    (parse-long text)))

(defn- variant-tag [value]
  (let [tag (and (map? value) (get value "tag"))]
    (when-not (string? tag)
      (fail! "unsupported ITF variant" {:value value}))
    tag))

(defn- writer-id [tag]
  (case tag
    "Writer1" :writer-1
    "Writer2" :writer-2
    (fail! "unknown writer variant" {:tag tag})))

(defn- object-id [tag]
  (case tag
    "Object1" :object-1
    "Object2" :object-2
    (fail! "unknown object variant" {:tag tag})))

(defn- attempt-id [tag]
  (case tag
    "Attempt1" :attempt-1
    "Attempt2" :attempt-2
    "Attempt3" :attempt-3
    "Attempt4" :attempt-4
    "Attempt5" :attempt-5
    "Attempt6" :attempt-6
    (fail! "unknown publication attempt variant" {:tag tag})))

(defn- some-pick [state name]
  (let [pick (get-in state ["mbt::nondetPicks" name])]
    (when-not (= "Some" (variant-tag pick))
      (fail! "missing MBT nondeterministic pick"
             {:name name :pick pick}))
    (get pick "value")))

(defn- command [state]
  (let [index (get-in state ["#meta" "index"])
        action (get state "mbt::actionTaken")]
    (case action
      "chooseAcquire"
      {:index index :op :acquire
       :writer (writer-id (variant-tag (some-pick state "writer")))}

      "choosePublish"
      {:index index :op :publish
       :writer (writer-id (variant-tag (some-pick state "writer")))
       :object (object-id (variant-tag (some-pick state "objectId")))
       :attempt (attempt-id (variant-tag (some-pick state "attemptId")))}

      "chooseCommit"
      {:index index :op :commit-attempt
       :writer (writer-id (variant-tag (some-pick state "writer")))
       :object (object-id (variant-tag (some-pick state "objectId")))
       :attempt (attempt-id (variant-tag (some-pick state "attemptId")))
       :mode (case (variant-tag (some-pick state "mode"))
               "Confirmed" :confirmed
               "AmbiguousLanded" :ambiguous-landed
               "AmbiguousDropped" :ambiguous-dropped
               (fail! "unknown commit mode" {:state-index index}))}

      "chooseRelease"
      {:index index :op :release-attempt
       :writer (writer-id (variant-tag (some-pick state "writer")))}

      (fail! "unknown MBT action" {:action action :state-index index}))))

(defn- model-reference [value]
  {:object (object-id (variant-tag (get value "objectId")))
   :attempt (attempt-id (variant-tag (get value "attemptId")))
   :generation (itf-int (get value "generation"))
   :sequence (itf-int (get value "sequence"))})

(defn- itf-set [value decode]
  (let [items (and (map? value) (get value "#set"))]
    (when-not (vector? items)
      (fail! "unsupported ITF set" {:value value}))
    (set (map decode items))))

(defn- model-used-attempts [model-state]
  (itf-set (get model-state "usedAttempts")
           #(attempt-id (variant-tag %))))

(defn- model-published [model-state]
  (itf-set (get model-state "published") model-reference))

(defn- optional-model-reference [value]
  (case (variant-tag value)
    "NoReference" nil
    "SomeReference" (model-reference (get value "value"))
    (fail! "unknown optional reference variant" {:value value})))

(defn- model-owner [value]
  (case (variant-tag value)
    "Released" nil
    "HeldBy" (writer-id (variant-tag (get value "value")))
    (fail! "unknown lease owner variant" {:value value})))

(defn- model-head [model-state]
  (let [head (get model-state "head")]
    {:generation (itf-int (get head "generation"))
     :owner (model-owner (get head "owner"))
     :sequence (itf-int (get head "sequence"))
     :reference (optional-model-reference (get head "reference"))}))

(defn- last-model-event [model-state]
  (or (peek (get model-state "events"))
      (fail! "noninitial ITF state has no model event" {})))

(defn- event-reference [event]
  (let [tag (variant-tag event)
        value (get event "value")]
    (when (contains? #{"Published" "CommitAttempted"} tag)
      (model-reference (get value "reference")))))

(defn- expected-outcome [event]
  (let [tag (variant-tag event)
        value (get event "value")]
    (case tag
      "Acquired" :acquired
      "Published" :published
      "ReleaseAttempted" (if (get value "accepted") :released :lease-fenced)
      "CommitAttempted"
      (case (variant-tag (get value "result"))
        "Committed" :committed
        "Reconciled" :reconciled
        "LeaseFenced" :lease-fenced
        "ObjectUnverified" :object-unverified
        "CommitAmbiguous" :commit-ambiguous
        (fail! "unknown commit result" {:event event}))
      (fail! "unknown model event" {:event event}))))

(defn- physical-reference
  [{:keys [object attempt generation sequence] :as reference}]
  (let [payload (get payloads object)]
    (when-not payload
      (fail! "unknown payload object" {:reference reference}))
    (let [{:keys [bytes digest]} payload
          attempt-token (case attempt
                          :attempt-1 "11111111"
                          :attempt-2 "22222222"
                          :attempt-3 "33333333"
                          :attempt-4 "44444444"
                          :attempt-5 "55555555"
                          :attempt-6 "66666666")]
      {"key" (str "wal/" generation "-" sequence "-"
                  attempt-token ".jsonl")
       "size" (alength bytes)
       "sha256" digest})))

(defn- physical-matches-model?
  [physical {:keys [object generation sequence]}]
  (let [{:keys [bytes digest]} (get payloads object)]
    (and (= (alength bytes) (get physical "size"))
         (= digest (get physical "sha256"))
         (boolean
          (re-matches
           (re-pattern
            (str "wal/" generation "-" sequence "-[0-9a-f]{8}\\.jsonl"))
           (get physical "key"))))))

(defn- forwarding-backend [delegate mode]
  (reify backend/ObjectBackend
    (get-bytes [_ key] (backend/get-bytes delegate key))
    (get-with-etag [_ key] (backend/get-with-etag delegate key))
    (put-file-if-absent! [_ key path]
      (backend/put-file-if-absent! delegate key path))
    (put-bytes-if-absent! [_ key bytes]
      (backend/put-bytes-if-absent! delegate key bytes))
    (replace-if-match! [_ key bytes etag]
      (case mode
        :ambiguous-landed
        (let [result (backend/replace-if-match! delegate key bytes etag)]
          (if (= :replaced (:status result)) {:status :ambiguous} result))
        :ambiguous-dropped {:status :ambiguous}
        (backend/replace-if-match! delegate key bytes etag)))
    (download-to-file! [_ key path]
      (backend/download-to-file! delegate key path))))

(defn- exception-outcome [error]
  (case (:type (ex-data error))
    ::control/lease-fenced :lease-fenced
    ::control/object-unverified :object-unverified
    ::control/commit-ambiguous :commit-ambiguous
    (throw error)))

(defn- call-outcome [f success]
  (try
    {:value (f) :outcome success :thrown? false}
    (catch Throwable error
      {:outcome (exception-outcome error) :thrown? true})))

(defn- actual-head [state]
  (let [document (:head (control/read-head! (:store state)))
        lease (get document "lease")
        manifest (get document "manifest")
        physical (or (peek (get manifest "wal")) (get manifest "base"))
        reference (when physical
                    (or (get (:physical->model state) physical)
                        (fail! "head names an unknown physical reference"
                               {:physical physical})))]
    {:generation (get lease "generation")
     :owner (some-> (get lease "owner") keyword)
     :sequence (get manifest "seq")
     :reference reference}))

(defn- model-observation [model-state]
  {:head (model-head model-state)
   :published (model-published model-state)
   :used-attempts (model-used-attempts model-state)})

(defn- actual-observation [state]
  {:head (actual-head state)
   :published (set (vals (:physical->model state)))
   :used-attempts (set (keys (:attempt->physical state)))})

(defn- acquire-options [writer index]
  (assoc base-options
         :owner (name writer)
         :instance (str (name writer) "-instance")
         :now (bigdec index)
         :expires-at (bigdec (+ 1000 index))
         :force? true))

(defn- execute-command [state command reference]
  (let [{:keys [op writer object attempt mode index]} command]
    (case op
      :acquire
      (let [result (call-outcome
                    #(control/acquire! (:store state)
                                       (acquire-options writer index))
                    :acquired)]
        [(if-let [token (some-> result :value :token)]
           (assoc-in state [:tokens writer] token)
           state)
         result])

      :publish
      (let [_ (when (contains? (:attempt->physical state) attempt)
                (fail! "model publication attempt was reused"
                       {:attempt attempt :command command}))
            bytes (get-in payloads [object :bytes])
            result (call-outcome
                    #(control/publish-wal-bytes!
                      (:store state) (get-in state [:tokens writer]) bytes)
                    :published)
            physical (get-in result [:value :reference])]
        (when (and (not (:thrown? result))
                   (not (physical-matches-model? physical reference)))
          (fail! "writer-aware publication differs from the Quint reference"
                 {:model-reference reference}))
        (when (and (not (:thrown? result))
                   (contains? (:physical->attempt state) physical))
          (fail! "distinct model attempts produced one physical reference"
                 {:attempt attempt :physical physical}))
        [(if (:thrown? result)
           state
           (-> state
               (assoc-in [:physical->model physical] reference)
               (assoc-in [:model->physical reference] physical)
               (assoc-in [:attempt->physical attempt] physical)
               (assoc-in [:physical->attempt physical] attempt)))
         result])

      :commit-attempt
      (let [physical (or (get-in state [:model->physical reference])
                         (physical-reference reference))
            selected-store (forwarding-backend (:store state) mode)
            result (call-outcome
                    #(control/commit-reference!
                      selected-store (get-in state [:tokens writer])
                      {:kind :wal
                       :reference physical
                       :verify-reference! control/verify-byte-reference!})
                    (if (= mode :ambiguous-landed) :reconciled :committed))]
        [state result])

      :release-attempt
      [state (call-outcome
              #(control/release! (:store state)
                                 (get-in state [:tokens writer]))
              :released)])))

(def ^:private journal-model
  (ht/event-model
   :durable-itf/semantic-journal
   {:initial {:generation 1 :sequence 0 :valid? true}
    :step
    (fn [state event]
      (if (contains? #{:return :throw} (:phase event))
        (let [head (get-in event [:value :head])]
          {:generation (:generation head)
           :sequence (:sequence head)
           :valid?
           (and (:valid? state)
                (contains? #{:acquired :published :committed :reconciled
                             :released :lease-fenced :object-unverified
                             :commit-ambiguous}
                           (get-in event [:value :outcome]))
                (>= (:generation head) (:generation state))
                (>= (:sequence head) (:sequence state)))})
        state))
    :invariant (fn [state _] (:valid? state))}))

(defn- validate-journal! [events]
  (event-contract/check-envelope!
   {:contract-id event-contract/contract-id
    :contract-revision event-contract/contract-revision
    :events events}
   {:max-events 64 :sequence-start 1})
  (ht/check!
   events
   [(ht/contiguous-sequence :durable-itf/contiguous 1)
    (ht/closed-lifecycles :durable-itf/closed-lifecycles)
    journal-model]
   {:max-events 64}))

(defn- append-lifecycle [events command result head]
  (let [operation-id (:index command)
        invoke-seq (inc (count events))
        terminal-seq (inc invoke-seq)
        operation (keyword "durable" (name (:op command)))
        input (dissoc command :index)]
    (conj events
          {:seq invoke-seq
           :operation-id operation-id
           :parent-operation-id nil
           :context-id "durable-itf-replay"
           :causal-links []
           :phase :invoke
           :operation operation
           :input input}
          {:seq terminal-seq
           :operation-id operation-id
           :phase (if (:thrown? result) :throw :return)
           :value {:outcome (:outcome result) :head head}})))

(defn- seed-state []
  (let [store (backend/memory-backend)
        acquired (control/acquire!
                  store
                  (assoc base-options
                         :owner "seed-writer"
                         :instance "seed-instance"))]
    (control/release! store (:token acquired))
    {:store store :tokens {} :physical->model {} :model->physical {}
     :attempt->physical {} :physical->attempt {}
     :events []}))

(defn- replay-trace! [trace]
  (let [model-var (one-model-var trace)
        states (get trace "states")
        initial-model (get (first states) model-var)
        initial-state (seed-state)]
    (when (get trace "loop")
      (fail! "lasso ITF traces are not replayable by this finite driver"
             {:loop (get trace "loop")}))
    (when-not (= (model-observation initial-model)
                 (actual-observation initial-state))
      (fail! "implementation seed does not match the model initial state"
             {:expected (model-observation initial-model)
              :actual (actual-observation initial-state)}))
    (let [final
          (reduce
           (fn [state itf-state]
             (let [model-state (get itf-state model-var)
                   event (last-model-event model-state)
                   command (command itf-state)
                   reference (event-reference event)
                   expected-observation (model-observation model-state)
                   expected-result (expected-outcome event)
                   [next-state result]
                   (execute-command state command reference)
                   actual-observation (actual-observation next-state)
                   actual-head (:head actual-observation)
                   events (append-lifecycle
                           (:events state) command result actual-head)
                   next-state (assoc next-state :events events)]
               (when-not (= expected-result (:outcome result))
                 (fail! "implementation outcome differs from the Quint state"
                        {:index (:index command)
                         :command command
                         :expected expected-result
                         :actual (:outcome result)}))
               (when-not (= expected-observation actual-observation)
                 (fail! "implementation state differs from the Quint state"
                        {:index (:index command)
                         :command command
                         :expected expected-observation
                         :actual actual-observation}))
               next-state))
           initial-state
           (rest states))]
      (validate-journal! (:events final))
      {:steps (dec (count states))
       :events (count (:events final))
       :journal (:events final)
       :head (actual-head final)})))

(defn replay! [path]
  (replay-trace! (read-trace path)))

(defn- rejected? [f]
  (try (f) false (catch Throwable _ true)))

(defn run-checks! []
  (println "Durable Quint ITF implementation replay")
  (let [trace (read-trace fixture-path)
        model-var (one-model-var trace)
        result (replay-trace! trace)
        publish-state-indexes
        (vec
         (keep-indexed
          (fn [index state]
            (when (= "choosePublish" (get state "mbt::actionTaken")) index))
          (get trace "states")))]
    (println "  ok  " (:steps result) "model steps and"
             (:events result) "canonical operation events")
    (when-not (rejected?
               #(replay-trace!
                 (assoc-in trace ["states" 1 "mbt::actionTaken"]
                           "UnknownAction")))
      (fail! "unknown MBT action mutation was accepted" {}))
    (println "  ok   unknown MBT action mutation rejected")
    (when-not (rejected?
               #(replay-trace!
                 (assoc-in trace
                           ["states" 1 model-var "head"
                            "generation" "#bigint"]
                           "99")))
      (fail! "model-state mutation was accepted" {}))
    (println "  ok   model-state mutation rejected")
    (when (< (count publish-state-indexes) 2)
      (fail! "checked fixture needs two publications for attempt reuse control"
             {:publish-state-indexes publish-state-indexes}))
    (let [[first-publish second-publish] publish-state-indexes
          first-attempt
          (get-in trace ["states" first-publish
                         "mbt::nondetPicks" "attemptId"])]
      (when-not
       (rejected?
        #(replay-trace!
          (assoc-in trace
                    ["states" second-publish
                     "mbt::nondetPicks" "attemptId"]
                    first-attempt)))
        (fail! "reused model publication attempt was accepted" {})))
    (println "  ok   reused publication attempt mutation rejected")
    (when-not (rejected? #(validate-journal! (pop (:journal result))))
      (fail! "incomplete aspect journal mutation was accepted" {}))
    (println "  ok   incomplete lifecycle mutation rejected")
    (when-not
     (rejected?
      #(validate-journal!
        (assoc (:journal result) 0
               (dissoc (first (:journal result)) :context-id))))
      (fail! "noncanonical invocation mutation was accepted" {}))
    (println "  ok   noncanonical invocation mutation rejected")
    true))

(defn -main [& args]
  (if (seq args)
    (do
      (doseq [path args]
        (replay! path))
      (println "Durable Quint ITF replay passed for" (count args) "trace(s)"))
    (run-checks!)))
