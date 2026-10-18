(ns s3-checksum-fault-lib
  "Small real-provider contract checks, not a throughput or WAL replay test."
  (:require [jdbc.chdb.durable.backend :as backend]
            [jdbc.chdb.durable.control :as control]
            [jdbc.chdb.durable.s3 :as s3]
            [jdbc.chdb.durable.s3-curl :as curl]
            [typed-storage-budget-lib :as budget]
            [typed-storage-confirmation-profile :as profile])
  (:import [java.nio.file Files OpenOption]
           [java.nio.file.attribute FileAttribute]))

(def cases [:new-upload :default-readback :existing-readback :injected-lost-ack :changed-source])
(def request-limit 128)
(def upload-limit 1048576)
(def check-labels
  #{:commit :stored-body :sequence :reference :seed :real-create :publication
    :publication-counts :completion-counts :publication-errors :readback-selection
    :native-proof :injection-reached :mutation-reached :provider400 :unchanged-sequence
    :unchanged-references :rejected-object-absent :release :released-head :settled})

(defn check! [value label]
  (when-not value
    (throw (ex-info "Checksum provider control failed"
                    {:type ::failed :check (when (contains? check-labels label) label)}))))

(defn- acquisition-options []
  (let [now (/ (bigdec (System/currentTimeMillis)) 1000M)]
    {:owner "checksum-provider-control" :instance "one" :now now
     :expires-at (+ now 600M) :clock-skew 0M :database "default"
     :engine-version "26.9.0" :backup-format 1 :min-reader "26.7.3"}))

(defn- reference-check! [store token publication]
  (check! (= :committed
             (:status (control/commit-reference! store token
                        {:kind :wal :reference (:reference publication)
                         :verified-publication publication
                         :verify-reference! control/verify-file-reference!}))) :commit)
  ;; An independent stored-body GET, not reuse of a response checksum.
  (check! (control/verify-file-reference! store (:reference publication)) :stored-body)
  (let [document (:head (control/read-head! store))]
    (check! (= 1 (get-in document ["manifest" "seq"])) :sequence)
    (check! (= [(:reference publication)] (get-in document ["manifest" "wal"])) :reference)))

(defn- publication-case! [store token path selected]
  (let [counts (atom (profile/initial-counts))
        original backend/put-file-with-checksum-if-absent!
        injected? (atom false)
        publication
        (profile/with-observers! counts
          (fn []
            (let [publish! #(control/publish-wal-file! store token path)
                  publication
                  (if (contains? #{:existing-readback :injected-lost-ack} selected)
                    (with-redefs [backend/put-file-with-checksum-if-absent!
                                  (fn [b key file n sha]
                                    (if (and (identical? b store) (compare-and-set! injected? false true))
                                      (case selected
                                        :existing-readback
                                        (do
                                          ;; Seed the exact selected key with a real conditional
                                          ;; S3 PUT, then require the checksum PUT's real412 path.
                                          (check! (= :created (:status (backend/put-file-if-absent! b key file))) :seed)
                                          (original b key file n sha))
                                        :injected-lost-ack
                                        (do
                                          (check! (= :created (:status (original b key file n sha))) :real-create)
                                          ;; Deliberately discard the actual successful result/proof.
                                          ;; This is client injection, not a real AWS timeout.
                                          {:status :ambiguous}))
                                      (original b key file n sha)))]
                      (publish!))
                    (publish!))]
              (reference-check! store token publication)
              publication)))
        expected-verifies (if (= selected :new-upload) 0 1)]
    (check! (= (case selected :existing-readback :already-published
                              :injected-lost-ack :reconciled :published)
               (:status publication)) :publication)
    (check! (= 1 (:file-publications @counts) (:put-completions @counts)) :publication-counts)
    (check! (= (if (= selected :existing-readback) 0 1)
               (:completed-publications @counts)) :completion-counts)
    (check! (zero? (:publication-errors @counts)) :publication-errors)
    (check! (= expected-verifies (:verification-completions @counts)) :readback-selection)
    (when (= selected :new-upload)
      (check! (pos? (:valid-proof-observations @counts)) :native-proof))
    (when (contains? #{:existing-readback :injected-lost-ack} selected)
      (check! @injected? :injection-reached))
    {:case selected :passed true :scope-returned false
     :publication-status (:status publication) :verification-completions (:verification-completions @counts)
     :valid-proof-observations (:valid-proof-observations @counts)
     :independent-body-verified true :head-sequence 1}))

(defn- changed-source! [store token path]
  (let [original backend/put-file-with-checksum-if-absent!
        changed? (atom false) selected-key (atom nil)
        error
        (with-redefs [backend/put-file-with-checksum-if-absent!
                      (fn [b key file n sha]
                        (when (and (identical? b store) (compare-and-set! changed? false true))
                          (reset! selected-key key)
                          (Files/write file (.getBytes "abd" "UTF-8") (make-array OpenOption 0)))
                        (original b key file n sha))]
          (try (control/publish-wal-file! store token path) nil (catch Throwable e e)))]
    (check! @changed? :mutation-reached)
    (check! (= [::s3/provider :put-file-with-checksum-if-absent 400]
               ((juxt :type :operation :status) (ex-data error))) :provider400)
    (let [document (:head (control/read-head! store))]
      (check! (= 0 (get-in document ["manifest" "seq"])) :unchanged-sequence)
      (check! (empty? (get-in document ["manifest" "wal"])) :unchanged-references))
    (check! (nil? (backend/get-bytes store @selected-key)) :rejected-object-absent)
    {:case :changed-source :passed true :scope-returned false
     :provider-status 400 :mutated-before-send true :head-sequence 0 :object-absent true}))

(defn run! [options root]
  (let [counts (atom {:requests 0 :completed 0 :upload-bytes 0})
        original curl/request!
        request! (budget/requester #(original (assoc % :max-response-bytes 65536))
                                   counts request-limit upload-limit #{:get :put})
        results
        ;; Meter below the existing authenticated native request-function seal.
        ;; Do not manufacture native provenance or return synthetic checksum proofs.
        (with-redefs [curl/request! request!]
          (curl/with-reused-transport!
            (fn [native-request]
              (mapv
                (fn [selected]
                  (let [store (backend/object-backend
                                (s3/s3-backend
                                  (cond-> (assoc options :request! native-request :max-attempts 1)
                                    (not= selected :default-readback)
                                    (assoc :wal-upload-confirmation :sha256-single-put)))
                                (name selected))
                        token (:token (control/acquire! store (acquisition-options)))
                        path (Files/createTempFile (.toPath (java.io.File. root)) "checksum-" ".bin"
                                                  (make-array FileAttribute 0))]
                    (Files/write path (.getBytes "abc" "UTF-8") (make-array OpenOption 0))
                    (let [result (if (= selected :changed-source)
                                   (changed-source! store token path)
                                   (publication-case! store token path selected))]
                      (check! (contains? #{:committed :reconciled}
                                        (:status (control/release! store token))) :release)
                      (let [lease (get-in (control/read-head! store) [:head "lease"])]
                        (check! (and (= (:generation token) (get lease "generation"))
                                     (every? nil? (map lease ["owner" "instance" "expires_at"])))
                                :released-head))
                      (assoc result :lease-released true)))) cases))))]
    (check! (= (:requests @counts) (:completed @counts)) :settled)
    {:scope :s3-checksum-provider-controls :scope-returned true
     :throughput-qualified false :native-replay-qualified false
     :cases (mapv #(assoc % :scope-returned true) results)
     :request-limit request-limit :upload-limit upload-limit :requests @counts}))
