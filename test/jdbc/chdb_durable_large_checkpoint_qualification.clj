(ns jdbc.chdb-durable-large-checkpoint-qualification
  "Isolated-process large-checkpoint RSS qualification worker."
  (:require [jdbc.chdb.durable :as durable]
            [jdbc.chdb.durable.backend :as backend]
            [jdbc.chdb.durable.control :as control]
            [jdbc.chdb.durable.digest :as digest]
            [jdbc.chdb.durable.reader :as reader]
            [jdbc.chdb.durable.s3 :as s3]
            [jdbc.chdb.durable.s3-curl :as s3-curl])
  (:import [java.io File]
           [java.nio.file Files LinkOption Path Paths]))

(def ^:private no-link-options (make-array LinkOption 0))
(def ^:private retained-control (atom nil))
(def ^:private zero-file-digests
  {(* 32 1024 1024)
   "83ee47245398adee79bd9c0a8bc57b821e92aba10f5f9ade8a5d1fae4d8c4302"
   (* 64 1024 1024)
   "3b6a07d0d404fab4e23b6d34bc6696a6a312dd92821332385e5af7c01c421351"
   (* 128 1024 1024)
   "254bcc3fc4f27172636df4bf32de9f107f620d559b20d760197e452b97453917"})

(defn- fail! [message]
  (throw (ex-info message {:type ::qualification-failed})))

(defn- path [value]
  (Paths/get value (make-array String 0)))

(defn- heap-snapshot []
  ;; bytes-allocated is live heap, not a cumulative allocation counter. Force a
  ;; full collection so start/end readings describe retained managed state.
  (System/gc)
  (let [runtime (Runtime/getRuntime)]
    {:live-bytes ((requiring-resolve 'jolt.host/bytes-allocated))
     :reserved-bytes (.totalMemory runtime)
     :gc-count ((requiring-resolve 'jolt.host/gc-count))}))

(defn- delete-tree! [file]
  (when (.exists ^File file)
    (doseq [child (or (.listFiles ^File file) (make-array File 0))]
      (delete-tree! child))
    (when-not (.delete ^File file)
      (fail! "qualification scratch cleanup failed"))))

(defn- recovery-operations [scratch-parent expected restored]
  {:durable-capability (fn [] {:status :supported
                               :native-version "26.7.2-rc.2"})
   :create-scratch! (fn [_]
                      (Files/createTempDirectory
                       (path scratch-parent) "recovery-"
                       (make-array java.nio.file.attribute.FileAttribute 0)))
   :cleanup-scratch! (fn [scratch] (delete-tree! (.toFile ^Path scratch)))
   :open-native! (fn [_] :qualification-handle)
   :close-native! (fn [_] nil)
   :restore-database!
   (fn [_ database archive]
     (let [archive-path (path archive)
           observation {:database database
                        :filename (str (.getFileName archive-path))
                        :size (Files/size archive-path)
                        :sha256 (digest/sha256-file archive-path)}]
       (when-not (= expected (select-keys observation [:size :sha256]))
         (fail! "recovery received an unverified checkpoint"))
       (reset! restored observation)))
   :create-database! (fn [& _] (fail! "checkpoint recovery was bypassed"))
   :use-database! (fn [_ _] nil)
   :analyze-execute! (fn [& _] nil)
   :execute-native! (fn [& _] nil)
   :classification-sql! (fn [sql _] sql)
   :query-native! (fn [& _] {:labels [] :rows [] :count 0})
   :query-bytes-native! (fn [& _] {:bytes (byte-array 0) :byte-count 0})})

(defn- qualified-store [endpoint observed]
  (backend/object-backend
   (s3/s3-backend
    {:endpoint endpoint
     :bucket "bucket"
     :prefix "large-checkpoint"
     :region "us-east-1"
     :access-key "ACCESS"
     :secret-key "SECRET"
     :session-token "SESSION"
     :max-attempts 1
     :request!
     (fn [request]
       (case (:operation request)
         :put-file-if-absent
         (let [body (:request-body request)]
           (when-not (and (:file body) (nil? (:bytes body)))
             (fail! "checkpoint upload did not use the file streaming seam"))
           (swap! observed update :upload-file-requests inc))

         :download-to-file
         (let [body (:response-body request)]
           (when-not (and (:file body) (:create-new? body))
             (fail! "checkpoint download did not use the file streaming seam"))
           (swap! observed update :download-file-requests inc))
         nil)
       (s3-curl/request!
        (assoc request :connect-timeout-ms 5000 :timeout-ms 300000)))})
   "object"))

(def acquisition-options
  {:owner "large-checkpoint-writer"
   :instance "qualification-process"
   :expires-at 200M
   :now 100M
   :clock-skew 0M
   :database "qualification"
   :engine-version "26.7.2-rc.2"
   :backup-format 1
   :min-reader "26.7.2-rc.2"})

(defn- run-stream! [endpoint source scratch-parent]
  (let [source (path source)
        expected {:size (Files/size source) :sha256 (digest/sha256-file source)}
        observed (atom {:upload-file-requests 0 :download-file-requests 0})
        restored (atom nil)
        start (heap-snapshot)
        store (qualified-store endpoint observed)
        token (:token (control/acquire! store acquisition-options))
        publication (control/publish-checkpoint-file! store token source)
        reference (:reference publication)
        commit (control/commit-reference!
                store token
                {:kind :checkpoint
                 :reference reference
                 :engine-metadata {:version "26.7.2-rc.2"
                                   :backup-format 1
                                   :min-reader "26.7.2-rc.2"}
                 :verify-reference! control/verify-file-reference!})]
    (when-not (= (get zero-file-digests (:size expected)) (:sha256 expected))
      (fail! "deterministic checkpoint digest differs from the known oracle"))
    (control/release! store token)
    (let [opened (durable/open-reader!
                  {:store store
                   :scratch-parent scratch-parent
                   :operations (recovery-operations
                                scratch-parent expected restored)})]
      (reader/close! opened))
    (when-not (= :published (:status publication))
      (fail! "checkpoint was not freshly published"))
    (when-not (= :committed (:status commit))
      (fail! "checkpoint reference was not committed"))
    (when-not (= expected {:size (get reference "size")
                           :sha256 (get reference "sha256")})
      (fail! "published checkpoint reference differs from its source"))
    (when-not (= {:upload-file-requests 1 :download-file-requests 3} @observed)
      (fail! "production streaming seam counts were unexpected"))
    (when-not (= {:database "qualification"
                  :filename "base.tar.gz"
                  :size (:size expected)
                  :sha256 (:sha256 expected)}
                 @restored)
      (fail! "verified recovery did not visit restore"))
    {:mode :stream
     :file-bytes (:size expected)
     :stream-seams @observed
     :restore-verified true
     :heap {:start start :end (heap-snapshot)}}))

(defn- run-retained-control! [source]
  (let [source (path source)
        start (heap-snapshot)
        bytes (Files/readAllBytes source)]
    ;; Keep the payload strongly reachable across the collected end reading.
    (reset! retained-control bytes)
    (let [end (heap-snapshot)
          sentinel (bit-and 255 (aget ^bytes @retained-control
                                      (dec (alength ^bytes @retained-control))))]
      {:mode :retained-control
       :file-bytes (alength bytes)
       :last-byte sentinel
       :heap {:start start :end end}})))

(defn -main [& args]
  (let [[mode endpoint source scratch-parent] args
        result (case mode
                 "baseline" {:mode :baseline :heap {:end (heap-snapshot)}}
                 "retained-control" (run-retained-control! source)
                 "stream" (run-stream! endpoint source scratch-parent)
                 (fail! "unknown large-checkpoint qualification mode"))]
    (println "QUALIFICATION" (pr-str result))))
