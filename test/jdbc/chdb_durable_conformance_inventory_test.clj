(ns jdbc.chdb-durable-conformance-inventory-test
  (:require [clojure.edn :as edn]
            [clojure.java.io :as io]
            [clojure.string :as str]
            [jdbc.chdb.durable.head :as head]))

(def ^:private inventory-path
  "resources/jdbc/chdb/durable_conformance_inventory.edn")
(def ^:private mapping-path
  "resources/jdbc/chdb/durable_conformance_mapping.edn")

(def ^:private expected-source
  {:repository "https://github.com/chdb-io/chdb.git"
   :commit "66643e5030fb73c30ac5cdd31d4c7858ea040ed0"
   :protocol {:path "docs/durable/protocol-v1.mdx"
              :sha256 "82538d958d2f522bea6e4a6ccbc27c6bb6e230e19a1a386c605d43a0c02d11ba"}
   :suite {:path "tests/test_durable.py"
           :sha256 "5b9a97a3bccc0c29b10aca6b6e9a72edceea6a79e19d9a54c900b10d24f5eee6"
           :case-count 49}})

(def ^:private expected-disposition-counts
  {:total 49 :mapped 45 :blocked 2 :not-applicable 2})

(def ^:private expected-case-names
  ["test_empty_object"
   "test_readonly_missing_object"
   "test_checkpoint_only"
   "test_checkpoint_plus_wal"
   "test_quoted_database_name"
   "test_missing_base_and_wal"
   "test_bad_base_size_and_digest"
   "test_bad_wal_size_and_digest"
   "test_unknown_reader_feature"
   "test_unknown_writer_feature"
   "test_future_protocol_version"
   "test_producer_version_differs_but_compatible"
   "test_engine_reader_too_old"
   "test_backup_format_too_new"
   "test_unknown_fields_round_trip"
   "test_json_shape_is_not_frozen"
   "test_gates_cannot_be_bypassed_by_method_choice"
   "test_multi_statement_and_parallel_with_refused"
   "test_writes_outside_the_object_refused"
   "test_database_lifecycle_refused"
   "test_mutating_global_refused"
   "test_control_statements_refused"
   "test_secret_mutation_refused_without_leaking"
   "test_secret_read_only_runs_and_is_not_logged"
   "test_unknown_statement_fails_closed"
   "test_limits_report_limit_exceeded"
   "test_conditional_create_race"
   "test_wal_put_lands_but_head_cas_fails"
   "test_head_cas_committed_but_response_lost"
   "test_unprovable_commit_is_ambiguous"
   "test_wal_upload_landed_but_response_lost"
   "test_checkpoint_put_lands_but_head_cas_fails"
   "test_restore_failure_frees_the_engine"
   "test_heartbeat_renews_without_moving_generation_or_seq"
   "test_writer_self_fences_when_renewal_fails"
   "test_takeover_fences_the_previous_writer"
   "test_close_releases_and_then_refuses"
   "test_lease_exclusion_and_same_owner_instance"
   "test_expired_lease_is_taken_without_force"
   "test_unexpired_lease_needs_force"
   "test_object_id_validation_and_path_traversal"
   "test_sibling_isolation"
   "test_destroy_refuses_an_active_lease"
   "test_scan_across_objects"
   "test_reopen_honors_persisted_db"
   "test_cold_open_lands_in_the_objects_database"
   "test_wal_replay_uses_the_objects_database"
   "test_wal_keys_are_unique"
   "test_constructor_validation"])

(defn- fail! [message data]
  (throw (ex-info message data)))

(defn- read-edn! [path]
  (edn/read-string (slurp path)))

(defn- stable-case-id [case-name]
  (keyword "upstream" (str/replace (subs case-name 5) "_" "-")))

(defn- require-equal! [message expected actual]
  (when-not (= expected actual)
    (fail! message {:expected expected :actual actual})))

(defn- validate-local-test! [{:keys [path anchor] :as local-test}]
  (when-not (and (string? path)
                 (str/starts-with? path "test/")
                 (not (str/includes? path ".."))
                 (string? anchor)
                 (not (str/blank? anchor)))
    (fail! "invalid local test reference" {:local-test local-test}))
  (let [file (io/file path)]
    (when-not (.isFile file)
      (fail! "mapped local test path does not exist" {:path path}))
    (when-not (str/includes? (slurp file) anchor)
      (fail! "mapped local test anchor does not exist"
             {:path path :anchor anchor}))))

(defn- validate-mapping! [{:keys [disposition local-tests blocker not-applicable]
                           case-id :case
                           :as mapping}]
  (case disposition
    :mapped
    (do
      (when-not (and (vector? local-tests) (seq local-tests))
        (fail! "mapped case has no local tests" {:case case-id}))
      (doseq [local-test local-tests]
        (validate-local-test! local-test)))

    :blocked
    (when-not (and (map? blocker)
                   (integer? (:issue blocker))
                   (pos? (:issue blocker))
                   (string? (:rationale blocker))
                   (not (str/blank? (:rationale blocker))))
      (fail! "blocked case needs a tracked issue and rationale" {:mapping mapping}))

    :not-applicable
    (when-not (and (map? not-applicable)
                   (string? (:rationale not-applicable))
                   (not (str/blank? (:rationale not-applicable))))
      (fail! "not-applicable case needs a rationale" {:mapping mapping}))

    (fail! "unknown conformance disposition"
           {:case case-id :disposition disposition})))

(defn validate-inventory! [inventory mapping]
  (require-equal! "inventory schema drift" 1 (:schema inventory))
  (require-equal! "mapping schema drift" 1 (:schema mapping))
  (require-equal! "mapping points at a stale inventory path"
                  inventory-path (:inventory mapping))
  (require-equal! "upstream source path, SHA, digest, or count drift"
                  expected-source (:source inventory))
  (require-equal! "protocol source disagrees with the runtime head contract"
                  (select-keys head/protocol-source [:repository :commit :document])
                  {:repository (get-in inventory [:source :repository])
                   :commit (get-in inventory [:source :commit])
                   :document (get-in inventory [:source :protocol :path])})
  (let [cases (:cases inventory)
        names (mapv :name cases)
        ids (mapv :id cases)
        mappings (:mappings mapping)
        mapped-ids (mapv :case mappings)]
    (require-equal! "upstream Durable case count drift"
                    (get-in expected-source [:suite :case-count]) (count cases))
    (require-equal! "upstream Durable case name or order drift"
                    expected-case-names names)
    (require-equal! "stable case ID drift"
                    (mapv stable-case-id names) ids)
    (when-not (= (count ids) (count (set ids)))
      (fail! "duplicate upstream case ID" {:ids ids}))
    (when-not (= (count mapped-ids) (count (set mapped-ids)))
      (fail! "duplicate conformance mapping" {:cases mapped-ids}))
    (require-equal! "unmapped or unknown upstream conformance case"
                    (set ids) (set mapped-ids))
    (doseq [entry mappings]
      (validate-mapping! entry))
    {:total (count cases)
     :mapped (count (filter #(= :mapped (:disposition %)) mappings))
     :blocked (count (filter #(= :blocked (:disposition %)) mappings))
     :not-applicable
     (count (filter #(= :not-applicable (:disposition %)) mappings))}))

(defn- rejected? [f]
  (try
    (f)
    false
    (catch Throwable _ true)))

(defn- apply-mutant [name inventory mapping]
  (case name
    "name-drift" [(assoc-in inventory [:cases 0 :name] "test_renamed") mapping]
    "sha-drift" [(assoc-in inventory [:source :commit] (apply str (repeat 40 "0"))) mapping]
    "missing-mapping" [inventory (update mapping :mappings pop)]
    "duplicate-mapping" [inventory (update mapping :mappings conj (first (:mappings mapping)))]
    "missing-local-test"
    [inventory (assoc-in mapping [:mappings 0 :local-tests 0 :anchor]
                         "deliberately nonexistent local test")]
    (fail! "unknown JOLT_CHDB_CONFORMANCE_MUTANT" {:mutant name})))

(defn -main [& _]
  (let [inventory (read-edn! inventory-path)
        mapping (read-edn! mapping-path)]
    (if-let [mutant (System/getenv "JOLT_CHDB_CONFORMANCE_MUTANT")]
      (let [[mutant-inventory mutant-mapping]
            (apply-mutant mutant inventory mapping)]
        ;; This call must throw. It is the externally runnable red control.
        (validate-inventory! mutant-inventory mutant-mapping))
      (let [summary (validate-inventory! inventory mapping)]
        (require-equal! "conformance disposition count drift"
                        expected-disposition-counts summary)
        (doseq [[name mutant]
                [["name drift" "name-drift"]
                 ["SHA drift" "sha-drift"]
                 ["missing mapping" "missing-mapping"]
                 ["duplicate mapping" "duplicate-mapping"]
                 ["missing local test" "missing-local-test"]]]
          (when-not
           (rejected?
            (fn []
              (let [[mutant-inventory mutant-mapping]
                    (apply-mutant mutant inventory mapping)]
                (validate-inventory! mutant-inventory mutant-mapping))))
            (fail! "deliberate drift control unexpectedly passed" {:control name})))
        (println "Durable upstream conformance inventory" summary)
        (println "  ok   provenance, case drift, mapping coverage, and local anchors")
        (println "  ok   five deliberate drift controls rejected")))))
