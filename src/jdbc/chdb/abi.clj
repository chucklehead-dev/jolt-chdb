(ns jdbc.chdb.abi
  "Validated, runtime-neutral description of the libchdb C surface.

  The descriptor records C semantics only. Runtime adapters may derive their
  literal bindings and layouts from it without maintaining another signature
  list."
  (:require [clojure.edn :as edn]
            [clojure.java.io :as io]
            [clojure.string :as str]))

(def descriptor-resource "jdbc/chdb/abi.edn")

(def ^:private scalar-types
  #{:void :int :uint32 :uint64 :size_t :double :pointer :string})

(defn- invalid! [message data]
  (throw (ex-info message (assoc data :type ::invalid-descriptor))))

(defn- nonblank-string? [value]
  (and (string? value) (not (str/blank? value))))

(defn validate-descriptor!
  "Validate the structural invariants every runtime backend relies on.
  Returns `descriptor`; throws a typed exception before native loading."
  [descriptor]
  (when-not (= 1 (:schema descriptor))
    (invalid! "unsupported chDB ABI descriptor schema"
              {:schema (:schema descriptor)}))
  (doseq [field [:repository :release :commit :header :c-oracle :python-oracle]]
    (when-not (nonblank-string? (get-in descriptor [:source field]))
      (invalid! "chDB ABI descriptor has invalid source provenance"
                {:field field :value (get-in descriptor [:source field])})))
  (when-not (= {:version 1 :minimum-native-version "26.7.2"}
               (get-in descriptor [:contracts :durable-v1]))
    (invalid! "chDB Durable contract identity does not match V1"
              {:durable-v1 (get-in descriptor [:contracts :durable-v1])}))
  (when-not (= {:read-only 0 :mutating 1 :mutating-global 2
                :control 3 :unknown 4}
               (get-in descriptor [:enums :query-class]))
    (invalid! "chDB Durable V1 query classes do not match chdb.h"
              {:query-class (get-in descriptor [:enums :query-class])}))
  (when-not (= {:has-secrets 1 :writes-only-target-database 2
                :changes-database-lifecycle 4}
               (get-in descriptor [:enums :query-analysis-flag]))
    (invalid! "chDB Durable V1 analysis flags do not match chdb.h"
              {:query-analysis-flag
               (get-in descriptor [:enums :query-analysis-flag])}))
  (let [{:keys [kind size fields]}
        (get-in descriptor [:types :query-analysis-v1])]
    (when-not (and (= :struct kind)
                   (= 16 size)
                   (= [[:struct-size :uint32]
                       [:statement-count :uint32]
                       [:flags :uint32]
                       [:query-class :uint32]]
                      fields))
      (invalid! "chDB query-analysis-v1 layout does not match chdb.h"
                {:layout (get-in descriptor [:types :query-analysis-v1])})))
  (let [functions (:functions descriptor)
        symbols (map :symbol (vals functions))]
    (when-not (and (map? functions) (seq functions))
      (invalid! "chDB ABI descriptor requires functions" {}))
    (when-not (= (count symbols) (count (distinct symbols)))
      (invalid! "chDB ABI descriptor contains duplicate C symbols"
                {:symbols symbols}))
    (doseq [[function-id {:keys [symbol args return blocking? contracts]}]
            functions]
      (when-not (keyword? function-id)
        (invalid! "chDB ABI function id must be a keyword"
                  {:function function-id}))
      (when-not (nonblank-string? symbol)
        (invalid! "chDB ABI function requires a symbol"
                  {:function function-id :symbol symbol}))
      (when-not (and (vector? args) (every? scalar-types args))
        (invalid! "chDB ABI function has unsupported argument types"
                  {:function function-id :args args}))
      (when-not (contains? scalar-types return)
        (invalid! "chDB ABI function has unsupported return type"
                  {:function function-id :return return}))
      (when-not (or (nil? blocking?) (boolean? blocking?))
        (invalid! "chDB ABI function has invalid blocking marker"
                  {:function function-id :blocking? blocking?}))
      (when-not (and (vector? contracts) (seq contracts)
                     (every? #(contains? (:contracts descriptor) %) contracts))
        (invalid! "chDB ABI function names an unknown contract"
                  {:function function-id :contracts contracts}))))
  descriptor)

(defonce ^:private descriptor*
  (delay
    (let [resource (io/resource descriptor-resource)]
      (when-not resource
        (invalid! "chDB ABI descriptor resource is missing"
                  {:resource descriptor-resource}))
      (validate-descriptor! (edn/read-string (slurp resource))))))

(defn descriptor [] @descriptor*)
(defn source-provenance [] (:source (descriptor)))
(defn function-spec [function-id]
  (or (get-in (descriptor) [:functions function-id])
      (invalid! "unknown chDB ABI function" {:function function-id})))
(defn type-spec [type-id]
  (or (get-in (descriptor) [:types type-id])
      (invalid! "unknown chDB ABI type" {:type-id type-id})))
(defn contract-spec [contract-id]
  (or (get-in (descriptor) [:contracts contract-id])
      (invalid! "unknown chDB ABI contract" {:contract contract-id})))

(defn contract-functions [contract-id]
  (contract-spec contract-id)
  (into (sorted-map)
        (filter (fn [[_ function]]
                  (some #(= contract-id %) (:contracts function))))
        (:functions (descriptor))))

(defn binding-spec
  "Return one runtime-neutral literal binding specification.

  Runtime adapters must derive their foreign declarations from this value
  rather than copying symbols or signatures into a second registry."
  [function-id]
  (let [{:keys [symbol args return blocking? contracts]}
        (function-spec function-id)]
    {:function function-id
     :symbol symbol
     :args args
     :return return
     :blocking? (boolean blocking?)
     :contracts contracts}))

(defn binding-specs
  "Return binding specifications in caller-requested order."
  [function-ids]
  (mapv binding-spec function-ids))

(defmacro defjoltfn
  "Define one literal Jolt binding derived from the canonical descriptor."
  [binding function-id]
  (let [{:keys [symbol args return blocking?]} (function-spec function-id)
        tail (when blocking? [:blocking])]
    (list* 'jolt.ffi/defcfn binding symbol args return tail)))

(defmacro jolt-layout
  "Compile a Jolt FFI layout from one descriptor struct."
  [type-id]
  (let [{:keys [kind fields]} (type-spec type-id)]
    (when-not (= :struct kind)
      (invalid! "Jolt layout requires a struct descriptor" {:type-id type-id}))
    (list 'jolt.ffi/layout [:struct fields])))
