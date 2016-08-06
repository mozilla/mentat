;; This Source Code Form is subject to the terms of the Mozilla Public
;; License, v. 2.0. If a copy of the MPL was not distributed with this
;; file, You can obtain one at http://mozilla.org/MPL/2.0/.

;; Purloined from DataScript.

(ns datomish.schema
  (:require
   [datomish.sqlite-schema :as sqlite-schema]
   [datomish.util :as util #?(:cljs :refer-macros :clj :refer) [raise]]))

(defprotocol ISchema
  (attrs-by
    [schema property]
    "TODO: document this, think more about making this part of the schema."))

(defn- #?@(:clj  [^Boolean is-attr?]
           :cljs [^boolean is-attr?]) [schema attr property]
  (contains? (attrs-by schema property) attr))

(defn #?@(:clj  [^Boolean multival?]
          :cljs [^boolean multival?]) [schema attr]
  (is-attr? schema attr :db.cardinality/many))

(defn #?@(:clj  [^Boolean ref?]
          :cljs [^boolean ref?]) [schema attr]
  (is-attr? schema attr :db.type/ref))

(defn #?@(:clj  [^Boolean kw?]
          :cljs [^boolean kw?]) [schema attr]
  (is-attr? schema attr :db.type/keyword))

(defn #?@(:clj  [^Boolean component?]
          :cljs [^boolean component?]) [schema attr]
  (is-attr? schema attr :db/isComponent))

(defn #?@(:clj  [^Boolean indexing?]
          :cljs [^boolean indexing?]) [schema attr]
  (is-attr? schema attr :db/index))

(defn #?@(:clj  [^Boolean fulltext?]
          :cljs [^boolean fulltext?]) [schema attr]
  (is-attr? schema attr :db/fulltext))

(defn #?@(:clj  [^Boolean unique?]
          :cljs [^boolean unique?]) [schema attr]
  (is-attr? schema attr :db/unique))

(defn #?@(:clj  [^Boolean unique-identity?]
          :cljs [^boolean unique-identity?]) [schema attr]
  (is-attr? schema attr :db.unique/identity))

(defn #?@(:clj  [^Boolean unique-value?]
          :cljs [^boolean unique-value?]) [schema attr]
  (is-attr? schema attr :db.unique/value))

(defn schema? [x]
  (satisfies? ISchema x))

(defrecord Schema [schema rschema]
  ISchema
  (attrs-by [schema property]
    ((.-rschema schema) property)))

(defn- attr->properties [k v]
  (cond
    (= [k v] [:db/isComponent true]) [:db/isComponent]
    (= v :db.type/ref)               [:db.type/ref :db/index]
    (= v :db.cardinality/many)       [:db.cardinality/many]
    (= v :db.unique/identity)        [:db/unique :db.unique/identity :db/index]
    (= v :db.unique/value)           [:db/unique :db.unique/value    :db/index]
    (= [k v] [:db/index true])       [:db/index]
    (= [k v] [:db/fulltext true])    [:db/fulltext :db/index]
    (= k :db/valueType)              [v]))

(defn- multimap [e m]
  (reduce
    (fn [acc [k v]]
      (update-in acc [k] (fnil conj e) v))
    {} m))

(defn- rschema [schema]
  (->>
    (for [[a kv] schema
          [k v]  kv
          prop   (attr->properties k v)]
      [prop a])
    (multimap #{})))

(defn- validate-schema-key [a k v expected]
  (when-not (or (nil? v)
                (contains? expected v))
    (throw (ex-info (str "Bad attribute specification for " (pr-str {a {k v}}) ", expected one of " expected)
                    {:error :schema/validation
                     :attribute a
                     :key k
                     :value v}))))

(def value-type-map
  {:db.type/ref     { :valid? #(and (integer? %) (pos? %)) }
   :db.type/keyword { :valid? keyword? }
   :db.type/string  { :valid? string? }
   :db.type/boolean { :valid? #?(:clj #(instance? Boolean %) :cljs #(= js/Boolean (type %))) }
   :db.type/long    { :valid? integer? }
   :db.type/double  { :valid? #?(:clj float? :cljs number?) }
   })

(defn #?@(:clj  [^Boolean ensure-valid-value]
          :cljs [^boolean ensure-valid-value]) [schema attr value]
  {:pre [(schema? schema)]}
  (let [schema (.-schema schema)]
    (if-let [valueType (get-in schema [attr :db/valueType])]
      (if-let [valid? (get-in value-type-map [valueType :valid?])]
        (when-not (valid? value)
          (raise "Invalid value for attribute " attr ", expected " valueType " but got " value
                 {:error :schema/valueType, :attribute attr, :value value}))
        (raise "Unknown valueType for attribute " attr ", expected one of " (sorted-set (keys value-type-map))
               {:error :schema/valueType, :attribute attr}))
      (raise "Unknown attribute " attr ", expected one of " (sorted-set (keys schema))
             {:error :schema/valueType, :attribute attr}))))

(defn ->SQLite [schema attr value]
  {:pre [(schema? schema)]}
  (let [schema (.-schema schema)]
    (if-let [valueType (get-in schema [attr :db/valueType])]
      (if-let [valid? (get-in value-type-map [valueType :valid?])]
        (if (valid? value)
          [(sqlite-schema/->SQLite value) (sqlite-schema/->tag valueType)]
          (raise "Invalid value for attribute " attr ", expected " valueType " but got " value
                 {:error :schema/valueType, :attribute attr, :value value}))
        (raise "Unknown valueType for attribute " attr ", expected one of " (sorted-set (keys value-type-map))
               {:error :schema/valueType, :attribute attr}))
      (raise "Unknown attribute " attr ", expected one of " (sorted-set (keys schema))
             {:error :schema/valueType, :attribute attr}))))

(defn <-SQLite [schema attr value]
  {:pre [(schema? schema)]}
  (let [schema (.-schema schema)]
    (if-let [valueType (get-in schema [attr :db/valueType])]
      (if (contains? value-type-map valueType)
        (sqlite-schema/<-SQLite valueType value)
        (raise "Unknown valueType for attribute " attr ", expected one of " (sorted-set (keys value-type-map))
               {:error :schema/valueType, :attribute attr}))
      (raise "Unknown attribute " attr ", expected one of " (sorted-set (keys schema))
             {:error :schema/valueType, :attribute attr}))))

(defn- validate-schema [schema]
  (doseq [[a kv] schema]
    (when-not (:db/valueType kv)
      (throw (ex-info (str "Bad attribute specification for " a ": should have {:db/valueType ...}")
                      {:error     :schema/validation
                       :attribute a
                       :key       :db/valueType})))
    (let [comp? (:db/isComponent kv false)]
      (validate-schema-key a :db/isComponent (:db/isComponent kv) #{true false})
      (when (and comp? (not= (:db/valueType kv) :db.type/ref))
        (throw (ex-info (str "Bad attribute specification for " a ": {:db/isComponent true} should also have {:db/valueType :db.type/ref}")
                        {:error     :schema/validation
                         :attribute a
                         :key       :db/isComponent}))))
    (let [fulltext? (:db/fulltext kv false)]
      (validate-schema-key a :db/fulltext (:db/fulltext kv) #{true false})
      (when (and fulltext? (not= (:db/valueType kv) :db.type/string))
        (throw (ex-info (str "Bad attribute specification for " a ": {:db/fulltext true} should also have {:db/valueType :db.type/string}")
                        {:error     :schema/validation
                         :attribute a
                         :key       :db/fulltext}))))
    (validate-schema-key a :db/unique (:db/unique kv) #{:db.unique/value :db.unique/identity})
    (validate-schema-key a :db/valueType (:db/valueType kv) (set (keys value-type-map)))
    (validate-schema-key a :db/cardinality (:db/cardinality kv) #{:db.cardinality/one :db.cardinality/many}))
  schema)

(defn schema [schema]
  {:pre [(or (nil? schema) (map? schema))]}
  (map->Schema {:schema  (validate-schema schema)
                :rschema (rschema schema)}))
