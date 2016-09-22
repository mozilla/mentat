;; This Source Code Form is subject to the terms of the Mozilla Public
;; License, v. 2.0. If a copy of the MPL was not distributed with this
;; file, You can obtain one at http://mozilla.org/MPL/2.0/.

(ns datomish.simple-schema
  #?(:cljs
     (:require-macros
      [datomish.pair-chan :refer [go-pair <?]]
      [cljs.core.async.macros :refer [go]]))
  (:require
   [clojure.set]
   [datomish.util :as util
    #?(:cljs :refer-macros :clj :refer) [raise raise-str cond-let]]
   [datomish.db :as db]
   [datomish.schema :as ds]
   [datomish.sqlite :as s]
   [datomish.sqlite-schema :as sqlite-schema]
   #?@(:clj [[datomish.pair-chan :refer [go-pair <?]]
             [clojure.core.async :as a :refer [chan go <! >!]]])
   #?@(:cljs [[datomish.pair-chan]
              [cljs.core.async :as a :refer [chan <! >!]]])))

(defn- name->ident [name]
  (when-not (and (string? name)
                 (not (empty? name)))
    (raise "Invalid name " name {:error :invalid-name :name name}))
  (keyword name))

(defn simple-schema-attributes->schema-parts [attrs]
  (let [{:keys [cardinality type name unique doc fulltext]} attrs
        value-type (when type (keyword (str "db.type/" type)))]

    (when-not (and value-type
                   (contains? ds/value-type-map value-type))
      (raise "Invalid type " type {:error :invalid-type :type type}))

    (let [unique
          (case unique
                "identity" :db.unique/identity
                "value" :db.unique/value
                nil nil
                (raise "Invalid unique " unique
                       {:error :invalid-unique :unique unique}))

          cardinality
          (case cardinality
                "one" :db.cardinality/one
                "many" :db.cardinality/many
                nil nil
                (raise "Invalid cardinality " cardinality
                       {:error :invalid-cardinality :cardinality cardinality}))]

      (util/assoc-if
        {:db/valueType value-type
         :db/ident (name->ident name)
         :db/id (db/id-literal :db.part/user)
         :db.install/_attribute :db.part/db}
        :db/doc doc
        :db/unique unique
        :db/fulltext fulltext
        :db/cardinality cardinality))))

(defn simple-schema->schema [simple-schema]
  (let [{:keys [name attributes]} simple-schema]
    (map simple-schema-attributes->schema-parts attributes)))

