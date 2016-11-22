;; Copyright 2016 Mozilla
;;
;; Licensed under the Apache License, Version 2.0 (the "License"); you may not use
;; this file except in compliance with the License. You may obtain a copy of the
;; License at http://www.apache.org/licenses/LICENSE-2.0
;; Unless required by applicable law or agreed to in writing, software distributed
;; under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
;; CONDITIONS OF ANY KIND, either express or implied. See the License for the
;; specific language governing permissions and limitations under the License.

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

