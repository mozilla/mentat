;; Copyright 2016 Mozilla
;;
;; Licensed under the Apache License, Version 2.0 (the "License"); you may not use
;; this file except in compliance with the License. You may obtain a copy of the
;; License at http://www.apache.org/licenses/LICENSE-2.0
;; Unless required by applicable law or agreed to in writing, software distributed
;; under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
;; CONDITIONS OF ANY KIND, either express or implied. See the License for the
;; specific language governing permissions and limitations under the License.

(ns datomish.transact.bootstrap)

(def v1-symbolic-schema
  {:db/ident             {:db/valueType   :db.type/keyword
                          :db/cardinality :db.cardinality/one
                          :db/unique      :db.unique/identity}
   :db.install/partition {:db/valueType   :db.type/ref
                          :db/cardinality :db.cardinality/many}
   :db.install/valueType {:db/valueType   :db.type/ref
                          :db/cardinality :db.cardinality/many}
   :db.install/attribute {:db/valueType   :db.type/ref
                          :db/cardinality :db.cardinality/many}
   ;; TODO: support user-specified functions in the future.
   ;; :db.install/function {:db/valueType :db.type/ref
   ;;                       :db/cardinality :db.cardinality/many}
   :db/txInstant         {:db/valueType   :db.type/long
                          :db/cardinality :db.cardinality/one
                          :db/index       true}
   :db/valueType         {:db/valueType   :db.type/ref
                          :db/cardinality :db.cardinality/one}
   :db/cardinality       {:db/valueType   :db.type/ref
                          :db/cardinality :db.cardinality/one}
   :db/doc               {:db/valueType   :db.type/string
                          :db/cardinality :db.cardinality/one}
   :db/unique            {:db/valueType   :db.type/ref
                          :db/cardinality :db.cardinality/one}
   :db/isComponent       {:db/valueType   :db.type/boolean
                          :db/cardinality :db.cardinality/one}
   :db/index             {:db/valueType   :db.type/boolean
                          :db/cardinality :db.cardinality/one}
   :db/fulltext          {:db/valueType   :db.type/boolean
                          :db/cardinality :db.cardinality/one}
   :db/noHistory         {:db/valueType   :db.type/boolean
                          :db/cardinality :db.cardinality/one}})

(def v2-symbolic-schema
  {:db.alter/attribute   {:db/valueType   :db.type/ref
                          :db/cardinality :db.cardinality/many}
   :db.schema/version    {:db/valueType   :db.type/long
                          :db/cardinality :db.cardinality/one}

   ;; unique-value because an attribute can only belong to a single
   ;; schema fragment.
   :db.schema/attribute  {:db/valueType   :db.type/ref
                          :db/unique      :db.unique/value
                          :db/cardinality :db.cardinality/many}})

(def symbolic-schema (merge v1-symbolic-schema v2-symbolic-schema))

(def v1-idents
  {:db/ident             1
   :db.part/db           2
   :db/txInstant         3
   :db.install/partition 4
   :db.install/valueType 5
   :db.install/attribute 6
   :db/valueType         7
   :db/cardinality       8
   :db/unique            9
   :db/isComponent       10
   :db/index             11
   :db/fulltext          12
   :db/noHistory         13
   :db/add               14
   :db/retract           15
   :db.part/user         16
   :db.part/tx           17
   :db/excise            18
   :db.excise/attrs      19
   :db.excise/beforeT    20
   :db.excise/before     21
   :db.alter/attribute   22
   :db.type/ref          23
   :db.type/keyword      24
   :db.type/long         25
   :db.type/double       26
   :db.type/string       27
   :db.type/boolean      28
   :db.type/instant      29
   :db.type/bytes        30
   :db.cardinality/one   31
   :db.cardinality/many  32
   :db.unique/value      33
   :db.unique/identity   34
   :db/doc               35})

(def v2-idents
  {:db.schema/version    36    ; Fragment -> version.
   :db.schema/attribute  37    ; Fragment -> attribute.
   })

(def idents (merge v1-idents v2-idents))

(def parts
  {:db.part/db   {:start 0 :idx (inc (apply max (vals idents)))}
   :db.part/user {:start 0x10000 :idx 0x10000}
   :db.part/tx   {:start 0x10000000 :idx 0x10000000}
   })

(defn tx-data [new-idents new-symbolic-schema]
  (concat
    (map (fn [[ident entid]] [:db/add entid :db/ident ident]) new-idents)
    ;; TODO: install partitions as well, like (map (fn [[ident entid]] [:db/add :db.part/db :db.install/partition ident])).
    (map (fn [[ident attrs]] (assoc attrs :db/id ident)) new-symbolic-schema)
    (map (fn [[ident attrs]] [:db/add :db.part/db :db.install/attribute (get idents ident)]) new-symbolic-schema) ;; TODO: fail if nil.
    ))
