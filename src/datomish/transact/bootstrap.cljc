;; This Source Code Form is subject to the terms of the Mozilla Public
;; License, v. 2.0. If a copy of the MPL was not distributed with this
;; file, You can obtain one at http://mozilla.org/MPL/2.0/.

(ns datomish.transact.bootstrap)

(def symbolic-schema
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
                          } ;; :db/index       true} TODO: Handle this using SQLite protocol.
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
                          :db/cardinality :db.cardinality/one}
   })

(def idents
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
   :db/doc               35
   })

(def parts
  {:db.part/db   {:start 0 :idx (inc (apply max (vals idents)))}
   :db.part/user {:start 0x10000 :idx 0x10000}
   :db.part/tx   {:start 0x10000000 :idx 0x10000000}
   })

(defn tx-data []
  (concat
    (map (fn [[ident entid]] [:db/add entid :db/ident ident]) idents)
    ;; TODO: install partitions as well, like (map (fn [[ident entid]] [:db/add :db.part/db :db.install/partition ident])).
    (map (fn [[ident attrs]] (assoc attrs :db/id ident)) symbolic-schema)
    (map (fn [[ident attrs]] [:db/add :db.part/db :db.install/attribute (get idents ident)]) symbolic-schema) ;; TODO: fail if nil.
    ))
