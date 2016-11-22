;; Copyright 2016 Mozilla
;;
;; Licensed under the Apache License, Version 2.0 (the "License"); you may not use
;; this file except in compliance with the License. You may obtain a copy of the
;; License at http://www.apache.org/licenses/LICENSE-2.0
;; Unless required by applicable law or agreed to in writing, software distributed
;; under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
;; CONDITIONS OF ANY KIND, either express or implied. See the License for the
;; specific language governing permissions and limitations under the License.

(ns datomish.db.debug
  #?(:cljs
     (:require-macros
      [datomish.pair-chan :refer [go-pair <?]]
      [cljs.core.async.macros :refer [go]]))
  (:require
   [datomish.db :as db]
   [datomish.datom :as dd :refer [datom datom? #?@(:cljs [Datom])]]
   [datomish.util :as util #?(:cljs :refer-macros :clj :refer) [raise raise-str cond-let]]
   [datomish.sqlite :as s]
   #?@(:clj [[datomish.pair-chan :refer [go-pair <?]]
             [clojure.core.async :as a :refer [chan go <! >!]]])
   #?@(:cljs [[datomish.pair-chan]
              [cljs.core.async :as a :refer [chan <! >!]]]))
  #?(:clj
     (:import
      [datomish.datom Datom])))

(defn <datoms-after [db tx]
  (go-pair
    (->>
      (s/all-rows (:sqlite-connection db) ["SELECT e, a, v, tx FROM datoms WHERE tx > ?" tx])
      (<?)
      (mapv #(vector (:e %) (db/ident db (:a %)) (:v %)))
      (filter #(not (= :db/txInstant (second %))))
      (set))))

(defn <datoms>= [db tx]
  (go-pair
    (->>
      (s/all-rows (:sqlite-connection db) ["SELECT e, a, v, tx FROM datoms WHERE tx >= ?" tx])
      (<?)
      (mapv #(vector (:e %) (db/ident db (:a %)) (:v %)))
      (filter #(not (= :db/txInstant (second %))))
      (set))))

(defn <datoms [db]
  (<datoms-after db 0))

(defn <shallow-entity [db eid]
  ;; TODO: make this actually be <entity.  Handle :db.cardinality/many and :db/isComponent.
  (go-pair
    (->>
      (s/all-rows (:sqlite-connection db) ["SELECT a, v FROM datoms WHERE e = ?" eid])
      (<?)
      (mapv #(vector (db/ident db (:a %)) (:v %)))
      (reduce conj {}))))

(defn <transactions-after [db tx]
  (go-pair
    (->>
      (s/all-rows (:sqlite-connection db) ["SELECT e, a, v, tx, added FROM transactions WHERE tx > ? ORDER BY tx ASC, e, a, v, added" tx])
      (<?)
      (mapv #(vector (:e %) (db/ident db (:a %)) (:v %) (:tx %) (:added %))))))

(defn <transactions [db]
  (<transactions-after db 0))

(defn <fulltext-values [db]
  (go-pair
    (->>
      (s/all-rows (:sqlite-connection db) ["SELECT rowid, text FROM fulltext_values"])
      (<?)
      (mapv #(vector (:rowid %) (:text %))))))
