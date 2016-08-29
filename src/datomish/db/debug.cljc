;; This Source Code Form is subject to the terms of the Mozilla Public
;; License, v. 2.0. If a copy of the MPL was not distributed with this
;; file, You can obtain one at http://mozilla.org/MPL/2.0/.

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
