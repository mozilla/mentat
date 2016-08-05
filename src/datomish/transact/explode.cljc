;; This Source Code Form is subject to the terms of the Mozilla Public
;; License, v. 2.0. If a copy of the MPL was not distributed with this
;; file, You can obtain one at http://mozilla.org/MPL/2.0/.

(ns datomish.transact.explode
  #?(:cljs
     (:require-macros
      [datomish.pair-chan :refer [go-pair <?]]
      [cljs.core.async.macros :refer [go]]))
  (:require
   [datomish.db :as db]
   [datomish.util :as util #?(:cljs :refer-macros :clj :refer) [raise raise-str cond-let]]
   [datomish.schema :as ds]
   #?@(:clj [[datomish.pair-chan :refer [go-pair <?]]
             [clojure.core.async :as a :refer [chan go <! >!]]])
   #?@(:cljs [[datomish.pair-chan]
              [cljs.core.async :as a :refer [chan <! >!]]])))

(defn- #?@(:clj  [^Boolean reverse-ref?]
           :cljs [^boolean reverse-ref?]) [attr]
  (if (keyword? attr)
    (= \_ (nth (name attr) 0))
    (raise "Bad attribute type: " attr ", expected keyword"
           {:error :transact/syntax, :attribute attr})))

(defn- reverse-ref [attr]
  (if (keyword? attr)
    (if (reverse-ref? attr)
      (keyword (namespace attr) (subs (name attr) 1))
      (keyword (namespace attr) (str "_" (name attr))))
    (raise "Bad attribute type: " attr ", expected keyword"
           {:error :transact/syntax, :attribute attr})))

(declare explode-entity)

(defn- explode-entity-a-v [db entity eid a v]
  ;; a should be symbolic at this point.  Map it.  TODO: use ident/entid to ensure we have a symbolic attr.
  (let [reverse?    (reverse-ref? a)
        straight-a  (if reverse? (reverse-ref a) a)
        straight-a* (get-in db [:idents straight-a] straight-a)
        _           (when (and reverse? (not (ds/ref? (db/schema db) straight-a*)))
                      (raise "Bad attribute " a ": reverse attribute name requires {:db/valueType :db.type/ref} in schema"
                             {:error :transact/syntax, :attribute a, :op entity}))
        a*          (get-in db [:idents a] a)]
    (cond
      reverse?
      (explode-entity-a-v db entity v straight-a eid)

      (and (map? v)
           (not (db/id-literal? v)))
      ;; Another entity is given as a nested map.
      (if (ds/ref? (db/schema db) straight-a*)
        (let [other (assoc v (reverse-ref a) eid
                           ;; TODO: make the new ID have the same part as the original eid.
                           ;; TODO: make the new ID not show up in the tempids map.  (Does Datomic exposed the new ID this way?)
                           :db/id (db/id-literal :db.part/user))]
          (explode-entity db other))
        (raise "Bad attribute " a ": nested map " v " given but attribute name requires {:db/valueType :db.type/ref} in schema"
               {:error :transact/entity-map-type-ref
                :op    entity }))

      (sequential? v)
      (if (ds/multival? (db/schema db) a*) ;; dm/schema
        (mapcat (partial explode-entity-a-v db entity eid a) v) ;; Allow sequences of nested maps, etc.  This does mean [[1]] will work.
        (raise "Sequential values " v " but attribute " a " is :db.cardinality/one"
               {:error :transact/entity-sequential-cardinality-one
                :op    entity }))

      true
      [[:db/add eid a* v]])))

(defn- explode-entity [db entity]
  (if (map? entity)
    (if-let [eid (:db/id entity)]
      (mapcat (partial apply explode-entity-a-v db entity eid) (dissoc entity :db/id))
      (raise "Map entity missing :db/id, got " entity
             {:error :transact/entity-missing-db-id
              :op    entity }))
    [entity]))

(defn explode-entities [db entities]
  "Explode map shorthand, such as {:db/id e :attr value :_reverse ref}, to a list of vectors,
  like [[:db/add e :attr value] [:db/add ref :reverse e]]."
  (mapcat (partial explode-entity db) entities))
