;; This Source Code Form is subject to the terms of the Mozilla Public
;; License, v. 2.0. If a copy of the MPL was not distributed with this
;; file, You can obtain one at http://mozilla.org/MPL/2.0/.

(ns datomish.places.import
  #?(:cljs
     (:require-macros
      [datomish.pair-chan :refer [go-pair <?]]
      [cljs.core.async.macros :refer [go]]))
  (:require
     [datomish.db :as db]
     [datomish.transact :as transact]
   [datomish.util :as util #?(:cljs :refer-macros :clj :refer) [raise raise-str cond-let]]
   [datomish.sqlite :as s]
   #?@(:clj [[datomish.pair-chan :refer [go-pair <?]]
             [clojure.core.async :as a :refer [chan go <! >!]]])
   #?@(:cljs [[datomish.pair-chan]
              [cljs.core.async :as a :refer [chan <! >!]]])))

(def places-schema-fragment
  [{:db/id        (db/id-literal :db.part/user)
    :db/ident     :page/url
    :db/unique    :db.unique/identity
    :db/valueType :db.type/string ;; TODO: uri
    :db.install/_attribute :db.part/db}
   {:db/id        (db/id-literal :db.part/user)
    :db/ident     :page/guid
    :db/unique    :db.unique/identity
    :db/valueType :db.type/string ;; TODO: uuid or guid?
    :db.install/_attribute :db.part/db}
   {:db/id          (db/id-literal :db.part/user)
    :db/ident       :page/title
    :db/cardinality :db.cardinality/one
    :db/valueType   :db.type/string
    :db.install/_attribute :db.part/db}
   {:db/id        (db/id-literal :db.part/user)
    :db/ident     :page/visitAt
    :db/cardinality :db.cardinality/many
    :db/valueType :db.type/long ;; TODO: instant
    :db.install/_attribute :db.part/db}
   ])

(defn assoc-if
  ([m k v]
   (if v
     (assoc m k v)
     m))
  ([m k v & kvs]
   (if kvs
     (let [[kk vv & remainder] kvs]
       (apply assoc-if
              (assoc-if m k v)
              kk vv remainder))
     (assoc-if m k v))))


(defn- place->entity [[id rows]]
  (let [title (:title (first rows))
        required {:db/id (db/id-literal :db.part/user)
                  :page/url (:url (first rows))
                  :page/guid (:guid (first rows))}
        visits (keep :visit_date rows)]

    (assoc-if required
              :page/title title
              :page/visitAt visits)))

(defn import-titles [conn places-connection]
  (go-pair
    (let [rows
          (<?
            (s/all-rows
              places-connection
              ["SELECT DISTINCT p.title AS title, p.guid
               FROM moz_places AS p
               WHERE p.title IS NOT NULL AND p.hidden = 0 LIMIT 10"]))]
      (<?
        (transact/<transact!
          conn
          (map (fn [row]
                 {:db/id [:page/guid (:guid row)]
                  :page/title (:title row)})
               rows))))))

(defn import-places [conn places-connection]
  (go-pair
    ;; Ensure schema fragment is in place, even though it may cost a (mostly empty) transaction.
    (<? (transact/<transact! conn places-schema-fragment))

    (let [rows
          (<?
            (s/all-rows
              places-connection
              ["SELECT DISTINCT p.id AS id, p.url AS url, p.title AS title, p.visit_count, p.last_visit_date, p.guid,
               hv.visit_date
               FROM moz_places AS p LEFT JOIN moz_historyvisits AS hv ON p.id = hv.place_id
               WHERE p.hidden = 0
               ORDER BY p.id, hv.visit_date"]))]
      (<?
        (transact/<transact!
          conn
          (map place->entity (group-by :id rows)))))))

(defn import-titles-from-path [db places]
  (go-pair
    (let [conn (transact/connection-with-db db)
          pdb (<? (s/<sqlite-connection places))]
      (import-titles conn pdb))))

(defn import-places-from-path [db places]
  (go-pair
    (let [conn (transact/connection-with-db db)
          pdb (<? (s/<sqlite-connection places))]
      (import-places conn pdb))))
