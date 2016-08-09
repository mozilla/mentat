;; This Source Code Form is subject to the terms of the Mozilla Public
;; License, v. 2.0. If a copy of the MPL was not distributed with this
;; file, You can obtain one at http://mozilla.org/MPL/2.0/.

(ns datomish.places.import
  #?(:cljs
     (:require-macros
      [datomish.pair-chan :refer [go-pair <?]]
      [cljs.core.async.macros :refer [go]]))
  (:require
   [datomish.util :as util #?(:cljs :refer-macros :clj :refer) [raise raise-str cond-let]]
   [datomish.sqlite :as s]
   [datomish.api :as d]
   #?@(:clj [[datomish.pair-chan :refer [go-pair <?]]
             [clojure.core.async :as a :refer [chan go <! >!]]])
   #?@(:cljs [[datomish.pair-chan]
              [cljs.core.async :as a :refer [chan <! >!]]])))

(def places-schema-fragment
  [{:db/id        (d/id-literal :db.part/user)
    :db/ident     :page/url
    :db/unique    :db.unique/identity
    :db/valueType :db.type/string ;; TODO: uri
    :db.install/_attribute :db.part/db}
   {:db/id        (d/id-literal :db.part/user)
    :db/ident     :page/guid
    :db/unique    :db.unique/identity
    :db/valueType :db.type/string ;; TODO: uuid or guid?
    :db.install/_attribute :db.part/db}
   {:db/id          (d/id-literal :db.part/user)
    :db/ident       :page/title
    :db/cardinality :db.cardinality/one
    :db/valueType   :db.type/string
    :db.install/_attribute :db.part/db}
   {:db/id        (d/id-literal :db.part/user)
    :db/ident     :page/visitAt
    :db/cardinality :db.cardinality/many
    :db/valueType :db.type/long ;; TODO: instant
    :db.install/_attribute :db.part/db}
   ])

(defn- place->entity [[id rows]]
  (let [title (:title (first (filter :page/title rows)))]
    (cond-> {:db/id (d/id-literal :db.part/user)
             :page/url (:url (first rows))
             :page/guid (:guid (first rows))
             :page/visitAt (map :visit_date rows)}
      title (assoc :page/title title))))

(defn import-places [conn places-connection]
  (go-pair
    ;; Ensure schema fragment is in place, even though it may cost a (mostly empty) transaction.
    (<? (d/<transact! conn places-schema-fragment))

    (->>
      ["SELECT DISTINCT p.id, p.url, p.title, p.visit_count, p.last_visit_date, p.guid,"
       "hv.visit_date"
       "FROM moz_places AS p LEFT JOIN moz_historyvisits AS hv"
       "WHERE p.hidden = 0 AND p.id = hv.place_id"
       "ORDER BY p.id, hv.visit_date"
       "LIMIT 20000"] ;; TODO: remove limit.
      (interpose " ")
      (apply str)
      (vector)

      (s/all-rows places-connection)
      (<?)

      (group-by :id)

      (map place->entity)

      (d/<transact! conn)
      (<?))))
