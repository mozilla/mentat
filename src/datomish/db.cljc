;; This Source Code Form is subject to the terms of the Mozilla Public
;; License, v. 2.0. If a copy of the MPL was not distributed with this
;; file, You can obtain one at http://mozilla.org/MPL/2.0/.

(ns datomish.db
  #?(:cljs
     (:require-macros
      [datomish.pair-chan :refer [go-pair <?]]
      [cljs.core.async.macros :refer [go]]))
  (:require
   [datomish.context :as context]
   [datomish.projection :as projection]
   [datomish.query :as query]
   [datomish.source :as source]
   [datomish.sqlite :as s]
   [datomish.sqlite-schema :as sqlite-schema]
   [datomish.util :as util :refer [raise raise-str]]
   #?@(:clj [[datomish.pair-chan :refer [go-pair <?]]
             [clojure.core.async :as a :refer [chan go <! >!]]])
   #?@(:cljs [[datomish.pair-chan]
              [cljs.core.async :as a :refer [chan <! >!]]])))

(defprotocol IDB
  (idents
    [db]
    "Return map {ident -> entid} if known idents.  See http://docs.datomic.com/identity.html#idents.")

  (query-context
    [db])

  (close
    [db]
    "Close this database. Returns a pair channel of [nil error]."))

(defn db? [x]
  (and (satisfies? IDB x)))

;; TODO: implement support for DB parts?
(def tx0 0x2000000)

;; TODO: write tx-meta to transaction.
(defrecord TxReport [tx-data tempids tx-meta])

;; TODO: persist max-tx and max-eid in SQLite.
(defn <allocate-tx [db]
  (go-pair
    (swap! (:current-tx db) inc)))

;; TODO: add fancy destructuring.
;; TODO: handle reading.
(deftype Datom [e a v tx added])

(defn datom? [x] (instance? Datom x))

(defrecord DB [sqlite-connection idents max-tx]
  IDB
  (idents [db] @(:idents db))
  (query-context [db] (context/->Context (source/datoms-source db) nil nil))
  (close [db] (s/close (.-sqlite-connection db))))

(defn <with-sqlite-connection [sqlite-connection]
  (go-pair
    (when-not (= sqlite-schema/current-version (<? (sqlite-schema/<ensure-current-version sqlite-connection)))
      (raise-str "Could not ensure current SQLite schema version."))
    (map->DB {:sqlite-connection sqlite-connection
              :idents            (atom {:db/txInstant 100 :x 101 :y 102}) ;; TODO: pre-populate idents and SQLite tables?
              :current-tx        (atom (dec tx0))}))) ;; TODO: get rid of dec.



#?(:clj
   (defmethod print-method Datom [^Datom d, ^java.io.Writer w]
     (.write w (str "#datomish/Datom "))
     (binding [*out* w]
       (pr [(.-e d) (.-a d) (.-v d) (.-tx d) (.-added d)]))))

;; TODO: implement schemas.
(defn multival? [db attr] false)

;; TODO: implement schemas.
(defn ref? [db attr] false)

(defn <entid [db eid]
  {:pre [(db? db)]}
  (go-pair
    (cond
      (number? eid)
      eid

      (keyword? eid)
      ;; Turn ident into entid if possible.
      (get (idents db) eid eid)

      (sequential? eid)
      (raise "Lookup ref for entity id not yet supported, got " eid
             {:error :entity-id/syntax
              :entity-id eid})

      :else
      (raise "Expected number or lookup ref for entity id, got " eid
             {:error :entity-id/syntax
              :entity-id eid}))))

(defn <entid-strict [db eid]
  {:pre [(db? db)]}
  (go-pair
    (or (<? (<entid db eid))
        (raise "Nothing found for entity id " eid
               {:error :entity-id/missing
                :entity-id eid}))))

(defn <entid-some [db eid]
  {:pre [(db? db)]}
  (go-pair
    (when eid
      (<? (<entid-strict db eid)))))

(defn <?run
  "Execute the provided query on the provided DB.
   Returns a transduced channel of [result err] pairs.
   Closes the channel when fully consumed."
  [db find args]
  (let [parsed (query/parse find)
        context (-> db
                  query-context
                  (query/find-into-context parsed))
        row-pair-transducer (projection/row-pair-transducer context)
        sql (query/context->sql-string context args)
        chan (chan 50 row-pair-transducer)]

    (s/<?all-rows (.-sqlite-connection db) sql chan)
    chan))

(defn reduce-error-pair [f [rv re] [v e]]
  (if re
    [nil re]
    (if e
      [nil e]
      [(f rv v) nil])))

(defn <?q
  "Execute the provided query on the provided DB.
   Returns a transduced pair-chan with one [[results] err] item."
  [db find args]
  (a/reduce (partial reduce-error-pair conj) [[] nil]
            (<?run db find args)))
