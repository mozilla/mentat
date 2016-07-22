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
   [datomish.util :as util :refer [raise-str]]
   #?@(:clj [[datomish.pair-chan :refer [go-pair <?]]
             [clojure.core.async :as a :refer [chan go <! >!]]])
   #?@(:cljs [[datomish.pair-chan]
              [cljs.core.async :as a :refer [chan <! >!]]])))

(defprotocol IDB
  (query-context
    [db])
  (close
    [db]
    "Close this database. Returns a pair channel of [nil error]."))

(defrecord DB [sqlite-connection]
  IDB
  (query-context [db] (context/->Context (source/datoms-source db) nil nil))
  (close [db] (s/close (.-sqlite-connection db))))

(defn <with-sqlite-connection [sqlite-connection]
  (go-pair
    (when-not (= sqlite-schema/current-version (<? (sqlite-schema/<ensure-current-version sqlite-connection)))
      (raise-str "Could not ensure current SQLite schema version."))
    (->DB sqlite-connection)))

(defn <?run
  "Execute the provided query on the provided DB.
   Returns a transduced channel of [result err] pairs.
   Closes the channel when fully consumed."
  [db find args]
  (let [parsed (query/parse find)
        context (-> db
                  query-context
                  (query/expand-find-into-context parsed))
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
    
