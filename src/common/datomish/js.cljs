;; This Source Code Form is subject to the terms of the Mozilla Public
;; License, v. 2.0. If a copy of the MPL was not distributed with this
;; file, You can obtain one at http://mozilla.org/MPL/2.0/.

(ns datomish.js
  (:refer-clojure :exclude [])
  (:require-macros
     [datomish.pair-chan :refer [go-pair <?]])
  (:require
     [cljs.core.async :as a :refer [take! <! >!]]
     [cljs.reader]
     [cljs-promises.core :refer [promise]]
     [datomish.cljify :refer [cljify]]
     [datomish.db :as db]
     [datomish.db-factory :as db-factory]
     [datomish.pair-chan]
     [datomish.sqlite :as sqlite]
     [datomish.simple-schema :as simple-schema]
     [datomish.js-sqlite :as js-sqlite]
     [datomish.transact :as transact]))

(defn- take-pair-as-promise! [ch f]
  ;; Just like take-as-promise!, but aware that it's handling a pair channel.
  ;; Also converts values, if desired.
  (promise
    (fn [resolve reject]
      (letfn [(split-pair [[v e]]
                (if e
                  (do
                    (println "Got error:" e)
                    (reject e))
                  (resolve (f v))))]
        (cljs.core.async/take! ch split-pair)))))

;; Public API.

(defn ^:export db [conn]
  (transact/db conn))

(defn ^:export q [db find options]
  (let [find (cljs.reader/read-string find)
        opts (cljify options)]
    (println "Running query " (pr-str find) (pr-str {:foo find}) (pr-str opts))
    (take-pair-as-promise!
      (go-pair
        (let [res (<? (db/<?q db find opts))]
          (println "Got results: " (pr-str res))
          (clj->js res)))
      identity)))

(defn ^:export ensure-schema [conn simple-schema]
  (let [simple-schema (cljify simple-schema)
        datoms (simple-schema/simple-schema->schema simple-schema)]
    (println "Transacting schema datoms" (pr-str datoms))
    (take-pair-as-promise!
      (transact/<transact!
        conn
        datoms)
      clj->js)))

(def js->tx-data cljify)

(def ^:export tempid (partial db/id-literal :db.part/user))

(defn ^:export transact [conn tx-data]
  ;; Expects a JS array as input.
  (try
    (let [tx-data (js->tx-data tx-data)]
      (println "Transacting:" (pr-str tx-data))
      (take-pair-as-promise!
        (go-pair
          (let [tx-result (<? (transact/<transact! conn tx-data))]
            (select-keys tx-result
                         [:tempids
                          :added-idents
                          :added-attributes
                          :tx
                          :txInstant])))
        clj->js))
    (catch js/Error e
      (println "Error in transact:" e))))

(defn ^:export open [path]
  ;; Eventually, URI.  For now, just a plain path (no file://).
  (take-pair-as-promise!
    (go-pair
      (let [conn (<? (sqlite/<sqlite-connection path))
            db (<? (db-factory/<db-with-sqlite-connection conn))]
        (let [c (transact/connection-with-db db)]
          (clj->js
            ;; We pickle the connection as a thunk here so it roundtrips through JS
            ;; without incident.
            {:conn (fn [] c)
             :roundtrip (fn [x] (clj->js (cljify x)))
             :db (fn [] (transact/db c))
             :ensureSchema (fn [simple-schema] (ensure-schema c simple-schema))
             :transact (fn [tx-data] (transact c tx-data))
             :close (fn [] (db/close-db db))
             :toString (fn [] (str "#<DB " path ">"))
             :path path}))))
    identity))
