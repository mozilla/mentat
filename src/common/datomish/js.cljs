;; This Source Code Form is subject to the terms of the Mozilla Public
;; License, v. 2.0. If a copy of the MPL was not distributed with this
;; file, You can obtain one at http://mozilla.org/MPL/2.0/.

(ns datomish.js
  (:refer-clojure :exclude [])
  (:require-macros
     [datomish.pair-chan :refer [go-pair <?]]
     [datomish.promises :refer [go-promise]])
  (:require
     [cljs.core.async :as a :refer [take! <! >!]]
     [cljs.reader]
     [cljs-promises.core :refer [promise]]
     [datomish.cljify :refer [cljify]]
     [datomish.api :as d]
     [datomish.db :as db]
     [datomish.db-factory :as db-factory]
     [datomish.pair-chan]
     [datomish.promises :refer [take-pair-as-promise!]]
     [datomish.sqlite :as sqlite]
     [datomish.simple-schema :as simple-schema]
     [datomish.js-sqlite :as js-sqlite]
     [datomish.transact :as transact]))


;; Public API.

(def ^:export db d/db)

(defn ^:export q [db find options]
  (let [find (cljs.reader/read-string find)
        opts (cljify options)]
    (take-pair-as-promise!
      (d/<q db find opts)
      clj->js)))

(defn ^:export ensure-schema [conn simple-schema]
  (let [simple-schema (cljify simple-schema)
        datoms (simple-schema/simple-schema->schema simple-schema)]
    (println "Transacting schema datoms" (pr-str datoms))
    (take-pair-as-promise!
      (d/<transact!
        conn
        datoms)
      clj->js)))

(def js->tx-data cljify)

(def ^:export tempid (partial db/id-literal :db.part/user))

(defn ^:export transact [conn tx-data]
  ;; Expects a JS array as input.
  (try
    (let [tx-data (js->tx-data tx-data)]
      (go-promise clj->js
        (let [tx-result (<? (d/<transact! conn tx-data))]
          (select-keys tx-result
                       [:tempids
                        :added-idents
                        :added-attributes
                        :tx
                        :txInstant]))))
    (catch js/Error e
      (println "Error in transact:" e))))

(defn ^:export open [path]
  ;; Eventually, URI.  For now, just a plain path (no file://).
  (go-promise clj->js
    (let [conn (<? (sqlite/<sqlite-connection path))
          db (<? (db-factory/<db-with-sqlite-connection conn))]
      (let [c (transact/connection-with-db db)]
        ;; We pickle the connection as a thunk here so it roundtrips through JS
        ;; without incident.
        {:conn (fn [] c)
         :db (fn [] (d/db c))
         :path path

         ;; Primary API.
         :ensureSchema (fn [simple-schema] (ensure-schema c simple-schema))
         :transact (fn [tx-data] (transact c tx-data))
         :q (fn [find opts] (q (d/db c) find opts))
         :close (fn [] (db/close-db db))

         ;; Some helpers for testing the bridge.
         :equal =
         :idx (fn [tempid] (:idx tempid))
         :cljify cljify
         :roundtrip (fn [x] (clj->js (cljify x)))

         :toString (fn [] (str "#<DB " path ">"))
         }))))
