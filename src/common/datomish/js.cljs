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
     [datomish.db :as db]
     [datomish.db-factory :as db-factory]
     [datomish.pair-chan]
     [datomish.sqlite :as sqlite]
     [datomish.js-sqlite :as js-sqlite]
     [datomish.transact :as transact]))

(defn- take-pair-as-promise! [ch]
  ;; Just like take-as-promise!, but aware that it's handling a pair channel.
  (promise
    (fn [resolve reject]
      (letfn [(split-pair [[v e]]
                (if e
                  (reject e)
                  (resolve v)))]
        (cljs.core.async/take! ch split-pair)))))

;; Public API.

(defn ^:export open [path]
  ;; Eventually, URI.  For now, just a plain path (no file://).
  (take-pair-as-promise!
    (go-pair
      (let [conn (<? (sqlite/<sqlite-connection path))
            db (<? (db-factory/<db-with-sqlite-connection conn))]
        (let [c (transact/connection-with-db db)]
        (clj->js
          {:conn c
           :close (fn [] (db/close-db db))
           :toString (fn [] (str "#<DB " path ">"))
           :path path}))))))

(defn ^:export q [query & sources]
  (let [query   (cljs.reader/read-string query)]
    (clj->js query)))
