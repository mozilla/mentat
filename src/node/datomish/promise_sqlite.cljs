;; This Source Code Form is subject to the terms of the Mozilla Public
;; License, v. 2.0. If a copy of the MPL was not distributed with this
;; file, You can obtain one at http://mozilla.org/MPL/2.0/.

(ns datomish.promise-sqlite
  (:require
   [datomish.sqlite :as s]
   [cljs-promises.async]
   [cljs.nodejs :as nodejs]))

(def sqlite (nodejs/require "promise-sqlite"))

(defrecord SQLite3Connection [db]
  s/ISQLiteConnection
  (-execute!
    [db sql bindings]
    (cljs-promises.async/pair-port
      (.run (.-db db) sql (or (clj->js bindings) #js []))))

  (-each
    [db sql bindings row-cb]
    (let [cb (fn [row]
               (row-cb (js->clj row :keywordize-keys true)))]
      (cljs-promises.async/pair-port
        (.each (.-db db) sql (or (clj->js bindings) #js []) (when row-cb cb)))))

  (close
    [db]
    (cljs-promises.async/pair-port
      (.close (.-db db)))))

(defn open
  [path & {:keys [mode] :or {mode 6}}]
  (cljs-promises.async/pair-port
    (->
      (.open sqlite.DB path (clj->js {:mode mode}))
      (.then ->SQLite3Connection))))
