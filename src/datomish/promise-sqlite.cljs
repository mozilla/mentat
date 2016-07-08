;; This Source Code Form is subject to the terms of the Mozilla Public
;; License, v. 2.0. If a copy of the MPL was not distributed with this
;; file, You can obtain one at http://mozilla.org/MPL/2.0/.

(ns datomish.promise-sqlite
  (:require
   [datomish.sqlite :as s]
   [cljs.nodejs :as nodejs]))

(def sqlite (nodejs/require "promise-sqlite"))

(defrecord SQLite3Connection [db]
  s/ISQLiteConnection
  (-each!
    [db sql bindings row-cb]
    (let [cb (fn [row]
               (row-cb (js->clj row :keywordize-keys true)))]
      (.each (.-db db) sql (or (clj->js bindings) #js []) (when row-cb cb))))

  (in-transaction!
    [db f]
    (.inTransaction (.-db db) f))

  (close!
    [db]
    (.close (.-db db))))

(defn sqlite-connection
  [path & {:keys [mode]}]
  (.then (.open sqlite.DB path (clj->js {:mode mode})) ->SQLite3Connection))

;; For testing!
(defn- <? [p]
  "Take a function producing a Promise, apply it with the given arguments and return an atom
  containing the result on success.  Throw a JS error on failure."
  (let [r (atom)]
    (.then p
           (fn [result] (reset! r result))
           (fn [e] (throw (new js/Error e))))
    r))

(comment
  (def db (<? (sqlite-connection "/Users/nalexander/test.db" :mode (bit-or 2 4))))
  (def x (<? (s/each! @db "CREATE TABLE IF NOT EXISTS datoms (e BLOB, a BLOB, v BLOB, t BLOB)" nil nil)))
  (def y (<? (s/each! @db "INSERT INTO datoms VALUES (?, ?, ?, ?)" nil :bindings [0, 1, 2, 8])))
  (def z (<? (s/each! @db "SELECT * FROM datoms" (partial println "row"))))
  (def w (<? (s/each! @db "DELETE FROM datoms" nil)))
  )
