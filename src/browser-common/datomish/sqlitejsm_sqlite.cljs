;; This Source Code Form is subject to the terms of the Mozilla Public
;; License, v. 2.0. If a copy of the MPL was not distributed with this
;; file, You can obtain one at http://mozilla.org/MPL/2.0/.

(ns datomish.sqlitejsm-sqlite
  (:require
   [cljs-promises.async]
   [datomish.sqlite :as s]
   [datomish.sqlite-module :refer [sqlite]]))

;; mozIStorageRow instances expose two methods: getResultByIndex and getResultByName.
;; Our code expects to treat rows as associative containers, from keyword to value.
;; So we implement ILookup (which has a different signature for ClojureScript than
;; Clojure!), hope that we handle nil/NULL correctly, and switch between integers
;; and keywords.
(deftype
  StorageRow
  [row]

  ILookup
  (-lookup [o k]
    (-lookup o k nil))

  (-lookup [o k not-found]
    (or (if (integer? k)
          (.getResultByIndex row k)
          (.getResultByName row (clj->js (name k))))
        not-found)))

(defrecord SQLite3Connection [db]
  s/ISQLiteConnection
  (-execute!
    [db sql bindings]
    (cljs-promises.async/pair-port
      (.execute (.-db db) sql (or (clj->js bindings) #js []))))

  (-each
    [db sql bindings row-cb]
    (let [cb (fn [row]
               (row-cb (StorageRow. row)))]
      (cljs-promises.async/pair-port
        (.execute (.-db db) sql (or (clj->js bindings) #js []) (when row-cb cb)))))

  (close
    [db]
    (cljs-promises.async/pair-port
      (.close (.-db db)))))

(defn open
  [path & {:keys [mode] :or {mode 6}}]
  (cljs-promises.async/pair-port
    (->
      (.openConnection (aget sqlite "Sqlite") (clj->js {:path path :sharedMemoryCache false}))
      (.then ->SQLite3Connection))))
