;; Copyright 2016 Mozilla
;;
;; Licensed under the Apache License, Version 2.0 (the "License"); you may not use
;; this file except in compliance with the License. You may obtain a copy of the
;; License at http://www.apache.org/licenses/LICENSE-2.0
;; Unless required by applicable law or agreed to in writing, software distributed
;; under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
;; CONDITIONS OF ANY KIND, either express or implied. See the License for the
;; specific language governing permissions and limitations under the License.

(ns datomish.sqlitejsm-sqlite
  (:require
   [cljs-promises.async]
   [datomish.sqlite :as s]))

(def sqlite (.import (aget (js/require "chrome") "Cu") "resource://gre/modules/Sqlite.jsm"))

(println "sqlite is" (pr-str sqlite))

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
