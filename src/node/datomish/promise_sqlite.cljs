;; Copyright 2016 Mozilla
;;
;; Licensed under the Apache License, Version 2.0 (the "License"); you may not use
;; this file except in compliance with the License. You may obtain a copy of the
;; License at http://www.apache.org/licenses/LICENSE-2.0
;; Unless required by applicable law or agreed to in writing, software distributed
;; under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
;; CONDITIONS OF ANY KIND, either express or implied. See the License for the
;; specific language governing permissions and limitations under the License.

(ns datomish.promise-sqlite
  (:require
   [datomish.sqlite :as s]
   [datomish.cljify :refer [cljify]]
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
               (row-cb (cljify row)))]
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
