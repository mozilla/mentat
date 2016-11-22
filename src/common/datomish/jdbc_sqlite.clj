;; Copyright 2016 Mozilla
;;
;; Licensed under the Apache License, Version 2.0 (the "License"); you may not use
;; this file except in compliance with the License. You may obtain a copy of the
;; License at http://www.apache.org/licenses/LICENSE-2.0
;; Unless required by applicable law or agreed to in writing, software distributed
;; under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
;; CONDITIONS OF ANY KIND, either express or implied. See the License for the
;; specific language governing permissions and limitations under the License.

(ns datomish.jdbc-sqlite
  (:require
   [datomish.pair-chan :refer [go-pair]]
   [datomish.sqlite :as s]
   [clojure.java.jdbc :as j]
   [clojure.core.async :as a]))

(deftype JDBCSQLiteConnection [spec]
  s/ISQLiteConnection
  (-execute!
    [db sql bindings]
    (go-pair
      (j/execute! (.-spec db) (into [sql] bindings) {:transaction? false})))

  (-each
    [db sql bindings row-cb]
    (go-pair
      (let [rows (j/query (.-spec db) (into [sql] bindings))]
        (when row-cb
          (doseq [row rows] (row-cb row)))
        (count rows))))

  (close [db]
    (go-pair
      (.close (:connection (.-spec db))))))

(defn open
  [path & {:keys [mode]}]
  (let [spec {:classname "org.sqlite.JDBC"
              :identifiers identity
              :subprotocol "sqlite"
              :subname path}] ;; TODO: use mode.
    (go-pair
      (->>
        (j/get-connection spec)
        (assoc spec :connection)
        (->JDBCSQLiteConnection)))))

(extend-protocol s/ISQLiteConnectionFactory
  String
  (<sqlite-connection [path]
    (open path))

  java.io.File
  (<sqlite-connection [path]
    (open path)))
