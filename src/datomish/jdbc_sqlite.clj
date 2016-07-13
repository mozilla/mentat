;; This Source Code Form is subject to the terms of the Mozilla Public
;; License, v. 2.0. If a copy of the MPL was not distributed with this
;; file, You can obtain one at http://mozilla.org/MPL/2.0/.

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
