;; This Source Code Form is subject to the terms of the Mozilla Public
;; License, v. 2.0. If a copy of the MPL was not distributed with this
;; file, You can obtain one at http://mozilla.org/MPL/2.0/.

(ns datomish.repl
  (:require
   [datomish.db :as db]
   [datomish.util :as util :refer [raise]]
   [datomish.sqlite :as s]
   [datomish.jdbc-sqlite :as j]
   [tempfile.core]
   [datomish.pair-chan :refer [go-pair <?]]
   [clojure.core.async :as a :refer [go <! >! <!! >!!]]))

(defn <?? [pair-chan]
  (datomish.pair-chan/consume-pair (<!! pair-chan)))

(defn debug-db [db]
  (<??
    (go-pair
      (let [ds (<? (s/all-rows (:sqlite-connection db) ["SELECT e, a, v, tx FROM datoms"]))]
        (println (count ds) "datoms.")
        (doseq [d ds] (println d)))
      (let [ts (<? (s/all-rows (:sqlite-connection db) ["SELECT * FROM transactions"]))]
        (println (count ts) "transactions.")
        (doseq [t ts] (println t)))))
  db)

(defn reset-db! [db]
  (<??
    (go-pair
      (<? (s/execute! (:sqlite-connection db) ["DELETE FROM datoms"]))
      (<? (s/execute! (:sqlite-connection db) ["DELETE FROM transactions"]))))
  db)

(defn db-with [datoms]
  (go-pair
    (let [c (<? (s/<sqlite-connection (tempfile.core/tempfile)))
          d (<? (db/<with-sqlite-connection c))]
      (<? (db/<transact! d datoms))
      d)))
