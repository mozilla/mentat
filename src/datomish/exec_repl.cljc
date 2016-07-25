;; This Source Code Form is subject to the terms of the Mozilla Public
;; License, v. 2.0. If a copy of the MPL was not distributed with this
;; file, You can obtain one at http://mozilla.org/MPL/2.0/.

(ns datomish.exec-repl
  #?(:cljs
     (:require-macros
      [datomish.util :refer [while-let]]
      [datomish.pair-chan :refer [go-pair <?]]
      [cljs.core.async.macros :refer [go]]))
  (:require
     [datomish.db :as db]
     [datomish.sqlite :as s]
     [datomish.sqlite-schema :as ss]
     [datomish.query :as dq]
     [datomish.transact :as transact]
     #?@(:clj
           [[datomish.jdbc-sqlite]
            [datomish.pair-chan :refer [go-pair <?]]
            [datomish.util :refer [while-let]]
            [clojure.core.async :refer [<!]]])
     #?@(:cljs
           [[datomish.promise-sqlite]
            [datomish.pair-chan]
            [datomish.util]
            [cljs.core.async :refer [<!]]])))

#?(:clj
   (defn pair-channel->lazy-seq
     "Returns a blocking lazy sequence of items taken from the provided channel."
     [channel]
     (lazy-seq
       (when-let [v (clojure.core.async/<!! channel)]
         (if (second v)
           (cons v nil)
           (cons v (pair-channel->lazy-seq channel)))))))

#?(:clj
(defn run-to-pair-seq
  "Given an open database, returns a lazy sequence of results.
   When fully consumed, underlying resources will be released."
  [db find]
  (pair-channel->lazy-seq (db/<?run db find))))

(defn xxopen []
    (datomish.pair-chan/go-pair
      (let [d (datomish.pair-chan/<? (s/<sqlite-connection "/tmp/foo.sqlite"))]
        (clojure.core.async/<!! (ss/<ensure-current-version d))
        (def db d))))

;; With an open DB…
#_(run-to-pair-seq
    db
    '[:find ?page :in $ :where [?page :page/starred true ?t]])

;; In a Clojure REPL with no open DB…
#_(clojure.core.async/<!!
    (datomish.exec-repl/<open-and-run-to-seq-promise
      "/tmp/foo.sqlite"
      '[:find ?page :in $ :where [?page :page/starred true ?t]]))

#_
(go-pair
  (let [connection (<? (s/<sqlite-connection "/tmp/foo.sqlite"))
        d (<? (db/<with-sqlite-connection connection))]
    (println
      "Result: "
      (<! (db/<?q d '[:find ?page :in $ :where [?page :page/starred true ?t]] {})))))


#_
(go-pair
  (let [connection (<? (s/<sqlite-connection "/tmp/foo.sqlite"))
        dd (<? (db/<with-sqlite-connection connection))]
    (def *db* dd)))
#_
(clojure.core.async/<!!
(go-pair
(let [now -1
      forms (mapcat (fn [i]
                      (map (fn [j]
                             [:db/add i :x j true])
                           (range 1000 (* i 2000) i)))
                    (range 1 10))]
  (println "Adding" (count forms) "forms")
  (<? (transact/<transact! *db* forms nil now)))))

#_
(go-pair
  (let [connection (<? (s/<sqlite-connection "/tmp/foo.sqlite"))
        dd (<? (db/<with-sqlite-connection connection))]
    (println
       (count
         (<? (db/<?q dd
                     '[:find ?e ?v :in $ :where
                       [?e :x ?v]
                       #_[(> ?v 1000)]] {}))))))
