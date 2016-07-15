;; This Source Code Form is subject to the terms of the Mozilla Public
;; License, v. 2.0. If a copy of the MPL was not distributed with this
;; file, You can obtain one at http://mozilla.org/MPL/2.0/.

(ns datomish.exec
  #?(:cljs
     (:require-macros
      [datomish.util :refer [while-let]]
      [datomish.pair-chan :refer [go-pair <?]]
      [cljs.core.async.macros :refer [go]]))
  (:require
     [datomish.sqlite :as s]
     [datomish.sqlite-schema :as ss]
     [datomish.query :as dq]
     #?@(:clj
           [[datomish.jdbc-sqlite]
            [datomish.pair-chan :refer [go-pair <?]]
            [datomish.util :refer [while-let]]
            [clojure.core.async :refer
             [go                   ; macro in cljs.
              <! >! chan close! take!]]])
     #?@(:cljs
           [[datomish.promise-sqlite]
            [datomish.pair-chan]
            [datomish.util]
            [cljs.core.async :as a :refer
             [<! >! chan close! take!]]])))

#_
(defn <run
  "Execute the provided query on the provided DB.
   Returns a transduced channel of results.
   Closes the channel when fully consumed."
  [db find]
  (let [context (dq/find->prepared-context (dq/parse find))
        row-transducer (dq/row-transducer context (dq/sql-projection context))
        chan (chan 50 row-transducer)]

    (s/<all-rows db (dq/context->sql-string context) chan)
    chan))

(defn channel->lazy-seq
  "Returns a blocking lazy sequence of items taken from the provided channel."
  [channel]
  (lazy-seq
   (when-let [v (<!! channel)]
     (cons v (seq!! channel)))))

(defn run-to-seq
  "Given an open database, returns a lazy sequence of results.
   When fully consumed, underlying resources will be released."
  [db find]
  (channel->lazy-seq (<run db find)))

(defn <open-and-run-to-seq-promise
  "Given a path, open the named database, run the provided query,
   and return the results as a sequence wrapped in a promise."
  [path find]
  (go-pair
    (let [d (<? (s/<sqlite-connection path))]
      (try
        (<? (ss/<ensure-current-version d))
        (doall (run-to-seq d find))
        (finally
          (s/close d))))))

(comment
  (defn xxopen []
    (go-pair
      (let [d (<? (j/open "/tmp/foo.sqlite"))]
        (<!! (ss/<ensure-current-version d))
        (def db d)))))

(comment
  (<!!
    (datomish.exec/<open-and-run-to-seq-promise
      "/tmp/foo.sqlite"
      '[:find ?page :in $ :where [?page :page/starred true ?t])))
