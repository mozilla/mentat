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
     [datomish.exec :as exec]
     [datomish.sqlite :as s]
     [datomish.sqlite-schema :as ss]
     [datomish.query :as dq]
     #?@(:clj
           [[datomish.jdbc-sqlite]
            [datomish.pair-chan :refer [go-pair <?]]
            [datomish.util :refer [while-let]]
            [clojure.core.async]])
     #?@(:cljs
           [[datomish.promise-sqlite]
            [datomish.pair-chan]
            [datomish.util]])))

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
  (pair-channel->lazy-seq (exec/<?run db find))))

#_(defn xxopen []
    (datomish.pair-chan/go-pair
      (let [d (datomish.pair-chan/<? (s/<sqlite-connection "/tmp/foo.sqlite"))]
        (clojure.core.async/<!! (ss/<ensure-current-version d))
        (def db d))))

;; With an open DB…
#_(datomish.exec/run-to-pair-seq
    db
    '[:find ?page :in $ :where [?page :page/starred true ?t]])

;; In a Clojure REPL with no open DB…
#_(clojure.core.async/<!!
    (datomish.exec-repl/<open-and-run-to-seq-promise
      "/tmp/foo.sqlite"
      '[:find ?page :in $ :where [?page :page/starred true ?t]]))

#_(defn test-cljs []
    (datomish.pair-chan/go-pair
      (let [d (datomish.pair-chan/<? (s/<sqlite-connection "/tmp/foo.sqlite"))]
        (cljs.core.async/<! (ss/<ensure-current-version d))
        (let [chan (exec/<?run d
                               '[:find ?page :in $ :where [?page :page/starred true ?t]])]
          (println (datomish.pair-chan/<? chan))
          (println (datomish.pair-chan/<? chan))
          (println (datomish.pair-chan/<? chan))))))

