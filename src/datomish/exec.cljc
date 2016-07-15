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

(defn <?run
  "Execute the provided query on the provided DB.
   Returns a transduced channel of [result err] pairs.
   Closes the channel when fully consumed."
  [db find]
  (let [initial-context (dq/make-context)
        context (dq/expand-find-into-context initial-context (dq/parse find))
        row-pair-transducer (dq/row-pair-transducer context (dq/sql-projection context))
        chan (chan 50 row-pair-transducer)]

    (s/<?all-rows db (dq/context->sql-string context) chan)
    chan))
