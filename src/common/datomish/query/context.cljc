;; This Source Code Form is subject to the terms of the Mozilla Public
;; License, v. 2.0. If a copy of the MPL was not distributed with this
;; file, You can obtain one at http://mozilla.org/MPL/2.0/.

;; A context, very simply, holds on to a default source and some knowledge
;; needed for aggregation.
(ns datomish.query.context
  (:require
     [datascript.parser :as dp
      #?@(:cljs [:refer [FindRel FindColl FindTuple FindScalar]])])
  #?(:clj
       (:import
          [datascript.parser FindRel FindColl FindTuple FindScalar])))

(defrecord Context
  [
   default-source
   find-spec       ; The parsed find spec. Used to decide how to process rows.
   elements        ; A list of Element instances, drawn from the :find-spec itself.
   has-aggregates?
   group-by-vars   ; A list of variables from :find and :with, used to generate GROUP BY.
   order-by-vars   ; A list of projected variables and directions, e.g., [:date :asc], [:_max_timestamp :desc].
   limit           ; The limit to apply to the final results of the query. Only makes sense with ORDER BY.
   cc              ; The main conjoining clause.
   ])

(defn scalar-or-tuple-query? [context]
  (when-let [find-spec (:find-spec context)]
    (or (instance? FindScalar find-spec)
        (instance? FindTuple find-spec))))

(defn make-context [source]
  (->Context source nil nil false nil nil nil nil))
