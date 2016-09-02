;; This Source Code Form is subject to the terms of the Mozilla Public
;; License, v. 2.0. If a copy of the MPL was not distributed with this
;; file, You can obtain one at http://mozilla.org/MPL/2.0/.

;; A context, very simply, holds on to a default source and some knowledge
;; needed for aggregation.
(ns datomish.query.context)

(defrecord Context
  [
   default-source
   elements        ; The :find list itself.
   has-aggregates?
   group-by-vars   ; A list of variables from :find and :with, used to generate GROUP BY.
   order-by-vars   ; A list of projected variables and directions, e.g., [:date :asc], [:_max_timestamp :desc].
   limit           ; The limit to apply to the final results of the query. Only makes sense with ORDER BY.
   cc              ; The main conjoining clause.
   ])

(defn make-context [source]
  (->Context source nil false nil nil nil nil))
