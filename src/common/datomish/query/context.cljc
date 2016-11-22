;; Copyright 2016 Mozilla
;;
;; Licensed under the Apache License, Version 2.0 (the "License"); you may not use
;; this file except in compliance with the License. You may obtain a copy of the
;; License at http://www.apache.org/licenses/LICENSE-2.0
;; Unless required by applicable law or agreed to in writing, software distributed
;; under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
;; CONDITIONS OF ANY KIND, either express or implied. See the License for the
;; specific language governing permissions and limitations under the License.

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
