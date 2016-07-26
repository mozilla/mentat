;; This Source Code Form is subject to the terms of the Mozilla Public
;; License, v. 2.0. If a copy of the MPL was not distributed with this
;; file, You can obtain one at http://mozilla.org/MPL/2.0/.

(ns datomish.query.source
  (:require
   [datomish.query.transforms :as transforms]))

;;;
;;; A source is something that can match patterns. For example:
;;;
;;; * The database itself.
;;; * The history of the database.
;;; * A filtered version of the database or the history.
;;;
;;; We model this in a SQL context as something that can:
;;;
;;; * Give us a table name.
;;; * Give us a new alias for the table name.
;;; * Provide us with a list of columns to match, positionally,
;;;   against patterns.
;;; * Provide us with a set of WHERE fragments that, in combination
;;;   with the table name, denote the source.
;;; * Transform constants and attributes into something usable
;;;   by the source.

(defrecord
  Source
  [table               ; e.g., :datoms
   columns             ; e.g., [:e :a :v :tx]

   ;; `attribute-transform` is a function from attribute to constant value. Used to
   ;; turn, e.g., :p/attribute into an interned integer.
   ;; `constant-transform` is a function from constant value to constant value. Used to
   ;; turn, e.g., the literal 'true' into 1.
   attribute-transform
   constant-transform
   
   ;; `table-alias` is a function from table to alias, e.g., :datoms => :datoms1234.
   table-alias

   ;; Not currently used.
   make-constraints    ; ?fn [source alias] => [where-clauses]
   ])

(defn gensym-table-alias [table]
  (gensym (name table)))

(defn datoms-source [db]
  (->Source :datoms
            [:e :a :v :tx :added]
            transforms/attribute-transform-string
            transforms/constant-transform-default
            gensym-table-alias
            nil))

(defn source->from [source]
  (let [table (:table source)]
    [table ((:table-alias source) table)]))

(defn source->constraints [source alias]
  (when-let [f (:make-constraints source)]
    (f alias)))

(defn attribute-in-source [source attribute]
  ((:attribute-transform source) attribute))

(defn constant-in-source [source constant]
  ((:constant-transform source) constant))
