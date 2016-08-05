;; This Source Code Form is subject to the terms of the Mozilla Public
;; License, v. 2.0. If a copy of the MPL was not distributed with this
;; file, You can obtain one at http://mozilla.org/MPL/2.0/.

(ns datomish.query.source
  (:require
     [datomish.query.transforms :as transforms]
     [datascript.parser
      #?@(:cljs
            [:refer [Variable Constant Placeholder]])])
  #?(:clj
       (:import [datascript.parser Variable Constant Placeholder])))

(defn- gensym-table-alias [table]
  (gensym (name table)))

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

(defprotocol Source
  (source->from [source attribute]
    "Returns a pair, `[table alias]` for a pattern with the provided attribute.")
  (source->non-fulltext-from [source])
  (source->fulltext-from [source]
    "Returns a pair, `[table alias]` for querying the source's fulltext index.")
  (source->constraints [source alias])
  (attribute-in-source [source attribute])
  (constant-in-source [source constant]))

(defrecord
  DatomsSource
  [table               ; Typically :datoms.
   fulltext-table           ; Typically :fulltext_values
   fulltext-view            ; Typically :fulltext_datoms.
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
   ]
  Source

  (source->from [source attribute]
    (let [table
          (if (and (instance? Constant attribute)
                   ;; TODO: look in the DB schema to see if `attribute` is known to not be
                   ;; a fulltext attribute.
                   true)
            (:table source)

            ;; It's variable. We must act as if it could be a fulltext datom.
            (:fulltext-view source))]
      [table ((:table-alias source) table)]))

  (source->non-fulltext-from [source]
    (let [table (:table source)]
      [table ((:table-alias source) table)]))

  (source->fulltext-from [source]
    (let [table (:fulltext-table source)]
      [table ((:table-alias source) table)]))

  (source->constraints [source alias]
    (when-let [f (:make-constraints source)]
      (f alias)))

  (attribute-in-source [source attribute]
    ((:attribute-transform source) attribute))

  (constant-in-source [source constant]
    ((:constant-transform source) constant)))

(defn datoms-source [db]
  (map->DatomsSource
    {:table :datoms
     :fulltext-table :fulltext_values
     :fulltext-view :fulltext_datoms
     :columns [:e :a :v :tx :added]
     :attribute-transform transforms/attribute-transform-string
     :constant-transform transforms/constant-transform-default
     :table-alias gensym-table-alias
     :make-constraints nil}))

