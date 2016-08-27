;; This Source Code Form is subject to the terms of the Mozilla Public
;; License, v. 2.0. If a copy of the MPL was not distributed with this
;; file, You can obtain one at http://mozilla.org/MPL/2.0/.

(ns datomish.query.source
  (:require
   [datomish.query.transforms :as transforms]
   [datomish.schema :as schema]
   [datomish.util :as util #?(:cljs :refer-macros :clj :refer) [raise raise-str]]
   [datascript.parser
    #?@(:cljs
        [:refer [Variable Constant Placeholder]])])
  #?(:clj
     (:import [datascript.parser Variable Constant Placeholder])))

(defn gensym-table-alias [table]
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
  (pattern->schema-value-type [source pattern])
  (attribute-in-source [source attribute])
  (constant-in-source [source constant]))

(defrecord
    DatomsSource
    [table               ; Typically :datoms.
     fulltext-table      ; Typically :fulltext_values
     fulltext-view       ; Typically :all_datoms
     columns             ; e.g., [:e :a :v :tx]
     schema              ; An ISchema instance.

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
    (let [schema (:schema source)
          int->table (fn [a]
                       (if (schema/fulltext? schema a)
                         (:fulltext-table source)
                         (:table source)))
          table
          (cond
            (integer? attribute)
            (int->table attribute)

            (instance? Constant attribute)
            (let [a (:value attribute)
                  id (if (keyword? a)
                       (attribute-in-source source a)
                       a)]
              (int->table id))

            ;; TODO: perhaps we know an external binding already?
            (or (instance? Variable attribute)
                (instance? Placeholder attribute))
            ;; It's variable. We must act as if it could be a fulltext datom.
            (:fulltext-view source)

            true
            (raise "Unknown source->from attribute " attribute {:attribute attribute}))]
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

  (pattern->schema-value-type [source pattern]
    (let [[_ a v _] pattern
          schema (:schema (:schema source))]
      (when (instance? Constant a)
        (let [val (:value a)]
          (if (keyword? val)
            ;; We need to find the entid for the keyword attribute,
            ;; because the schema stores attributes by ID.
            (let [id (attribute-in-source source val)]
              (get-in schema [id :db/valueType]))
            (when (integer? val)
              (get-in schema [val :db/valueType])))))))

  (attribute-in-source [source attribute]
    ((:attribute-transform source) attribute))

  (constant-in-source [source constant]
    ((:constant-transform source) constant)))
