;; This Source Code Form is subject to the terms of the Mozilla Public
;; License, v. 2.0. If a copy of the MPL was not distributed with this
;; file, You can obtain one at http://mozilla.org/MPL/2.0/.

(ns datomish.sqlite-schema
  #?(:cljs
     (:require-macros
      [datomish.pair-chan :refer [go-pair <?]]
      [cljs.core.async.macros :refer [go]]))
  (:require
   [datomish.util :as util #?(:cljs :refer-macros :clj :refer) [raise-str cond-let]]
   [datomish.sqlite :as s]
   #?@(:clj [[datomish.pair-chan :refer [go-pair <?]]
             [clojure.core.async :refer [go <! >!]]])
   #?@(:cljs [[datomish.pair-chan]
              [cljs.core.async :as a :refer [<! >!]]])))

(def current-version 1)

(def v1-statements
  ["CREATE TABLE datoms (e INTEGER NOT NULL, a SMALLINT NOT NULL, v BLOB NOT NULL, tx INTEGER NOT NULL,
                         index_avet TINYINT NOT NULL DEFAULT 0, index_vaet TINYINT NOT NULL DEFAULT 0,
                         index_fulltext TINYINT NOT NULL DEFAULT 0,
                         unique_value TINYINT NOT NULL DEFAULT 0, unique_identity TINYINT NOT NULL DEFAULT 0)"
   "CREATE INDEX idx_datoms_eavt ON datoms (e, a, v)"
   "CREATE INDEX idx_datoms_aevt ON datoms (a, e, v)"

   ;; Opt-in index: only if a has :db/index true.
   "CREATE UNIQUE INDEX idx_datoms_avet ON datoms (a, v, e) WHERE index_avet IS NOT 0"

   ;; Opt-in index: only if a has :db/valueType :db.type/ref.
   "CREATE UNIQUE INDEX idx_datoms_vaet ON datoms (v, a, e) WHERE index_vaet IS NOT 0"

   ;; Opt-in index: only if a has :db/fulltext true; thus, it has :db/valueType :db.type/string,
   ;; which is not :db/valueType :db.type/ref.  That is, index_vaet and index_fulltext are mutually
   ;; exclusive.
   "CREATE INDEX idx_datoms_fulltext ON datoms (v, a, e) WHERE index_fulltext IS NOT 0"

   ;; TODO: possibly remove this index.  :db.unique/value should be asserted by the transactor in
   ;; all cases, but the index may speed up some of SQLite's query planning.  For now, it services
   ;; to validate the transactor implementation.
   "CREATE UNIQUE INDEX idx_datoms_unique_value ON datoms (v) WHERE unique_value IS NOT 0"
   ;; TODO: possibly remove this index.  :db.unique/identity should be asserted by the transactor in
   ;; all cases, but the index may speed up some of SQLite's query planning.  For now, it serves to
   ;; validate the transactor implementation.
   "CREATE UNIQUE INDEX idx_datoms_unique_identity ON datoms (a, v) WHERE unique_identity IS NOT 0"

   "CREATE TABLE transactions (e INTEGER NOT NULL, a SMALLINT NOT NULL, v BLOB NOT NULL, tx INTEGER NOT NULL, added TINYINT NOT NULL DEFAULT 1)"
   "CREATE INDEX idx_transactions_tx ON transactions (tx)"

   ;; Fulltext indexing.
   ;; A fulltext indexed value v is an integer rowid referencing fulltext_values.

   ;; Optional settings:
   ;; tokenize="porter"
   ;; prefix='2,3'
   ;; By default we use Unicode-aware tokenizing (particularly for case folding), but preserve
   ;; diacritics.
   "CREATE VIRTUAL TABLE fulltext_values
     USING FTS4 (text NOT NULL, tokenize=unicode61 \"remove_diacritics=0\")"

   ;; A view transparently interpolating fulltext indexed values into the datom structure.
   "CREATE VIEW fulltext_datoms AS
     SELECT e, a, fulltext_values.text AS v, tx, index_avet, index_vaet, index_fulltext, unique_value, unique_identity
       FROM datoms, fulltext_values
       WHERE datoms.index_fulltext IS NOT 0 AND datoms.v = fulltext_values.rowid"

   ;; A view transparently interpolating all entities (fulltext and non-fulltext) into the datom structure.
   "CREATE VIEW all_datoms AS
     SELECT e, a, v, tx, index_avet, index_vaet, index_fulltext, unique_value, unique_identity
       FROM datoms
       WHERE index_fulltext IS 0
     UNION ALL
     SELECT e, a, v, tx, index_avet, index_vaet, index_fulltext, unique_value, unique_identity
       FROM fulltext_datoms"

   ;; Materialized views of the schema.
   "CREATE TABLE idents (ident TEXT NOT NULL PRIMARY KEY, entid INTEGER UNIQUE NOT NULL)"
   "CREATE TABLE schema (ident TEXT NOT NULL, attr TEXT NOT NULL, value TEXT NOT NULL, FOREIGN KEY (ident) REFERENCES idents (ident))"
   "CREATE INDEX idx_schema_unique ON schema (ident, attr, value)"
   ])

(defn <create-current-version
  [db]
  (->>
    #(go-pair
       (doseq [statement v1-statements]
         (try
           (<? (s/execute! db [statement]))
           (catch #?(:clj Throwable :cljs js/Error) e
             (throw (ex-info "Failed to execute statement" {:statement statement} e)))))
       (<? (s/set-user-version db current-version))
       (<? (s/get-user-version db)))
    (s/in-transaction! db)))

(defn <update-from-version
  [db from-version]
  {:pre [(> from-version 0)]} ;; Or we'd create-current-version instead.
  {:pre [(< from-version current-version)]} ;; Or we wouldn't need to update-from-version.
  (go-pair
    (raise-str "No migrations yet defined!")
    (<? (s/set-user-version db current-version))
    (<? (s/get-user-version db))))

(defn <ensure-current-version
  [db]
  (go-pair
    (let [v (<? (s/get-user-version db))]
      (cond
        (= v current-version)
        v

        (= v 0)
        (<? (<create-current-version db))

        (< v current-version)
        (<? (<update-from-version db v))))))
