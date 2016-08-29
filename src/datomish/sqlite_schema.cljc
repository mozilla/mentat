;; This Source Code Form is subject to the terms of the Mozilla Public
;; License, v. 2.0. If a copy of the MPL was not distributed with this
;; file, You can obtain one at http://mozilla.org/MPL/2.0/.

(ns datomish.sqlite-schema
  #?(:cljs
     (:require-macros
      [datomish.pair-chan :refer [go-pair <?]]
      [cljs.core.async.macros :refer [go]]))
  (:require
   [datomish.util :as util #?(:cljs :refer-macros :clj :refer) [raise raise-str cond-let]]
   [datomish.sqlite :as s]
   #?@(:clj [[datomish.pair-chan :refer [go-pair <?]]
             [clojure.core.async :refer [go <! >!]]])
   #?@(:cljs [[datomish.pair-chan]
              [cljs.core.async :as a :refer [<! >!]]])))

(def current-version 1)

(def v1-statements
  ["CREATE TABLE datoms (e INTEGER NOT NULL, a SMALLINT NOT NULL, v BLOB NOT NULL, tx INTEGER NOT NULL,
                         value_type_tag SMALLINT NOT NULL,
                         index_avet TINYINT NOT NULL DEFAULT 0, index_vaet TINYINT NOT NULL DEFAULT 0,
                         index_fulltext TINYINT NOT NULL DEFAULT 0,
                         unique_value TINYINT NOT NULL DEFAULT 0)"
   "CREATE UNIQUE INDEX idx_datoms_eavt ON datoms (e, a, value_type_tag, v)"
   "CREATE UNIQUE INDEX idx_datoms_aevt ON datoms (a, e, value_type_tag, v)"

   ;; n.b., v0/value_type_tag0 can be NULL, in which case we look up v from datoms;
   ;; and the datom columns are NULL into the LEFT JOIN fills them in.
   ;; TODO: update comment about sv.
   "CREATE TABLE tx_lookup (e0 INTEGER NOT NULL, a0 SMALLINT NOT NULL, v0 BLOB NOT NULL, tx0 INTEGER NOT NULL, added0 TINYINT NOT NULL,
                         value_type_tag0 SMALLINT NOT NULL,
                         index_avet0 TINYINT, index_vaet0 TINYINT,
                         index_fulltext0 TINYINT,
                         unique_value0 TINYINT,
                         sv BLOB,
                         svalue_type_tag SMALLINT,
                         rid INTEGER,
                         e INTEGER, a SMALLINT, v BLOB, tx INTEGER, value_type_tag SMALLINT)"

   ;; Note that `id_tx_lookup_added` is created and dropped
   ;; after insertion, which makes insertion slightly faster.
   ;; Prevent overlapping transactions.  TODO: drop added0?
   "CREATE UNIQUE INDEX idx_tx_lookup_eavt ON tx_lookup (e0, a0, v0, added0, value_type_tag0) WHERE sv IS NOT NULL"

   ;; Opt-in index: only if a has :db/index true.
   "CREATE UNIQUE INDEX idx_datoms_avet ON datoms (a, value_type_tag, v, e) WHERE index_avet IS NOT 0"

   ;; Opt-in index: only if a has :db/valueType :db.type/ref.  No need for tag here since all
   ;; indexed elements are refs.
   "CREATE UNIQUE INDEX idx_datoms_vaet ON datoms (v, a, e) WHERE index_vaet IS NOT 0"

   ;; Opt-in index: only if a has :db/fulltext true; thus, it has :db/valueType :db.type/string,
   ;; which is not :db/valueType :db.type/ref.  That is, index_vaet and index_fulltext are mutually
   ;; exclusive.
   "CREATE INDEX idx_datoms_fulltext ON datoms (value_type_tag, v, a, e) WHERE index_fulltext IS NOT 0"

   ;; TODO: possibly remove this index.  :db.unique/{value,identity} should be asserted by the
   ;; transactor in all cases, but the index may speed up some of SQLite's query planning.  For now,
   ;; it serves to validate the transactor implementation.  Note that tag is needed here to
   ;; differentiate, e.g., keywords and strings.
   "CREATE UNIQUE INDEX idx_datoms_unique_value ON datoms (a, value_type_tag, v) WHERE unique_value IS NOT 0"

   "CREATE TABLE transactions (e INTEGER NOT NULL, a SMALLINT NOT NULL, v BLOB NOT NULL, tx INTEGER NOT NULL, added TINYINT NOT NULL DEFAULT 1, value_type_tag SMALLINT NOT NULL)"
   "CREATE INDEX idx_transactions_tx ON transactions (tx, added)"

   ;; Fulltext indexing.
   ;; A fulltext indexed value v is an integer rowid referencing fulltext_values.

   ;; Optional settings:
   ;; tokenize="porter"
   ;; prefix='2,3'
   ;; By default we use Unicode-aware tokenizing (particularly for case folding), but preserve
   ;; diacritics.
   "CREATE VIRTUAL TABLE fulltext_values
     USING FTS4 (text NOT NULL, searchid INT, tokenize=unicode61 \"remove_diacritics=0\")"

   ;; This combination of view and triggers allows you to transparently
   ;; update-or-insert into FTS. Just INSERT INTO fulltext_values_view (text, searchid).
   "CREATE VIEW fulltext_values_view AS SELECT * FROM fulltext_values"
   "CREATE TRIGGER replace_fulltext_searchid
   INSTEAD OF INSERT ON fulltext_values_view
   WHEN EXISTS (SELECT 1 FROM fulltext_values WHERE text = new.text)
   BEGIN
     UPDATE fulltext_values SET searchid = new.searchid WHERE text = new.text;
   END"
   "CREATE TRIGGER insert_fulltext_searchid
   INSTEAD OF INSERT ON fulltext_values_view
   WHEN NOT EXISTS (SELECT 1 FROM fulltext_values WHERE text = new.text)
   BEGIN
     INSERT INTO fulltext_values (text, searchid) VALUES (new.text, new.searchid);
   END"

   ;; A view transparently interpolating fulltext indexed values into the datom structure.
   "CREATE VIEW fulltext_datoms AS
     SELECT e, a, fulltext_values.text AS v, tx, value_type_tag, index_avet, index_vaet, index_fulltext, unique_value
       FROM datoms, fulltext_values
       WHERE datoms.index_fulltext IS NOT 0 AND datoms.v = fulltext_values.rowid"

   ;; A view transparently interpolating all entities (fulltext and non-fulltext) into the datom structure.
   "CREATE VIEW all_datoms AS
     SELECT e, a, v, tx, value_type_tag, index_avet, index_vaet, index_fulltext, unique_value
       FROM datoms
       WHERE index_fulltext IS 0
     UNION ALL
     SELECT e, a, v, tx, value_type_tag, index_avet, index_vaet, index_fulltext, unique_value
       FROM fulltext_datoms"

   ;; Materialized views of the schema.
   "CREATE TABLE idents (ident TEXT NOT NULL PRIMARY KEY, entid INTEGER UNIQUE NOT NULL)"
   ;; TODO: allow arbitrary schema values (true/false) and tag the resulting values.
   "CREATE TABLE schema (ident TEXT NOT NULL, attr TEXT NOT NULL, value TEXT NOT NULL, FOREIGN KEY (ident) REFERENCES idents (ident))"
   "CREATE INDEX idx_schema_unique ON schema (ident, attr, value)"
   "CREATE TABLE parts (part INTEGER NOT NULL PRIMARY KEY, start INTEGER NOT NULL, idx INTEGER NOT NULL)"
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

;; This is close to the SQLite schema since it may impact the value tag bit.
(defprotocol IEncodeSQLite
  (->SQLite [x] "Transforms Clojure{Script} values to SQLite."))

(extend-protocol IEncodeSQLite
  #?@(:clj
      [String
       (->SQLite [x] x)

       clojure.lang.Keyword
       (->SQLite [x] (str x))

       Boolean
       (->SQLite [x] (if x 1 0))

       Integer
       (->SQLite [x] x)

       Long
       (->SQLite [x] x)

       java.util.Date
       (->SQLite [x] (.getTime x))

       Float
       (->SQLite [x] x)

       Double
       (->SQLite [x] x)]

      :cljs
      [string
       (->SQLite [x] x)

       Keyword
       (->SQLite [x] (str x))

       boolean
       (->SQLite [x] (if x 1 0))

       js/Date
       (->SQLite [x] (.getTime x))

       number
       (->SQLite [x] x)]))

;; Datomish rows are tagged with a numeric representation of :db/valueType:
;; The tag is used to limit queries, and therefore is placed carefully in the relevant indices to
;; allow searching numeric longs and doubles quickly.  The tag is also used to convert SQLite values
;; to the correct Datomish value type on query egress.
(def value-type-tag-map
  {:db.type/ref     0
   :db.type/boolean 1
   :db.type/instant 4
   :db.type/long    5 ;; SQLite distinguishes integral from decimal types, allowing long and double to share a tag.
   :db.type/double  5 ;; SQLite distinguishes integral from decimal types, allowing long and double to share a tag.
   :db.type/string  10
   :db.type/uuid    11
   :db.type/uri     12
   :db.type/keyword 13})

(defn ->tag [valueType]
  (or
    (valueType value-type-tag-map)
    (raise "Unknown valueType " valueType ", expected one of " (sorted-set (keys value-type-tag-map))
           {:error :SQLite/tag, :valueType valueType})))

#?(:clj
(defn <-tagged-SQLite
  "Transforms SQLite values to Clojure with tag awareness."
  [tag value]
  (case tag
    ;; In approximate commonality order.
    0  value                               ; ref.
    1  (= value 1)                         ; boolean
    4  (java.util.Date. value)             ; instant
    13 (keyword (subs value 1))            ; keyword
    12 (java.net.URI. value)               ; URI
    11 (java.util.UUID/fromString value)   ; UUID
    ;  5 value                             ; numeric
    ; 10 value                             ; string
    value
    )))

#?(:cljs
(defn <-tagged-SQLite
  "Transforms SQLite values to ClojureScript with tag awareness."
  [tag value]
  ;; In approximate commonality order.
  (case tag
    0  value                               ; ref.
    1  (= value 1)                         ; boolean
    4  (js/Date. value)                    ; instant
    13 (keyword (subs value 1))            ; keyword
    ; 12 value                             ; URI
    ; 11 value                             ; UUID
    ;  5 value                             ; numeric
    ; 10 value                             ; string
    value
    )))

(defn tagged-SQLite-to-JS
  "Transforms SQLite values to JavaScript-compatible values."
  [tag value]
  (case tag
    1  (= value 1)     ; boolean.
    ;  0 value         ; No point trying to ident.
    ;  4 value         ; JS doesn't have a Date representation.
    ; 13 value         ; Return the keyword string from the DB: ":foobar".
    value))

(defn <-SQLite
  "Transforms SQLite values to Clojure{Script}."
  [valueType value]
  (case valueType
    :db.type/ref     value
    :db.type/keyword (keyword (subs value 1))
    :db.type/string  value
    :db.type/boolean (not= value 0)
    :db.type/long    value
    :db.type/instant (<-tagged-SQLite 4 value)
    :db.type/double  value))
