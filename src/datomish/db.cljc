;; This Source Code Form is subject to the terms of the Mozilla Public
;; License, v. 2.0. If a copy of the MPL was not distributed with this
;; file, You can obtain one at http://mozilla.org/MPL/2.0/.

(ns datomish.db
  #?(:cljs
     (:require-macros
      [datomish.pair-chan :refer [go-pair <?]]
      [cljs.core.async.macros :refer [go]]))
  (:require
   [clojure.set]
   [datomish.query.context :as context]
   [datomish.query.projection :as projection]
   [datomish.query.source :as source]
   [datomish.query :as query]
   [datomish.datom :as dd :refer [datom datom? #?@(:cljs [Datom])]]
   [datomish.util :as util
    #?(:cljs :refer-macros :clj :refer) [raise raise-str cond-let]]
   [datomish.schema :as ds]
   [datomish.sqlite :as s]
   [datomish.sqlite-schema :as sqlite-schema]
   [taoensso.tufte :as tufte
    #?(:cljs :refer-macros :clj :refer) [defnp p profiled profile]]
   #?@(:clj [[datomish.pair-chan :refer [go-pair <?]]
             [clojure.core.async :as a :refer [chan go <! >!]]])
   #?@(:cljs [[datomish.pair-chan]
              [cljs.core.async :as a :refer [chan <! >!]]]))
  #?(:clj
     (:import
      [datomish.datom Datom])))

#?(:clj
   ;; From https://stuartsierra.com/2015/05/27/clojure-uncaught-exceptions
   ;; Assuming require [clojure.tools.logging :as log]
   (Thread/setDefaultUncaughtExceptionHandler
     (reify Thread$UncaughtExceptionHandler
       (uncaughtException [_ thread ex]
         (println ex "Uncaught exception on" (.getName thread))))))

(def max-sql-vars 999)    ;; TODO: generalize.


;; ----------------------------------------------------------------------------
;; define data-readers to be made available to EDN readers. in CLJS
;; they're magically available. in CLJ, data_readers.clj may or may
;; not work, but you can always simply do
;;
;;  (clojure.edn/read-string {:readers datomish/data-readers} "...")
;;

(defonce -id-literal-idx (atom -1000000))

(defrecord TempId [part idx])

(defn id-literal
  ([part]
   (if (sequential? part)
     (apply id-literal part)
     (->TempId part (swap! -id-literal-idx dec))))
  ([part idx]
   (->TempId part idx)))

(defn id-literal? [x]
  (instance? TempId x))

(defprotocol IClock
  (now
    [clock]
    "Return integer milliseconds since the Unix epoch."))

(defprotocol IDB
  (query-context
    [db])

  (close-db
    [db]
    "Close this database. Returns a pair channel of [nil error].")

  (schema
    [db]
    "Return the schema of this database.")

  (entid
    [db ident]
    "Returns the entity id associated with a symbolic keyword, or the id itself if passed.")

  (ident
    [db eid]
    "Returns the keyword associated with an id, or the key itself if passed.")

  (in-transaction!
    [db chan-fn]
    "Evaluate the given pair-chan `chan-fn` in an exclusive transaction. If it returns non-nil,
    commit the transaction; otherwise, rollback the transaction.  Returns a pair-chan resolving to
    the pair-chan returned by `chan-fn`.")

  (<bootstrapped? [db]
    "Return true if this database has no transactions yet committed.")

  (<av
    [db a v]
    "Search for a single matching datom using the AVET index.")

  (<apply-entities
    [db tx entities]
    "Apply entities to the store, returning sequence of datoms transacted.")

  (<apply-db-ident-assertions
    [db added-idents merge]
    "Apply added idents to the store, using `merge` as a `merge-with` function.")

  (<apply-db-install-assertions
    [db fragment merge]
    "Apply added schema fragment to the store, using `merge` as a `merge-with` function.")

  (<next-eid
    [db id-literal]
    "Return a unique integer for the given id-literal, accounting for the literal's partition.  The
    returned integer should never be returned again."))

(defn db? [x]
  (and (satisfies? IDB x)
       (satisfies? IClock x)))

(defn- row->Datom [schema row]
  (let [e (:e row)
        a (:a row)
        v (:v row)]
    (Datom. e a (ds/<-SQLite schema a v) (:tx row) (and (some? (:added row)) (not= 0 (:added row))))))

(defn- <insert-fulltext-value [db value]
  (go-pair
    ;; This dance is necessary to keep fulltext_values.text unique.  We want uniqueness so that we
    ;; can work with string values, maintaining consistency throughout the transactor
    ;; implementation.  (Without this, we'd need to handle a [rowid text] pair specially when
    ;; comparing in the transactor.)  Unfortunately, it's not possible to declare a unique
    ;; constraint on a virtual table, including an FTS table.  External content tables (see
    ;; http://www.sqlite.org/fts3.html#section_6_2_2) don't appear to address our use case, so we
    ;; maintain uniqueness ourselves.
    (let [rowid
          (if-let [row (first (<? (s/all-rows (:sqlite-connection db) ["SELECT rowid FROM fulltext_values WHERE text = ?" value])))]
            (:rowid row)
            (do
              (<? (s/execute! (:sqlite-connection db) ["INSERT INTO fulltext_values VALUES (?)" value]))
              (:rowid (first (<? (s/all-rows (:sqlite-connection db) ["SELECT last_insert_rowid() AS rowid"]))))))
          ]
      rowid)))

(defn datoms-attribute-transform
  [db x]
  {:pre [(db? db)]}
  (entid db x))

(defn datoms-constant-transform
  [db x]
  {:pre [(db? db)]}
  (sqlite-schema/->SQLite x))

(defn datoms-source [db]
  (source/map->DatomsSource
    {:table :datoms
     :fulltext-table :fulltext_values
     :fulltext-view :all_datoms
     :columns [:e :a :v :tx :added]
     :attribute-transform (partial datoms-attribute-transform db)
     :constant-transform (partial datoms-constant-transform db)
     :table-alias source/gensym-table-alias
     :make-constraints nil}))

(defn- retractions->queries [retractions tx fulltext? ->SQLite]
  (let
    [f-q
     "WITH vv AS (SELECT rowid FROM fulltext_values WHERE text = ?)
     INSERT INTO tx_lookup (e0, a0, v0, tx0, added0, value_type_tag0, sv, svalue_type_tag)
     VALUES (?, ?, (SELECT rowid FROM vv), ?, 0, ?, (SELECT rowid FROM vv), ?)"

     non-f-q
     "INSERT INTO tx_lookup (e0, a0, v0, tx0, added0, value_type_tag0, sv, svalue_type_tag)
     VALUES (?, ?, ?, ?, 0, ?, ?, ?)"]
    (map
      (fn [[_ e a v]]
        (let [[v tag] (->SQLite a v)]
          (if (fulltext? a)
            [f-q
             v e a tx tag tag]
            [non-f-q
             e a v tx tag v tag])))
      retractions)))

(defn- non-fts-many->queries [ops tx ->SQLite indexing? ref? unique?]
  (let [q "INSERT INTO tx_lookup (e0, a0, v0, tx0, added0, value_type_tag0, index_avet0, index_vaet0, index_fulltext0, unique_value0, sv, svalue_type_tag) VALUES "

        values-part
        ;; e0, a0, v0, tx0, added0, value_type_tag0
        ;; index_avet0, index_vaet0, index_fulltext0,
        ;; unique_value0, sv, svalue_type_tag
        "(?, ?, ?, ?, 1, ?, ?, ?, 0, ?, ?, ?)"

        repeater (memoize (fn [n] (interpose ", " (repeat n values-part))))]

    ;; This query takes ten variables per item. So we partition into max-sql-vars / 10.
    (map
      (fn [chunk]
        (cons
          ;; Query string.
          (apply str q (repeater (count chunk)))

          ;; Bindings.
          (mapcat (fn [[_ e a v]]
                    (let [[v tag] (->SQLite a v)]
                      [e a v tx tag
                       (indexing? a)      ; index_avet
                       (ref? a)           ; index_vaet
                       (unique? a)        ; unique_value
                       v tag]))
                  chunk)))

      (partition-all (quot max-sql-vars 10) ops))))

(defn- non-fts-one->queries [ops tx ->SQLite indexing? ref? unique?]
  (let [first-values-part
        ;; TODO: order value and tag closer together.
        ;; flags0
        ;; sv, svalue_type_tag
        "(?, ?, ?, ?, ?, ?, ?, ?, 0, ?, ?, ?)"
        first-repeater (memoize (fn [n] (interpose ", " (repeat n first-values-part))))

        second-values-part
        "(?, ?, ?, ?, ?, ?)"
        second-repeater (memoize (fn [n] (interpose ", " (repeat n second-values-part))))
        ]

    ;; :db.cardinality/one takes two queries.
    (mapcat
      (fn [chunk]
        [(cons
           (apply
             str
             "INSERT INTO tx_lookup (e0, a0, v0, tx0, added0, value_type_tag0, index_avet0, index_vaet0, index_fulltext0, unique_value0, sv, svalue_type_tag) VALUES "
             (first-repeater (count chunk)))
           (mapcat (fn [[_ e a v]]
                     (let [[v tag] (->SQLite a v)]
                       [e a v tx 1 tag
                        (indexing? a)   ; index_avet
                        (ref? a)        ; index_vaet
                        (unique? a)     ; unique_value
                        v tag]))
                   chunk))

         (cons
           (apply
             str
             "INSERT INTO tx_lookup (e0, a0, v0, tx0, added0, value_type_tag0) VALUES "
             (second-repeater (count chunk)))
           (mapcat (fn [[_ e a v]]
                     (let [[v tag] (->SQLite a v)]
                       [e a v tx 0 tag]))
                   chunk))])
      (partition-all (quot max-sql-vars 11) ops))))

;;; An FTS insertion happens in two parts.
;;; Firstly, we ensure that the fulltext value is present in the store.
;;; This is effectively an INSERT OR IGNORE… but FTS tables don't support
;;; uniqueness constraints. So we do it through a trigger on a view.
;;; When we insert the value, we pass with it a searchid. We'll use this
;;; later when inserting the datom.
;;; Secondly, we insert a row just like for non-FTS. The only difference
;;; is that the value is the rowid into the fulltext_values table.
(defn- fts-many->queries [ops tx ->SQLite indexing? ref? unique?]
  ;; TODO: operations with the same text value should be
  ;; coordinated here!
  ;; It'll work fine without so long as queries are executed
  ;; in order and not combined, but even so it's inefficient.
  (conj
    (mapcat
      (fn [[_ e a v] searchid]
        (let [[v tag] (->SQLite a v)]
          ;; First query: ensure the value exists.
          [["INSERT INTO fulltext_values_view (text, searchid) VALUES (?, ?)"
            v searchid]

           ;; Second query: tx_lookup.
           [(str
              "WITH vv(rowid) AS (SELECT rowid FROM fulltext_values WHERE searchid = ?) "
              "INSERT INTO tx_lookup (e0, a0, v0, tx0, added0, value_type_tag0, index_avet0, index_vaet0, index_fulltext0, unique_value0, sv, svalue_type_tag) VALUES "
              "(?, ?, (SELECT rowid FROM vv), ?, 1, ?, ?, ?, 1, ?, (SELECT rowid FROM vv), ?)")
            searchid
            e a tx tag
            (indexing? a)      ; index_avet
            (ref? a)           ; index_vaet
            (unique? a)        ; unique_value
            tag]]))
      ops
      (range 2000 999999999))
    ["UPDATE fulltext_values SET searchid = NULL WHERE searchid IS NOT NULL"]))

(defn fts-one->queries [ops tx ->SQLite indexing? ref? unique?]
  (conj
    (mapcat
      (fn [[_ e a v] searchid]
        (let [[v tag] (->SQLite a v)]
          ;; First query: ensure the value exists.
          [["INSERT INTO fulltext_values_view (text, searchid) VALUES (?, ?)"
            v searchid]

           ;; Second and third queries: tx_lookup.
           [(str
              "WITH vv(rowid) AS (SELECT rowid FROM fulltext_values WHERE searchid = ?) "
              "INSERT INTO tx_lookup (e0, a0, v0, tx0, added0, value_type_tag0, index_avet0, index_vaet0, index_fulltext0, unique_value0, sv, svalue_type_tag) VALUES "
              "(?, ?, (SELECT rowid FROM vv), ?, 1, ?, ?, ?, 1, ?, (SELECT rowid FROM vv), ?)")
            searchid
            e a tx tag
            (indexing? a)      ; index_avet
            (ref? a)           ; index_vaet
            (unique? a)        ; unique_value
            tag]

           [(str
              "INSERT INTO tx_lookup (e0, a0, v0, tx0, added0, value_type_tag0) VALUES "
              "(?, ?, (SELECT rowid FROM fulltext_values WHERE searchid = ?), ?, 0, ?)")
            e a searchid tx tag]]))
      ops
      (range 3000 999999999))
    ["UPDATE fulltext_values SET searchid = NULL WHERE searchid IS NOT NULL"]))

(defn- -run-queries [conn queries exception-message]
  (go-pair
    (try
      (doseq [q queries]
        (<? (s/execute! conn q)))
      (catch #?(:clj java.sql.SQLException :cljs js/Error) e
        (throw (ex-info exception-message {} e))))))

(defn- -preamble-drop [conn]
  (let [preamble-drop-index ["DROP INDEX IF EXISTS id_tx_lookup_added"]
        preamble-delete-tx-lookup ["DELETE FROM tx_lookup"]]
    (go-pair
      (p :preamble
         (doseq [q [preamble-drop-index preamble-delete-tx-lookup]]
           (<? (s/execute! conn q)))))))

(defn- -after-drop [conn]
  (go-pair
    (doseq [q [;; The lookup table takes space on disk, so we purge it aggressively.
               ["DROP INDEX IF EXISTS id_tx_lookup_added"]
               ["DELETE FROM tx_lookup"]]]
      (<? (s/execute! conn q)))))

(defn- -build-transaction [conn tx]
  (let [build-indices ["CREATE INDEX IF NOT EXISTS idx_tx_lookup_added ON tx_lookup (added0)"]

        ;; First is fast, only one table walk: lookup by exact eav.
        ;; Second is slower, but still only one table walk: lookup old value by ea.
        insert-into-tx-lookup
        ["INSERT INTO tx_lookup
          SELECT t.e0, t.a0, t.v0, t.tx0, t.added0 + 2, t.value_type_tag0, t.index_avet0, t.index_vaet0, t.index_fulltext0, t.unique_value0, t.sv, t.svalue_type_tag, d.rowid, d.e, d.a, d.v, d.tx, d.value_type_tag
          FROM tx_lookup AS t
          LEFT JOIN datoms AS d
          ON t.e0 = d.e AND
             t.a0 = d.a AND
             t.sv = d.v AND
             t.svalue_type_tag = d.value_type_tag AND
             t.sv IS NOT NULL

          UNION ALL
          SELECT t.e0, t.a0, t.v0, t.tx0, t.added0 + 2, t.value_type_tag0, t.index_avet0, t.index_vaet0, t.index_fulltext0, t.unique_value0, t.sv, t.svalue_type_tag, d.rowid, d.e, d.a, d.v, d.tx, d.value_type_tag
          FROM tx_lookup AS t,
               datoms AS d
          WHERE t.sv IS NULL AND
                t.e0 = d.e AND
                t.a0 = d.a"]

        t-datoms-not-already-present
        ["INSERT INTO transactions (e, a, v, tx, added, value_type_tag)
          SELECT e0, a0, v0, ?, 1, value_type_tag0
          FROM tx_lookup
          WHERE added0 IS 3 AND e IS NULL" tx] ;; TODO: get rid of magic value 3.

        t-retract-datoms-carefully
        ["INSERT INTO transactions (e, a, v, tx, added, value_type_tag)
          SELECT e, a, v, ?, 0, value_type_tag
          FROM tx_lookup
          WHERE added0 IS 2 AND ((sv IS NOT NULL) OR (sv IS NULL AND v0 IS NOT v)) AND v IS NOT NULL" tx] ;; TODO: get rid of magic value 2.
]
    (go-pair
      (doseq [q [build-indices insert-into-tx-lookup
                 t-datoms-not-already-present
                 t-retract-datoms-carefully]]
        (<? (s/execute! conn q))))))

(defn- -build-datoms [conn tx]
  (let [d-datoms-not-already-present
        ["INSERT INTO datoms (e, a, v, tx, value_type_tag, index_avet, index_vaet, index_fulltext, unique_value)
          SELECT e0, a0, v0, ?, value_type_tag0,
          index_avet0, index_vaet0, index_fulltext0, unique_value0
          FROM tx_lookup
          WHERE added0 IS 3 AND e IS NULL" tx] ;; TODO: get rid of magic value 3.

        ;; TODO: retract fulltext datoms correctly.
        d-retract-datoms-carefully
        ["WITH ids AS (SELECT l.rid FROM tx_lookup AS l WHERE l.added0 IS 2 AND ((l.sv IS NOT NULL) OR (l.sv IS NULL AND l.v0 IS NOT l.v)))
         DELETE FROM datoms WHERE rowid IN ids" ;; TODO: get rid of magic value 2.
         ]]
    (-run-queries conn [d-datoms-not-already-present d-retract-datoms-carefully]
                  "Transaction violates unique constraint")))

(defn- -<apply-entities [db tx entities]
  (let [schema         (.-schema db)
        ->SQLite       (partial ds/->SQLite schema)
        fulltext?      (memoize (partial ds/fulltext? schema))
        many?          (memoize (fn [a] (ds/multival? schema a)))
        indexing?      (memoize (fn [a] (ds/indexing? schema a)))
        ref?           (memoize (fn [a] (ds/ref? schema a)))
        unique?        (memoize (fn [a] (ds/unique? schema a)))
        conn           (:sqlite-connection db)

        ;; Collect all the queries we need to run.
        queries (atom [])
        operations (group-by first entities)]

    (when-not (clojure.set/subset? (keys operations) #{:db/retract :db/add})
      (raise (str "Unknown operations " (keys operations))
             {:error :transact/syntax, :operations (dissoc operations :db/retract :db/add)}))

    ;; We can turn all non-FTS operations into simple SQL queries that we run serially.
    ;; FTS queries require us to get a rowid from the FTS table and use that for
    ;; insertion, so we need another pass.
    ;; We can't just freely use `go-pair` here, because this function is so complicated
    ;; that ClojureScript blows the stack trying to compile it.

    (when-let [retractions (:db/retract operations)]
      (swap!
        queries concat (retractions->queries retractions tx fulltext? ->SQLite)))

    ;; We want to partition our additions into four groups according to two
    ;; characteristics: whether they require writing to the FTS value table,
    ;; and whether the attribute has a 'many' cardinality constraint. Each of
    ;; these four requires different queries.
    (let [additions
          (group-by (fn [[op e a v]]
                      (if (fulltext? a)
                        (if (many? a)
                          :fts-many
                          :fts-one)
                        (if (many? a)
                          :non-fts-many
                          :non-fts-one)))
                    (:db/add operations))
          transforms
          {:fts-one fts-one->queries
           :fts-many fts-many->queries
           :non-fts-one non-fts-one->queries
           :non-fts-many non-fts-many->queries}]

      (doseq [[key ops] additions]
        (when-let [transform (key transforms)]
          (swap!
            queries concat
            (transform ops tx ->SQLite indexing? ref? unique?)))))

    ;; Now run each query.
    ;; This code is a little tortured to avoid blowing the compiler stack in cljs.

    (go-pair
      (<? (-preamble-drop conn))

      (p :run-insert-queries
         (<? (-run-queries conn @queries "Transaction violates cardinality constraint")))

      ;; Follow up by building indices, then do the work.
      (p :build-and-transaction
         (<? (-build-transaction conn tx)))

      (p :update-materialized-datoms
         (<? (-build-datoms conn tx)))

      (<? (-after-drop conn))

      ;; Return the written transaction.
      (p :select-tx-data
         (mapv (partial row->Datom schema)
               (<?
                 (s/all-rows
                   (:sqlite-connection db)
                   ;; We index on tx, so the following is fast.
                   ["SELECT * FROM transactions WHERE tx = ?" tx])))))))

(defrecord DB [sqlite-connection schema ident-map]
  ;; ident-map maps between keyword idents and integer entids.  The set of idents and entids is
  ;; disjoint, so we represent both directions of the mapping in the same map for simplicity.  Also
  ;; for simplicity, we assume that an entid has at most one associated ident, and vice-versa.  See
  ;; http://docs.datomic.com/identity.html#idents.

  ;; TODO: cache parts.  parts looks like {:db.part/db {:start 0 :current 10}}.  It maps between
  ;; keyword ident part names and integer ranges.
  IDB
  (query-context [db] (context/->Context (datoms-source db) nil nil))

  (schema [db] (.-schema db))

  (entid [db ident]
    (if (keyword? ident)
      (get (.-ident-map db) ident ident)
      ident))

  (ident [db eid]
    (if-not (keyword? eid)
      (get (.-ident-map db) eid eid)
      eid))

  (in-transaction! [db chan-fn]
    (s/in-transaction!
      (:sqlite-connection db) chan-fn))

  (<bootstrapped? [db]
    (go-pair
      (->
        (:sqlite-connection db)
        (s/all-rows ["SELECT EXISTS(SELECT 1 FROM transactions LIMIT 1) AS bootstrapped"])
        (<?)
        (first)
        (:bootstrapped)
        (not= 0))))

  (<av [db a v]
    (let [schema (.-schema db) ;; TODO: understand why (schema db) fails.
          a (entid db a)
          [v tag] (ds/->SQLite schema a v)
          yield-datom
          (fn [rows]
            (when-let [row (first rows)]
              (row->Datom schema row)))]
      (go-pair
        (->>
          ;; TODO: generalize columns.
          ["SELECT e, a, v, tx, 1 AS added FROM all_datoms
           WHERE index_avet = 1 AND a = ? AND value_type_tag = ? AND v = ?
           LIMIT 1" a tag v]

          (s/all-rows (:sqlite-connection db))
          <?
          yield-datom))))

  (<next-eid [db tempid]
    {:pre [(id-literal? tempid)]}
    {:post [ds/entid?]}
    (go-pair
      ;; TODO: keep all of these eid allocations in the transaction report and apply them at the end
      ;; of the transaction.
      (let [exec (partial s/execute! (:sqlite-connection db))
            part (entid db (:part tempid))]
        (when-not (ds/entid? part) ;; TODO: cache parts materialized view.
          (raise "Cannot allocate entid for id-literal " tempid " because part " (:part tempid) " is not known"
                 {:error :db/bad-part
                  :part (:part tempid)}))

        (p :next-eid-body
           (<? (exec ["UPDATE parts SET idx = idx + 1 WHERE part = ?" part]))
        (:eid (first (<? (s/all-rows (:sqlite-connection db) ["SELECT (start + idx) AS eid FROM parts WHERE part = ?" part]))))))))

  (<apply-entities [db tx entities]
    {:pre [(db? db) (sequential? entities)]}
    (-<apply-entities db tx entities))


  (<apply-db-ident-assertions [db added-idents merge]
    (go-pair
      (let [exec (partial s/execute! (:sqlite-connection db))]
        ;; TODO: batch insert.
        (doseq [[ident entid] added-idents]
          (<? (exec
                ["INSERT INTO idents VALUES (?, ?)" (sqlite-schema/->SQLite ident) entid]))))

      (let [db (update db :ident-map #(merge-with merge % added-idents))
            db (update db :ident-map #(merge-with merge % (clojure.set/map-invert added-idents)))]
        db)))

  (<apply-db-install-assertions [db fragment merge]
    (go-pair
      (let [exec (partial s/execute! (:sqlite-connection db))]
        ;; TODO: batch insert.
        (doseq [[ident attr-map] fragment]
          (doseq [[attr value] attr-map]
            (<? (exec
                  ["INSERT INTO schema VALUES (?, ?, ?)" (sqlite-schema/->SQLite ident) (sqlite-schema/->SQLite attr) (sqlite-schema/->SQLite value)])))))

      (let [symbolic-schema (merge-with merge (:symbolic-schema db) fragment)
            schema          (ds/schema (into {} (map (fn [[k v]] [(entid db k) v]) symbolic-schema)))]
        (assoc db
               :symbolic-schema symbolic-schema
               :schema schema))))

  (close-db [db] (s/close (.-sqlite-connection db)))

  IClock
  (now [db]
    #?(:clj
       (System/currentTimeMillis)
       :cljs
       (.getTime (js/Date.)))))

(defn with-ident [db ident entid]
  (update db :ident-map #(assoc % ident entid, entid ident)))

(defn db [sqlite-connection idents schema]
  {:pre [(map? idents)
         (every? keyword? (keys idents))
         (map? schema)
         (every? keyword? (keys schema))]}
  (let [entid-schema (ds/schema (into {} (map (fn [[k v]] [(k idents) v]) schema))) ;; TODO: fail if ident missing.
        ident-map    (into idents (clojure.set/map-invert idents))]
    (map->DB
      {:sqlite-connection sqlite-connection
       :ident-map         ident-map
       :symbolic-schema   schema
       :schema            entid-schema
       ;; TODO :parts
       })))

;; TODO: factor this into the overall design.
(defn <?run
  "Execute the provided query on the provided DB.
   Returns a transduced channel of [result err] pairs.
   Closes the channel when fully consumed."
  [db find args]
  (let [parsed (query/parse find)
        context (-> db
                    query-context
                    (query/find-into-context parsed))
        row-pair-transducer (projection/row-pair-transducer context)
        sql (query/context->sql-string context args)
        chan (chan 50 row-pair-transducer)]

    (s/<?all-rows (.-sqlite-connection db) sql chan)
    chan))

(defn reduce-error-pair [f [rv re] [v e]]
  (if re
    [nil re]
    (if e
      [nil e]
      [(f rv v) nil])))

(defn <?q
  "Execute the provided query on the provided DB.
   Returns a transduced pair-chan with one [[results] err] item."
  [db find args]
  (a/reduce (partial reduce-error-pair conj) [[] nil]
            (<?run db find args)))
