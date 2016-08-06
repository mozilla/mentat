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
   [datomish.util :as util #?(:cljs :refer-macros :clj :refer) [raise raise-str cond-let]]
   [datomish.schema :as ds]
   [datomish.sqlite :as s]
   [datomish.sqlite-schema :as sqlite-schema]
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
  (and (instance? TempId x)))

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

  (current-tx
    [db]
    "TODO: document this interface.")

  (in-transaction!
    [db chan-fn]
    "Evaluate the given pair-chan `chan-fn` in an exclusive transaction. If it returns non-nil,
    commit the transaction; otherwise, rollback the transaction.  Returns a pair-chan resolving to
    the pair-chan returned by `chan-fn`.")

  (<ea [db e a]
    "Search for datoms using the EAVT index.")

  (<eav [db e a v]
    "Search for datoms using the EAVT index.")

  (<av
    [db a v]
    "Search for datoms using the AVET index.")

  (<apply-datoms
    [db datoms]
    "Apply datoms to the store.")

  (<apply-db-ident-assertions
    [db added-idents merge]
    "Apply added idents to the store, using `merge` as a `merge-with` function.")

  (<apply-db-install-assertions
    [db fragment merge]
    "Apply added schema fragment to the store, using `merge` as a `merge-with` function.")

  (<advance-tx
    [db]
    "TODO: document this interface."))

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

(defrecord DB [sqlite-connection schema entids ident-map current-tx]
  ;; ident-map maps between keyword idents and integer entids.  The set of idents and entids is
  ;; disjoint, so we represent both directions of the mapping in the same map for simplicity.  Also
  ;; for simplicity, we assume that an entid has at most one associated ident, and vice-versa.  See
  ;; http://docs.datomic.com/identity.html#idents.
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

  (current-tx
    [db]
    (inc (:current-tx db)))

  (in-transaction! [db chan-fn]
    (s/in-transaction!
      (:sqlite-connection db) chan-fn))

  ;; TODO: use q for searching?  Have q use this for searching for a single pattern?
  (<ea [db e a]
    (go-pair
      (->>
        {:select [:e :a :v :tx [1 :added]]
         :from   [:all_datoms]
         :where  [:and [:= :e e] [:= :a a]]}
        (s/format) ;; TODO: format these statements only once.

        (s/all-rows (:sqlite-connection db))
        (<?)

        (mapv (partial row->Datom (.-schema db))))))

  (<eav [db e a v]
    (let [[v tag] (ds/->SQLite schema a v)]
      (go-pair
        (->>
          {:select [:e :a :v :tx [1 :added]] ;; TODO: generalize columns.
           :from   [:all_datoms]
           :where  [:and [:= :e e] [:= :a a] [:= :value_type_tag tag] [:= :v v]]}
          (s/format) ;; TODO: format these statements only once.

          (s/all-rows (:sqlite-connection db))
          (<?)

          (mapv (partial row->Datom (.-schema db))))))) ;; TODO: understand why (schema db) fails.

  (<av [db a v]
    (let [[v tag] (ds/->SQLite schema a v)]
      (go-pair
        (->>
          {:select [:e :a :v :tx [1 :added]] ;; TODO: generalize columns.
           :from   [:all_datoms]
           :where  [:and [:= :index_avet 1] [:= :a a] [:= :value_type_tag tag] [:= :v v]]}
          (s/format) ;; TODO: format these statements only once.

          (s/all-rows (:sqlite-connection db))
          (<?)

          (mapv (partial row->Datom (.-schema db))))))) ;; TODO: understand why (schema db) fails.

  (<apply-datoms [db datoms]
    (go-pair
      (let [exec   (partial s/execute! (:sqlite-connection db))
            schema (.-schema db)] ;; TODO: understand why (schema db) fails.
        ;; TODO: batch insert, batch delete.
        (doseq [datom datoms]
          (let [[e a v tx added] datom
                [v tag]          (ds/->SQLite schema a v)
                fulltext?        (ds/fulltext? schema a)]
            ;; Append to transaction log.
            (<? (exec
                  ["INSERT INTO transactions VALUES (?, ?, ?, ?, ?, ?)" e a v tx (if added 1 0) tag]))
            ;; Update materialized datom view.
            (if (.-added datom)
              (let [v (if fulltext?
                        (<? (<insert-fulltext-value db v))
                        v)]
                (<? (exec
                      ["INSERT INTO datoms VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)" e a v tx
                       tag ;; value_type_tag
                       (ds/indexing? schema a) ;; index_avet
                       (ds/ref? schema a) ;; index_vaet
                       fulltext? ;; index_fulltext
                       (ds/unique-value? schema a) ;; unique_value
                       (ds/unique-identity? schema a) ;; unique_identity
                       ])))
              (if fulltext?
                (<? (exec
                      ;; TODO: in the future, purge fulltext values from the fulltext_datoms table.
                      ["DELETE FROM datoms WHERE (e = ? AND a = ? AND value_type_tag = ? AND v IN (SELECT rowid FROM fulltext_values WHERE text = ?))" e a tag v]))
                (<? (exec
                      ["DELETE FROM datoms WHERE (e = ? AND a = ? AND value_type_tag = ? AND v = ?)" e a tag v])))))))
      db))

  (<advance-tx [db]
    (go-pair
      (let [exec (partial s/execute! (:sqlite-connection db))]
        ;; (let [ret (<? (exec
        ;;                 ;; TODO: be more clever about UPDATE OR ...?
        ;;                 ["UPDATE metadata SET current_tx = ? WHERE current_tx = ?" (inc (:current-tx db)) (:current-tx db)]))]

        ;; TODO: handle exclusion across transactions here.
        (update db :current-tx inc))))

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

(defn db [sqlite-connection idents schema current-tx]
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
       :current-tx        current-tx})))

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
