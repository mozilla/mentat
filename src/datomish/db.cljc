;; This Source Code Form is subject to the terms of the Mozilla Public
;; License, v. 2.0. If a copy of the MPL was not distributed with this
;; file, You can obtain one at http://mozilla.org/MPL/2.0/.

(ns datomish.db
  #?(:cljs
     (:require-macros
      [datomish.pair-chan :refer [go-pair <?]]
      [cljs.core.async.macros :refer [go]]))
  (:require
   [datomish.query.context :as context]
   [datomish.query.projection :as projection]
   [datomish.query.source :as source]
   [datomish.query :as query]
   [honeysql.core :as sql]
   [datomish.datom :as dd :refer [datom datom? #?@(:cljs [Datom])]]
   [datomish.util :as util #?(:cljs :refer-macros :clj :refer) [raise raise-str cond-let]]
   [datomish.schema :as ds]
   [datomish.schema-changes]
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

(defprotocol IDB
  (query-context
    [db])

  (close-db
    [db]
    "Close this database. Returns a pair channel of [nil error].")

  (schema
    [db]
    "Return the schema of this database.")

  (idents
    [db]
    "Return the known idents of this database, as a map from keyword idents to entids.")

  (current-tx
    [db]
    "TODO: document this interface.")

  (<eavt
    [db pattern]
    "Search for datoms using the EAVT index.")

  (<avet
    [db pattern]
    "Search for datoms using the AVET index.")

  (<apply-datoms
    [db datoms]
    "Apply datoms to the store.")

  (<advance-tx
    [db]
    "TODO: document this interface."))

(defn db? [x]
  (and (satisfies? IDB x)))

(defn- row->Datom [schema row]
  (let [e (:e row)
        a (:a row)
        v (:v row)]
    (Datom. e a (ds/<-SQLite schema a v) (:tx row) (and (some? (:added row)) (not= 0 (:added row))))))

(defrecord DB [sqlite-connection schema idents current-tx]
  ;; idents is map {ident -> entid} of known idents.  See http://docs.datomic.com/identity.html#idents.
  IDB
  (query-context [db] (context/->Context (source/datoms-source db) nil nil))

  (schema [db] (.-schema db))

  (idents [db] (.-idents db))

  (current-tx
    [db]
    (inc (:current-tx db)))

  ;; TODO: use q for searching?  Have q use this for searching for a single pattern?
  (<eavt [db pattern]
    (let [[e a v tx] pattern
          v (and v (ds/->SQLite schema a v))] ;; We assume e and a are always given.
      (go-pair
        (->>
          {:select [:e :a :v :tx [1 :added]] ;; TODO: generalize columns.
           :from [:datoms]
           :where (cons :and (map #(vector := %1 %2) [:e :a :v :tx] (take-while (comp not nil?) [e a v tx])))} ;; Must drop nils.
          (sql/format)

          (s/all-rows (:sqlite-connection db))
          (<?)

          (mapv (partial row->Datom (.-schema db))))))) ;; TODO: understand why (schema db) fails.

  (<avet [db pattern]
    (let [[a v] pattern
          v (ds/->SQLite schema a v)]
      (go-pair
        (->>
          {:select [:e :a :v :tx [1 :added]] ;; TODO: generalize columns.
           :from [:datoms]
           :where [:and [:= :a a] [:= :v v] [:= :index_avet 1]]}
          (sql/format)

          (s/all-rows (:sqlite-connection db))
          (<?)

          (mapv (partial row->Datom (.-schema db))))))) ;; TODO: understand why (schema db) fails.

  (<apply-datoms [db datoms]
    (go-pair
      (let [exec (partial s/execute! (:sqlite-connection db))]
        ;; TODO: batch insert, batch delete.
        (doseq [datom datoms]
          (let [[e a v tx added] datom
                v (ds/->SQLite (.-schema db) a v)] ;; TODO: understand why (schema db) fails.
            ;; Append to transaction log.
            (<? (exec
                  ["INSERT INTO transactions VALUES (?, ?, ?, ?, ?)" e a v tx (if added 1 0)]))
            ;; Update materialized datom view.
            (if (.-added datom)
              (<? (exec
                    ["INSERT INTO datoms VALUES (?, ?, ?, ?, ?, ?, ?, ?)" e a v tx
                     (ds/indexing? (.-schema db) a) ;; index_avet
                     (ds/ref? (.-schema db) a) ;; index_vaet
                     (ds/unique-value? (.-schema db) a) ;; unique_value
                     (ds/unique-identity? (.-schema db) a) ;; unique_identity
                     ]))
              (<? (exec
                    ;; TODO: verify this is correct.
                    ["DELETE FROM datoms WHERE (e = ? AND a = ? AND v = ?)" e a v]))))))
      db))

  (<advance-tx [db]
    (go-pair
      (let [exec (partial s/execute! (:sqlite-connection db))]
        ;; (let [ret (<? (exec
        ;;                 ;; TODO: be more clever about UPDATE OR ...?
        ;;                 ["UPDATE metadata SET current_tx = ? WHERE current_tx = ?" (inc (:current-tx db)) (:current-tx db)]))]

        ;; TODO: handle exclusion across transactions here.
        (update db :current-tx inc))))
  ;;  )

  (close-db [db] (s/close (.-sqlite-connection db))))

(defprotocol IConnection
  (close
    [conn]
    "Close this connection. Returns a pair channel of [nil error].")

  (db
    [conn]
    "Get the current DB associated with this connection.")

  (history
    [conn]
    "Get the full transaction history DB associated with this connection."))

(defrecord Connection [current-db]
  IConnection
  (close [conn] (close-db @(:current-db conn)))

  (db [conn] @(:current-db conn))

  (history [conn]
    (raise "Datomic's history is not yet supported." {})))

(defn conn? [x]
  (and (satisfies? IConnection x)))

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

(defrecord TxReport [db-before ;; The DB before the transaction.
                     db-after  ;; The DB after the transaction.
                     entities  ;; The set of entities (like [:db/add e a v tx]) processed.
                     tx-data   ;; The set of datoms applied to the database, like (Datom. e a v tx added).
                     tempids   ;; The map from id-literal -> numeric entid.
                     added-parts ;; The set of parts added during the transaction via :db.part/db :db.install/part.
                     added-idents ;; The map of idents -> entid added during the transaction, via e :db/ident ident.
                     added-attributes ;; The map of schema attributes (ident -> schema fragment) added during the transaction, via :db.part/db :db.install/attribute.
                     ])

(defn- report? [x]
  (and (instance? TxReport x)))

(defonce -eid (atom (- 0x200 1)))

;; TODO: better here.
(defn- next-eid [db]
  (swap! -eid inc))

(defn- allocate-eid
  [report id-literal eid]
  {:pre [(report? report) (id-literal? id-literal) (and (integer? eid) (not (neg? eid)))]}

  (assoc-in report [:tempids id-literal] eid))

;; (def data-readers {'db/id id-literal})

;; #?(:cljs
;;    (doseq [[tag cb] data-readers] (cljs.reader/register-tag-parser! tag cb)))

;; TODO: implement support for DB parts?
(def tx0 0x2000000)

(def ^{:private true} bootstrap-symbolic-schema
  {:db/ident             {:db/valueType   :db.type/keyword
                          :db/cardinality :db.cardinality/one
                          :db/unique      :db.unique/identity}
   :db.install/partition {:db/valueType   :db.type/ref
                          :db/cardinality :db.cardinality/many}
   :db.install/valueType {:db/valueType   :db.type/ref
                          :db/cardinality :db.cardinality/many}
   :db.install/attribute {:db/valueType   :db.type/ref
                          :db/cardinality :db.cardinality/many}
   ;; TODO: support user-specified functions in the future.
   ;; :db.install/function {:db/valueType :db.type/ref
   ;;                       :db/cardinality :db.cardinality/many}
   :db/txInstant         {:db/valueType   :db.type/integer
                          :db/cardinality :db.cardinality/one
                          } ;; :db/index       true} TODO: Handle this using SQLite protocol.
   :db/valueType         {:db/valueType   :db.type/ref
                          :db/cardinality :db.cardinality/one}
   :db/cardinality       {:db/valueType   :db.type/ref
                          :db/cardinality :db.cardinality/one}
   :db/unique            {:db/valueType   :db.type/ref
                          :db/cardinality :db.cardinality/one}
   :db/isComponent       {:db/valueType   :db.type/boolean
                          :db/cardinality :db.cardinality/one}
   :db/index             {:db/valueType   :db.type/boolean
                          :db/cardinality :db.cardinality/one}
   :db/fulltext          {:db/valueType   :db.type/boolean
                          :db/cardinality :db.cardinality/one}
   :db/noHistory         {:db/valueType   :db.type/boolean
                          :db/cardinality :db.cardinality/one}
   })

(def ^{:private true} bootstrap-idents
  {:db/ident             1
   :db.part/db           2
   :db/txInstant         3
   :db.install/partition 4
   :db.install/valueType 5
   :db.install/attribute 6
   :db/valueType         7
   :db/cardinality       8
   :db/unique            9
   :db/isComponent       10
   :db/index             11
   :db/fulltext          12
   :db/noHistory         13
   :db/add               14
   :db/retract           15
   :db.part/tx           16
   :db.part/user         17
   :db/excise            18
   :db.excise/attrs      19
   :db.excise/beforeT    20
   :db.excise/before     21
   :db.alter/attribute   22
   :db.type/ref          23
   :db.type/keyword      24
   :db.type/integer      25 ;; TODO: :db.type/long, to match Datomic?
   :db.type/string       26
   :db.type/boolean      27
   :db.type/instant      28
   :db.type/bytes        29
   :db.cardinality/one   30
   :db.cardinality/many  31
   :db.unique/value      32
   :db.unique/identity   33})

(defn- bootstrap-tx-data []
  (concat
    (map (fn [[ident entid]] [:db/add entid :db/ident ident]) bootstrap-idents)
    (map (fn [[ident attrs]] (assoc attrs :db/id ident)) bootstrap-symbolic-schema)
    (map (fn [[ident attrs]] [:db/add :db.part/db :db.install/attribute (get bootstrap-idents ident)]) bootstrap-symbolic-schema) ;; TODO: fail if nil.
    ))

(defn <idents [sqlite-connection]
  "Read the ident map materialized view from the given SQLite store.
  Returns a map (keyword ident) -> (integer entid), like {:db/ident 0}."

  (let [<-SQLite (get-in ds/value-type-map [:db.type/keyword :<-SQLite])] ;; TODO: make this a protocol.
    (go-pair
      (let [rows (<? (->>
                       {:select [:ident :entid] :from [:idents]}
                       (sql/format)
                       (s/all-rows sqlite-connection)))]
        (into {} (map (fn [row] [(<-SQLite (:ident row)) (:entid row)])) rows)))))

(defn <current-tx [sqlite-connection]
  "Find the largest tx written to the SQLite store.
  Returns an integer, -1 if no transactions have been written yet."

  (go-pair
    (let [rows (<? (s/all-rows sqlite-connection ["SELECT COALESCE(MAX(tx), -1) AS current_tx FROM transactions"]))]
      (:current_tx (first rows)))))

(defn <symbolic-schema [sqlite-connection]
  "Read the schema map materialized view from the given SQLite store.
  Returns a map (keyword ident) -> (map (keyword attribute -> keyword value)), like
  {:db/ident {:db/cardinality :db.cardinality/one}}."

  (let [<-SQLite (get-in ds/value-type-map [:db.type/keyword :<-SQLite])] ;; TODO: make this a protocol.
    (go-pair
      (->>
        (->>
          {:select [:ident :attr :value] :from [:schema]}
          (sql/format)
          (s/all-rows sqlite-connection))
        (<?)

        (group-by (comp <-SQLite :ident))
        (map (fn [[ident rows]]
               [ident
                (into {} (map (fn [row]
                                [(<-SQLite (:attr row)) (<-SQLite (:value row))]) rows))]))
        (into {})))))

(declare <with-internal)

(defn <db-with-sqlite-connection
  ([sqlite-connection]
   (<db-with-sqlite-connection sqlite-connection {}))

  ([sqlite-connection schema]
   (go-pair
     (when-not (= sqlite-schema/current-version (<? (sqlite-schema/<ensure-current-version sqlite-connection)))
       (raise "Could not ensure current SQLite schema version."))

     (let [current-tx   (<? (<current-tx sqlite-connection))
           bootstrapped (>= current-tx 0)
           current-tx   (max current-tx tx0)]
       (when-not bootstrapped
         ;; We need to bootstrap the DB.
         (let [fail-alter-ident (fn [old new] (if-not (= old new)
                                                (raise "Altering idents is not yet supported, got " new " altering existing ident " old
                                                       {:error :schema/alter-idents :old old :new new})
                                                new))
               fail-alter-attr  (fn [old new] (if-not (= old new)
                                                (raise "Altering schema attributes is not yet supported, got " new " altering existing schema attribute " old
                                                       {:error :schema/alter-schema :old old :new new})
                                                new))]
           (-> (map->DB
                 {:sqlite-connection sqlite-connection
                  :idents            bootstrap-idents
                  :symbolic-schema   bootstrap-symbolic-schema
                  :schema            (ds/schema (into {} (map (fn [[k v]] [(k bootstrap-idents) v]) bootstrap-symbolic-schema))) ;; TODO: fail if ident missing.
                  :current-tx        current-tx})
               ;; We use <with rather than <transact! to apply the bootstrap transaction data but to
               ;; not follow the regular schema application process.  We can't apply the schema
               ;; changes, since the applied datoms would conflict with the bootstrapping idents and
               ;; schema.  (The bootstrapping idents and schema are required to be able to write to
               ;; the database conveniently; without them, we'd have to manually write datoms to the
               ;; store.  It's feasible but awkward.)  After bootstrapping, we read back the idents
               ;; and schema, just like when we re-open.
               (<with-internal (bootstrap-tx-data) fail-alter-ident fail-alter-attr)
               (<?))))

       ;; We just bootstrapped, or we are returning to an already bootstrapped DB.
       (let [idents          (<? (<idents sqlite-connection))
             symbolic-schema (<? (<symbolic-schema sqlite-connection))]
         (when-not bootstrapped
           (when (not (= idents bootstrap-idents))
             (raise "After bootstrapping database, expected new materialized idents and old bootstrapped idents to be identical"
                    {:error :bootstrap/bad-idents,
                     :new idents :old bootstrap-idents
                     }))
           (when (not (= symbolic-schema bootstrap-symbolic-schema))
             (raise "After bootstrapping database, expected new materialized symbolic schema and old bootstrapped symbolic schema to be identical"
                    {:error :bootstrap/bad-symbolic-schema,
                     :new symbolic-schema :old bootstrap-symbolic-schema
                     })))
         (map->DB
           {:sqlite-connection sqlite-connection
            :idents            idents
            :symbolic-schema   symbolic-schema
            :schema            (ds/schema (into {} (map (fn [[k v]] [(k idents) v]) symbolic-schema))) ;; TODO: fail if ident missing.
            :current-tx        (inc current-tx)}))))))

(defn connection-with-db [db]
  (map->Connection {:current-db (atom db)}))

;; ;; TODO: persist max-tx and max-eid in SQLite.

(defn maybe-datom->entity [entity]
  (cond
    (datom? entity)
    (->
      (let [[e a v tx added] entity]
        (if added
          [:db/add [e a v tx]]
          [:db/retract [e a v tx]]))
      (with-meta (get (meta entity) :source)))

    true
    entity))

(defn explode-entities [schema report]
  (let [initial-es     (:entities report)
        initial-report (assoc report :entities [])]
    (loop [report initial-report
           es     initial-es]
      (let [[entity & entities] es]
        (cond
          (nil? entity)
          report

          (map? entity)
          ;; TODO: reverse refs, lists, nested maps
          (if-let [eid (:db/id entity)]
            (let [exploded (for [[a v] (dissoc entity :db/id)]
                             [:db/add eid a v])]
              (recur report (concat exploded entities)))
            (raise "Map entity missing :db/id, got " entity
                   {:error :transact/entity-missing-db-id
                    :op    entity }))

          true
          (recur (util/conj-in report [:entities] entity) entities))))))

(defn maybe-ident->entid [db [op e a v tx :as orig]]
  (let [e (get (idents db) e e) ;; TODO: use ident, entid here.
        a (get (idents db) a a)
        v (if (ds/kw? (schema db) a) ;; TODO: decide if this is best.  We could also check for ref and numeric types.
            v
            (get (idents db) v v))]
    [op e a v tx]))

(defrecord Transaction [db tempids entities])

(defn- tx-entity [db]
  (let [tx (current-tx db)]
    [:db/add tx :db/txInstant 0xdeadbeef tx])) ;; TODO: now.

(defn maybe-add-current-tx [current-tx entity]
  (let [[op e a v tx] entity]
    [op e a v (or tx current-tx)]))

(defn ensure-entity-form [[op e a v tx :as entity]]
  (cond
    (not (sequential? entity))
    (raise "Bad entity " entity ", should be sequential at this point"
           {:error :transact/bad-entity, :entity entity})

    (not (contains? #{:db/add :db/retract} op))
    (raise "Unrecognized operation " op " expected one of :db/add :db/retract at this point"
           {:error :transact/bad-operation :entity entity })

    (not e)
    (raise "Bad entity: nil e in " entity
           {:error :transact/bad-entity :entity entity })

    (not a)
    (raise "Bad entity: nil a in " entity
           {:error :transact/bad-entity :entity entity })

    (not v)
    (raise "Bad entity: nil v in " entity
           {:error :transact/bad-entity :entity entity })

    true
    entity))

(defn preprocess [db report]
  {:pre [(db? db) (report? report)]}

  (let [initial-es (or (:entities report) [])]
    (when-not (sequential? initial-es)
      (raise "Bad transaction data " initial-es ", expected sequential collection"
             {:error :transact/syntax, :tx-data initial-es}))

    ;; TODO: find an approach that generates less garbage.
    (->
      report

      (update :entities conj (tx-entity db))

      ;; Normalize Datoms into :db/add or :db/retract vectors.
      (update :entities (partial map maybe-datom->entity))

      ;; Explode map shorthand, such as {:db/id e :attr value :_reverse ref},
      ;; to a list of vectors, like
      ;; [[:db/add e :attr value] [:db/add ref :reverse e]].
      (->> (explode-entities (schema db)))

      (update :entities (partial map ensure-entity-form))

      ;; Replace idents with entids where possible.
      (update :entities (partial map (partial maybe-ident->entid db)))

      ;; Add tx if not given.
      (update :entities (partial map (partial maybe-add-current-tx (current-tx db)))))))

(defn- lookup-ref? [x]
  (and (sequential? x)
       (= (count x) 2)))

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


(defn <resolve-lookup-refs [db report]
  {:pre [(db? db) (report? report)]}

  (go-pair
    (->>
      (vec (for [[op & entity] (:entities report)]
             (into [op] (for [field entity]
                          (if (lookup-ref? field)
                            (first (<? (<eavt db field))) ;; TODO improve this -- this should be avet, shouldn't it?
                            field)))))
      (assoc-in report [:entities])))) ;; TODO: meta.

(declare <resolve-id-literals)

(defn <retry-with-tempid [db report es tempid upserted-eid]
  (if (contains? (:tempids report) tempid)
    (go-pair
      (raise "Conflicting upsert: " tempid " resolves"
             " both to " upserted-eid " and " (get (:tempids report) tempid)
             { :error :transact/upsert }))
    ;; try to re-run from the beginning
    ;; but remembering that `old-eid` will resolve to `upserted-eid`
    (<resolve-id-literals db
                          (->
                            report
                            (assoc-in [:tempids tempid] upserted-eid)
                            (assoc-in [:entities] es)))))

(defn- transact-entity [report entity]
  (update-in report [:entities] conj entity))

(defn <resolve-id-literals
  "Upsert uniquely identified literals when possible and allocate new entids for all other id literals.

  It's worth noting that some amount of trial and error is probably
  necessary here, since [[-1 :ref -2] [-2 :ref -1]] is a valid input.
  It's my belief that no graph algorithm can correctly order the
  id-literals in quasi-linear time, since that algorithm will need to
  accept all permutations of the id-literals.  Therefore, we simplify
  by accepting that we may process the input multiple times, and we
  regain some efficiency by sorting so that upserts happen earlier and
  we are most likely to find a successful entid allocation without
  multiple trials.

  Concretely, we sort [-1 a v] < [-1 a -2] < [e a -1] < [e a v].  This
  means simple upserts will be processed early, followed by entities
  with multiple id-literals that we hope will reduce to simple upserts
  based on the earlier upserts.  After that, we handle what should be
  simple allocations."

  [db report]
  {:pre [(db? db) (report? report)]}

  (go-pair
    (let [keyfn (fn [[op e a v tx]]
                  (if (and (id-literal? e)
                           (not-any? id-literal? [a v tx]))
                    (- 5)
                    (- (count (filter id-literal? [e a v tx])))))
          initial-report (assoc report :entities []) ;; TODO.
          initial-entities (sort-by keyfn (:entities report))]
      (loop [report initial-report
             es initial-entities]
        (let [[[op e a v tx :as entity] & entities] es]
          (cond
            (nil? entity)
            report

            (and (not= op :db/add)
                 (not (empty? (filter id-literal? [e a v tx]))))
            (raise "id-literals are resolved for :db/add only"
                   {:error :transact/syntax
                    :op    entity })

            ;; Upsert!
            (and (id-literal? e)
                 (ds/unique-identity? (schema db) a)
                 (not-any? id-literal? [a v tx]))
            (let [upserted-eid (:e (first (<? (<avet db [a v]))))
                  allocated-eid (get-in report [:tempids e])]
              (if (and upserted-eid allocated-eid (not= upserted-eid allocated-eid))
                (<? (<retry-with-tempid db initial-report initial-entities e upserted-eid)) ;; TODO: not initial report, just the sorted entities here.
                (let [eid (or upserted-eid allocated-eid (next-eid db))]
                  (recur (allocate-eid report e eid) (cons [op eid a v tx] entities)))))

            ;; Start allocating and retrying.  We try with e last, so as to eventually upsert it.
            (id-literal? tx)
            ;; TODO: enforce tx part only?
            (let [eid (or (get-in report [:tempids tx]) (next-eid db))]
              (recur (allocate-eid report tx eid) (cons [op e a v eid] entities)))

            (id-literal? v)
            ;; We can't fail with unbound literals here, since we could have multiple
            (let [eid (or (get-in report [:tempids v]) (next-eid db))]
              (recur (allocate-eid report v eid) (cons [op e a eid tx] entities)))

            (id-literal? a)
            ;; TODO: should we even allow id-literal attributes?  Datomic fails in some cases here.
            (let [eid (or (get-in report [:tempids a]) (next-eid db))]
              (recur (allocate-eid report a eid) (cons [op e eid v tx] entities)))

            (id-literal? e)
            (let [eid (or (get-in report [:tempids e]) (next-eid db))]
              (recur (allocate-eid report e eid) (cons [op eid a v tx] entities)))

            true
            (recur (transact-entity report entity) entities)
            ))))))

(defn- transact-report [report datom]
  (update-in report [:tx-data] conj datom))

(defn- <ensure-schema-constraints
  "Throw unless all entities in :entities obey the schema constraints."

  [db report]
  {:pre [(db? db) (report? report)]}

  ;; TODO: consider accumulating errors to show more meaningful error reports.
  ;; TODO: constrain entities; constrain attributes.

  (go-pair
    (doseq [[op e a v tx] (:entities report)]
      (ds/ensure-valid-value (schema db) a v))
    report))

(defn- <ensure-unique-constraints
  "Throw unless all datoms in :tx-data obey the uniqueness constraints."

  [db report]
  {:pre [(db? db) (report? report)]}

  ;; TODO: consider accumulating errors to show more meaningful error reports.
  ;; TODO: constrain entities; constrain attributes.

  (go-pair
    ;; TODO: comment on applying datoms that violate uniqueness.
    (let [unique-datoms (transient {})] ;; map (nil, a, v)|(e, a, nil)|(e, a, v) -> datom.
      (doseq [[e a v tx added :as datom] (:tx-data report)]

        (when added
          ;; Check for violated :db/unique constraint between datom and existing store.
          (when (ds/unique? (schema db) a)
            (when-let [found (first (<? (<avet db [a v])))]
              (raise "Cannot add " datom " because of unique constraint: " found
                     {:error :transact/unique
                      :attribute a ;; TODO: map attribute back to ident.
                      :entity datom})))

          ;; Check for violated :db/unique constraint between datoms.
          (when (ds/unique? (schema db) a)
            (let [key [nil a v]]
              (when-let [other (get unique-datoms key)]
                (raise "Cannot add " datom " and " other " because together they violate unique constraint"
                       {:error :transact/unique
                        :attribute a ;; TODO: map attribute back to ident.
                        :entity datom}))
              (assoc! unique-datoms key datom)))

          ;; Check for violated :db/cardinality :db.cardinality/one constraint between datoms.
          (when-not (ds/multival? (schema db) a)
            (let [key [e a nil]]
              (when-let [other (get unique-datoms key)]
                (raise "Cannot add " datom " and " other " because together they violate cardinality constraint"
                       {:error :transact/unique
                        :entity datom}))
              (assoc! unique-datoms key datom)))

          ;; Check for duplicated datoms.  Datomic doesn't allow overlapping writes, and we don't
          ;; want to guarantee order, so we don't either.
          (let [key [e a v]]
            (when-let [other (get unique-datoms key)]
              (raise "Cannot add duplicate " datom
                     {:error :transact/unique
                      :entity datom}))
            (assoc! unique-datoms key datom)))))
    report))

(defn <entities->tx-data [db report]
  {:pre [(db? db) (report? report)]}
  (go-pair
    (let [initial-report report]
      (loop [report initial-report
             es (:entities initial-report)]
        (let [[[op e a v tx :as entity] & entities] es]
          (cond
            (nil? entity)
            report

            (= op :db/add)
            (if (ds/multival? (schema db) a)
              (if (empty? (<? (<eavt db [e a v])))
                (recur (transact-report report (datom e a v tx true)) entities)
                (recur report entities))
              (if-let [^Datom old-datom (first (<? (<eavt db [e a])))]
                (if  (= (.-v old-datom) v)
                  (recur report entities)
                  (recur (-> report
                             (transact-report (datom e a (.-v old-datom) tx false))
                             (transact-report (datom e a v tx true)))
                         entities))
                (recur (transact-report report (datom e a v tx true)) entities)))

            (= op :db/retract)
            (if (first (<? (<eavt db [e a v])))
              (recur (transact-report report (datom e a v tx false)) entities)
              (recur report entities))

            true
            (raise "Unknown operation at " entity ", expected :db/add, :db/retract"
                   {:error :transact/syntax, :operation op, :tx-data entity})))))))

(defn <transact-tx-data
  [db now report]
  {:pre [(db? db) (report? report)]}

  (go-pair
    (->>
      report
      (preprocess db)

      (<resolve-lookup-refs db)
      (<?)

      (<resolve-id-literals db)
      (<?)

      (<ensure-schema-constraints db)
      (<?)

      (<entities->tx-data db)
      (<?)

      (<ensure-unique-constraints db)
      (<?))))

;; Normalize as [op int|id-literal int|id-literal value|id-literal tx|id-literal]. ;; TODO: mention lookup-refs.

;; Replace lookup-refs with entids where possible.

;; Upsert or allocate id-literals.

(defn- is-ident? [db [_ a & _]]
  (= a (get-in db [:idents :db/ident])))

(defn collect-db-ident-assertions
  "Transactions may add idents, install new partitions, and install new schema attributes.
  Collect :db/ident assertions into :added-idents here."
  [db report]
  {:pre [(db? db) (report? report)]}

  ;; TODO: use q to filter the report!
  (let [original-report report
        tx-data (:tx-data report)
        original-ident-assertions (filter (partial is-ident? db) tx-data)]
    (loop [report original-report
           ident-assertions original-ident-assertions]
      (let [[ia & ias] ident-assertions]
        (cond
          (nil? ia)
          report

          (not (:added ia))
          (raise "Retracting a :db/ident is not yet supported, got " ia
                 {:error :schema/idents
                  :op    ia })

          :else
          ;; Added.
          (let [ident (:v ia)]
            (if (keyword? ident)
              (recur (assoc-in report [:added-idents ident] (:e ia)) ias)
              (raise "Cannot assert a :db/ident with a non-keyword value, got " ia
                     {:error :schema/idents
                      :op    ia }))))))))

(defn- symbolicate-datom [db [e a v tx added]]
  (let [entids (zipmap (vals (idents db)) (keys (idents db)))
        symbolicate (fn [x]
                      (get entids x x))]
    (datom
      (symbolicate e)
      (symbolicate a)
      (symbolicate v)
      (symbolicate tx)
      added)))

(defn collect-db-install-assertions
  "Transactions may add idents, install new partitions, and install new schema attributes.
  Collect [:db.part/db :db.install/attribute] assertions here."
  [db report]
  {:pre [(db? db) (report? report)]}

  ;; TODO: be more efficient; symbolicating each datom is expensive!
  (let [datoms (map (partial symbolicate-datom db) (:tx-data report))
        schema-fragment (datomish.schema-changes/datoms->schema-fragment datoms)]
    (assoc-in report [:added-attributes] schema-fragment)))

;; TODO: lift to IDB.
(defn <apply-db-ident-assertions [db added-idents]
  (go-pair
    (let [->SQLite (get-in ds/value-type-map [:db.type/keyword :->SQLite]) ;; TODO: make this a protocol.
          exec     (partial s/execute! (:sqlite-connection db))]
      ;; TODO: batch insert.
      (doseq [[ident entid] added-idents]
        (<? (exec
              ["INSERT INTO idents VALUES (?, ?)" (->SQLite ident) entid]))))
    db))

(defn <apply-db-install-assertions [db fragment]
  (go-pair
    (let [->SQLite (get-in ds/value-type-map [:db.type/keyword :->SQLite]) ;; TODO: make this a protocol.
          exec     (partial s/execute! (:sqlite-connection db))]
      ;; TODO: batch insert.
      (doseq [[ident attr-map] fragment]
        (doseq [[attr value] attr-map]
          (<? (exec
                ["INSERT INTO schema VALUES (?, ?, ?)" (->SQLite ident) (->SQLite attr) (->SQLite value)])))))
    db))

(defn- <with-internal [db tx-data merge-ident merge-attr]
  (go-pair
    (let [report (->>
                   (map->TxReport
                     {:db-before        db
                      :db-after         db
                      ;; :current-tx current-tx
                      :entities         tx-data
                      :tx-data          []
                      :tempids          {}
                      :added-parts      {}
                      :added-idents     {}
                      :added-attributes {}
                      })

                   (<transact-tx-data db 0xdeadbeef) ;; TODO: timestamp properly.
                   (<?)

                   (collect-db-ident-assertions db)

                   (collect-db-install-assertions db))
          idents          (merge-with merge-ident (:idents db) (:added-idents report))
          symbolic-schema (merge-with merge-attr (:symbolic-schema db) (:added-attributes report))
          schema          (ds/schema (into {} (map (fn [[k v]] [(k idents) v]) symbolic-schema)))
          db-after        (->
                            db

                            (<apply-datoms (:tx-data report))
                            (<?)

                            (<apply-db-ident-assertions (:added-idents report))
                            (<?)

                            (<apply-db-install-assertions (:added-attributes report))
                            (<?)

                            (assoc :idents idents
                                   :symbolic-schema symbolic-schema
                                   :schema schema)

                            (<advance-tx)
                            (<?))]
      (-> report
          (assoc-in [:db-after] db-after)))))

(defn- <with [db tx-data]
  (let [fail-touch-ident (fn [old new] (raise "Altering idents is not yet supported, got " new " altering existing ident " old
                                              {:error :schema/alter-idents :old old :new new}))
        fail-touch-attr  (fn [old new] (raise "Altering schema attributes is not yet supported, got " new " altering existing schema attribute " old
                                              {:error :schema/alter-schema :old old :new new}))]
    (<with-internal db tx-data fail-touch-ident fail-touch-attr)))

(defn <db-with [db tx-data]
  (go-pair
    (:db-after (<? (<with db tx-data)))))

(defn <transact!
  ([conn tx-data]
   (<transact! conn tx-data 0xdeadbeef)) ;; TODO: timestamp!
  ([conn tx-data now]
   {:pre [(conn? conn)]}
   (let [db (db conn)] ;; TODO: be careful with swapping atoms.
     (s/in-transaction!
       (:sqlite-connection db)
       #(go-pair
          (let [report (<? (<with db tx-data))] ;; TODO: timestamp!
            (reset! (:current-db conn) (:db-after report))
            report))))))
