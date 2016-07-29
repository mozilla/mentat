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

(defrecord TxReport [db-before db-after entities tx-data tempids])

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

(def default-schema
  {
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
                          :db/index       true}
   :db/ident             {:db/valueType   :db.type/keyword
                          :db/cardinality :db.cardinality/one
                          :db/unique      :db.unique/identity}
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

(defn <idents [sqlite-connection]
  (go-pair
    (let [rows (<? (->>
                     {:select [:e :v] :from [:datoms] :where [:= :a ":db/ident"]} ;; TODO: use raw entid.
                     (sql/format)
                     (s/all-rows sqlite-connection)))]
      (into {} (map #(-> {(keyword (:v %)) (:e %)})) rows))))

(defn <db-with-sqlite-connection
  ([sqlite-connection]
   (<db-with-sqlite-connection sqlite-connection {}))

  ([sqlite-connection schema]
   (go-pair
     (when-not (= sqlite-schema/current-version (<? (sqlite-schema/<ensure-current-version sqlite-connection)))
       (raise "Could not ensure current SQLite schema version."))
     (let [idents (clojure.set/union [:db/txInstant :db/ident :db.part/db :db.install/attribute :db.type/string :db.type/integer :db.type/ref :db/id :db.cardinality/one :db.cardinality/many :db/cardinality :db/valueType :x :y :name :aka :test/kw :age :email :spouse] (keys default-schema))
           idents (into {} (map-indexed #(vector %2 %1) idents))
           idents (into (<? (<idents sqlite-connection)) idents) ;; TODO: pre-populate idents and SQLite tables?
           symbolic-schema (merge schema default-schema)]
       (map->DB
         {:sqlite-connection sqlite-connection
          :idents            idents
          :symbolic-schema   symbolic-schema
          :schema            (ds/schema (into {} (map (fn [[k v]] [(k idents) v]) symbolic-schema)))
          :current-tx        tx0})))))

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

(defn maybe-ident->entid [db [op & entity :as orig]]
  ;; TODO: use something faster than `into` here.
  (->
    (into [op] (for [field entity]
                 (get (idents db) field field))) ;; TODO: schema, not db.
    ;; (with-meta (get (meta orig) :source {:source orig}))
    ))

(defrecord Transaction [db tempids entities])

(defn- tx-entity [db]
  (let [tx (current-tx db)]
    [:db/add tx :db/txInstant 0xdeadbeef tx])) ;; TODO: now.

(defn maybe-add-current-tx [current-tx entity]
  (let [[op e a v tx] entity]
    [op e a v (or tx current-tx)]))

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

(defn process-db-ident-assertions
  "Transactions may add idents, install new partitions, and install new schema attributes.
  Handle :db/ident assertions here."
  [db report]
  {:pre [(db? db) (report? report)]}

  ;; TODO: use q to filter the report!
  (let [original-db db
        tx-data (:tx-data report)
        original-ident-assertions (filter (partial is-ident? db) tx-data)]
    (loop [db original-db
           ident-assertions original-ident-assertions]
      (let [[ia & ias] ident-assertions]
        (cond
          (nil? ia)
          db

          (not (:added ia))
          (raise "Retracting a :db/ident is not yet supported, got " ia
                 {:error :schema/idents
                  :op    ia })

          :else
          ;; Added.
          (let [ident (:v ia)]
            ;; TODO: accept re-assertions?
            (when (get-in db [:idents ident])
              (raise "Re-asserting a :db/ident is not yet supported, got " ia
                     {:error :schema/idents
                      :op    ia }))
            (if (keyword? ident)
              (recur (assoc-in db [:idents ident] (:e ia)) ias)
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

(defn process-db-install-assertions
  "Transactions may add idents, install new partitions, and install new schema attributes.
  Handle [:db.part/db :db.install/attribute] assertions here."
  [db report]
  {:pre [(db? db) (report? report)]}

  ;; TODO: be more efficient; symbolicating each datom is expensive!
  (let [datoms (map (partial symbolicate-datom db) (:tx-data report))
        schema-fragment (datomish.schema-changes/datoms->schema-fragment datoms)
        fail (fn [old new] (raise "Altering schema elements is not yet supported, got " new " altering existing schema element " old
                                  {:error :schema/alter-schema :old old :new new}))]

    (if (empty? schema-fragment)
      db
      (let [symbolic-schema (merge-with fail (:symbolic-schema db) schema-fragment)
            schema (ds/schema (into {} (map (fn [[k v]] [(k (idents db)) v]) symbolic-schema)))]
        (assoc db
               :symbolic-schema symbolic-schema
               :schema schema)))))

(defn <with [db tx-data]
  (go-pair
    (let [report (<? (<transact-tx-data db 0xdeadbeef ;; TODO
                                        (map->TxReport
                                          {:db-before  db
                                           :db-after   db
                                           ;; :current-tx current-tx
                                           :entities   tx-data
                                           :tx-data    []
                                           :tempids    {}})))
          db-after (->
                     db

                     (<apply-datoms (:tx-data report))
                     (<?)

                     (<advance-tx)
                     (<?)

                     (process-db-ident-assertions report)

                     (process-db-install-assertions report))]
      (-> report
          (assoc-in [:db-after] db-after)))))

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
