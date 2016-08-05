;; This Source Code Form is subject to the terms of the Mozilla Public
;; License, v. 2.0. If a copy of the MPL was not distributed with this
;; file, You can obtain one at http://mozilla.org/MPL/2.0/.

(ns datomish.transact
  #?(:cljs
     (:require-macros
      [datomish.pair-chan :refer [go-pair <?]]
      [cljs.core.async.macros :refer [go]]))
  (:require
   [datomish.query.context :as context]
   [datomish.query.projection :as projection]
   [datomish.query.source :as source]
   [datomish.query :as query]
   [datomish.db :as db]
   [datomish.datom :as dd :refer [datom datom? #?@(:cljs [Datom])]]
   [datomish.util :as util #?(:cljs :refer-macros :clj :refer) [raise raise-str cond-let]]
   [datomish.schema :as ds]
   [datomish.schema-changes]
   [datomish.sqlite :as s]
   [datomish.sqlite-schema :as sqlite-schema]
   [datomish.transact.bootstrap :as bootstrap]
   #?@(:clj [[datomish.pair-chan :refer [go-pair <?]]
             [clojure.core.async :as a :refer [chan go <! >!]]])
   #?@(:cljs [[datomish.pair-chan]
              [cljs.core.async :as a :refer [chan <! >!]]]))
  #?(:clj
     (:import
      [datomish.datom Datom])))

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
  (close [conn] (db/close-db @(:current-db conn)))

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
                     current-tx ;; The tx ID represented by the transaction in this report.
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

(defn- #?@(:clj  [^Boolean reverse-ref?]
           :cljs [^boolean reverse-ref?]) [attr]
  (if (keyword? attr)
    (= \_ (nth (name attr) 0))
    (raise "Bad attribute type: " attr ", expected keyword"
           {:error :transact/syntax, :attribute attr})))

(defn- reverse-ref [attr]
  (if (keyword? attr)
    (if (reverse-ref? attr)
      (keyword (namespace attr) (subs (name attr) 1))
      (keyword (namespace attr) (str "_" (name attr))))
    (raise "Bad attribute type: " attr ", expected keyword"
           {:error :transact/syntax, :attribute attr})))

(declare explode-entity)

(defn- explode-entity-a-v [db entity eid a v]
  ;; a should be symbolic at this point.  Map it.  TODO: use ident/entid to ensure we have a symbolic attr.
  (let [reverse?    (reverse-ref? a)
        straight-a  (if reverse? (reverse-ref a) a)
        straight-a* (get-in db [:idents straight-a] straight-a)
        _           (when (and reverse? (not (ds/ref? (db/schema db) straight-a*)))
                      (raise "Bad attribute " a ": reverse attribute name requires {:db/valueType :db.type/ref} in schema"
                             {:error :transact/syntax, :attribute a, :op entity}))
        a*          (get-in db [:idents a] a)]
    (cond
      reverse?
      (explode-entity-a-v db entity v straight-a eid)

      (and (map? v)
           (not (id-literal? v)))
      ;; Another entity is given as a nested map.
      (if (ds/ref? (db/schema db) straight-a*)
        (let [other (assoc v (reverse-ref a) eid
                           ;; TODO: make the new ID have the same part as the original eid.
                           ;; TODO: make the new ID not show up in the tempids map.  (Does Datomic exposed the new ID this way?)
                           :db/id (id-literal :db.part/user))]
          (explode-entity db other))
        (raise "Bad attribute " a ": nested map " v " given but attribute name requires {:db/valueType :db.type/ref} in schema"
               {:error :transact/entity-map-type-ref
                :op    entity }))

      (sequential? v)
      (if (ds/multival? (db/schema db) a*) ;; dm/schema
        (mapcat (partial explode-entity-a-v db entity eid a) v) ;; Allow sequences of nested maps, etc.  This does mean [[1]] will work.
        (raise "Sequential values " v " but attribute " a " is :db.cardinality/one"
               {:error :transact/entity-sequential-cardinality-one
                :op    entity }))

      true
      [[:db/add eid a* v]])))

(defn- explode-entity [db entity]
  (if (map? entity)
    (if-let [eid (:db/id entity)]
      (mapcat (partial apply explode-entity-a-v db entity eid) (dissoc entity :db/id))
      (raise "Map entity missing :db/id, got " entity
             {:error :transact/entity-missing-db-id
              :op    entity }))
    [entity]))

(defn explode-entities [db entities]
  "Explode map shorthand, such as {:db/id e :attr value :_reverse ref}, to a list of vectors,
  like [[:db/add e :attr value] [:db/add ref :reverse e]]."
  (mapcat (partial explode-entity db) entities))

(defn maybe-ident->entid [db [op e a v tx :as orig]]
  (let [e (get (db/idents db) e e) ;; TODO: use ident, entid here.
        a (get (db/idents db) a a)
        v (if (ds/kw? (db/schema db) a) ;; TODO: decide if this is best.  We could also check for ref and numeric types.
            v
            (get (db/idents db) v v))]
    [op e a v tx]))

(defrecord Transaction [db tempids entities])

(defn- tx-entity [db report]
  {:pre [(db/db? db) (report? report)]}
  (let [tx        (:tx report)
        txInstant (:txInstant report)]
    ;; n.b., this must not be symbolic -- it's inserted after we map idents -> entids.
    [:db/add tx (get-in db [:idents :db/txInstant]) txInstant]))

(defn ensure-entity-form [[op e a v & rest :as entity]]
  (cond
    (not (sequential? entity))
    (raise "Bad entity " entity ", should be sequential at this point"
           {:error :transact/bad-entity, :entity entity})

    (not (contains? #{:db/add :db/retract} op))
    (raise "Unrecognized operation " op " expected one of :db/add :db/retract at this point"
           {:error :transact/bad-operation :entity entity })

    (nil? e)
    (raise "Bad entity: nil e in " entity
           {:error :transact/bad-entity :entity entity })

    (nil? a)
    (raise "Bad entity: nil a in " entity
           {:error :transact/bad-entity :entity entity })

    (nil? v)
    (raise "Bad entity: nil v in " entity
           {:error :transact/bad-entity :entity entity })

    (some? rest)
    (raise "Bad entity: too long " entity
           {:error :transact/bad-entity :entity entity })

    true
    entity))

(defn- tx-instant? [db [op e a & _]]
  (and (= op :db/add)
       (= e (get-in db [:idents :db/tx]))
       (= a (get-in db [:idents :db/txInstant]))))

(defn- update-txInstant [db report]
  "Extract [:db/add :db/tx :db/txInstant ...], and update :txInstant with that value."
  {:pre [(db/db? db) (report? report)]}

  ;; TODO: be more efficient here: don't iterate all entities.
  (if-let [[_ _ _ txInstant] (first (filter (partial tx-instant? db) (:entities report)))]
    (assoc report :txInstant txInstant)
    report))

(defn preprocess [db report]
  {:pre [(db/db? db) (report? report)]}

  (let [initial-es (or (:entities report) [])
        ;; :db/tx is a "dynamic enum ident" that maps to the current transaction ID.  This approach
        ;; mimics DataScript's :db/current-tx.  (We don't follow DataScript because
        ;; current-txInstant is awkward.)  It's much simpler than Datomic's approach, which appears
        ;; to unify all id-literals in :db.part/tx to the current transaction value, but also seems
        ;; inconsistent.
        tx         (:tx report)
        db*        (assoc-in db [:idents :db/tx] tx)]
    (when-not (sequential? initial-es)
      (raise "Bad transaction data " initial-es ", expected sequential collection"
             {:error :transact/syntax, :tx-data initial-es}))

    ;; TODO: find an approach that generates less garbage.
    (->
      report

      ;; Normalize Datoms into :db/add or :db/retract vectors.
      (update :entities (partial map maybe-datom->entity))

      (update :entities (partial explode-entities db))

      (update :entities (partial map ensure-entity-form))

      ;; Replace idents with entids where possible, using db* to capture :db/tx.
      (update :entities (partial map (partial maybe-ident->entid db*)))

      ;; If an explicit [:db/add :db/tx :db/txInstant] is not given, add one.  Use db* to
      ;; capture :db/tx.
      (update :entities (fn [entities]
                          (if (first (filter (partial tx-instant? db*) entities))
                            entities
                            (conj entities (tx-entity db report)))))

      ;; Extract the current txInstant for the report.
      (->> (update-txInstant db*)))))

(defn- lookup-ref? [x]
  "Return true if `x` is like [:attr value]."
  (and (sequential? x)
       (= (count x) 2)
       (or (keyword? (first x))
           (integer? (first x)))))

(defn <resolve-lookup-refs [db report]
  {:pre [(db/db? db) (report? report)]}

  (go-pair
    (->>
      (vec (for [[op & entity] (:entities report)]
             (into [op] (for [field entity]
                          (if (lookup-ref? field)
                            (first (<? (db/<eavt db field))) ;; TODO improve this -- this should be avet, shouldn't it?
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
  {:pre [(db/db? db) (report? report)]}

  (go-pair
    (let [keyfn (fn [[op e a v]]
                  (if (and (id-literal? e)
                           (not-any? id-literal? [a v]))
                    (- 5)
                    (- (count (filter id-literal? [e a v])))))
          initial-report (assoc report :entities []) ;; TODO.
          initial-entities (sort-by keyfn (:entities report))]
      (loop [report initial-report
             es initial-entities]
        (let [[[op e a v :as entity] & entities] es]
          (cond
            (nil? entity)
            report

            (and (not= op :db/add)
                 (not (empty? (filter id-literal? [e a v]))))
            (raise "id-literals are resolved for :db/add only"
                   {:error :transact/syntax
                    :op    entity })

            ;; Upsert!
            (and (id-literal? e)
                 (ds/unique-identity? (db/schema db) a)
                 (not-any? id-literal? [a v]))
            (let [upserted-eid (:e (first (<? (db/<avet db [a v]))))
                  allocated-eid (get-in report [:tempids e])]
              (if (and upserted-eid allocated-eid (not= upserted-eid allocated-eid))
                (<? (<retry-with-tempid db initial-report initial-entities e upserted-eid)) ;; TODO: not initial report, just the sorted entities here.
                (let [eid (or upserted-eid allocated-eid (next-eid db))]
                  (recur (allocate-eid report e eid) (cons [op eid a v] entities)))))

            ;; Start allocating and retrying.  We try with e last, so as to eventually upsert it.
            (id-literal? v)
            ;; We can't fail with unbound literals here, since we could have multiple.
            (let [eid (or (get-in report [:tempids v]) (next-eid db))]
              (recur (allocate-eid report v eid) (cons [op e a eid] entities)))

            (id-literal? a)
            ;; TODO: should we even allow id-literal attributes?  Datomic fails in some cases here.
            (let [eid (or (get-in report [:tempids a]) (next-eid db))]
              (recur (allocate-eid report a eid) (cons [op e eid v] entities)))

            (id-literal? e)
            (let [eid (or (get-in report [:tempids e]) (next-eid db))]
              (recur (allocate-eid report e eid) (cons [op eid a v] entities)))

            true
            (recur (transact-entity report entity) entities)
            ))))))

(defn- transact-report [report datom]
  (update-in report [:tx-data] conj datom))

(defn- <ensure-schema-constraints
  "Throw unless all entities in :entities obey the schema constraints."

  [db report]
  {:pre [(db/db? db) (report? report)]}

  ;; TODO: consider accumulating errors to show more meaningful error reports.
  ;; TODO: constrain entities; constrain attributes.

  (go-pair
    (let [schema (db/schema db)]
      (doseq [[op e a v] (:entities report)]
        (ds/ensure-valid-value schema a v)))
    report))

(defn- <ensure-unique-constraints
  "Throw unless all datoms in :tx-data obey the uniqueness constraints."

  [db report]
  {:pre [(db/db? db) (report? report)]}

  ;; TODO: consider accumulating errors to show more meaningful error reports.
  ;; TODO: constrain entities; constrain attributes.

  (go-pair
    ;; TODO: comment on applying datoms that violate uniqueness.
    (let [schema (db/schema db)
          unique-datoms (transient {})] ;; map (nil, a, v)|(e, a, nil)|(e, a, v) -> datom.
      (doseq [[e a v tx added :as datom] (:tx-data report)]

        (when added
          ;; Check for violated :db/unique constraint between datom and existing store.
          (when (ds/unique? schema a)
            (when-let [found (first (<? (db/<avet db [a v])))]
              (raise "Cannot add " datom " because of unique constraint: " found
                     {:error :transact/unique
                      :attribute a ;; TODO: map attribute back to ident.
                      :entity datom})))

          ;; Check for violated :db/unique constraint between datoms.
          (when (ds/unique? schema a)
            (let [key [nil a v]]
              (when-let [other (get unique-datoms key)]
                (raise "Cannot add " datom " and " other " because together they violate unique constraint"
                       {:error :transact/unique
                        :attribute a ;; TODO: map attribute back to ident.
                        :entity datom}))
              (assoc! unique-datoms key datom)))

          ;; Check for violated :db/cardinality :db.cardinality/one constraint between datoms.
          (when-not (ds/multival? schema a)
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
  {:pre [(db/db? db) (report? report)]}
  (go-pair
    (let [initial-report report
          {tx :tx}       report
          schema         (db/schema db)]
      (loop [report initial-report
             es (:entities initial-report)]
        (let [[[op e a v :as entity] & entities] es]
          (cond
            (nil? entity)
            report

            (= op :db/add)
            (if (ds/multival? schema a)
              (if (empty? (<? (db/<eavt db [e a v])))
                (recur (transact-report report (datom e a v tx true)) entities)
                (recur report entities))
              (if-let [^Datom old-datom (first (<? (db/<eavt db [e a])))]
                (if  (= (.-v old-datom) v)
                  (recur report entities)
                  (recur (-> report
                             (transact-report (datom e a (.-v old-datom) tx false))
                             (transact-report (datom e a v tx true)))
                         entities))
                (recur (transact-report report (datom e a v tx true)) entities)))

            (= op :db/retract)
            (if (first (<? (db/<eavt db [e a v])))
              (recur (transact-report report (datom e a v tx false)) entities)
              (recur report entities))

            true
            (raise "Unknown operation at " entity ", expected :db/add, :db/retract"
                   {:error :transact/syntax, :operation op, :tx-data entity})))))))

(defn <transact-tx-data
  [db report]
  {:pre [(db/db? db) (report? report)]}

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

;; Normalize as [op int|id-literal int|id-literal value|id-literal]. ;; TODO: mention lookup-refs.

;; Replace lookup-refs with entids where possible.

;; Upsert or allocate id-literals.

(defn- is-ident? [db [_ a & _]]
  (= a (get-in db [:idents :db/ident])))

(defn collect-db-ident-assertions
  "Transactions may add idents, install new partitions, and install new schema attributes.
  Collect :db/ident assertions into :added-idents here."
  [db report]
  {:pre [(db/db? db) (report? report)]}

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

(defn- symbolicate-datom [db [e a v added]]
  (let [entids (zipmap (vals (db/idents db)) (keys (db/idents db)))
        symbolicate (fn [x]
                      (get entids x x))]
    (datom
      (symbolicate e)
      (symbolicate a)
      (symbolicate v)
      added)))

(defn collect-db-install-assertions
  "Transactions may add idents, install new partitions, and install new schema attributes.
  Collect [:db.part/db :db.install/attribute] assertions here."
  [db report]
  {:pre [(db/db? db) (report? report)]}

  ;; TODO: be more efficient; symbolicating each datom is expensive!
  (let [datoms (map (partial symbolicate-datom db) (:tx-data report))
        schema-fragment (datomish.schema-changes/datoms->schema-fragment datoms)]
    (assoc-in report [:added-attributes] schema-fragment)))

;; TODO: expose this in a more appropriate way.
(defn <with-internal [db tx-data merge-ident merge-attr]
  (go-pair
    (let [report (->>
                   (map->TxReport
                     {:db-before         db
                      :db-after          db
                      ;; This mimics DataScript.  It's convenient to be able to extract the
                      ;; transaction ID and transaction timestamp directly from the report; Datomic
                      ;; makes this surprisingly difficult: one needs a :db.part/tx temporary and an
                      ;; explicit upsert of that temporary.
                      :tx                (db/current-tx db)
                      :txInstant         (db/now db)
                      :entities          tx-data
                      :tx-data           []
                      :tempids           {}
                      :added-parts       {}
                      :added-idents      {}
                      :added-attributes  {}
                      })

                   (<transact-tx-data db)
                   (<?)

                   (collect-db-ident-assertions db)

                   (collect-db-install-assertions db))
          idents          (merge-with merge-ident (:idents db) (:added-idents report))
          symbolic-schema (merge-with merge-attr (:symbolic-schema db) (:added-attributes report))
          schema          (ds/schema (into {} (map (fn [[k v]] [(k idents) v]) symbolic-schema)))
          db-after        (->
                            db

                            (db/<apply-datoms (:tx-data report))
                            (<?)

                            (db/<apply-db-ident-assertions (:added-idents report))
                            (<?)

                            (db/<apply-db-install-assertions (:added-attributes report))
                            (<?)

                            ;; TODO: abstract this.
                            (assoc :idents idents
                                   :symbolic-schema symbolic-schema
                                   :schema schema)

                            (db/<advance-tx)
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
  [conn tx-data]
  {:pre [(conn? conn)]}
  (let [db (db conn)] ;; TODO: be careful with swapping atoms.
    (db/in-transaction!
      db
      #(go-pair
         (let [report (<? (<with db tx-data))]
           (reset! (:current-db conn) (:db-after report))
           report)))))
