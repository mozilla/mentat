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
   [datomish.db :as db :refer [id-literal id-literal?]]
   [datomish.db.debug :as debug]
   [datomish.datom :as dd :refer [datom datom? #?@(:cljs [Datom])]]
   [datomish.util :as util #?(:cljs :refer-macros :clj :refer) [raise raise-str cond-let]]
   [datomish.schema :as ds]
   [datomish.schema-changes]
   [datomish.sqlite :as s]
   [datomish.sqlite-schema :as sqlite-schema]
   [datomish.transact.bootstrap :as bootstrap]
   [datomish.transact.explode :as explode]
   [taoensso.tufte :as tufte
    #?(:cljs :refer-macros :clj :refer) [defnp p profiled profile]]
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

(defrecord TxReport [db-before ;; The DB before the transaction.
                     db-after  ;; The DB after the transaction.
                     tx        ;; The tx ID represented by the transaction in this report; refer :db/tx.
                     txInstant ;; The timestamp instant when the the transaction was processed/committed in this report; refer :db/txInstant.
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

(defn maybe-ident->entid [db [op e a v tx :as orig]]
  (let [e (db/entid db e)
        a (db/entid db a)
        v (if (ds/kw? (db/schema db) a) ;; TODO: decide if this is best.  We could also check for ref and numeric types.
            v
            (db/entid db v))]
    [op e a v tx]))

(defrecord Transaction [db tempids entities])

(defn- tx-entity [db report]
  {:pre [(db/db? db) (report? report)]}
  (let [tx        (:tx report)
        txInstant (:txInstant report)]
    ;; n.b., this must not be symbolic -- it's inserted after we map idents -> entids.
    [:db/add tx (db/entid db :db/txInstant) txInstant]))

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
       (= (db/entid db e) (db/entid db :db/tx))
       (= (db/entid db a) (db/entid db :db/txInstant))))

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
        db*        (db/with-ident db :db/tx tx)]
    (when-not (sequential? initial-es)
      (raise "Bad transaction data " initial-es ", expected sequential collection"
             {:error :transact/syntax, :tx-data initial-es}))

    ;; TODO: find an approach that generates less garbage.
    (->
      report

      ;; Normalize Datoms into :db/add or :db/retract vectors.
      (update :entities (partial map maybe-datom->entity))

      (update :entities (partial explode/explode-entities db))

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
                            (first (<? (apply db/<av db field)))
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
            (let [upserted-eid (:e (first (<? (db/<av db a v))))
                  allocated-eid (get-in report [:tempids e])]
              (if (and upserted-eid allocated-eid (not= upserted-eid allocated-eid))
                (<? (<retry-with-tempid db initial-report initial-entities e upserted-eid)) ;; TODO: not initial report, just the sorted entities here.
                (let [eid (or upserted-eid allocated-eid (<? (db/<next-eid db e)))]
                  (recur (allocate-eid report e eid) (cons [op eid a v] entities)))))

            ;; Start allocating and retrying.  We try with e last, so as to eventually upsert it.
            (id-literal? v)
            ;; We can't fail with unbound literals here, since we could have multiple.
            (let [eid (or (get-in report [:tempids v]) (<? (db/<next-eid db e)))]
              (recur (allocate-eid report v eid) (cons [op e a eid] entities)))

            (id-literal? a)
            ;; TODO: should we even allow id-literal attributes?  Datomic fails in some cases here.
            (let [eid (or (get-in report [:tempids a]) (<? (db/<next-eid db e)))]
              (recur (allocate-eid report a eid) (cons [op e eid v] entities)))

            (id-literal? e)
            (let [eid (or (get-in report [:tempids e]) (<? (db/<next-eid db e)))]
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

(defn <transact-tx-data
  [db report]
  {:pre [(db/db? db) (report? report)]}

  (let [<apply-entities (fn [db report]
                          (go-pair
                            (let [tx-data (<? (db/<apply-entities db (:tx report) (:entities report)))]
                              (assoc report :tx-data tx-data))))]
    (go-pair
      (->>
        report
        (preprocess db)

        (<resolve-lookup-refs db)
        (<?)
        (p :resolve-lookup-refs)

        (<resolve-id-literals db)
        (<?)
        (p :resolve-id-literals)

        (<ensure-schema-constraints db)
        (<?)
        (p :ensure-schema-constraints)

        (<apply-entities db)
        (<?)
        (p :apply-entities)
        ))))

(defn- is-ident? [db [_ a & _]]
  (= a (db/entid db :db/ident)))

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

(defn- symbolicate-datom [db [e a v tx added]]
  (datom
    (db/ident db e)
    (db/ident db a)
    (db/ident db v)
    tx
    added))

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
                      :tx                (<? (db/<next-eid db (id-literal :db.part/tx)))
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
                   (p :transact-tx-data)

                   (collect-db-ident-assertions db)
                   (p :collect-db-ident-assertions)

                   (collect-db-install-assertions db)
                   (p :collect-db-install-assertions))

          db-after (->
                     db

                     (db/<apply-db-ident-assertions (:added-idents report) merge-ident)
                     (<?)
                     (->> (p :apply-db-ident-assertions))

                     (db/<apply-db-install-assertions (:added-attributes report) merge-attr)
                     (<?)
                     (->> (p :apply-db-install-assertions)))
          ]
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
