;; This Source Code Form is subject to the terms of the Mozilla Public
;; License, v. 2.0. If a copy of the MPL was not distributed with this
;; file, You can obtain one at http://mozilla.org/MPL/2.0/.

(ns datomish.transact
  #?(:cljs
     (:require-macros
      [datomish.pair-chan :refer [go-pair <?]]
      [cljs.core.async.macros :refer [go go-loop]]))
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
             [clojure.core.async :as a :refer [chan go go-loop <! >!]]])
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

(defrecord Connection [current-db transact-chan]
  IConnection
  (close [conn]
    (a/close! (:transact-chan conn))
    (db/close-db @(:current-db conn)))

  (db [conn] @(:current-db conn))

  (history [conn]
    (raise "Datomic's history is not yet supported." {})))

(defn conn? [x]
  (and (satisfies? IConnection x)))

(defrecord TxReport [db-before ;; The DB before the transaction.
                     db-after  ;; The DB after the transaction.
                     tx        ;; The tx ID represented by the transaction in this report; refer :db/tx.
                     txInstant ;; The timestamp instant when the the transaction was processed/committed in this report; refer :db/txInstant.
                     entities  ;; The set of entities (like [:db/add e a v]) processed.
                     tx-data   ;; The set of datoms applied to the database, like (Datom. e a v tx added).
                     tempids   ;; The map from id-literal -> numeric entid.
                     part-map  ;; Map {:db.part/user {:start 0x10000 :idx 0x10000}, ...}.
                     added-parts ;; The set of parts added during the transaction via :db.part/db :db.install/part.
                     added-idents ;; The map of idents -> entid added during the transaction, via e :db/ident ident.
                     added-attributes ;; The map of schema attributes (ident -> schema fragment) added during the transaction, via :db.part/db :db.install/attribute.
                     ])

(defn- report? [x]
  (and (instance? TxReport x)))

(defn- -next-eid! [part-map-atom tempid]
  "Advance {:db.part/user {:start 0x10 :idx 0x11}, ...} to {:db.part/user {:start 0x10 :idx 0x12}, ...} and return 0x12."
  {:pre [(id-literal? tempid)]}
  (let [part (:part tempid)
        next (fn [part-map]
               (let [idx (get-in part-map [part :idx])]
                 (when-not idx
                   (raise "Cannot allocate entid for id-literal " tempid " because part " part " is not known"
                          {:error :db/bad-part
                           :parts (sorted-set (keys part-map))
                           :part  part}))
                 (update-in part-map [part :idx] inc)))]
    (get-in (swap! part-map-atom next) [part :idx])))

(defn- allocate-eid
  [report id-literal eid]
  {:pre [(report? report) (id-literal? id-literal) (and (integer? eid) (not (neg? eid)))]}

  (assoc-in report [:tempids id-literal] eid))

;; (def data-readers {'db/id id-literal})

;; #?(:cljs
;;    (doseq [[tag cb] data-readers] (cljs.reader/register-tag-parser! tag cb)))

(declare start-transactor)

(defn connection-with-db [db]
  ;; Puts to listener-source may park if listener-mult can't distribute them fast enough.  Since the
  ;; underlying taps are asserted to be be unblocking, the parking time should be very short.
  (let [listener-source
        (a/chan 1)

        listener-mult
        (a/mult listener-source) ;; Just for tapping.

        connection
        (map->Connection {:current-db       (atom db)
                          :listener-source  listener-source
                          :listener-mult    listener-mult
                          :transact-chan    (a/chan (util/unlimited-buffer))
                          })]
    (start-transactor connection)
    connection))

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

(defn maybe-ident->entid [db [op e a v :as orig]]
  ;; We have to handle all ops, including those when a or v are not defined.
  (let [e (db/entid db e)
        a (db/entid db a)
        v (if (and a (ds/kw? (db/schema db) a)) ;; TODO: decide if this is best.  We could also check for ref and numeric types.
            v
            (db/entid db v))]
    (when (and a (not (integer? a)))
      (raise "Unknown attribute " a
             {:form orig :attribute a :entity orig}))
    [op e a v]))

(defrecord Transaction [db tempids entities])

(defn- tx-entity [db report]
  {:pre [(db/db? db) (report? report)]}
  (let [tx        (:tx report)
        txInstant (:txInstant report)]
    ;; n.b., this must not be symbolic -- it's inserted after we map idents -> entids.
    [:db/add tx (db/entid db :db/txInstant) txInstant]))

(defn ensure-entity-form [entity]
  (when-not (sequential? entity)
    (raise "Bad entity " entity ", should be sequential at this point"
           {:error :transact/bad-entity, :entity entity}))

  (let [[op] entity]
    (case op
      (:db/add :db/retract)
      (let [[_ e a v & rest] entity]
        (cond
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
                 {:error :transact/bad-entity :entity entity })))

      :db.fn/retractAttribute
      (let [[_ e a & rest] entity]
        (cond
          (nil? e)
          (raise "Bad entity: nil e in " entity
                 {:error :transact/bad-entity :entity entity })

          (nil? a)
          (raise "Bad entity: nil a in " entity
                 {:error :transact/bad-entity :entity entity })

          (some? rest)
          (raise "Bad entity: too long " entity
                 {:error :transact/bad-entity :entity entity })))

      :db.fn/retractEntity
      (let [[_ e & rest] entity]
        (cond
          (nil? e)
          (raise "Bad entity: nil e in " entity
                 {:error :transact/bad-entity :entity entity })

          (some? rest)
          (raise "Bad entity: too long " entity
                 {:error :transact/bad-entity :entity entity })))

      ;; Default
      (raise "Unrecognized operation " op " expected one of :db/add :db/retract :db/fn.retractAttribute :db/fn.retractEntity at this point"
             {:error :transact/bad-operation :entity entity })))

  entity)

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

(defn <resolve-lookup-refs [db report]
  {:pre [(db/db? db) (report? report)]}

  (let [unique-identity? (memoize (partial ds/unique-identity? (db/schema db)))
        ;; Map lookup-ref -> entities containing lookup-ref, like {[[:a :v] [[[:a :v] :b :w] ...]] ...}.
        groups           (group-by (partial keep db/lookup-ref?) (:entities report))
        ;; Entities with no lookup-ref are grouped under the key (lazy-seq).
        entities         (get groups (lazy-seq)) ;; No lookup-refs? Pass through.
        to-resolve       (dissoc groups (lazy-seq)) ;; The ones with lookup-refs.
        ;; List [[:a :v] ...] to lookup.
        avs              (set (map (juxt :a :v) (apply concat (keys to-resolve))))
        ->av             (fn [r] ;; Conditional (juxt :a :v) that passes through nil.
                           (when r [(:a r) (:v r)]))]
    (go-pair
      (let [av->e    (<? (db/<avs db avs))
            resolve1 (fn [field]
                       (if-let [[a v] (->av (db/lookup-ref? field))]
                         (if-not (unique-identity? (db/entid db a))
                           (raise "Lookup-ref found with non-unique-identity attribute " a " and value " v
                                  {:error :transact/lookup-ref-with-non-unique-identity-attribute
                                   :a     a
                                   :v     v})
                           (or
                             (get av->e [a v])
                             (raise "No entity found for lookup-ref with attribute " a " and value " v
                                    {:error :transact/lookup-ref-not-found
                                     :a     a
                                     :v     v})))
                         field))
            resolve  (fn [entity]
                       (mapv resolve1 entity))]
        (assoc
          report
          :entities
          (concat
            entities
            (map resolve (apply concat (vals to-resolve)))))))))

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
                 (or (id-literal? e)
                     (id-literal? a)
                     (id-literal? v)))
            (raise "id-literals are resolved for :db/add only"
                   {:error :transact/syntax
                    :op    entity })

            ;; Upsert!
            (and (id-literal? e)
                 (ds/unique-identity? (db/schema db) a)
                 (not-any? id-literal? [a v]))
            (let [upserted-eid (:e (<? (db/<av db a v)))
                  allocated-eid (get-in report [:tempids e])]
              (if (and upserted-eid allocated-eid (not= upserted-eid allocated-eid))
                (<? (<retry-with-tempid db initial-report initial-entities e upserted-eid)) ;; TODO: not initial report, just the sorted entities here.
                (let [eid (or upserted-eid allocated-eid (-next-eid! (:part-map-atom report) e))]
                  (recur (allocate-eid report e eid) (cons [op eid a v] entities)))))

            ;; Start allocating and retrying.  We try with e last, so as to eventually upsert it.
            (id-literal? v)
            ;; We can't fail with unbound literals here, since we could have multiple.
            (let [eid (or (get-in report [:tempids v]) (-next-eid! (:part-map-atom report) e))]
              (recur (allocate-eid report v eid) (cons [op e a eid] entities)))

            (id-literal? a)
            ;; TODO: should we even allow id-literal attributes?  Datomic fails in some cases here.
            (let [eid (or (get-in report [:tempids a]) (-next-eid! (:part-map-atom report) e))]
              (recur (allocate-eid report a eid) (cons [op e eid v] entities)))

            (id-literal? e)
            (let [eid (or (get-in report [:tempids e]) (-next-eid! (:part-map-atom report) e))]
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
        (if (and e a v)
          (ds/ensure-valid-value schema a v))))
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

(defn collect-db-install-assertions
  "Transactions may add idents, install new partitions, and install new schema attributes.
  Collect [:db.part/db :db.install/attribute] assertions here."
  [db report]
  {:pre [(db/db? db) (report? report)]}

  ;; Symbolicating is not expensive.
  (let [symbolicate-install-datom
        (fn [[e a v tx added]]
          (datom
            (db/ident db e)
            (db/ident db a)
            (db/ident db v)
            tx
            added))
        datoms (map symbolicate-install-datom (:tx-data report))
        schema-fragment (datomish.schema-changes/datoms->schema-fragment datoms)]
    (assoc-in report [:added-attributes] schema-fragment)))

;; TODO: expose this in a more appropriate way.
(defn <with-internal [db tx-data merge-ident merge-attr]
  (go-pair
    (let [part-map-atom
          (atom (db/part-map db))

          tx
          (-next-eid! part-map-atom (id-literal :db.part/tx))

          report (->>
                   (map->TxReport
                     {:db-before        db
                      :db-after         db
                      ;; This mimics DataScript.  It's convenient to be able to extract the
                      ;; transaction ID and transaction timestamp directly from the report; Datomic
                      ;; makes this surprisingly difficult: one needs a :db.part/tx temporary and an
                      ;; explicit upsert of that temporary.
                      :part-map-atom    part-map-atom
                      :tx               tx
                      :txInstant        (db/now db)
                      :entities         tx-data
                      :tx-data          []
                      :tempids          {}
                      :added-parts      {}
                      :added-idents     {}
                      :added-attributes {}
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

                     (db/<apply-db-part-map @(:part-map-atom report))
                     (<?)
                     (->> (p :apply-db-part-changes))

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
  "Submits a transaction to the database for writing.

  Returns a pair-chan resolving to `[result error]`."
  [conn tx-data]
  {:pre [(conn? conn)]}
  (let [result (a/chan 1)]
    ;; Any race to put! is a real race between callers of <transact!.  We can't just park on put!,
    ;; because the parked putter that is woken is non-deterministic.
    (a/put! (:transact-chan conn) [tx-data result])
    result))

(defn- start-transactor [conn]
  (let [token-chan (a/chan 1)]
    (go
      (>! token-chan (gensym "transactor-token"))
      (loop []
        (let [token (<! token-chan)]
          (when-let [[tx-data result] (<! (:transact-chan conn))]
            (let [pair
                  (<! (go-pair ;; Catch exceptions, return the pair.
                        (let [db (db conn)
                              report (<? (db/in-transaction!
                                           db
                                           #(-> (<with db tx-data))))]
                          ;; We only get here if the transaction is committed.
                          (reset! (:current-db conn) (:db-after report))
                          (>! (:listener-source conn) report)
                          report)))]
              (>! result pair))
            (a/close! result)
            (>! token-chan token)
            (recur)))))))

(defn listen-chan!
  "Put reports successfully transacted against the given connection onto the given channel.

  The listener sink channel must be unblocking.

  Returns the channel listened to, for future unlistening."
  [conn listener-sink]
  {:pre [(conn? conn)]}
  (when-not (util/unblocking-chan? listener-sink)
    (raise "Listener sinks must be channels backed by unblocking buffers"
           {:error :transact/bad-listener :listener-sink listener-sink}))
  ;; Tapping an already registered sink is a no-op.
  (a/tap (:listener-mult conn) listener-sink)
  listener-sink)

(defn- -listen-chan [f]
  (let [c (a/chan (a/sliding-buffer 10))]
    (go-loop []
      (when-let [v (<! c)]
        (do
          (f v)
          (recur))))
    c))

(defn listen!
  "Evaluate the given function with reports successfully transacted against the given connection.

  `f` should be a function of one argument, the transaction report.

  Returns the channel listened to, for future calls to `unlisten-chan!`."
  ([conn f]
   {:pre [(fn? f)]}
   (listen-chan! conn (-listen-chan f))))

(defn unlisten-chan! [conn listener-sink]
  "Stop putting reports successfully transacted against the given connection onto the given channel."
  {:pre [(conn? conn)]}
  ;; Untapping an un-registered sink is a no-op.
  (a/untap (:listener-mult conn) listener-sink))
