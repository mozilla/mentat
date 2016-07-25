;; This Source Code Form is subject to the terms of the Mozilla Public
;; License, v. 2.0. If a copy of the MPL was not distributed with this
;; file, You can obtain one at http://mozilla.org/MPL/2.0/.

(ns datomish.transact
  #?(:cljs
     (:require-macros
      [datomish.pair-chan :refer [go-pair <?]]
      [cljs.core.async.macros :refer [go]]))
  #?(:clj (:import [datomish.db Datom TxReport]))
  (:require
   [datomish.context :as context]
   [datomish.db :as db :refer [#?@(:cljs [Datom TxReport]) db?]]
   [datomish.projection :as projection]
   [datomish.query :as query]
   [datomish.source :as source]
   [datomish.sqlite :as s]
   [datomish.sqlite-schema :as sqlite-schema]
   [datomish.util :as util :refer [raise raise-str]]
   [honeysql.core :as sql]
   #?@(:clj [[datomish.pair-chan :refer [go-pair <?]]
             [clojure.core.async :as a :refer [chan go <! >!]]])
   #?@(:cljs [[datomish.pair-chan]
              [cljs.core.async :as a :refer [chan <! >!]]])))

(defn- tx-id? [e]
  (= e :db/current-tx))

(defn- validate-eid [eid at]
  (when-not (number? eid)
    (raise "Bad entity id " eid " at " at ", expected number"
           {:error :transact/syntax, :entity-id eid, :context at})))

(defn- validate-attr [attr at]
  (when-not (number? attr)
    (raise "Bad entity attribute " attr " at " at ", expected number"
           {:error :transact/syntax, :attribute attr, :context at})))

(defn- validate-val [v at]
  (when (nil? v)
    (raise "Cannot store nil as a value at " at
           {:error :transact/syntax, :value v, :context at})))

;; TODO: handle _?
(defn search->sql-clause [pattern]
  (merge
    {:select [:*] ;; e :a :v :tx] ;; TODO: generalize columns.
     :from [:datoms]}
    (if-not (empty? pattern)
      {:where (cons :and (map #(vector := %1 %2) [:e :a :v :tx] pattern))} ;; TODO: use schema to v.
      {})))

(defn <search [db pattern]
  {:pre [(db/db? db)]}
  (go-pair
    ;; TODO: find a better expression of this pattern.
    (let [rows (<? (->>
                     (search->sql-clause pattern)
                     (sql/format)
                     (s/all-rows (:sqlite-connection db))))]
      (mapv #(Datom. (:e %) (:a %) (:v %) (:tx %) true) rows))))

(defn- <transact-report [db report datom]
  {:pre [(db/db? db)]}
  (go-pair
    (let [exec (partial s/execute! (:sqlite-connection db))
          [e a v tx added] [(.-e datom) (.-a datom) (.-v datom) (.-tx datom) (.-added datom)]] ;; TODO: destructuring.
      (validate-eid e [e a v tx added]) ;; TODO: track original vs. transformed?
      ;; Append to transaction log.
      (<? (exec
            ["INSERT INTO transactions VALUES (?, ?, ?, ?, ?)" e a v tx added]))
      ;; Update materialized datom view.
      (if (.-added datom)
        (<? (exec
              ;; TODO: use schema to insert correct indexing flags.
              ["INSERT INTO datoms VALUES (?, ?, ?, ?, 0, 0)" e a v tx]))
        (<? (exec
              ;; TODO: verify this is correct.
              ["DELETE FROM datoms WHERE (e = ? AND a = ? AND v = ?)" e a v])))
      (-> report
          (update-in [:tx-data] conj datom)))))

(defn- <transact-add [db report [_ e a v tx :as entity]]
  {:pre [(db/db? db)]}
  (go-pair
    (validate-attr a entity)
    (validate-val  v entity)
    (let [tx    (or tx (:current-tx report))
          e     (<? (db/<entid-strict db e))
          v     (if (db/ref? db a) (<? (db/<entid-strict db v)) v)
          datom (Datom. e a v tx true)]
      (if (db/multival? db a)
        ;; TODO: consider adding a UNIQUE CONSTRAINT and using INSERT OR IGNORE.
        (if (empty? (<? (<search db [e a v])))
          (<? (<transact-report db report datom))
          report)
        (if-let [^Datom old-datom (first (<? (<search db [e a])))]
          (if (= (.-v old-datom) v)
            report
            (let [ra (<? (<transact-report db report (Datom. e a (.-v old-datom) tx false)))
                  rb (<? (<transact-report db ra datom))]
              rb)) ;; TODO: express this better.
          (<? (<transact-report db report datom)))))))

(defn- <transact-retract [db report [_ e a v _ :as entity]] ;; TODO: think about retracting with tx.
  {:pre [(db/db? db)]}
  (go-pair
    (let [tx (:current-tx report)]
      (if-let [e (<? (db/<entid db e))]
        (let [v (if (db/ref? db a) (<? (db/<entid-strict db v)) v)]
          (validate-attr a entity)
          (validate-val v entity)
          (if-let [old-datom (first (<? (<search db [e a v])))]
            (<? (<transact-report db report (Datom. e a v tx false)))
            report))
        report))))
(defn- #?@(:clj  [^Boolean neg-number?]
           :cljs [^boolean neg-number?])
  [x]
  (and (number? x) (neg? x)))

(defn <transact-tx-data
  [db now initial-report initial-es]
  {:pre [(db/db? db)]}
  (go-pair
    (when-not (or (nil? initial-es)
                  (sequential? initial-es))
      (raise "Bad transaction data " initial-es ", expected sequential collection"
             {:error :transact/syntax, :tx-data initial-es}))
    (loop [report initial-report
           es     initial-es]
      (let [[entity & entities] es
            current-tx (:current-tx report)]
        (cond
          (nil? entity)
          ;; We're done!  Add transaction datom to the report.
          (do
            ;; TODO: don't special case :db/txInstant attribute.
            (<? (<transact-report db report (Datom. current-tx (get (db/idents db) :db/txInstant) now current-tx true)))
            (-> report
                (assoc-in [:tempids :db/current-tx] current-tx)))

          (map? entity)
          (raise "Map entities are not yet supported, got " entity
                 {:error :transact/syntax
                  :op    entity })

          (sequential? entity)
          (let [[op e a v] entity]
            (cond
              (keyword? a)
              (if-let [entid (get (db/idents db) a)]
                (recur report (cons [op e entid v] entities))
                (raise "No entid found for ident " a
                       {:error :transact/syntax
                        :op entity}))

              (= op :db.fn/call)
              (raise "DataScript's transactor functions are not yet supported, got " entity
                     {:error :transact/syntax
                      :op    entity })

              (= op :db.fn/cas)
              (raise "Datomic's compare-and-swap is not yet supported, got " entity
                     {:error :transact/syntax
                      :op    entity })

              (tx-id? e)
              (recur report (cons [op current-tx a v] entities))

              (and (db/ref? db a) (tx-id? v))
              (recur report (cons [op e a current-tx] entities))

              (neg-number? e)
              (if (not= op :db/add)
                (raise "Negative entity ids are resolved for :db/add only"
                       {:error :transact/syntax
                        :op    entity })
                (raise "Negative entity ids are not yet supported, got " entity
                       {:error :transact/syntax
                        :op    entity }))

              (and (db/ref? db a) (neg-number? v))
              (raise "Negative entity ids are not yet supported, got " entity
                     {:error :transact/syntax
                      :op    entity })

              (= op :db/add)
              (recur (<? (<transact-add db report entity)) entities)

              (= op :db/retract)
              (recur (<? (<transact-retract db report entity)) entities)

              (= op :db.fn/retractAttribute)
              (raise "DataScript's :db.fn/retractAttribute shortcut is not yet supported, got " entity
                     {:error :transact/syntax
                      :op    entity })

              (= op :db.fn/retractEntity)
              (raise "Datomic's :db.fn/retractEntity shortcut is not yet supported, got " entity
                     {:error :transact/syntax
                      :op    entity })

              :else
              (raise "Unknown operation at " entity ", expected :db/add, :db/retract, :db.fn/call, :db.fn/retractAttribute or :db.fn/retractEntity"
                     {:error :transact/syntax, :operation op, :tx-data entity})))

          (db/datom? entity)
          (raise "Datom entities are not yet supported, got " entity
                 {:error :transact/syntax
                  :op    entity })

          :else
          (raise "Bad entity type at " entity ", expected map or vector"
                 {:error :transact/syntax, :tx-data entity})
          )))))

(defn <process-db-part
  "Transactions may add idents, install new partitions, and install new schema attributes.  Handle
  them, atomically, here."
  [db report]
  (go-pair
    nil))

(defn <transact!
  ([db tx-data]
   (<transact! db tx-data nil 0xdeadbeef)) ;; TODO: timestamp!
  ([db tx-data tx-meta now]
   {:pre [(db? db)]}
   (s/in-transaction!
     (:sqlite-connection db)
     #(go-pair
        (let [current-tx (<? (db/<allocate-tx db))
              report     (<? (<transact-tx-data db now
                                                (db/map->TxReport
                                                  {:current-tx current-tx
                                                   :tx-data    []
                                                   :tempids    {}
                                                   :tx-meta    tx-meta}) tx-data))]
          (<? (<process-db-part db report))
          report)))))


