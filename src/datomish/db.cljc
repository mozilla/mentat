;; This Source Code Form is subject to the terms of the Mozilla Public
;; License, v. 2.0. If a copy of the MPL was not distributed with this
;; file, You can obtain one at http://mozilla.org/MPL/2.0/.

(ns datomish.db
  #?(:cljs
     (:require-macros
      [datomish.pair-chan :refer [go-pair <?]]
      [cljs.core.async.macros :refer [go]]))
  (:require
   [honeysql.core :as sql]
   [datomish.util :as util :refer [raise]]
   [datomish.sqlite :as s]
   [datomish.sqlite-schema :as sqlite-schema]
   #?@(:clj [[datomish.pair-chan :refer [go-pair <?]]
             [clojure.core.async :as a :refer [go <! >!]]])
   #?@(:cljs [[datomish.pair-chan]
              [cljs.core.async :as a :refer [<! >!]]])))

(defprotocol IDB
  (close
    [db]
    "Close this database. Returns a pair channel of [nil error]."))

(defrecord DB [sqlite-connection max-tx]
  IDB
  (close [db] (s/close (.-sqlite-connection db))))

(def tx0 0x2000000)

(defn <with-sqlite-connection [sqlite-connection]
  (go-pair
    (when-not (= sqlite-schema/current-version (<? (sqlite-schema/<ensure-current-version sqlite-connection)))
      (raise "Could not ensure current SQLite schema version."))
    (map->DB {:sqlite-connection sqlite-connection
              :current-tx        (atom (dec tx0))}))) ;; TODO: get rid of dec.

(defn- #?@(:clj  [^Boolean tx-id?]
           :cljs [^boolean tx-id?])
  [e]
  (or (= e :db/current-tx)
      (= e ":db/current-tx"))) ;; for datascript.js interop

(defrecord TxReport [;; db-before db-after
                     tx-data tempids tx-meta])

#?(:clj
   (defmacro cond-let [& clauses]
     (when-let [[test expr & rest] clauses]
       `(~(if (vector? test) 'if-let 'if) ~test
         ~expr
         (cond-let ~@rest)))))

(defn <allocate-tx [db]
  (go-pair
    (swap! (:current-tx db) inc)))

(deftype Datom [e a v tx added])

;; printing and reading
;; #datomic/DB {:schema <map>, :datoms <vector of [e a v tx]>}

;; (defn ^Datom datom-from-reader [vec]
;;   (apply datom vec))

#?(:clj
   (defmethod print-method Datom [^Datom d, ^java.io.Writer w]
     (.write w (str "#datomish/Datom "))
     (binding [*out* w]
       (pr [(.-e d) (.-a d) (.-v d) (.-tx d) (.-added d)]))))

(defn- validate-eid [eid at]
  (when-not (number? eid)
    (raise "Bad entity id " eid " at " at ", expected number"
           {:error :transact/syntax, :entity-id eid, :context at})))

(defn- validate-attr [attr at]
  (when-not (or (keyword? attr) (string? attr))
    (raise "Bad entity attribute " attr " at " at ", expected keyword or string"
           {:error :transact/syntax, :attribute attr, :context at})))

(defn- validate-val [v at]
  (when (nil? v)
    (raise "Cannot store nil as a value at " at
           {:error :transact/syntax, :value v, :context at})))

(defn multival? [db attr] false)

(defn ref? [db attr] false)

(defn <entid [db eid]
  ;; {:pre [(db? db)]}
  (go-pair
    (cond
      (number? eid) eid
      ;; (sequential? eid)
      ;; (cond
      ;;   (not= (count eid) 2)
      ;;   (raise "Lookup ref should contain 2 elements: " eid
      ;;          {:error :lookup-ref/syntax, :entity-id eid})
      ;;   (not (is-attr? db (first eid) :db.unique/identity))
      ;;   (raise "Lookup ref attribute should be marked as :db.unique/identity: " eid
      ;;          {:error :lookup-ref/unique
      ;;           :entity-id eid})
      ;;   (nil? (second eid))
      ;;   nil
      ;;   :else
      ;;   (:e (first (-datoms db :avet eid))))
      :else
      (raise "Expected number or lookup ref for entity id, got " eid
             {:error :entity-id/syntax
              :entity-id eid}))))

(defn <entid-strict [db eid]
  (go-pair
    (or (<? (<entid db eid))
        (raise "Nothing found for entity id " eid
               {:error :entity-id/missing
                :entity-id eid}))))

(defn <entid-some [db eid]
  (go-pair
    (when eid
      (<? (<entid-strict db eid)))))

;; TODO: handle _?
(defn search->sql-clause [pattern]
  (merge
    {:select [:e :a :v :tx] ;; TODO: generalize columns.
     :from [:datoms]}
    (if-not (empty? pattern)
      {:where (cons :and (map #(vector := %1 (if (keyword? %2) (str %2) %2)) [:e :a :v :tx] pattern))} ;; TODO: use schema to intern a and v.
      {})))

(defn <search [db pattern]
  (go-pair
    ;; TODO: find a better expression of this pattern.
    (let [rows (<? (->>
                     (search->sql-clause pattern)
                     (sql/format)
                     (s/all-rows (:sqlite-connection db))))]
      (map #(Datom. (:e %) (:a %) (:v %) (:tx %) true) rows))))

(defn- <transact-report [db report datom]
  (go-pair
    (let [exec (partial s/execute! (:sqlite-connection db))]
      ;; Append to transaction log.
      (<? (exec
            ["INSERT INTO transactions VALUES (?, ?, ?, ?, ?)" (.-e datom) (str (.-a datom)) (.-v datom) (.-tx datom) (.-added datom)]))
      ;; Update materialized datom view.
      (if (.-added datom)
        (<? (exec
              ;; TODO: use schema to insert correct indexing flags.
              ["INSERT INTO datoms VALUES (?, ?, ?, ?, 0, 0)" (.-e datom) (str (.-a datom)) (.-v datom) (.-tx datom)]))
        (<? (exec
              ;; TODO: verify this is correct.
              ["DELETE FROM datoms WHERE (e = ? AND a = ? AND v = ?)" (.-e datom) (.-a datom) (.-v datom)])))
      (-> report
          (update-in [:tx-data] conj datom)))))

(defn- <transact-add [db report [_ e a v tx :as entity]]
  (go-pair
    (validate-attr a entity)
    (validate-val  v entity)
    (let [tx    (or tx (:current-tx report))
          e     (<? (<entid-strict db e))
          v     (if (ref? db a) (<? (<entid-strict db v)) v)
          datom (Datom. e a v tx true)]
      (if (multival? db a)
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
  (go-pair
    (let [tx (:current-tx report)]
      (if-let [e (<? (<entid db e))]
        (let [v (if (ref? db a) (<? (<entid-strict db v)) v)]
          (validate-attr a entity)
          (validate-val v entity)
          (if-let [old-datom (first (<? (<search db [e a v])))]
            (<? (<transact-report db report (Datom. e a v tx false)))
            report))
        report))))

(defn <transact-tx-data
  [db now initial-report initial-es]
  (go-pair
    (when-not (or (nil? initial-es)
                  (sequential? initial-es))
      (raise "Bad transaction data " initial-es ", expected sequential collection"
             {:error :transact/syntax, :tx-data initial-es}))
    (loop [report initial-report
           es     initial-es]
      (let [[entity & entities] es]
        (cond
          (nil? entity)
          (let [current-tx (:current-tx report)]
            (<? (<transact-report db report (Datom. current-tx :db/txInstant now current-tx true)))
            (-> report
                (assoc-in [:tempids :db/current-tx] current-tx)))

          ;; (map? entity)
          ;; (let [old-eid (:db/id entity)]
          ;;   (cond-let
          ;;     ;; :db/current-tx => tx
          ;;     (tx-id? old-eid)
          ;;     (let [id (current-tx report)]
          ;;       (recur (allocate-eid report old-eid id)
          ;;              (cons (assoc entity :db/id id) entities)))

          ;;     ;; lookup-ref => resolved | error
          ;;     (sequential? old-eid)
          ;;     (let [id (entid-strict db old-eid)]
          ;;       (recur report
          ;;              (cons (assoc entity :db/id id) entities)))

          ;;     ;; upserted => explode | error
          ;;     [upserted-eid (upsert-eid db entity)]
          ;;     (if (and (neg-number? old-eid)
          ;;              (contains? (:tempids report) old-eid)
          ;;              (not= upserted-eid (get (:tempids report) old-eid)))
          ;;       (retry-with-tempid initial-report initial-es old-eid upserted-eid)
          ;;       (recur (allocate-eid report old-eid upserted-eid)
          ;;              (concat (explode db (assoc entity :db/id upserted-eid)) entities)))

          ;;     ;; resolved | allocated-tempid | tempid | nil => explode
          ;;     (or (number? old-eid)
          ;;         (nil?    old-eid))
          ;;     (let [new-eid (cond
          ;;                     (nil? old-eid) (next-eid db)
          ;;                     (neg? old-eid) (or (get (:tempids report) old-eid)
          ;;                                        (next-eid db))
          ;;                     :else          old-eid)
          ;;           new-entity (assoc entity :db/id new-eid)]
          ;;       (recur (allocate-eid report old-eid new-eid)
          ;;              (concat (explode db new-entity) entities)))

          ;;     ;; trash => error
          ;;     :else
          ;;     (raise "Expected number or lookup ref for :db/id, got " old-eid
          ;;            { :error :entity-id/syntax, :entity entity })))

          (sequential? entity)
          (let [[op e a v] entity]
            (cond
              ;; (= op :db.fn/call)
              ;; (let [[_ f & args] entity]
              ;;   (recur report (concat (apply f db args) entities)))

              ;; (= op :db.fn/cas)
              ;; (let [[_ e a ov nv] entity
              ;;       e (entid-strict db e)
              ;;       _ (validate-attr a entity)
              ;;       ov (if (ref? db a) (entid-strict db ov) ov)
              ;;       nv (if (ref? db a) (entid-strict db nv) nv)
              ;;       _ (validate-val nv entity)
              ;;       datoms (<searchdb [e a])]
              ;;   (if (multival? db a)
              ;;     (if (some (fn [^Datom d] (= (.-v d) ov)) datoms)
              ;;       (recur (transact-add report [:db/add e a nv]) entities)
              ;;       (raise ":db.fn/cas failed on datom [" e " " a " " (map :v datoms) "], expected " ov
              ;;              {:error :transact/cas, :old datoms, :expected ov, :new nv}))
              ;;     (let [v (:v (first datoms))]
              ;;       (if (= v ov)
              ;;         (recur (transact-add report [:db/add e a nv]) entities)
              ;;         (raise ":db.fn/cas failed on datom [" e " " a " " v "], expected " ov
              ;;                {:error :transact/cas, :old (first datoms), :expected ov, :new nv })))))

              (tx-id? e)
              (recur report (cons [op (:current-tx report) a v] entities))

              ;; (and (ref? db a) (tx-id? v))
              ;; (recur report (cons [op e a (current-tx report)] entities))

              ;; (neg-number? e)
              ;; (if (not= op :db/add)
              ;;   (raise "Negative entity ids are resolved for :db/add only"
              ;;          {:error :transact/syntax
              ;;           :op    entity })
              ;;   (let [upserted-eid  (when (is-attr? db a :db.unique/identity)
              ;;                         (:e (first (-datoms db :avet [a v]))))
              ;;         allocated-eid (get-in report [:tempids e])]
              ;;     (if (and upserted-eid allocated-eid (not= upserted-eid allocated-eid))
              ;;       (retry-with-tempid initial-report initial-es e upserted-eid)
              ;;       (let [eid (or upserted-eid allocated-eid (next-eid db))]
              ;;         (recur (allocate-eid report e eid) (cons [op eid a v] entities))))))

              ;; (and (ref? db a) (neg-number? v))
              ;; (if-let [vid (get-in report [:tempids v])]
              ;;   (recur report (cons [op e a vid] entities))
              ;;   (recur (allocate-eid report v (next-eid db)) es))

              (= op :db/add)
              (recur (<? (<transact-add db report entity)) entities)

              (= op :db/retract)
              (recur (<? (<transact-retract db report entity)) entities)

              ;; (= op :db.fn/retractAttribute)
              ;; (if-let [e (entid db e)]
              ;;   (let [_ (validate-attr a entity)
              ;;         datoms (<search db [e a])]
              ;;     (recur (reduce transact-retract-datom report datoms)
              ;;            (concat (retract-components db datoms) entities)))
              ;;   (recur report entities))

              ;; (= op :db.fn/retractEntity)
              ;; (if-let [e (entid db e)]
              ;;   (let [e-datoms (<search db [e])
              ;;         v-datoms (mapcat (fn [a] (<search db [nil a e])) (-attrs-by db :db.type/ref))]
              ;;     (recur (reduce transact-retract-datom report (concat e-datoms v-datoms))
              ;;            (concat (retract-components db e-datoms) entities)))
              ;;   (recur report entities))

              :else
              (raise "Unknown operation at " entity ", expected :db/add, :db/retract, :db.fn/call, :db.fn/retractAttribute or :db.fn/retractEntity"
                     {:error :transact/syntax, :operation op, :tx-data entity})))

          ;; (datom? entity)
          ;; (let [[e a v tx added] entity]
          ;;   (if added
          ;;     (recur (transact-add report [:db/add e a v tx]) entities)
          ;;     (recur report (cons [:db/retract e a v] entities))))

          :else
          (raise "Bad entity type at " entity ", expected map or vector"
                 {:error :transact/syntax, :tx-data entity})
          )))))

(defn <transact!
  ([db tx-data]
   (<transact! db tx-data nil 0xdeadbeef)) ;; TODO: timestamp!
  ([db tx-data tx-meta now]
   ;; {:pre [(db/db? db)]}
   (go-pair
     (let [current-tx (<? (<allocate-tx db))]
       (<? (<transact-tx-data db now
                              (map->TxReport
                                {:current-tx current-tx
                                 :tx-data    []
                                 :tempids    {}
                                 :tx-meta    tx-meta}) tx-data))))))

#_ (a/<!! (<transact! db []))

#_ .



#_ (def db (<?? (<with-sqlite-connection (<?? (s/<sqlite-connection "/Users/nalexander/test5.db")))))

#_ (<?? (<transact! db [[:db/add 0 1 "test"]]))

#_ (<?? (<transact! db [[:db/retract 0 1 "test"]]))
