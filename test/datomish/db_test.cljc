;; This Source Code Form is subject to the terms of the Mozilla Public
;; License, v. 2.0. If a copy of the MPL was not distributed with this
;; file, You can obtain one at http://mozilla.org/MPL/2.0/.

(ns datomish.db-test
  #?(:cljs
     (:require-macros
      [datomish.pair-chan :refer [go-pair <?]]
      [datomish.node-tempfile-macros :refer [with-tempfile]]
      [cljs.core.async.macros :as a :refer [go]]))
  (:require
   [datomish.util :as util #?(:cljs :refer-macros :clj :refer) [raise cond-let]]
   [datomish.sqlite :as s]
   [datomish.sqlite-schema]
   [datomish.datom]

   [datascript.core :as d]
   [datascript.db :as db]

   [datomish.db :as dm]
   #?@(:clj [[datomish.pair-chan :refer [go-pair <?]]
             [tempfile.core :refer [tempfile with-tempfile]]
             [datomish.test-macros :refer [deftest-async]]
             [clojure.test :as t :refer [is are deftest testing]]
             [clojure.core.async :refer [go <! >!]]])
   #?@(:cljs [[datomish.pair-chan]
              [datomish.test-macros :refer-macros [deftest-async]]
              [datomish.node-tempfile :refer [tempfile]]
              [cljs.test :as t :refer-macros [is are deftest testing async]]
              [cljs.core.async :as a :refer [<! >!]]]))
  #?(:clj
     (:import [clojure.lang ExceptionInfo]))
  #?(:clj
     (:import [datascript.db DB])))

#?(:cljs
   (def Throwable js/Error))

(defn- <datoms-after [db tx]
  (let [entids (zipmap (vals (dm/idents db)) (keys (dm/idents db)))]
    (go-pair
      (->>
        (s/all-rows (:sqlite-connection db) ["SELECT e, a, v, tx FROM datoms WHERE tx >= ?" tx])
        (<?)
        (mapv #(vector (:e %) (get entids (:a %) (str "fail" (:a %))) (:v %)))
        (filter #(not (= :db/txInstant (second %))))
        (set)))))

(defn- <datoms [db]
  (<datoms-after db 0))

(defn- <shallow-entity [db eid]
  (let [entids (zipmap (vals (dm/idents db)) (keys (dm/idents db)))]
    (go-pair
      (->>
        (s/all-rows (:sqlite-connection db) ["SELECT a, v FROM datoms WHERE e = ?" eid])
        (<?)
        (mapv #(vector (entids (:a %)) (:v %)))
        (reduce conj {})))))

(defn- <transactions-after [db tx]
  (let [entids (zipmap (vals (dm/idents db)) (keys (dm/idents db)))]
    (go-pair
      (->>
        (s/all-rows (:sqlite-connection db) ["SELECT e, a, v, tx, added FROM transactions WHERE tx >= ? ORDER BY tx ASC, e, a, v, added" tx])
        (<?)
        (mapv #(vector (:e %) (entids (:a %)) (:v %) (:tx %) (:added %)))))))

(defn- <transactions [db]
  (<transactions-after db 0))

(defn tx [report]
  (get-in report [:db-after :current-tx]))

;; TODO: use reverse refs!
(def test-schema
  [{:db/id        (dm/id-literal :test -1)
    :db/ident     :x
    :db/unique    :db.unique/identity
    :db/valueType :db.type/integer}
   {:db/id :db.part/db :db.install/attribute (dm/id-literal :test -1)}
   {:db/id        (dm/id-literal :test -2)
    :db/ident     :name
    :db/unique    :db.unique/identity
    :db/valueType :db.type/string}
   {:db/id :db.part/db :db.install/attribute (dm/id-literal :test -2)}
   {:db/id          (dm/id-literal :test -3)
    :db/ident       :y
    :db/cardinality :db.cardinality/many
    :db/valueType   :db.type/integer}
   {:db/id :db.part/db :db.install/attribute (dm/id-literal :test -3)}
   {:db/id          (dm/id-literal :test -5)
    :db/ident       :aka
    :db/cardinality :db.cardinality/many
    :db/valueType   :db.type/string}
   {:db/id :db.part/db :db.install/attribute (dm/id-literal :test -5)}
   {:db/id        (dm/id-literal :test -6)
    :db/ident     :age
    :db/valueType :db.type/integer}
   {:db/id :db.part/db :db.install/attribute (dm/id-literal :test -6)}
   {:db/id        (dm/id-literal :test -7)
    :db/ident     :email
    :db/unique    :db.unique/identity
    :db/valueType :db.type/string}
   {:db/id :db.part/db :db.install/attribute (dm/id-literal :test -7)}
   {:db/id        (dm/id-literal :test -8)
    :db/ident     :spouse
    :db/unique    :db.unique/value
    :db/valueType :db.type/string}
   {:db/id :db.part/db :db.install/attribute (dm/id-literal :test -8)}
   ])

(deftest-async test-add-one
  (with-tempfile [t (tempfile)]
    (let [c    (<? (s/<sqlite-connection t))
          db   (<? (dm/<db-with-sqlite-connection c test-schema))
          conn (dm/connection-with-db db)
          now  0xdeadbeef]
      (try
        (is (not (= nil (<? (dm/<transact! conn test-schema now)))))

        (let [;; TODO: drop now, allow to set :db/txInstant.
              report (<? (dm/<transact! conn [[:db/add 0 :name "valuex"]] now))
              tx     (tx report)]
          (is (= (<? (<datoms-after (dm/db conn) tx))
                 #{[0 :name "valuex"]}))
          (is (= (<? (<transactions-after (dm/db conn) tx))
                 [[0 :name "valuex" tx 1] ;; TODO: true, not 1.
                  [tx :db/txInstant now tx 1]])))
        (finally
          (<? (dm/close-db db)))))))

(deftest-async test-add-two
  (with-tempfile [t (tempfile)]
    (let [c    (<? (s/<sqlite-connection t))
          db   (<? (dm/<db-with-sqlite-connection c test-schema))
          conn (dm/connection-with-db db)
          now  0xdeadbeef]
      (try
        (is (not (= nil (<? (dm/<transact! conn test-schema now)))))

        (let [tx1 (tx (<? (dm/<transact! conn [[:db/add 1 :name "Ivan"]] now)))
              tx2 (tx (<? (dm/<transact! conn [[:db/add 1 :name "Petr"]] now)))
              tx3 (tx (<? (dm/<transact! conn [[:db/add 1 :aka "Tupen"]] now)))
              tx4 (tx (<? (dm/<transact! conn [[:db/add 1 :aka "Devil"]] now)))]
          (is (= (<? (<datoms-after (dm/db conn) tx1))
                 #{[1 :name "Petr"]
                   [1 :aka  "Tupen"]
                   [1 :aka  "Devil"]}))

          (is (= (<? (<transactions-after (dm/db conn) tx1))
                 [[1 :name "Ivan" tx1 1] ;; TODO: true, not 1.
                  [tx1 :db/txInstant now tx1 1]
                  [1 :name "Ivan" tx2 0]
                  [1 :name "Petr" tx2 1]
                  [tx2 :db/txInstant now tx2 1]
                  [1 :aka "Tupen" tx3 1]
                  [tx3 :db/txInstant now tx3 1]
                  [1 :aka "Devil" tx4 1]
                  [tx4 :db/txInstant now tx4 1]])))
        (finally
          (<? (dm/close-db db)))))))

;; TODO: fail multiple :add and :retract of the same datom in the same transaction.
(deftest-async test-retract
  (with-tempfile [t (tempfile)]
    (let [c    (<? (s/<sqlite-connection t))
          db   (<? (dm/<db-with-sqlite-connection c test-schema))
          conn (dm/connection-with-db db)
          now  0xdeadbeef]
      (try
        (is (not (= nil (<? (dm/<transact! conn test-schema now)))))

        (let [txa (tx (<? (dm/<transact! conn [[:db/add     0 :x 123]] now)))
              txb (tx (<? (dm/<transact! conn [[:db/retract 0 :x 123]] now)))]
          (is (= (<? (<datoms-after (dm/db conn) txa))
                 #{}))
          (is (= (<? (<transactions-after (dm/db conn) txa))
                 [[0 :x 123 txa 1] ;; TODO: true, not 1.
                  [txa :db/txInstant now txa 1]
                  [0 :x 123 txb 0]
                  [txb :db/txInstant now txb 1]])))
        (finally
          (<? (dm/close-db db)))))))

(deftest-async test-id-literal-1
  (with-tempfile [t (tempfile)]
    (let [c    (<? (s/<sqlite-connection t))
          db   (<? (dm/<db-with-sqlite-connection c test-schema))
          conn (dm/connection-with-db db)
          now  -1]
      (try
        (is (not (= nil (<? (dm/<transact! conn test-schema now)))))

        (let [report (<? (dm/<transact! conn [[:db/add (dm/id-literal :db.part/user -1) :x 0]
                                              [:db/add (dm/id-literal :db.part/user -1) :y 1]
                                              [:db/add (dm/id-literal :db.part/user -2) :y 2]
                                              [:db/add (dm/id-literal :db.part/user -2) :y 3]] now))]
          (is (= (keys (:tempids report)) ;; TODO: include values.
                 [(dm/id-literal :db.part/user -1)
                  (dm/id-literal :db.part/user -2)]))

          (let [tx   (get-in report [:db-after :current-tx])
                eid1 (get-in report [:tempids (dm/id-literal :db.part/user -1)])
                eid2 (get-in report [:tempids (dm/id-literal :db.part/user -2)])]
            (is (= (<? (<datoms-after (dm/db conn) tx))
                   #{[eid1 :x 0]
                     [eid1 :y 1]
                     [eid2 :y 2]
                     [eid2 :y 3]}))))

        (finally
          (<? (dm/close-db db)))))))

(deftest-async test-unique
  (with-tempfile [t (tempfile)]
    (let [c    (<? (s/<sqlite-connection t))
          db   (<? (dm/<db-with-sqlite-connection c test-schema))
          conn (dm/connection-with-db db)
          now  -1]
      (try
        (is (not (= nil (<? (dm/<transact! conn test-schema now)))))

        (testing "Multiple :db/unique values in tx-data violate unique constraint, no tempid"
          (is (thrown-with-msg?
                ExceptionInfo #"unique constraint"
                (<? (dm/<transact! conn [[:db/add 1 :x 0]
                                         [:db/add 2 :x 0]] now)))))

        (testing "Multiple :db/unique values in tx-data violate unique constraint, tempid"
          (is (thrown-with-msg?
                ExceptionInfo #"unique constraint"
                (<? (dm/<transact! conn [[:db/add (dm/id-literal :db.part/user -1) :spouse "Dana"]
                                         [:db/add (dm/id-literal :db.part/user -2) :spouse "Dana"]] now)))))

        (finally
          (<? (dm/close-db db)))))))

(deftest-async test-valueType-keyword
  (with-tempfile [t (tempfile)]
    (let [c    (<? (s/<sqlite-connection t))
          db   (<? (dm/<db-with-sqlite-connection c test-schema))
          conn (dm/connection-with-db db)
          now  -1]
      (try
        (<? (dm/<transact! conn [{:db/id        (dm/id-literal :db.part/user -1)
                                  :db/ident     :test/kw
                                  :db/unique    :db.unique/identity
                                  :db/valueType :db.type/keyword}
                                 {:db/id :db.part/db :db.install/attribute (dm/id-literal :db.part/user -1)}] now))

        (let [report (<? (dm/<transact! conn [[:db/add (dm/id-literal :db.part/user -1) :test/kw :test/kw1]] now))
              tx     (get-in report [:db-after :current-tx])
              eid    (get-in report [:tempids (dm/id-literal :db.part/user -1)])]
          (is (= (<? (<datoms-after (dm/db conn) tx))
                 #{[eid :test/kw ":test/kw1"]})) ;; Value is raw.

          (testing "Adding the same value compares existing values correctly."
            (<? (dm/<transact! conn [[:db/add eid :test/kw :test/kw1]] now))
            (is (= (<? (<datoms-after (dm/db conn) tx))
                   #{[eid :test/kw ":test/kw1"]}))) ;; Value is raw.

          (testing "Upserting retracts existing value correctly."
            (<? (dm/<transact! conn [[:db/add eid :test/kw :test/kw2]] now))
            (is (= (<? (<datoms-after (dm/db conn) tx))
                   #{[eid :test/kw ":test/kw2"]}))) ;; Value is raw.

          (testing "Retracting compares values correctly."
            (<? (dm/<transact! conn [[:db/retract eid :test/kw :test/kw2]] now))
            (is (= (<? (<datoms-after (dm/db conn) tx))
                   #{}))))

        (finally
          (<? (dm/close-db db)))))))

(deftest-async test-vector-upsert
  (with-tempfile [t (tempfile)]
    (let [c    (<? (s/<sqlite-connection t))
          db   (<? (dm/<db-with-sqlite-connection c test-schema))
          conn (dm/connection-with-db db)
          now  0xdeadbeef
          tempids (fn [tx] (into {} (map (juxt (comp :idx first) second) (:tempids tx))))]
      (try
        (is (not (= nil (<? (dm/<transact! conn test-schema now)))))

        ;; Not having DB-as-value really hurts us here.  This test only works because all upserts
        ;; succeed on top of each other, so we never need to reset the underlying store.
        (<? (dm/<transact! conn test-schema now))
        (<? (dm/<transact! conn [{:db/id 101 :name "Ivan" :email "@1"}
                                 {:db/id 102 :name "Petr" :email "@2"}] now))

        (testing "upsert with tempid"
          (let [report (<? (dm/<transact! conn [[:db/add (dm/id-literal :db.part/user -1) :name "Ivan"]
                                                [:db/add (dm/id-literal :db.part/user -1) :age 12]] now))]
            (is (= (<? (<shallow-entity (dm/db conn) 101))
                   {:name "Ivan" :age 12 :email "@1"}))
            (is (= (tempids report)
                   {-1 101}))))

        (testing "upsert with tempid, order does not matter"
          (let [report (<? (dm/<transact! conn [[:db/add (dm/id-literal :db.part/user -1) :age 13]
                                                [:db/add (dm/id-literal :db.part/user -1) :name "Petr"]] now))]
            (is (= (<? (<shallow-entity (dm/db conn) 102))
                   {:name "Petr" :age 13 :email "@2"}))
            (is (= (tempids report)
                   {-1 102}))))

        (testing "Conflicting upserts fail"
          (is (thrown-with-msg? Throwable #"Conflicting upsert: #datomish.db.TempId\{:part :db.part/user, :idx -\d+\} resolves both to \d+ and \d+"
                                (<? (dm/<transact! conn [[:db/add (dm/id-literal :db.part/user -1) :name "Ivan"]
                                                         [:db/add (dm/id-literal :db.part/user -1) :age 35]
                                                         [:db/add (dm/id-literal :db.part/user -1) :name "Petr"]
                                                         [:db/add (dm/id-literal :db.part/user -1) :age 36]] now)))))
        (finally
          (<? (dm/close-db db)))))))

(deftest-async test-map-upsert
  (with-tempfile [t (tempfile)]
    (let [c    (<? (s/<sqlite-connection t))
          db   (<? (dm/<db-with-sqlite-connection c test-schema))
          conn (dm/connection-with-db db)
          now  0xdeadbeef
          tempids (fn [tx] (into {} (map (juxt (comp :idx first) second) (:tempids tx))))]
      (try
        ;; Not having DB-as-value really hurts us here.  This test only works because all upserts
        ;; succeed on top of each other, so we never need to reset the underlying store.
        (<? (dm/<transact! conn test-schema now))
        (<? (dm/<transact! conn [{:db/id 101 :name "Ivan" :email "@1"}
                                 {:db/id 102 :name "Petr" :email "@2"}] now))

        (testing "upsert with tempid"
          (let [tx (<? (dm/<transact! conn [{:db/id (dm/id-literal :db.part/user -1) :name "Ivan" :age 35}] now))]
            (is (= (<? (<shallow-entity (dm/db conn) 101))
                   {:name "Ivan" :email "@1" :age 35}))
            (is (= (tempids tx)
                   {-1 101}))))

        (testing "upsert by 2 attrs with tempid"
          (let [tx (<? (dm/<transact! conn [{:db/id (dm/id-literal :db.part/user -1) :name "Ivan" :email "@1" :age 35}] now))]
            (is (= (<? (<shallow-entity (dm/db conn) 101))
                   {:name "Ivan" :email "@1" :age 35}))
            (is (= (tempids tx)
                   {-1 101}))))

        (testing "upsert with existing id"
          (let [tx (<? (dm/<transact! conn [{:db/id 101 :name "Ivan" :age 36}] now))]
            (is (= (<? (<shallow-entity (dm/db conn) 101))
                   {:name "Ivan" :email "@1" :age 36}))
            (is (= (tempids tx)
                   {}))))

        (testing "upsert by 2 attrs with existing id"
          (let [tx (<? (dm/<transact! conn [{:db/id 101 :name "Ivan" :email "@1" :age 37}] now))]
            (is (= (<? (<shallow-entity (dm/db conn) 101))
                   {:name "Ivan" :email "@1" :age 37}))
            (is (= (tempids tx)
                   {}))))

        (testing "upsert to two entities, resolve to same tempid, fails due to overlapping writes"
          (is (thrown-with-msg? Throwable #"cardinality constraint"
                                (<? (dm/<transact! conn [{:db/id (dm/id-literal :db.part/user -1) :name "Ivan" :age 35}
                                                         {:db/id (dm/id-literal :db.part/user -1) :name "Ivan" :age 36}] now)))))

        (testing "upsert to two entities, two tempids, fails due to overlapping writes"
          (is (thrown-with-msg? Throwable #"cardinality constraint"
                                (<? (dm/<transact! conn [{:db/id (dm/id-literal :db.part/user -1) :name "Ivan" :age 35}
                                                         {:db/id (dm/id-literal :db.part/user -2) :name "Ivan" :age 36}] now)))))

        (finally
          (<? (dm/close-db db)))))))

(deftest-async test-map-upsert-conflicts
  (with-tempfile [t (tempfile)]
    (let [c    (<? (s/<sqlite-connection t))
          db   (<? (dm/<db-with-sqlite-connection c test-schema))
          conn (dm/connection-with-db db)
          now  0xdeadbeef
          tempids (fn [tx] (into {} (map (juxt (comp :idx first) second) (:tempids tx))))]
      (try
        ;; Not having DB-as-value really hurts us here.  This test only works because all upserts
        ;; fail until the final one, so we never need to reset the underlying store.
        (<? (dm/<transact! conn test-schema now))
        (<? (dm/<transact! conn [{:db/id 101 :name "Ivan" :email "@1"}
                                 {:db/id 102 :name "Petr" :email "@2"}] now))

        ;; TODO: improve error message to refer to upsert inputs.
        (testing "upsert conficts with existing id"
          (is (thrown-with-msg? Throwable #"unique constraint"
                                (<? (dm/<transact! conn [{:db/id 102 :name "Ivan" :age 36}] now)))))

        ;; TODO: improve error message to refer to upsert inputs.
        (testing "upsert conficts with non-existing id"
          (is (thrown-with-msg? Throwable #"unique constraint"
                                (<? (dm/<transact! conn [{:db/id 103 :name "Ivan" :age 36}] now)))))

        ;; TODO: improve error message to refer to upsert inputs.
        (testing "upsert by 2 conflicting fields"
          (is (thrown-with-msg? Throwable #"Conflicting upsert: #datomish.db.TempId\{:part :db.part/user, :idx -\d+\} resolves both to \d+ and \d+"
                                (<? (dm/<transact! conn [{:db/id (dm/id-literal :db.part/user -1) :name "Ivan" :email "@2" :age 35}] now)))))

        (testing "upsert by non-existing value resolves as update"
          (let [report (<? (dm/<transact! conn [{:db/id (dm/id-literal :db.part/user -1) :name "Ivan" :email "@3" :age 35}] now))]
            (is (= (<? (<shallow-entity (dm/db conn) 101))
                   {:name "Ivan" :email "@3" :age 35}))
            (is (= (tempids report)
                   {-1 101}))))

        (finally
          (<? (dm/close-db db)))))))

(deftest-async test-add-ident
  (with-tempfile [t (tempfile)]
    (let [c    (<? (s/<sqlite-connection t))
          db   (<? (dm/<db-with-sqlite-connection c test-schema))
          conn (dm/connection-with-db db)
          now  -1]
      (try
        (let [report   (<? (dm/<transact! conn [[:db/add (dm/id-literal :db.part/db -1) :db/ident :test/ident]] now))
              db-after (:db-after report)
              tx       (:current-tx db-after)]
          (is (= (:test/ident (dm/idents db-after)) (get-in report [:tempids (dm/id-literal :db.part/db -1)]))))

        ;; TODO: This should fail, but doesn't, due to stringification of :test/ident.
        ;; (is (thrown-with-msg?
        ;;       ExceptionInfo #"Retracting a :db/ident is not yet supported, got "
        ;;       (<? (dm/<transact! conn [[:db/retract 44 :db/ident :test/ident]] now))))

        ;; ;; Renaming looks like retraction and then assertion.
        ;; (is (thrown-with-msg?
        ;;       ExceptionInfo #"Retracting a :db/ident is not yet supported, got"
        ;;       (<? (dm/<transact! conn [[:db/add 44 :db/ident :other-name]] now))))

        ;; (is (thrown-with-msg?
        ;;       ExceptionInfo #"Re-asserting a :db/ident is not yet supported, got"
        ;;       (<? (dm/<transact! conn [[:db/add 55 :db/ident :test/ident]] now))))

        (finally
          (<? (dm/close-db db)))))))

(deftest-async test-add-schema
  (with-tempfile [t (tempfile)]
    (let [c    (<? (s/<sqlite-connection t))
          db   (<? (dm/<db-with-sqlite-connection c test-schema))
          conn (dm/connection-with-db db)
          now  -1]
      (try
        (let [es       [[:db/add :db.part/db :db.install/attribute (dm/id-literal :db.part/db -1)]
                        {:db/id (dm/id-literal :db.part/db -1)
                         :db/ident :test/attr
                         :db/valueType :db.type/string
                         :db/cardinality :db.cardinality/one}]
              report   (<? (dm/<transact! conn es now))
              db-after (:db-after report)
              tx       (:current-tx db-after)]

          (testing "New ident is allocated"
            (is (some? (get-in db-after [:idents :test/attr]))))

          (testing "Schema is modified"
            (is (= (get-in db-after [:symbolic-schema :test/attr])
                   {:db/valueType :db.type/string,
                    :db/cardinality :db.cardinality/one})))

          (testing "Schema is used in subsequent transaction"
            (<? (dm/<transact! conn [{:db/id 100 :test/attr "value 1"}]))
            (<? (dm/<transact! conn [{:db/id 100 :test/attr "value 2"}]))
            (is (= (<? (<shallow-entity (dm/db conn) 100))
                   {:test/attr "value 2"}))))

        (finally
          (<? (dm/close-db db)))))))
