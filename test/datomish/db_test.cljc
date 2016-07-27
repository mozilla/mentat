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

(defn- <datoms [db]
  (let [entids (zipmap (vals (dm/idents db)) (keys (dm/idents db)))]
    (go-pair
      (->>
        (s/all-rows (:sqlite-connection db) ["SELECT e, a, v, tx FROM datoms"])
        (<?)
        (mapv #(vector (:e %) (entids (:a %)) (:v %)))
        (filter #(not (= :db/txInstant (second %))))
        (set)))))

(defn- <transactions [db]
  (let [entids (zipmap (vals (dm/idents db)) (keys (dm/idents db)))]
    (go-pair
      (->>
        (s/all-rows (:sqlite-connection db) ["SELECT e, a, v, tx, added FROM transactions ORDER BY tx ASC, e, a, v, added"])
        (<?)
        (mapv #(vector (:e %) (entids (:a %)) (:v %) (:tx %) (:added %)))))))

(defn tx [report]
  (get-in report [:db-after :current-tx]))

(deftest-async test-add-one
  (with-tempfile [t (tempfile)]
    (let [c    (<? (s/<sqlite-connection t))
          db   (<? (dm/<db-with-sqlite-connection c))
          conn (dm/connection-with-db db)
          now  0xdeadbeef]
      (try
        (let [;; TODO: drop now, allow to set :db/txInstant.
              report (<? (dm/<transact! conn [[:db/add 0 :x "valuex"]] now))
              tx     (tx report)]
          (is (= (<? (<datoms (dm/db conn)))
                 #{[0 :x "valuex"]}))
          (is (= (<? (<transactions (dm/db conn)))
                 [[0 :x "valuex" tx 1] ;; TODO: true, not 1.
                  [tx :db/txInstant now tx 1]])))
        (finally
          (<? (dm/close-db db)))))))

(deftest-async test-add-two
  (with-tempfile [t (tempfile)]
    (let [c    (<? (s/<sqlite-connection t))
          db   (<? (dm/<db-with-sqlite-connection c
                                                  {:x {:db/unique :db.unique/identity} ;; TODO: :name and :aka.
                                                   :y {:db/cardinality :db.cardinality/many}}))
          conn (dm/connection-with-db db)
          now  0xdeadbeef]
      (try
        (let [tx1 (tx (<? (dm/<transact! conn [[:db/add 1 :x "Ivan"]] now)))
              tx2 (tx (<? (dm/<transact! conn [[:db/add 1 :x "Petr"]] now)))
              tx3 (tx (<? (dm/<transact! conn [[:db/add 1 :y "Tupen"]] now)))
              tx4 (tx (<? (dm/<transact! conn [[:db/add 1 :y "Devil"]] now)))]
          (is (= (<? (<datoms (dm/db conn)))
                 #{[1 :x "Petr"]
                   [1 :y "Tupen"]
                   [1 :y "Devil"]}))

          (is (= (<? (<transactions (dm/db conn)))
                 [[1 :x "Ivan" tx1 1] ;; TODO: true, not 1.
                  [tx1 :db/txInstant now tx1 1]
                  [1 :x "Ivan" tx2 0]
                  [1 :x "Petr" tx2 1]
                  [tx2 :db/txInstant now tx2 1]
                  [1 :y "Tupen" tx3 1]
                  [tx3 :db/txInstant now tx3 1]
                  [1 :y "Devil" tx4 1]
                  [tx4 :db/txInstant now tx4 1]])))
        (finally
          (<? (dm/close-db db)))))))

;; TODO: fail multiple :add and :retract of the same datom in the same transaction.
(deftest-async test-retract
  (with-tempfile [t (tempfile)]
    (let [c    (<? (s/<sqlite-connection t))
          db   (<? (dm/<db-with-sqlite-connection c))
          conn (dm/connection-with-db db)
          now  0xdeadbeef]
      (try
        (let [txa (tx (<? (dm/<transact! conn [[:db/add     0 :x "valuex"]] now)))
              txb (tx (<? (dm/<transact! conn [[:db/retract 0 :x "valuex"]] now)))]
          (is (= (<? (<datoms db))
                 #{}))
          (is (= (<? (<transactions db))
                 [[0 :x "valuex" txa 1] ;; TODO: true, not 1.
                  [txa :db/txInstant now txa 1]
                  [0 :x "valuex" txb 0]
                  [txb :db/txInstant now txb 1]])))
        (finally
          (<? (dm/close-db db)))))))

(deftest-async test-id-literal-1
  (with-tempfile [t (tempfile)]
    (let [c    (<? (s/<sqlite-connection t))
          db   (<? (dm/<db-with-sqlite-connection c))
          conn (dm/connection-with-db db)
          now  -1]
      (try
        (let [report (<? (dm/<transact! conn [[:db/add (dm/id-literal :db.part/user -1) :x 0]
                                              [:db/add (dm/id-literal :db.part/user -1) :y 1]
                                              [:db/add (dm/id-literal :db.part/user -2) :y 2]
                                              [:db/add (dm/id-literal :db.part/user -2) :y 3]] now))]
          (is (= (keys (:tempids report)) ;; TODO: include values.
                 [(dm/id-literal :db.part/user -1)
                  (dm/id-literal :db.part/user -2)]))

          (let [eid1 (get-in report [:tempids (dm/id-literal :db.part/user -1)])
                eid2 (get-in report [:tempids (dm/id-literal :db.part/user -2)])]
            (is (= (<? (<datoms db))
                   #{[eid1 :x 0]
                     [eid1 :y 1]
                     [eid2 :y 2]
                     [eid2 :y 3]}))))

        (finally
          (<? (dm/close-db db)))))))

(deftest-async test-add-ident
  (with-tempfile [t (tempfile)]
    (let [c    (<? (s/<sqlite-connection t))
          db   (<? (dm/<db-with-sqlite-connection c))
          conn (dm/connection-with-db db)
          now  -1]
      (try
        (let [report   (<? (dm/<transact! conn [[:db/add 44 :db/ident :name]] now))
              db-after (:db-after report)
              tx       (:current-tx db-after)]
          (is (= (:name (dm/idents db-after)) 44)))

        ;; TODO: This should fail, but doesn't, due to stringification of :name.
        ;; (is (thrown-with-msg?
        ;;       ExceptionInfo #"Retracting a :db/ident is not yet supported, got "
        ;;       (<? (dm/<transact! conn [[:db/retract 44 :db/ident :name]] now))))

        ;; ;; Renaming looks like retraction and then assertion.
        ;; (is (thrown-with-msg?
        ;;       ExceptionInfo #"Retracting a :db/ident is not yet supported, got"
        ;;       (<? (dm/<transact! conn [[:db/add 44 :db/ident :other-name]] now))))

        ;; (is (thrown-with-msg?
        ;;       ExceptionInfo #"Re-asserting a :db/ident is not yet supported, got"
        ;;       (<? (dm/<transact! conn [[:db/add 55 :db/ident :name]] now))))

        (finally
          (<? (dm/close-db db)))))))
