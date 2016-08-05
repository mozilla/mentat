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
   [datomish.api :as d]
   [datomish.util :as util #?(:cljs :refer-macros :clj :refer) [raise cond-let]]
   [datomish.sqlite :as s]
   [datomish.sqlite-schema]
   [datomish.datom]
   [datomish.db :as db]
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

(defn- tempids [tx]
  (into {} (map (juxt (comp :idx first) second) (:tempids tx))))

(defn- <datoms-after [db tx]
  (let [entids (zipmap (vals (db/idents db)) (keys (db/idents db)))]
    (go-pair
      (->>
        (s/all-rows (:sqlite-connection db) ["SELECT e, a, v, tx FROM datoms WHERE tx > ?" tx])
        (<?)
        (mapv #(vector (:e %) (get entids (:a %) (str "fail" (:a %))) (:v %)))
        (filter #(not (= :db/txInstant (second %))))
        (set)))))

(defn- <datoms [db]
  (<datoms-after db 0))

(defn- <shallow-entity [db eid]
  ;; TODO: make this actually be <entity.  Handle :db.cardinality/many and :db/isComponent.
  (let [entids (zipmap (vals (db/idents db)) (keys (db/idents db)))]
    (go-pair
      (->>
        (s/all-rows (:sqlite-connection db) ["SELECT a, v FROM datoms WHERE e = ?" eid])
        (<?)
        (mapv #(vector (entids (:a %)) (:v %)))
        (reduce conj {})))))

(defn- <transactions-after [db tx]
  (let [entids (zipmap (vals (db/idents db)) (keys (db/idents db)))]
    (go-pair
      (->>
        (s/all-rows (:sqlite-connection db) ["SELECT e, a, v, tx, added FROM transactions WHERE tx > ? ORDER BY tx ASC, e, a, v, added" tx])
        (<?)
        (mapv #(vector (:e %) (entids (:a %)) (:v %) (:tx %) (:added %)))))))

(defn- <transactions [db]
  (<transactions-after db 0))

(defn- <fulltext-values [db]
  (go-pair
    (->>
      (s/all-rows (:sqlite-connection db) ["SELECT rowid, text FROM fulltext_values"])
      (<?)
      (mapv #(vector (:rowid %) (:text %))))))

;; TODO: use reverse refs!
(def test-schema
  [{:db/id        (d/id-literal :test -1)
    :db/ident     :x
    :db/unique    :db.unique/identity
    :db/valueType :db.type/integer}
   {:db/id :db.part/db :db.install/attribute (d/id-literal :test -1)}
   {:db/id        (d/id-literal :test -2)
    :db/ident     :name
    :db/unique    :db.unique/identity
    :db/valueType :db.type/string}
   {:db/id :db.part/db :db.install/attribute (d/id-literal :test -2)}
   {:db/id          (d/id-literal :test -3)
    :db/ident       :y
    :db/cardinality :db.cardinality/many
    :db/valueType   :db.type/integer}
   {:db/id :db.part/db :db.install/attribute (d/id-literal :test -3)}
   {:db/id          (d/id-literal :test -5)
    :db/ident       :aka
    :db/cardinality :db.cardinality/many
    :db/valueType   :db.type/string}
   {:db/id :db.part/db :db.install/attribute (d/id-literal :test -5)}
   {:db/id        (d/id-literal :test -6)
    :db/ident     :age
    :db/valueType :db.type/integer}
   {:db/id :db.part/db :db.install/attribute (d/id-literal :test -6)}
   {:db/id        (d/id-literal :test -7)
    :db/ident     :email
    :db/unique    :db.unique/identity
    :db/valueType :db.type/string}
   {:db/id :db.part/db :db.install/attribute (d/id-literal :test -7)}
   {:db/id        (d/id-literal :test -8)
    :db/ident     :spouse
    :db/unique    :db.unique/value
    :db/valueType :db.type/string}
   {:db/id :db.part/db :db.install/attribute (d/id-literal :test -8)}
   {:db/id          (d/id-literal :test -9)
    :db/ident       :friends
    :db/cardinality :db.cardinality/many
    :db/valueType   :db.type/ref}
   {:db/id :db.part/db :db.install/attribute (d/id-literal :test -9)}
   ])

(deftest-async test-add-one
  (with-tempfile [t (tempfile)]
    (let [conn (<? (d/<connect t))]
      (try
        (let [{tx0 :tx} (<? (d/<transact! conn test-schema))]
          (let [{:keys [tx txInstant]} (<? (d/<transact! conn [[:db/add 0 :name "valuex"]]))]
            (is (= (<? (<datoms-after (d/db conn) tx0))
                   #{[0 :name "valuex"]}))
            (is (= (<? (<transactions-after (d/db conn) tx0))
                   [[0 :name "valuex" tx 1] ;; TODO: true, not 1.
                    [tx :db/txInstant txInstant tx 1]]))))
        (finally
          (<? (d/<close conn)))))))

(deftest-async test-add-two
  (with-tempfile [t (tempfile)]
    (let [conn (<? (d/<connect t))]
      (try
        (let [{tx0 :tx} (<? (d/<transact! conn test-schema))
              {tx1 :tx txInstant1 :txInstant} (<? (d/<transact! conn [[:db/add 1 :name "Ivan"]]))
              {tx2 :tx txInstant2 :txInstant} (<? (d/<transact! conn [[:db/add 1 :name "Petr"]]))
              {tx3 :tx txInstant3 :txInstant} (<? (d/<transact! conn [[:db/add 1 :aka "Tupen"]]))
              {tx4 :tx txInstant4 :txInstant} (<? (d/<transact! conn [[:db/add 1 :aka "Devil"]]))]
          (is (= (<? (<datoms-after (d/db conn) tx0))
                 #{[1 :name "Petr"]
                   [1 :aka  "Tupen"]
                   [1 :aka  "Devil"]}))

          (is (= (<? (<transactions-after (d/db conn) tx0))
                 [[1 :name "Ivan" tx1 1] ;; TODO: true, not 1.
                  [tx1 :db/txInstant txInstant1 tx1 1]
                  [1 :name "Ivan" tx2 0]
                  [1 :name "Petr" tx2 1]
                  [tx2 :db/txInstant txInstant2 tx2 1]
                  [1 :aka "Tupen" tx3 1]
                  [tx3 :db/txInstant txInstant3 tx3 1]
                  [1 :aka "Devil" tx4 1]
                  [tx4 :db/txInstant txInstant4 tx4 1]])))

        (finally
          (<? (d/<close conn)))))))

(deftest-async test-retract
  (with-tempfile [t (tempfile)]
    (let [conn (<? (d/<connect t))]
      (try
        (let [{tx0 :tx} (<? (d/<transact! conn test-schema))
              {tx1 :tx txInstant1 :txInstant} (<? (d/<transact! conn [[:db/add     0 :x 123]]))
              {tx2 :tx txInstant2 :txInstant} (<? (d/<transact! conn [[:db/retract 0 :x 123]]))]
          (is (= (<? (<datoms-after (d/db conn) tx0))
                 #{}))
          (is (= (<? (<transactions-after (d/db conn) tx0))
                 [[0 :x 123 tx1 1]
                  [tx1 :db/txInstant txInstant1 tx1 1]
                  [0 :x 123 tx2 0]
                  [tx2 :db/txInstant txInstant2 tx2 1]])))
        (finally
          (<? (d/<close conn)))))))

(deftest-async test-id-literal-1
  (with-tempfile [t (tempfile)]
    (let [conn (<? (d/<connect t))]
      (try
        (let [tx0 (:tx (<? (d/<transact! conn test-schema)))
              report (<? (d/<transact! conn [[:db/add (d/id-literal :db.part/user -1) :x 0]
                                             [:db/add (d/id-literal :db.part/user -1) :y 1]
                                             [:db/add (d/id-literal :db.part/user -2) :y 2]
                                             [:db/add (d/id-literal :db.part/user -2) :y 3]]))]
          (is (= (keys (:tempids report)) ;; TODO: include values.
                 [(d/id-literal :db.part/user -1)
                  (d/id-literal :db.part/user -2)]))

          (let [eid1 (get-in report [:tempids (d/id-literal :db.part/user -1)])
                eid2 (get-in report [:tempids (d/id-literal :db.part/user -2)])]
            (is (= (<? (<datoms-after (d/db conn) tx0))
                   #{[eid1 :x 0]
                     [eid1 :y 1]
                     [eid2 :y 2]
                     [eid2 :y 3]}))))

        (finally
          (<? (d/<close conn)))))))

(deftest-async test-unique
  (with-tempfile [t (tempfile)]
    (let [conn (<? (d/<connect t))]
      (try
        (let [tx0 (:tx (<? (d/<transact! conn test-schema)))]
          (testing "Multiple :db/unique values in tx-data violate unique constraint, no tempid"
            (is (thrown-with-msg?
                  ExceptionInfo #"unique constraint"
                  (<? (d/<transact! conn [[:db/add 1 :x 0]
                                          [:db/add 2 :x 0]])))))

          (testing "Multiple :db/unique values in tx-data violate unique constraint, tempid"
            (is (thrown-with-msg?
                  ExceptionInfo #"unique constraint"
                  (<? (d/<transact! conn [[:db/add (d/id-literal :db.part/user -1) :spouse "Dana"]
                                          [:db/add (d/id-literal :db.part/user -2) :spouse "Dana"]]))))))

        (finally
          (<? (d/<close conn)))))))

(deftest-async test-valueType-keyword
  (with-tempfile [t (tempfile)]
    (let [conn (<? (d/<connect t))]
      (try
        (let [tx0 (:tx (<? (d/<transact! conn [{:db/id        (d/id-literal :db.part/user -1)
                                                :db/ident     :test/kw
                                                :db/unique    :db.unique/identity
                                                :db/valueType :db.type/keyword}
                                               {:db/id :db.part/db :db.install/attribute (d/id-literal :db.part/user -1)}])))]

          (let [report (<? (d/<transact! conn [[:db/add (d/id-literal :db.part/user -1) :test/kw :test/kw1]]))
                eid    (get-in report [:tempids (d/id-literal :db.part/user -1)])]
            (is (= (<? (<datoms-after (d/db conn) tx0))
                   #{[eid :test/kw ":test/kw1"]})) ;; Value is raw.

            (testing "Adding the same value compares existing values correctly."
              (<? (d/<transact! conn [[:db/add eid :test/kw :test/kw1]]))
              (is (= (<? (<datoms-after (d/db conn) tx0))
                     #{[eid :test/kw ":test/kw1"]}))) ;; Value is raw.

            (testing "Upserting retracts existing value correctly."
              (<? (d/<transact! conn [[:db/add eid :test/kw :test/kw2]]))
              (is (= (<? (<datoms-after (d/db conn) tx0))
                     #{[eid :test/kw ":test/kw2"]}))) ;; Value is raw.

            (testing "Retracting compares values correctly."
              (<? (d/<transact! conn [[:db/retract eid :test/kw :test/kw2]]))
              (is (= (<? (<datoms-after (d/db conn) tx0))
                     #{})))))

        (finally
          (<? (d/<close conn)))))))

(deftest-async test-vector-upsert
  (with-tempfile [t (tempfile)]
    (let [conn (<? (d/<connect t))]
      (try
        ;; Not having DB-as-value really hurts us here.  This test only works because all upserts
        ;; succeed on top of each other, so we never need to reset the underlying store.
        (<? (d/<transact! conn test-schema))
        (let [tx0 (:tx (<? (d/<transact! conn [{:db/id 101 :name "Ivan" :email "@1"}
                                               {:db/id 102 :name "Petr" :email "@2"}])))]

          (testing "upsert with tempid"
            (let [report (<? (d/<transact! conn [[:db/add (d/id-literal :db.part/user -1) :name "Ivan"]
                                                 [:db/add (d/id-literal :db.part/user -1) :age 12]]))]
              (is (= (<? (<shallow-entity (d/db conn) 101))
                     {:name "Ivan" :age 12 :email "@1"}))
              (is (= (tempids report)
                     {-1 101}))))

          (testing "upsert with tempid, order does not matter"
            (let [report (<? (d/<transact! conn [[:db/add (d/id-literal :db.part/user -1) :age 13]
                                                 [:db/add (d/id-literal :db.part/user -1) :name "Petr"]]))]
              (is (= (<? (<shallow-entity (d/db conn) 102))
                     {:name "Petr" :age 13 :email "@2"}))
              (is (= (tempids report)
                     {-1 102}))))

          (testing "Conflicting upserts fail"
            (is (thrown-with-msg? Throwable #"Conflicting upsert"
                                  (<? (d/<transact! conn [[:db/add (d/id-literal :db.part/user -1) :name "Ivan"]
                                                          [:db/add (d/id-literal :db.part/user -1) :age 35]
                                                          [:db/add (d/id-literal :db.part/user -1) :name "Petr"]
                                                          [:db/add (d/id-literal :db.part/user -1) :age 36]]))))))
        (finally
          (<? (d/<close conn)))))))

(deftest-async test-map-upsert
  (with-tempfile [t (tempfile)]
    (let [conn (<? (d/<connect t))]
      (try
        ;; Not having DB-as-value really hurts us here.  This test only works because all upserts
        ;; succeed on top of each other, so we never need to reset the underlying store.
        (<? (d/<transact! conn test-schema))
        (let [tx0 (:tx (<? (d/<transact! conn [{:db/id 101 :name "Ivan" :email "@1"}
                                               {:db/id 102 :name "Petr" :email "@2"}])))]

          (testing "upsert with tempid"
            (let [tx (<? (d/<transact! conn [{:db/id (d/id-literal :db.part/user -1) :name "Ivan" :age 35}]))]
              (is (= (<? (<shallow-entity (d/db conn) 101))
                     {:name "Ivan" :email "@1" :age 35}))
              (is (= (tempids tx)
                     {-1 101}))))

          (testing "upsert by 2 attrs with tempid"
            (let [tx (<? (d/<transact! conn [{:db/id (d/id-literal :db.part/user -1) :name "Ivan" :email "@1" :age 35}]))]
              (is (= (<? (<shallow-entity (d/db conn) 101))
                     {:name "Ivan" :email "@1" :age 35}))
              (is (= (tempids tx)
                     {-1 101}))))

          (testing "upsert with existing id"
            (let [tx (<? (d/<transact! conn [{:db/id 101 :name "Ivan" :age 36}]))]
              (is (= (<? (<shallow-entity (d/db conn) 101))
                     {:name "Ivan" :email "@1" :age 36}))
              (is (= (tempids tx)
                     {}))))

          (testing "upsert by 2 attrs with existing id"
            (let [tx (<? (d/<transact! conn [{:db/id 101 :name "Ivan" :email "@1" :age 37}]))]
              (is (= (<? (<shallow-entity (d/db conn) 101))
                     {:name "Ivan" :email "@1" :age 37}))
              (is (= (tempids tx)
                     {}))))

          (testing "upsert to two entities, resolve to same tempid, fails due to overlapping writes"
            (is (thrown-with-msg? Throwable #"cardinality constraint"
                                  (<? (d/<transact! conn [{:db/id (d/id-literal :db.part/user -1) :name "Ivan" :age 35}
                                                          {:db/id (d/id-literal :db.part/user -1) :name "Ivan" :age 36}])))))

          (testing "upsert to two entities, two tempids, fails due to overlapping writes"
            (is (thrown-with-msg? Throwable #"cardinality constraint"
                                  (<? (d/<transact! conn [{:db/id (d/id-literal :db.part/user -1) :name "Ivan" :age 35}
                                                          {:db/id (d/id-literal :db.part/user -2) :name "Ivan" :age 36}]))))))

        (finally
          (<? (d/<close conn)))))))

(deftest-async test-map-upsert-conflicts
  (with-tempfile [t (tempfile)]
    (let [conn (<? (d/<connect t))]
      (try
        ;; Not having DB-as-value really hurts us here.  This test only works because all upserts
        ;; fail until the final one, so we never need to reset the underlying store.
        (<? (d/<transact! conn test-schema))
        (let [tx0 (:tx (<? (d/<transact! conn [{:db/id 101 :name "Ivan" :email "@1"}
                                               {:db/id 102 :name "Petr" :email "@2"}])))]

          ;; TODO: improve error message to refer to upsert inputs.
          (testing "upsert conficts with existing id"
            (is (thrown-with-msg? Throwable #"unique constraint"
                                  (<? (d/<transact! conn [{:db/id 102 :name "Ivan" :age 36}])))))

          ;; TODO: improve error message to refer to upsert inputs.
          (testing "upsert conficts with non-existing id"
            (is (thrown-with-msg? Throwable #"unique constraint"
                                  (<? (d/<transact! conn [{:db/id 103 :name "Ivan" :age 36}])))))

          ;; TODO: improve error message to refer to upsert inputs.
          (testing "upsert by 2 conflicting fields"
            (is (thrown-with-msg? Throwable #"Conflicting upsert"
                                  (<? (d/<transact! conn [{:db/id (d/id-literal :db.part/user -1) :name "Ivan" :email "@2" :age 35}])))))

          (testing "upsert by non-existing value resolves as update"
            (let [report (<? (d/<transact! conn [{:db/id (d/id-literal :db.part/user -1) :name "Ivan" :email "@3" :age 35}]))]
              (is (= (<? (<shallow-entity (d/db conn) 101))
                     {:name "Ivan" :email "@3" :age 35}))
              (is (= (tempids report)
                     {-1 101})))))

        (finally
          (<? (d/<close conn)))))))

(deftest-async test-add-ident
  (with-tempfile [t (tempfile)]
    (let [conn (<? (d/<connect t))]
      (try
        (let [report   (<? (d/<transact! conn [[:db/add (d/id-literal :db.part/db -1) :db/ident :test/ident]]))
              db-after (:db-after report)
              tx       (:tx db-after)]
          (is (= (:test/ident (db/idents db-after)) (get-in report [:tempids (d/id-literal :db.part/db -1)]))))

        ;; TODO: This should fail, but doesn't, due to stringification of :test/ident.
        ;; (is (thrown-with-msg?
        ;;       ExceptionInfo #"Retracting a :db/ident is not yet supported, got "
        ;;       (<? (d/<transact! conn [[:db/retract 44 :db/ident :test/ident]]))))

        ;; ;; Renaming looks like retraction and then assertion.
        ;; (is (thrown-with-msg?
        ;;       ExceptionInfo #"Retracting a :db/ident is not yet supported, got"
        ;;       (<? (d/<transact! conn [[:db/add 44 :db/ident :other-name]]))))

        ;; (is (thrown-with-msg?
        ;;       ExceptionInfo #"Re-asserting a :db/ident is not yet supported, got"
        ;;       (<? (d/<transact! conn [[:db/add 55 :db/ident :test/ident]]))))

        (finally
          (<? (d/<close conn)))))))

(deftest-async test-add-schema
  (with-tempfile [t (tempfile)]
    (let [conn (<? (d/<connect t))]
      (try
        (let [es       [[:db/add :db.part/db :db.install/attribute (d/id-literal :db.part/db -1)]
                        {:db/id (d/id-literal :db.part/db -1)
                         :db/ident :test/attr
                         :db/valueType :db.type/string
                         :db/cardinality :db.cardinality/one}]
              report   (<? (d/<transact! conn es))
              db-after (:db-after report)
              tx       (:tx db-after)]

          (testing "New ident is allocated"
            (is (some? (get-in db-after [:idents :test/attr]))))

          (testing "Schema is modified"
            (is (= (get-in db-after [:symbolic-schema :test/attr])
                   {:db/valueType :db.type/string,
                    :db/cardinality :db.cardinality/one})))

          (testing "Schema is used in subsequent transaction"
            (<? (d/<transact! conn [{:db/id 100 :test/attr "value 1"}]))
            (<? (d/<transact! conn [{:db/id 100 :test/attr "value 2"}]))
            (is (= (<? (<shallow-entity (d/db conn) 100))
                   {:test/attr "value 2"}))))

        (finally
          (<? (d/<close conn)))))))

(deftest-async test-fulltext
  (with-tempfile [t (tempfile)]
    (let [conn (<? (d/<connect t))
          schema [{:db/id (d/id-literal :db.part/db -1)
                   :db/ident :test/fulltext
                   :db/valueType :db.type/string
                   :db/fulltext true
                   :db/unique :db.unique/identity}
                  {:db/id :db.part/db :db.install/attribute (d/id-literal :db.part/db -1)}
                  {:db/id (d/id-literal :db.part/db -2)
                   :db/ident :test/other
                   :db/valueType :db.type/string
                   :db/fulltext true
                   :db/cardinality :db.cardinality/one}
                  {:db/id :db.part/db :db.install/attribute (d/id-literal :db.part/db -2)}
                  ]
          tx0 (:tx (<? (d/<transact! conn schema)))]
      (try
        (testing "Can add fulltext indexed datoms"
          (let [r (<? (d/<transact! conn [[:db/add 101 :test/fulltext "test this"]]))]
            (is (= (<? (<fulltext-values (d/db conn)))
                   [[1 "test this"]]))
            (is (= (<? (<datoms-after (d/db conn) tx0))
                   #{[101 :test/fulltext 1]})) ;; Values are raw; 1 is the rowid into fulltext_values.
            ))

        (testing "Can replace fulltext indexed datoms"
          (let [r (<? (d/<transact! conn [[:db/add 101 :test/fulltext "alternate thing"]]))]
            (is (= (<? (<fulltext-values (d/db conn)))
                   [[1 "test this"]
                    [2 "alternate thing"]]))
            (is (= (<? (<datoms-after (d/db conn) tx0))
                   #{[101 :test/fulltext 2]})) ;; Values are raw; 2 is the rowid into fulltext_values.
            ))

        (testing "Can upsert keyed by fulltext indexed datoms"
          (let [r (<? (d/<transact! conn [{:db/id (d/id-literal :db.part/user) :test/fulltext "alternate thing" :test/other "other"}]))]
            (is (= (<? (<fulltext-values (d/db conn)))
                   [[1 "test this"]
                    [2 "alternate thing"]
                    [3 "other"]]))
            (is (= (<? (<datoms-after (d/db conn) tx0))
                   #{[101 :test/fulltext 2] ;; Values are raw; 2, 3 are the rowids into fulltext_values.
                     [101 :test/other 3]}))
            ))

        (testing "Can re-use fulltext indexed datoms"
          (let [r (<? (d/<transact! conn [[:db/add 102 :test/other "test this"]]))]
            (is (= (<? (<fulltext-values (d/db conn)))
                   [[1 "test this"]
                    [2 "alternate thing"]
                    [3 "other"]]))
            (is (= (<? (<datoms-after (d/db conn) tx0))
                   #{[101 :test/fulltext 2]
                     [101 :test/other 3]
                     [102 :test/other 1]})) ;; Values are raw; 1, 2, 3 are the rowids into fulltext_values.
            ))

        (testing "Can retract fulltext indexed datoms"
          (let [r (<? (d/<transact! conn [[:db/retract 101 :test/fulltext "alternate thing"]]))]
            (is (= (<? (<fulltext-values (d/db conn)))
                   [[1 "test this"]
                    [2 "alternate thing"]
                    [3 "other"]]))
            (is (= (<? (<datoms-after (d/db conn) tx0))
                   #{[101 :test/other 3]
                     [102 :test/other 1]})) ;; Values are raw; 1, 3 are the rowids into fulltext_values.
            ))

        (finally
          (<? (d/<close conn)))))))

(deftest-async test-txInstant
  (with-tempfile [t (tempfile)]
    (let [conn      (<? (d/<connect t))
          {tx0 :tx} (<? (d/<transact! conn test-schema))]
      (try
        (let [{txa :tx txInstantA :txInstant} (<? (d/<transact! conn []))]
          (testing ":db/txInstant is set by default"
            (is (= (<? (<transactions-after (d/db conn) tx0))
                   [[txa :db/txInstant txInstantA txa 1]])))

          ;; TODO: range check txInstant values against DB clock.
          (testing ":db/txInstant can be set explicitly"
            (let [{txb :tx txInstantB :txInstant} (<? (d/<transact! conn [[:db/add :db/tx :db/txInstant (+ txInstantA 1)]]))]
              (is (= txInstantB (+ txInstantA 1)))
              (is (= (<? (<transactions-after (d/db conn) txa))
                     [[txb :db/txInstant txInstantB txb 1]]))

              (testing ":db/txInstant can be set explicitly, with additional datoms"
                (let [{txc :tx txInstantC :txInstant} (<? (d/<transact! conn [[:db/add :db/tx :db/txInstant (+ txInstantB 2)]
                                                                              [:db/add :db/tx :x 123]]))]
                  (is (= txInstantC (+ txInstantB 2)))
                  (is (= (<? (<transactions-after (d/db conn) txb))
                         [[txc :db/txInstant txInstantC txc 1]
                          [txc :x 123 txc 1]]))

                  (testing "additional datoms can be added, without :db/txInstant explicitly"
                    (let [{txd :tx txInstantD :txInstant} (<? (d/<transact! conn [[:db/add :db/tx :x 456]]))]
                      (is (= (<? (<transactions-after (d/db conn) txc))
                             [[txd :db/txInstant txInstantD txd 1]
                              [txd :x 456 txd 1]])))))))))

        (finally
          (<? (d/<close conn)))))))

(deftest-async test-no-tx
  (with-tempfile [t (tempfile)]
    (let [conn      (<? (d/<connect t))
          {tx0 :tx} (<? (d/<transact! conn test-schema))]
      (try
        (testing "Cannot specificy an explicit tx"
          (is (thrown-with-msg?
                ExceptionInfo #"Bad entity: too long"
                (<? (d/<transact! conn [[:db/add (d/id-literal :db.part/user) :x 0 10101]])))))

        (finally
          (<? (d/<close conn)))))))

(deftest-async test-explode-sequences
  (with-tempfile [t (tempfile)]
    (let [conn      (<? (d/<connect t))
          {tx0 :tx} (<? (d/<transact! conn test-schema))]
      (try
        (testing ":db.cardinality/many sequences are accepted"
          (<? (d/<transact! conn [{:db/id 101 :aka ["first" "second"]}]))
          (is (= (<? (<datoms-after (d/db conn) tx0))
                 #{[101 :aka "first"]
                   [101 :aka "second"]})))

        (testing ":db.cardinality/many sequences are recursively applied, allowing unexpected sequence nesting"
          (<? (d/<transact! conn [{:db/id 102 :aka [[["first"]] ["second"]]}]))
          (is (= (<? (<datoms-after (d/db conn) tx0))
                 #{[101 :aka "first"]
                   [101 :aka "second"]
                   [102 :aka "first"]
                   [102 :aka "second"]})))

        (testing ":db.cardinality/one sequences fail"
          (is (thrown-with-msg?
                ExceptionInfo #"Sequential values"
                (<? (d/<transact! conn [{:db/id 101 :email ["@1" "@2"]}])))))

        (finally
          (<? (d/<close conn)))))))

(deftest-async test-explode-maps
  (with-tempfile [t (tempfile)]
    (let [conn      (<? (d/<connect t))
          {tx0 :tx} (<? (d/<transact! conn test-schema))]
      (try
        (testing "nested maps are accepted"
          (<? (d/<transact! conn [{:db/id 101 :friends {:name "Petr"}}]))
          ;; TODO: this works only because we have a single friend.
          (let [{petr :friends} (<? (<shallow-entity (d/db conn) 101))]
            (is (= (<? (<datoms-after (d/db conn) tx0))
                   #{[101 :friends petr]
                     [petr :name "Petr"]}))))

        (testing "recursively nested maps are accepted"
          (<? (d/<transact! conn [{:db/id 102 :friends {:name "Ivan" :friends {:name "Petr"}}}]))
          ;; This would be much easier with `entity` and lookup refs.
          (let [{ivan :friends} (<? (<shallow-entity (d/db conn) 102))
                {petr :friends} (<? (<shallow-entity (d/db conn) ivan))]
            (is (= (<? (<datoms-after (d/db conn) tx0))
                   #{[101 :friends petr]
                     [petr :name "Petr"]
                     [102 :friends ivan]
                     [ivan :name "Ivan"]
                     [ivan :friends petr]}))))

        (testing "nested maps without :db.type/ref fail"
          (is (thrown-with-msg?
                ExceptionInfo #"\{:db/valueType :db.type/ref\}"
                (<? (d/<transact! conn [{:db/id 101 :aka {:name "Petr"}}])))))

        (finally
          (<? (d/<close conn)))))))

(deftest-async test-explode-reverse-refs
  (with-tempfile [t (tempfile)]
    (let [conn      (<? (d/<connect t))
          {tx0 :tx} (<? (d/<transact! conn test-schema))]
      (try
        (testing "reverse refs are accepted"
          (<? (d/<transact! conn [{:db/id 101 :name "Igor"}]))
          (<? (d/<transact! conn [{:db/id 102 :name "Oleg" :_friends 101}]))
          (is (= (<? (<datoms-after (d/db conn) tx0))
                 #{[101 :name "Igor"]
                   [102 :name "Oleg"]
                   [101 :friends 102]})))

        (testing "reverse refs without :db.type/ref fail"
          (is (thrown-with-msg?
                ExceptionInfo #"\{:db/valueType :db.type/ref\}"
                (<? (d/<transact! conn [{:db/id 101 :_aka 102}])))))

        (finally
          (<? (d/<close conn)))))))
