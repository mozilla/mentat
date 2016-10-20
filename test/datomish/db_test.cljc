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
   [datomish.db.debug :refer [<datoms-after <datoms>= <transactions-after <shallow-entity <fulltext-values]]
   [datomish.util :as util #?(:cljs :refer-macros :clj :refer) [raise cond-let]]
   [datomish.schema :as ds]
   [datomish.simple-schema]
   [datomish.sqlite :as s]
   [datomish.sqlite-schema]
   [datomish.datom]
   #?@(:clj [[datomish.jdbc-sqlite]
             [datomish.pair-chan :refer [go-pair <?]]
             [tempfile.core :refer [tempfile with-tempfile]]
             [datomish.test-macros :refer [deftest-async deftest-db]]
             [clojure.test :as t :refer [is are deftest testing]]
             [clojure.core.async :refer [go <! >!]]])
   #?@(:cljs [[datomish.js-sqlite]
              [datomish.pair-chan]
              [datomish.test-macros :refer-macros [deftest-async deftest-db]]
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

(def test-schema
  [{:db/id        (d/id-literal :db.part/user)
    :db/ident     :x
    :db/unique    :db.unique/identity
    :db/valueType :db.type/long
    :db.install/_attribute :db.part/db}
   {:db/id        (d/id-literal :db.part/user)
    :db/ident     :name
    :db/unique    :db.unique/identity
    :db/valueType :db.type/string
    :db.install/_attribute :db.part/db}
   {:db/id          (d/id-literal :db.part/user)
    :db/ident       :y
    :db/cardinality :db.cardinality/many
    :db/valueType   :db.type/long
    :db.install/_attribute :db.part/db}
   {:db/id          (d/id-literal :db.part/user)
    :db/ident       :aka
    :db/cardinality :db.cardinality/many
    :db/valueType   :db.type/string
    :db.install/_attribute :db.part/db}
   {:db/id        (d/id-literal :db.part/user)
    :db/ident     :age
    :db/valueType :db.type/long
    :db.install/_attribute :db.part/db}
   {:db/id        (d/id-literal :db.part/user)
    :db/ident     :email
    :db/unique    :db.unique/identity
    :db/valueType :db.type/string
    :db.install/_attribute :db.part/db}
   {:db/id        (d/id-literal :db.part/user)
    :db/ident     :spouse
    :db/unique    :db.unique/value
    :db/valueType :db.type/string
    :db.install/_attribute :db.part/db}
   {:db/id          (d/id-literal :db.part/user)
    :db/ident       :friends
    :db/cardinality :db.cardinality/many
    :db/valueType   :db.type/ref
    :db.install/_attribute :db.part/db}
   ])

(deftest-db test-add-one conn
  (let [{tx0 :tx} (<? (d/<transact! conn test-schema))]
    (let [{:keys [tx txInstant]} (<? (d/<transact! conn [[:db/add 0 :name "valuex"]]))]
      (is (= (<? (<datoms-after (d/db conn) tx0))
             #{[0 :name "valuex"]}))
      (is (= (<? (<transactions-after (d/db conn) tx0))
             [[0 :name "valuex" tx 1] ;; TODO: true, not 1.
              [tx :db/txInstant txInstant tx 1]])))))

(deftest-db test-add-two conn
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
            [tx4 :db/txInstant txInstant4 tx4 1]]))))

(deftest-db test-retract conn
  (let [{tx0 :tx} (<? (d/<transact! conn test-schema))
        {tx1 :tx txInstant1 :txInstant} (<? (d/<transact! conn [[:db/add     0 :x 123]]))
        {tx2 :tx txInstant2 :txInstant} (<? (d/<transact! conn [[:db/retract 0 :x 123]]))]
    (is (= (<? (<datoms-after (d/db conn) tx0))
           #{}))
    (is (= (<? (<transactions-after (d/db conn) tx0))
           [[0 :x 123 tx1 1]
            [tx1 :db/txInstant txInstant1 tx1 1]
            [0 :x 123 tx2 0]
            [tx2 :db/txInstant txInstant2 tx2 1]]))))

(deftest-db test-id-literal-1 conn
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
               [eid2 :y 3]})))))

(deftest-db test-unique conn
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
                                    [:db/add (d/id-literal :db.part/user -2) :spouse "Dana"]])))))))

(deftest-db test-valueType-keyword conn
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
               #{}))))))

(deftest-db test-vector-upsert conn
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
                                                    [:db/add (d/id-literal :db.part/user -1) :age 36]])))))))

(deftest-db test-multistep-upsert conn
  (<? (d/<transact! conn test-schema))
  ;; The upsert algorithm will first try to resolve -1, fail, and then allocate both -1 and -2.
  (let [tx0 (<? (d/<transact! conn [{:db/id (d/id-literal :db.part/user -1) :name "Ivan" :email "@1"}
                                    {:db/id (d/id-literal :db.part/user -2) :name "Petr" :friends (d/id-literal :db.part/user -1)}]))]

    ;; Sanity checks that these are freshly allocated, not resolved.
    (is (> (get (tempids tx0) -1) 1000))
    (is (> (get (tempids tx0) -1) 1000))

    ;; This time, we can resolve both, but we have to try -1, succeed, and then resolve -2.
    (let [tx1 (<? (d/<transact! conn [{:db/id (d/id-literal :db.part/user -1) :name "Ivan" :email "@1"}
                                      {:db/id (d/id-literal :db.part/user -2) :name "Petr" :friends (d/id-literal :db.part/user -1)}]))]

      ;; Ensure these are resolved, not freshly allocated.
      (is (= (tempids tx0)
             (tempids tx1))))))

(deftest-db test-map-upsert conn
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
                                                    {:db/id (d/id-literal :db.part/user -2) :name "Ivan" :age 36}])))))))

(deftest-db test-map-upsert-conflicts conn
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
               {-1 101}))))))

(deftest-db test-add-schema conn
  (let [es       [[:db/add :db.part/db :db.install/attribute (d/id-literal :db.part/db -1)]
                  {:db/id (d/id-literal :db.part/db -1)
                   :db/ident :test/attr
                   :db/valueType :db.type/string
                   :db/cardinality :db.cardinality/one}]
        report   (<? (d/<transact! conn es))
        db-after (:db-after report)
        tx       (:tx db-after)]

    (testing "New ident is allocated"
      (is (some? (d/entid db-after :test/attr))))

    (testing "Schema is modified"
      (is (= (get-in db-after [:symbolic-schema :test/attr])
             {:db/valueType :db.type/string,
              :db/cardinality :db.cardinality/one})))

    (testing "Schema is used in subsequent transaction"
      (<? (d/<transact! conn [{:db/id 100 :test/attr "value 1"}]))
      (<? (d/<transact! conn [{:db/id 100 :test/attr "value 2"}]))
      (is (= (<? (<shallow-entity (d/db conn) 100))
             {:test/attr "value 2"})))))

(deftest-db test-fulltext conn
  (let [schema [{:db/id (d/id-literal :db.part/db -1)
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
    (testing "Schema checks"
      (is (ds/fulltext? (d/schema (d/db conn))
                        (d/entid (d/db conn) :test/fulltext))))

    (testing "Can add fulltext indexed datoms"
      (let [{tx1 :tx txInstant1 :txInstant}
            (<? (d/<transact! conn [[:db/add 101 :test/fulltext "test this"]]))]
        (is (= (<? (<fulltext-values (d/db conn)))
               [[1 "test this"]]))
        (is (= (<? (<datoms-after (d/db conn) tx0))
               #{[101 :test/fulltext 1]})) ;; Values are raw; 1 is the rowid into fulltext_values.
        (is (= (<? (<transactions-after (d/db conn) tx0))
               [[101 :test/fulltext 1 tx1 1] ;; Values are raw; 1 is the rowid into fulltext_values.
                [tx1 :db/txInstant txInstant1 tx1 1]]))

        (testing "Can replace fulltext indexed datoms"
          (let [{tx2 :tx txInstant2 :txInstant} (<? (d/<transact! conn [[:db/add 101 :test/fulltext "alternate thing"]]))]
            (is (= (<? (<fulltext-values (d/db conn)))
                   [[1 "test this"]
                    [2 "alternate thing"]]))
            (is (= (<? (<datoms-after (d/db conn) tx0))
                   #{[101 :test/fulltext 2]})) ;; Values are raw; 2 is the rowid into fulltext_values.
            (is (= (<? (<transactions-after (d/db conn) tx0))
                   [[101 :test/fulltext 1 tx1 1] ;; Values are raw; 1 is the rowid into fulltext_values.
                    [tx1 :db/txInstant txInstant1 tx1 1]
                    [101 :test/fulltext 1 tx2 0] ;; Values are raw; 1 is the rowid into fulltext_values.
                    [101 :test/fulltext 2 tx2 1] ;; Values are raw; 2 is the rowid into fulltext_values.
                    [tx2 :db/txInstant txInstant2 tx2 1]]))

            (testing "Can upsert keyed by fulltext indexed datoms"
              (let [{tx3 :tx txInstant3 :txInstant} (<? (d/<transact! conn [{:db/id (d/id-literal :db.part/user) :test/fulltext "alternate thing" :test/other "other"}]))]
                (is (= (<? (<fulltext-values (d/db conn)))
                       [[1 "test this"]
                        [2 "alternate thing"]
                        [3 "other"]]))
                (is (= (<? (<datoms-after (d/db conn) tx0))
                       #{[101 :test/fulltext 2] ;; Values are raw; 2, 3 are the rowids into fulltext_values.
                         [101 :test/other 3]}))
                (is (= (<? (<transactions-after (d/db conn) tx0))
                       [[101 :test/fulltext 1 tx1 1] ;; Values are raw; 1 is the rowid into fulltext_values.
                        [tx1 :db/txInstant txInstant1 tx1 1]
                        [101 :test/fulltext 1 tx2 0] ;; Values are raw; 1 is the rowid into fulltext_values.
                        [101 :test/fulltext 2 tx2 1] ;; Values are raw; 2 is the rowid into fulltext_values.
                        [tx2 :db/txInstant txInstant2 tx2 1]
                        [101 :test/other 3 tx3 1] ;; Values are raw; 3 is the rowid into fulltext_values.
                        [tx3 :db/txInstant txInstant3 tx3 1]]))

                ))))))

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
        ))))

(deftest-db test-txInstant conn
  (let [{tx0 :tx} (<? (d/<transact! conn test-schema))
        {txa :tx txInstantA :txInstant} (<? (d/<transact! conn []))]
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
                        [txd :x 456 txd 1]]))))))))))

(deftest-db test-no-tx conn
  (let [{tx0 :tx} (<? (d/<transact! conn test-schema))]
    (testing "Cannot specificy an explicit tx"
      (is (thrown-with-msg?
            ExceptionInfo #"Bad entity: too long"
            (<? (d/<transact! conn [[:db/add (d/id-literal :db.part/user) :x 0 10101]])))))))

(deftest-db test-explode-sequences conn
  (let [{tx0 :tx} (<? (d/<transact! conn test-schema))]
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
            (<? (d/<transact! conn [{:db/id 101 :email ["@1" "@2"]}])))))))

(deftest-db test-explode-maps conn
  (let [{tx0 :tx} (<? (d/<transact! conn test-schema))]
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
            (<? (d/<transact! conn [{:db/id 101 :aka {:name "Petr"}}])))))))

(deftest-db test-explode-reverse-refs conn
  (let [{tx0 :tx} (<? (d/<transact! conn test-schema))]
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
            (<? (d/<transact! conn [{:db/id 101 :_aka 102}])))))))

;; We don't use deftest-db in order to be able to re-open an on disk file.
(deftest-async test-next-eid
  (with-tempfile [t (tempfile)]
    (let [conn      (<? (d/<connect t))
          {tx0 :tx} (<? (d/<transact! conn test-schema))]
      (testing "entids are increasing, tx ids are larger than user ids"
        (let [r1 (<? (d/<transact! conn [{:db/id (d/id-literal :db.part/user -1) :name "Igor"}]))
              r2 (<? (d/<transact! conn [{:db/id (d/id-literal :db.part/user -2) :name "Oleg"}]))
              e1 (get (tempids r1) -1)
              e2 (get (tempids r2) -2)]
          (is (< e1 (:tx r1)))
          (is (< e2 (:tx r2)))
          (is (< e1 e2))
          (is (< (:tx r1) (:tx r2)))

          ;; Close and re-open same DB.
          (<? (d/<close conn))
          (let [conn (<? (d/<connect t))]
            (try
              (testing "entid counters are persisted across re-opens"
                (let [r3 (<? (d/<transact! conn [{:db/id (d/id-literal :db.part/user -3) :name "Petr"}]))
                      e3 (get (tempids r3) -3)]
                  (is (< e3 (:tx r3)))
                  (is (< e2 e3))
                  (is (< (:tx r2) (:tx r3)))))

              (finally
                (<? (d/<close conn))))))))))

(deftest-db test-unique-value conn
  (let [tx0 (:tx (<? (d/<transact! conn [{:db/id                 (d/id-literal :db.part/user -1)
                                          :db/ident              :test/x
                                          :db/unique             :db.unique/value
                                          :db/valueType          :db.type/long
                                          :db.install/_attribute :db.part/db}
                                         {:db/id                 (d/id-literal :db.part/user -2)
                                          :db/ident              :test/y
                                          :db/unique             :db.unique/value
                                          :db/valueType          :db.type/long
                                          :db.install/_attribute :db.part/db}])))]

    (testing "can insert different :db.unique/value attributes with the same value"
      (let [report1 (<? (d/<transact! conn [[:db/add (d/id-literal :db.part/user -1) :test/x 12345]]))
            eid1    (get-in report1 [:tempids (d/id-literal :db.part/user -1)])
            report2 (<? (d/<transact! conn [[:db/add (d/id-literal :db.part/user -2) :test/y 12345]]))
            eid2    (get-in report2 [:tempids (d/id-literal :db.part/user -2)])]
        (is (= (<? (<datoms-after (d/db conn) tx0))
               #{[eid1 :test/x 12345]
                 [eid2 :test/y 12345]}))))

    (testing "can't upsert a :db.unique/value field"
      (is (thrown-with-msg?
            ExceptionInfo #"unique constraint"
            (<? (d/<transact! conn [{:db/id (d/id-literal :db.part/user -1) :test/x 12345 :test/y 99999}])))))))

(def retract-schema
  [{:db/id                 (d/id-literal :db.part/user -1)
    :db/ident              :test/long
    :db/cardinality        :db.cardinality/many
    :db/valueType          :db.type/long
    :db.install/_attribute :db.part/db}
   {:db/id                 (d/id-literal :db.part/user -2)
    :db/ident              :test/fulltext
    :db/cardinality        :db.cardinality/many
    :db/valueType          :db.type/string
    :db/fulltext           true
    :db.install/_attribute :db.part/db}
   {:db/id                 (d/id-literal :db.part/user -3)
    :db/ident              :test/ref
    :db/valueType          :db.type/ref
    :db.install/_attribute :db.part/db}])

(deftest-db test-retract-attribute conn
  (let [tx0 (:tx (<? (d/<transact! conn retract-schema)))]

    (testing "retractAttribute"
      (let [report (<? (d/<transact! conn [{:db/id (d/id-literal :db.part/user -1) :test/long [12345 123456]}
                                           {:db/id (d/id-literal :db.part/user -2) :test/ref (d/id-literal :db.part/user -1)}]))
            eid1    (get-in report [:tempids (d/id-literal :db.part/user -1)])
            eid2    (get-in report [:tempids (d/id-literal :db.part/user -2)])]
        (is (= (<? (<datoms-after (d/db conn) tx0))
               #{[eid1 :test/long 12345]
                 [eid1 :test/long 123456]
                 [eid2 :test/ref eid1]}))

        (testing "retractAttribute with no matching datoms succeeds"
          (<? (d/<transact! conn [[:db.fn/retractAttribute eid1 :test/ref]]))
          (is (= (<? (<datoms-after (d/db conn) tx0))
                 #{[eid1 :test/long 12345]
                   [eid1 :test/long 123456]
                   [eid2 :test/ref eid1]})))

        (testing "retractAttribute retracts datoms"
          (let [{tx1 :tx} (<? (d/<transact! conn [[:db.fn/retractAttribute eid2 :test/ref]]))]
            (is (= (<? (<datoms-after (d/db conn) tx0))
                   #{[eid1 :test/long 12345]
                     [eid1 :test/long 123456]})))

          (let [{tx2 :tx} (<? (d/<transact! conn [[:db.fn/retractAttribute eid1 :test/long]]))]
            (is (= (<? (<datoms-after (d/db conn) tx0))
                   #{}))))))))

(deftest-db test-retract-attribute-multiple conn
  (let [tx0 (:tx (<? (d/<transact! conn retract-schema)))]

    (let [report (<? (d/<transact! conn [{:db/id (d/id-literal :db.part/user -1) :test/long [12345 123456]}
                                         {:db/id (d/id-literal :db.part/user -1) :test/fulltext ["1 fulltext value" "2 fulltext value"]}]))
          eid1   (get-in report [:tempids (d/id-literal :db.part/user -1)])]
      (is (= (<? (<fulltext-values (d/db conn)))
             [[1 "1 fulltext value"]
              [2 "2 fulltext value"]]))
      (is (= (<? (<datoms-after (d/db conn) tx0))
             #{[eid1 :test/fulltext 1]
               [eid1 :test/fulltext 2]
               [eid1 :test/long 12345]
               [eid1 :test/long 123456]}))

      (testing "multiple retractAttribute in one transaction"
        (let [{tx1 :tx} (<? (d/<transact! conn [[:db.fn/retractAttribute eid1 :test/long]
                                                [:db.fn/retractAttribute eid1 :test/fulltext]]))]
          (is (= (<? (<datoms-after (d/db conn) tx0))
                 #{})))))))

(deftest-db test-retract-attribute-fulltext conn
  (let [tx0 (:tx (<? (d/<transact! conn retract-schema)))]

    (testing "retractAttribute, fulltext"
      (let [report (<? (d/<transact! conn [{:db/id (d/id-literal :db.part/user -1) :test/fulltext ["1 fulltext value" "2 fulltext value"]}
                                           {:db/id (d/id-literal :db.part/user -2) :test/ref (d/id-literal :db.part/user -1)}]))
            eid1    (get-in report [:tempids (d/id-literal :db.part/user -1)])
            eid2    (get-in report [:tempids (d/id-literal :db.part/user -2)])]
        (is (= (<? (<fulltext-values (d/db conn)))
               [[1 "1 fulltext value"]
                [2 "2 fulltext value"]]))
        (is (= (<? (<datoms-after (d/db conn) tx0))
               #{[eid1 :test/fulltext 1]
                 [eid1 :test/fulltext 2]
                 [eid2 :test/ref eid1]}))

        (testing "retractAttribute retracts datoms, fulltext"
          (let [{tx1 :tx} (<? (d/<transact! conn [[:db.fn/retractAttribute eid2 :test/ref]]))]
            ;; fulltext values are not purged.
            (is (= (<? (<fulltext-values (d/db conn)))
                   [[1 "1 fulltext value"]
                    [2 "2 fulltext value"]]))
            (is (= (<? (<datoms-after (d/db conn) tx0))
                   #{[eid1 :test/fulltext 1]
                     [eid1 :test/fulltext 2]})))

          (let [{tx2 :tx} (<? (d/<transact! conn [[:db.fn/retractAttribute eid1 :test/fulltext]]))]
            ;; fulltext values are not purged.
            (is (= (<? (<fulltext-values (d/db conn)))
                   [[1 "1 fulltext value"]
                    [2 "2 fulltext value"]]))
            (is (= (<? (<datoms-after (d/db conn) tx0))
                   #{}))))))))

(deftest-db test-retract-entity conn
  (let [tx0 (:tx (<? (d/<transact! conn retract-schema)))]

    (testing "retractEntity"
      (let [report (<? (d/<transact! conn [{:db/id (d/id-literal :db.part/user -1) :test/long [12345 123456]}
                                           {:db/id (d/id-literal :db.part/user -2) :test/ref (d/id-literal :db.part/user -1)}
                                           {:db/id (d/id-literal :db.part/user -3) :test/long 0xdeadbeef}]))
            eid1    (get-in report [:tempids (d/id-literal :db.part/user -1)])
            eid2    (get-in report [:tempids (d/id-literal :db.part/user -2)])
            eid3    (get-in report [:tempids (d/id-literal :db.part/user -3)])]
        (is (= (<? (<datoms-after (d/db conn) tx0))
               #{[eid1 :test/long 12345]
                 [eid1 :test/long 123456]
                 [eid2 :test/ref eid1]
                 [eid3 :test/long 0xdeadbeef]}))

        (testing "retractEntity with no matching datoms succeeds"
          (<? (d/<transact! conn [[:db.fn/retractEntity 0xdeadbeef]]))
          (is (= (<? (<datoms-after (d/db conn) tx0))
                 #{[eid1 :test/long 12345]
                   [eid1 :test/long 123456]
                   [eid2 :test/ref eid1]
                   [eid3 :test/long 0xdeadbeef]})))

        (testing "retractEntity retracts datoms"
          (let [{tx1 :tx} (<? (d/<transact! conn [[:db.fn/retractEntity eid3]]))]
            (is (= (<? (<datoms-after (d/db conn) tx0))
                   #{[eid1 :test/long 12345]
                     [eid1 :test/long 123456]
                     [eid2 :test/ref eid1]}))))

        (testing "retractEntity retracts datoms and references"
          (let [{tx2 :tx} (<? (d/<transact! conn [[:db.fn/retractEntity eid1]]))]
            (is (= (<? (<datoms-after (d/db conn) tx0))
                   ;; [eid2 :test/ref eid1] is gone, since the ref eid1 is gone.
                   #{}))))))))

(deftest-db test-retract-entity-multiple conn
  (let [tx0 (:tx (<? (d/<transact! conn retract-schema)))]

    (let [report (<? (d/<transact! conn [{:db/id (d/id-literal :db.part/user -1) :test/long [12345 123456]}
                                         {:db/id (d/id-literal :db.part/user -2) :test/fulltext ["1 fulltext value" "2 fulltext value"]}]))
          eid1   (get-in report [:tempids (d/id-literal :db.part/user -1)])
          eid2   (get-in report [:tempids (d/id-literal :db.part/user -2)])]
      (is (= (<? (<fulltext-values (d/db conn)))
             [[1 "1 fulltext value"]
              [2 "2 fulltext value"]]))
      (is (= (<? (<datoms-after (d/db conn) tx0))
             #{[eid2 :test/fulltext 1]
               [eid2 :test/fulltext 2]
               [eid1 :test/long 12345]
               [eid1 :test/long 123456]}))

      (testing "multiple retractEntity in one transaction"
        (let [{tx1 :tx} (<? (d/<transact! conn [[:db.fn/retractEntity eid1]
                                                [:db.fn/retractEntity eid2]]))]
          (is (= (<? (<datoms-after (d/db conn) tx0))
                 #{})))))))

(deftest-db test-retract-entity-fulltext conn
  (let [tx0 (:tx (<? (d/<transact! conn retract-schema)))]

    (testing "retractEntity, fulltext"
      (let [report (<? (d/<transact! conn [{:db/id (d/id-literal :db.part/user -1) :test/fulltext ["1 fulltext value" "2 fulltext value"]}
                                           {:db/id (d/id-literal :db.part/user -2) :test/ref (d/id-literal :db.part/user -1)}
                                           {:db/id (d/id-literal :db.part/user -3) :test/fulltext "3 fulltext value"}]))
            eid1    (get-in report [:tempids (d/id-literal :db.part/user -1)])
            eid2    (get-in report [:tempids (d/id-literal :db.part/user -2)])
            eid3    (get-in report [:tempids (d/id-literal :db.part/user -3)])]
        (is (= (<? (<fulltext-values (d/db conn)))
               [[1 "1 fulltext value"]
                [2 "2 fulltext value"]
                [3 "3 fulltext value"]]))
        (is (= (<? (<datoms-after (d/db conn) tx0))
               #{[eid1 :test/fulltext 1]
                 [eid1 :test/fulltext 2]
                 [eid2 :test/ref eid1]
                 [eid3 :test/fulltext 3]}))

        (testing "retractEntity with no matching datoms succeeds, fulltext"
          (<? (d/<transact! conn [[:db.fn/retractEntity 0xdeadbeef]]))
          (is (= (<? (<datoms-after (d/db conn) tx0))
                 #{[eid1 :test/fulltext 1]
                   [eid1 :test/fulltext 2]
                   [eid2 :test/ref eid1]
                   [eid3 :test/fulltext 3]})))

        (testing "retractEntity retracts datoms, fulltext"
          (let [{tx1 :tx} (<? (d/<transact! conn [[:db.fn/retractEntity eid3]]))]
            ;; fulltext values are not purged.
            (is (= (<? (<fulltext-values (d/db conn)))
                   [[1 "1 fulltext value"]
                    [2 "2 fulltext value"]
                    [3 "3 fulltext value"]]))
            (is (= (<? (<datoms-after (d/db conn) tx0))
                   #{[eid1 :test/fulltext 1]
                     [eid1 :test/fulltext 2]
                     [eid2 :test/ref eid1]}))))

        (testing "retractEntity retracts datoms and references, fulltext"
          (let [{tx2 :tx} (<? (d/<transact! conn [[:db.fn/retractEntity eid1]]))]
            (is (= (<? (<datoms-after (d/db conn) tx0))
                   ;; [eid2 :test/ref eid1] is gone, since the ref eid1 is gone.
                   #{}))))))))

;; We don't use deftest-db in order to be able to re-open an on disk file.
(deftest-async test-reopen-schema
  (with-tempfile [t (tempfile)]
    (let [conn        (<? (d/<connect t))
          test-schema [{:db/id                 (d/id-literal :db.part/user -1)
                        :db/ident              :test/fulltext
                        :db/cardinality        :db.cardinality/many
                        :db/valueType          :db.type/string
                        :db/fulltext           true
                        :db/doc                "Documentation string"
                        :db.install/_attribute :db.part/db}]
          {tx0 :tx}   (<? (d/<transact! conn test-schema))]
      (testing "Values in schema are correct initially"
        (let [db     (d/db conn)
              schema (d/schema db)]
          (is (= true (ds/indexing? schema (d/entid db :db/txInstant))))
          (is (= true (ds/fulltext? schema (d/entid db :test/fulltext))))
          (is (= "Documentation string" (ds/doc schema (d/entid db :test/fulltext))))
          (is (= :db.type/string (ds/valueType schema (d/entid db :test/fulltext))))))

      ;; Close and re-open same DB.
      (<? (d/<close conn))
      (let [conn (<? (d/<connect t))]
        (try
          (testing "Boolean values in schema are correct after re-opening"
            (let [db     (d/db conn)
                  schema (d/schema db)]
              (is (= true (ds/indexing? schema (d/entid db :db/txInstant))))
              (is (= true (ds/fulltext? schema (d/entid db :test/fulltext))))
              (is (= "Documentation string" (ds/doc schema (d/entid db :test/fulltext))))
              (is (= :db.type/string (ds/valueType schema (d/entid db :test/fulltext))))))

          (finally
            (<? (d/<close conn))))))))

(deftest-db test-simple-schema conn
  (let [in {:name "mystuff"
            :attributes [{:name "foo/age"
                          :type "long"
                          :cardinality "one"}
                         {:name "foo/name"
                          :type "string"
                          :cardinality "many"
                          :doc "People can have many names."}
                         {:name "foo/id"
                          :type "string"
                          :cardinality "one"
                          :unique "value"}]}
        expected [{:db/ident :foo/age
                   :db/valueType :db.type/long
                   :db/cardinality :db.cardinality/one
                   :db.install/_attribute :db.part/db}
                  {:db/ident :foo/name
                   :db/valueType :db.type/string
                   :db/cardinality :db.cardinality/many
                   :db/doc "People can have many names."
                   :db.install/_attribute :db.part/db}
                  {:db/ident :foo/id
                   :db/valueType :db.type/string
                   :db/cardinality :db.cardinality/one
                   :db/unique :db.unique/value
                   :db.install/_attribute :db.part/db}]]

    (testing "Simple schemas are expanded."
      (is (= (map #(dissoc %1 :db/id) (datomish.simple-schema/simple-schema->schema in))
             expected)))))

(deftest-db test-lookup-refs conn
  (let [{tx0 :tx} (<? (d/<transact! conn test-schema))
        {tx1 :tx} (<? (d/<transact! conn [[:db/add 1 :name "Ivan"]
                                          [:db/add 2 :name "Phil"]
                                          [:db/add 3 :name "Petr"]]))]
    (testing "Looks up entity refs"
      (let [{tx :tx} (<? (d/<transact! conn [[:db/add (d/lookup-ref :name "Ivan") :aka "Devil"]
                                             [:db/add (d/lookup-ref :name "Phil") :email "@1"]]))]
        (is (= #{[1 :name "Ivan"]
                 [2 :name "Phil"]
                 [3 :name "Petr"]
                 [1 :aka "Devil"]
                 [2 :email "@1"]}
               (<? (<datoms>= (d/db conn) tx1))))))

    (testing "Looks up value refs"
      (let [{tx :tx} (<? (d/<transact! conn [[:db/add 1 :friends (d/lookup-ref :name "Petr")]
                                             [:db/add 3 :friends (d/lookup-ref :name "Ivan")]]))]
        (is (= #{[1 :friends 3]
                 [3 :friends 1]}
               (<? (<datoms>= (d/db conn) tx))))))

    (testing "Looks up entity refs in maps"
      (let [{tx :tx} (<? (d/<transact! conn [{:db/id (d/lookup-ref :name "Phil") :friends 1}]))]
        (is (= #{[2 :friends 1]}
               (<? (<datoms>= (d/db conn) tx))))))

    (testing "Looks up value refs in maps"
      (let [{tx :tx} (<? (d/<transact! conn [{:db/id 2 :friends (d/lookup-ref :name "Petr")}]))]
        (is (=  #{[2 :friends 3]}
                (<? (<datoms>= (d/db conn) tx))))))

    (testing "Looks up value refs in sequences in maps"
      (let [{tx :tx} (<? (d/<transact! conn [{:db/id 1 :friends [(d/lookup-ref :name "Ivan") (d/lookup-ref :name "Phil")]}]))]
        (is (= #{[1 :friends 1]
                 [1 :friends 2]}
               (<? (<datoms>= (d/db conn) tx))))))

    (testing "Looks up refs when there are more than 999 refs (all present)"
      (let
          [bound (* 999 2)
           make-add #(vector :db/add (+ 1000 %) :name (str "Ivan-" %))
           make-ref #(-> {:db/id (d/lookup-ref :name (str "Ivan-" %)) :email (str "Ivan-" % "@" %)})
           {tx-data1 :tx-data} (<? (d/<transact! conn (map make-add (range bound))))
           {tx-data2 :tx-data} (<? (d/<transact! conn (map make-ref (range bound))))]
        (is (= bound (dec (count tx-data1)))) ;; Each :name is new; dec to account for :db/tx.
        (is (= bound (dec (count tx-data2)))) ;; Each lookup-ref exists, each :email is new; dec for :db/tx.
        ))

    (testing "Fails for missing entities"
      (is (thrown-with-msg?
            ExceptionInfo #"No entity found for lookup-ref"
            (<? (d/<transact! conn [[:db/add (d/lookup-ref :name "Mysterioso") :aka "The Magician"]]))))
      (is (thrown-with-msg?
            ExceptionInfo #"No entity found for lookup-ref"
            (<? (d/<transact! conn [[:db/add 1 :friends (d/lookup-ref :name "Mysterioso")]])))))

    (testing "Fails for non-identity attributes"
      (is (thrown-with-msg?
            ExceptionInfo #"Lookup-ref found with non-unique-identity attribute"
            (<? (d/<transact! conn [[:db/add (d/lookup-ref :aka "The Magician") :email "@2"]]))))
      (is (thrown-with-msg?
            ExceptionInfo #"Lookup-ref found with non-unique-identity attribute"
            (<? (d/<transact! conn [[:db/add 1 :friends (d/lookup-ref :aka "The Magician")]])))))))

(deftest-db test-fulltext-lookup-refs conn
  (let [schema [{:db/id (d/id-literal :db.part/db -1)
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

    (testing "Can look up fulltext refs"
      (<? (d/<transact! conn [[:db/add 101 :test/fulltext "test this"]]))

      (let [{tx :tx} (<? (d/<transact! conn [{:db/id (d/lookup-ref :test/fulltext "test this") :test/other "test other"}]))]
        (is (= (<? (<fulltext-values (d/db conn)))
               [[1 "test this"]
                [2 "test other"]]))
        (is (= #{[101 :test/other 2]} ;; Values are raw; 2 is the rowid into fulltext_values.
               (<? (<datoms>= (d/db conn) tx))))))

    (testing "Fails for missing fulltext entities"
      (is (thrown-with-msg?
            ExceptionInfo #"No entity found for lookup-ref"
            (<? (d/<transact! conn [[:db/add (d/lookup-ref :test/fulltext "not found") :test/other "test random"]])))))))

#_ (time (t/run-tests))

#_ (time (clojure.test/test-vars [#'test-lookup-refs]))
