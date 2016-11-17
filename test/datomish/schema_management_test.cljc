;; This Source Code Form is subject to the terms of the Mozilla Public
;; License, v. 2.0. If a copy of the MPL was not distributed with this
;; file, You can obtain one at http://mozilla.org/MPL/2.0/.

(ns datomish.schema-management-test
  #?(:cljs
     (:require-macros
      [datomish.pair-chan :refer [go-pair <?]]
      [datomish.node-tempfile-macros :refer [with-tempfile]]
      [cljs.core.async.macros :as a :refer [go]]))
  (:require
   [datomish.schema-management :as sm]
   [datomish.api :as d]
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

(def id-schema (d/id-literal :db.part/db))
(def id-foobar (d/id-literal :db.part/user))

(def trivial-schema-managed-fragment-v1
  {:name :com.example.foo
   :version 1
   :attributes
   {:foo/bar
    {:db/cardinality :db.cardinality/one
     :db/valueType :db.type/string}}})

(def additional-schema-managed-fragment-v7
  {:name :com.example.bar
   :version 7
   :attributes
   {:bar/noo
    {:db/cardinality :db.cardinality/one
     :db/unique :db.unique/value
     :db/valueType :db.type/long}}})

(def additional-schema-managed-fragment-v8
  {:name :com.example.bar
   :version 8
   :attributes
   {:bar/choo
    {:db/cardinality :db.cardinality/many
     :db/fulltext true
     :db/valueType :db.type/string}
    :bar/noo
    {:db/cardinality :db.cardinality/one
     :db/unique :db.unique/value
     :db/valueType :db.type/long}}})

(def trivial-schema-managed-fragment-v2
  {:name :com.example.foo
   :version 2
   :attributes
   {:foo/bar
    {:db/cardinality :db.cardinality/many
     :db/valueType :db.type/string}}})

(def trivial-schema-v1
  (sm/managed-schema-fragment->datoms trivial-schema-managed-fragment-v1))

;; TODO
(deftest test-managed-schema-fragment->datoms)

(defn <initialize-with-schema [conn schema]
  (go-pair
    (let [tx (<? (d/<transact! conn schema))]
      (let [idents (map :db/ident schema)
            db (d/db conn)]
        (into {}
          (map (fn [ident]
                 [ident (d/entid db ident)])
               idents))))))

(deftest-db test-schema-management-downgrades conn
  (is (empty? (<? (sm/<collect-schema-fragment-versions (d/db conn)))))
  (testing "Downgrades cause errors."
    (let [fragment {:name :com.a
                    :version 4
                    :attributes
                    {:foo/bar
                     {:db/cardinality :db.cardinality/one
                      :db/valueType :db.type/string}}}]
      (<? (<initialize-with-schema
            conn
            (sm/managed-schema-fragment->datoms fragment)))

      (let [current (<? (sm/<collect-schema-fragment-versions (d/db conn)))]
        (is (= {:com.a 4} current)))

      (is (thrown-with-msg?
            Throwable
            #"Existing version of :com.a is 4, which is later than requested 3"
            (<?
              (sm/<prepare-schema-application
                (d/db conn)
                {:fragments
                 [(assoc fragment :version 3)]})))))))

(deftest-db test-schema-management-conflicting-ownership conn
  (is (empty? (<? (sm/<collect-schema-fragment-versions (d/db conn)))))
  (testing "Conflicting managed fragments cause errors."
    (is (thrown-with-msg?
          Throwable #"Attributes appear in more than one"
          (<? (sm/<prepare-schema-application
                (d/db conn)
                {:fragments
                 [{:name :com.a
                   :version 1
                   :attributes
                   {:foo/bar
                    {:db/cardinality :db.cardinality/one
                     :db/valueType :db.type/string}}}
                  {:name :com.b
                   :version 1
                   :attributes
                   {:foo/bar
                    {:db/cardinality :db.cardinality/one
                     :db/valueType :db.type/string}}}]}))))))

(defn- without-tempids
  "Return the map without any k-v pairs for which v is a TempId."
  [m]
  (into {} (keep (fn [[k v]] (when-not (d/id-literal? v) [k v])) m)))

(defn- op-seq-without-tempids [ops]
  (map (fn [[op val]]
         (if (= op :transact)
           [op (map without-tempids val)]
           [op val]))
       ops))

;; This is much more convenient than trying to make tempids match
;; in our tests.
(defn- =-op-seq-without-tempids
  "Compare two sequences of operations (e.g., [[:call foo]])
   without comparing TempIds within :transact operations."
  [expected actual]
  (= (op-seq-without-tempids expected)
     (op-seq-without-tempids actual)))

;;;
;;; In this test we use simple strings to denote pre/post functions.
;;; That makes everything a little easier to test.
;;;
;;; Note carefully that we manually transact some schema fragments,
;;; then we use the schema management functions to get a _plan_. That's
;;; not the same as executing the plan.
;;;
(deftest-db test-schema-management conn
  (is (empty? (<? (sm/<collect-schema-fragment-versions (d/db conn)))))

  ;; Start off with the trivial schema at v1.
  (let [attrs (<? (<initialize-with-schema conn trivial-schema-v1))
        db (d/db conn)]

    (testing "We have a version, and it's 1. There are no other schemas."
      (is (= {:com.example.foo 1}
             (<? (sm/<collect-schema-fragment-versions db)))))

    (testing "This schema fragment has one attribute."
      (is (= {:foo/bar :com.example.foo}
             (<? (sm/<collect-schema-fragment-attributes db)))))

    (testing "The symbolic schema contains our attribute."
      (is (= :db.type/string
             (get-in (sm/db->symbolic-schema db)
                     [:foo/bar :db/valueType]))))

    (testing "An empty fragment yields no work."
      (is (nil? (<? (sm/<prepare-schema-application
                      db
                      {:pre "X"
                       :post "Y"
                       :fragments []})))))

    (testing "The same fragment, expressed as a managed schema fragment, yields no work."
      (is (nil? (<? (sm/<prepare-schema-application
                      db
                      {:fragments [trivial-schema-managed-fragment-v1]})))))

    (testing "If we try to add a second fragment, we run all-pre, new fragment, all-post."
      (is (=-op-seq-without-tempids
            [[:call "X"]
             [:transact (sm/managed-schema-fragment->datoms
                          additional-schema-managed-fragment-v7)]
             [:call "Y"]]
            (<? (sm/<prepare-schema-application
                  db
                  {:pre "X"
                   :post "Y"
                   :fragment-pre {}
                   :fragment-post {}
                   :fragments [additional-schema-managed-fragment-v7]})))))

    (testing "If we upgrade one of the fragments, we run all-pre, the appropriate fragment pre, changed fragment, fragment post, all-post."
      (is (=-op-seq-without-tempids
            [[:call "X"]
             [:call "A"]
             [:transact
              [{:db/ident :com.example.foo
                :db.schema/version 2}
               {:db/ident :foo/bar
                :db.alter/_attribute :db.part/db
                :db.schema/_attribute (d/entid db :com.example.foo)
                :db/cardinality :db.cardinality/many}]]
             [:call "B"]
             [:call "Y"]]
            (<? (sm/<prepare-schema-application
                  db
                  {:pre "X"
                   :post "Y"
                   :fragment-pre {:com.example.foo {1 "A"}}
                   :fragment-post {:com.example.foo {1 "B"}}
                   :fragments [trivial-schema-managed-fragment-v2]})))))

    (testing "If we upgrade both, we run all-pre, each fragment pre, fragment, fragment post, all post."
      (let [db (:db-after
                  (<?
                    (d/<transact! conn (sm/managed-schema-fragment->datoms
                                         additional-schema-managed-fragment-v7))))

            next-up
            {:pre "XX"
             :post "YY"
             :fragment-pre {:com.example.foo {1 "AA"}}
             :fragment-post {:com.example.foo {1 "BB"}
                             :com.example.bar {7 "CC"}}
             :fragments [trivial-schema-managed-fragment-v2
                         additional-schema-managed-fragment-v8
                         ]}

            counter (atom 0)
            pre (atom nil)
            post (atom nil)
            fragment-pre-foo (atom nil)
            fragment-post-foo (atom nil)
            fragment-post-bar (atom nil)

            next-up-but-with-functions
            {:pre (fn [db _]
                    (reset! pre (swap! counter inc))
                    nil)
             :post (fn [db _]
                     (reset! post (swap! counter inc))
                     nil)
             :fragment-pre {:com.example.foo
                            {1 (fn [db _]
                                 (reset! fragment-pre-foo (swap! counter inc))
                                 nil)}}
             :fragment-post {:com.example.foo
                             {1 (fn [db _]
                                  (reset! fragment-post-foo (swap! counter inc))
                                  nil)}
                             :com.example.bar
                             {7 (fn [db _]
                                  (reset! fragment-post-bar (swap! counter inc))
                                  nil)}}
             :fragments [trivial-schema-managed-fragment-v2
                         additional-schema-managed-fragment-v8]}]

        (testing "Make sure our fragment was added correctly."
          (let [bar (d/entid db :com.example.bar)]
            (is (integer? bar))
            (is (= :com.example.bar (d/ident db bar))))

          (is (= {:com.example.foo 1
                  :com.example.bar 7}
                 (<? (sm/<collect-schema-fragment-versions db)))))

        ;; Now we'll see the addition of the new attribute, the
        ;; bump of both versions, and the change to foo.
        (is (=-op-seq-without-tempids
              [[:call "XX"]
               [:call "AA"]
               [:transact
                [{:db/ident :com.example.foo
                  :db.schema/version 2}
                 {:db/ident :foo/bar
                  :db.alter/_attribute :db.part/db
                  :db.schema/_attribute (d/entid db :com.example.foo)
                  :db/cardinality :db.cardinality/many}]]
               [:call "BB"]
               [:transact
                [{:db/ident :com.example.bar
                  :db.schema/version 8}
                 {:db/ident :bar/choo
                  :db.install/_attribute :db.part/db
                  :db.schema/_attribute (d/entid db :com.example.bar)
                  :db/fulltext true
                  :db/valueType :db.type/string
                  :db/cardinality :db.cardinality/many}]]
               [:call "CC"]
               [:call "YY"]]
              (<? (sm/<prepare-schema-application
                    db
                    next-up))))

        ;; No change? Nothing to do.
        (is (nil? (<? (sm/<apply-schema-alteration conn {:fragments []}))))

        ;; Now let's try the write!
        (let [report (<? (sm/<apply-schema-alteration conn next-up-but-with-functions))
              db (:db-after report)]
          (is (not (nil? report)))

          (testing "Things happened in the right order."
            (is (= 1 @pre))
            (is (= 2 @fragment-pre-foo))
            (is (= 3 @fragment-post-foo))
            (is (= 4 @fragment-post-bar))
            (is (= 5 @post)))

          (testing "Our changes were applied."
            (is (= {:com.example.foo 2
                    :com.example.bar 8}
                   (<? (sm/<collect-schema-fragment-versions db))))
            (is (= {:foo/bar :com.example.foo
                    :bar/choo :com.example.bar
                    :bar/noo :com.example.bar}
                   (<? (sm/<collect-schema-fragment-attributes db))))))))))

(deftest-db test-functions-can-do-work conn
  (let
    [;; Use an atom to keep this long test fairly flat.
     db (atom (d/db conn))

     v1-fragment
     {:name :com.example.bar
      :version 1
      :attributes
      {:bar/thoo
       {:db/valueType :db.type/long
        :db/cardinality :db.cardinality/many}
       :bar/choo
       {:db/cardinality :db.cardinality/one
        :db/fulltext true
        :db/valueType :db.type/string}}}

     ;; This is what we'll change the schema into.
     v2-fragment
     (assoc
       (assoc-in v1-fragment [:attributes :bar/thoo :db/cardinality] :db.cardinality/one)
       :version 2)

     ;; This is the migration we're going to test.
     ;; We're going to remove all existing :bar/thoo, and turn it into
     ;; cardinality one. We're going to make :bar/choo
     ;; unique, and we're going to use :bar/thoo to count the number of
     ;; chars in an entity's :bar/choo.
     ;; Note that the contents of the migration work:
     ;; - Look just like regular queries and transacts, except for
     ;;   working with the internal API
     ;; - Don't have to transact the schema change at all.
     migration
     {:fragments [v2-fragment]
      :pre (fn [db do-transact]
             ;; Retract all existing uses of :bar/thoo.
             (go-pair
               (<?
                 (do-transact
                   db
                   (map (fn [e]
                          [:db.fn/retractAttribute e :bar/thoo])
                        (<? (d/<q db '[:find [?e ...]
                                       :in $
                                       :where [?e :bar/thoo _]])))))))
      :post (fn [db do-transact]
              ;; Transact some new use for :bar/thoo.
              (go-pair
                (<?
                  (do-transact
                    db
                    (map (fn [[e c]]
                           [:db/add e :bar/thoo (count c)])
                         (<? (d/<q db '[:find ?e ?c
                                        :in $
                                        :where [?e :bar/choo ?c]])))))))}]

    ;; We start empty.
    (is (empty? (<? (sm/<collect-schema-fragment-versions @db))))

    ;; Apply the v1 schema.
    (reset!
      db
      (:db-after
         (<? (sm/<apply-schema-alteration
               conn
               {:fragments [v1-fragment]}))))

    ;; Make sure it's applied.
    (testing "We can begin."
      (is (= {:com.example.bar 1}
             (<? (sm/<collect-schema-fragment-versions @db))))
      (is (= {:bar/choo :com.example.bar
              :bar/thoo :com.example.bar}
             (<? (sm/<collect-schema-fragment-attributes @db)))))

    ;; Add some data.
    (let [x (d/id-literal :db.part/user)
          y (d/id-literal :db.part/user)]
      (reset!
        db
        (:db-after
           (<?
             (d/<transact! conn [{:db/id x
                                  :bar/thoo 99}
                                 {:db/id y
                                  :bar/thoo 88}
                                 {:db/id x
                                  :bar/choo "hello, world."}])))))

    ;; Alter the schema to v2, running our migration.
    (reset! db (:db-after (<? (sm/<apply-schema-alteration conn migration))))

    ;; It worked!
    (is (not (nil? @db)))

    ;; It persisted!
    (is (= @db @(:current-db conn)))

    (testing "The schema change applied."
      (is (= :db.cardinality/one
             (get-in (sm/db->symbolic-schema @db) [:bar/thoo :db/cardinality]))))

    (is (= (count "hello, world.")   ; 13.
           (<? (d/<q @db
                     '[:find ?thoo .
                       :in $
                       :where
                       [?x :bar/choo "hello, world."]
                       [?x :bar/thoo ?thoo]]))))

    ;; Now that we changed its cardinality, we can transact another :bar/thoo
    ;; for the same entity, and we will get only the new value.
    (let [x (<? (d/<q @db '[:find ?x . :in $ :where [?x :bar/choo "hello, world."]]))]
      (is (= [13] (<? (d/<q @db [:find ['?thoo '...] :in '$ :where [x :bar/thoo '?thoo]]))))

      (<? (d/<transact! conn [[:db/add x :bar/thoo 1492]]))
      (is (= [1492] (<? (d/<q @db [:find ['?thoo '...] :in '$ :where [x :bar/thoo '?thoo]])))))))
