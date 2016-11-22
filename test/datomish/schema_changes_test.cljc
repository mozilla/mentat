;; Copyright 2016 Mozilla
;;
;; Licensed under the Apache License, Version 2.0 (the "License"); you may not use
;; this file except in compliance with the License. You may obtain a copy of the
;; License at http://www.apache.org/licenses/LICENSE-2.0
;; Unless required by applicable law or agreed to in writing, software distributed
;; under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
;; CONDITIONS OF ANY KIND, either express or implied. See the License for the
;; specific language governing permissions and limitations under the License.

(ns datomish.schema-changes-test
  #?(:cljs
     (:require-macros
      [datomish.pair-chan :refer [go-pair <?]]
      [datomish.node-tempfile-macros :refer [with-tempfile]]
      [cljs.core.async.macros :as a :refer [go]]))
  (:require
   [datomish.api :as d]
   [datomish.datom :refer [datom]]
   [datomish.schema-changes :refer [datoms->schema-fragment]]
   [datomish.schema :as ds]
   [datomish.sqlite :as s]
   [datomish.util :as util #?(:cljs :refer-macros :clj :refer) [raise cond-let]]

   [datomish.db :as dm]
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

(deftest test-datoms->schema-fragment
  (let [tx 10101
        ->datom (fn [xs]
                  (apply datom (conj xs tx)))]
    (are [i o]
        (= (datoms->schema-fragment (map ->datom i))
           o)
      ;; Base case.
      []
      {}

      ;; No matches.
      [[0 :not-db/add :not-db/install]]
      {}

      ;; Interesting case.
      [[:db.part/db :db.install/attribute 1]
       [:db.part/db :db.install/attribute 2]
       [1 :db/ident :test/attr1]
       [1 :db/valueType :db.value/string]
       [1 :db/cardinalty :db.cardinality/one]
       [1 :db/unique :db.unique/identity]
       [2 :db/ident :test/attr2]
       [2 :db/valueType :db.value/integer]
       [2 :db/cardinalty :db.cardinality/many]]
      {:test/attr1
       {:db/valueType :db.value/string
        :db/cardinalty :db.cardinality/one
        :db/unique :db.unique/identity}
       :test/attr2
       {:db/valueType :db.value/integer
        :db/cardinalty :db.cardinality/many}})

    ;; :db/ident, :db/valueType, and :db/cardinality are required.  valueType and cardinality are
    ;; enforced at the schema level.
    (testing "Fails without entity"
      (is (thrown-with-msg?
            ExceptionInfo #":db.install/attribute requires :db/ident, got \{\} for 1"
            (->>
              [[:db.part/db :db.install/attribute 1]]
              (map ->datom)
              (datoms->schema-fragment)))))

    (testing "Fails without :db/ident"
      (is (thrown-with-msg?
            ExceptionInfo #":db.install/attribute requires :db/ident, got \{:db/valueType :db.value/string\} for 1"
            (->>
              [[:db.part/db :db.install/attribute 1]
               [1 :db/valueType :db.value/string]]
              (map ->datom)
              (datoms->schema-fragment)))))))

(deftest-db test-add-and-change-ident conn
  ;; Passes through on failure.
  (is (= :test/ident (d/entid (d/db conn) :test/ident)))

  (let [report   (<? (d/<transact! conn [[:db/add (d/id-literal :db.part/db -1) :db/ident :test/ident]]))
        eid      (get-in report [:tempids (d/id-literal :db.part/db -1)])]
    (is (= eid (d/entid (d/db conn) :test/ident)))
    (is (= :test/ident (d/ident (d/db conn) eid)))

    (testing "idents can be reasserted."
      (<? (d/<transact! conn [[:db/add eid :db/ident :test/ident]])))

    (testing "idents can't be reused while they're still active."
      (is (thrown-with-msg?
            ExceptionInfo #"Transaction violates unique constraint"
            (<? (d/<transact! conn [[:db/add 5555 :db/ident :test/ident]])))))

    (testing "idents can be changed."
      ;; You can change an entity's ident.
      (<? (d/<transact! conn [[:db/add eid :db/ident :test/anotherident]]))
      (is (= eid (d/entid (d/db conn) :test/anotherident)))
      (is (= :test/anotherident (d/ident (d/db conn) eid)))
      (is (not (= eid (d/entid (d/db conn) :test/ident))))

      ;; Passes through on failure.
      (is (= :test/ident (d/entid (d/db conn) :test/ident))))

    (testing "Once freed up, an ident can be reused."
      (<? (d/<transact! conn [[:db/add 5555 :db/ident :test/ident]]))
      (is (= 5555 (d/entid (d/db conn) :test/ident))))))

(deftest-db test-change-schema-ident conn
  ;; If an ident names an attribute, and is altered, then that attribute has
  ;; changed in the schema.
  (let [tempid (d/id-literal :db.part/db -1)
        es [[:db/add :db.part/db :db.install/attribute tempid]
            {:db/id tempid
             :db/ident :test/someattr
             :db/valueType :db.type/string
             :db/cardinality :db.cardinality/one}]
        report (<? (d/<transact! conn es))
        db-after (:db-after report)
        eid (get-in report [:tempids tempid])]

    (testing "New ident is allocated"
      (is (some? (d/entid db-after :test/someattr))))

    (testing "Schema is modified"
      (is (= (get-in db-after [:symbolic-schema :test/someattr])
             {:db/valueType :db.type/string,
              :db/cardinality :db.cardinality/one})))

    (is (= eid (d/entid (d/db conn) :test/someattr)))

    (testing "schema idents can be altered."
      (let [report (<? (d/<transact! conn [{:db/id eid
                                            :db/ident :test/otherattr}]))
            db-after (:db-after report)]
        (is (= eid (d/entid (d/db conn) :test/otherattr)))

        ;; Passes through on failure.
        (is (keyword? (d/entid (d/db conn) :test/someattr)))

        (is (nil? (get-in db-after [:symbolic-schema :test/someattr])))
        (is (= (get-in db-after [:symbolic-schema :test/otherattr])
               {:db/valueType :db.type/string,
                :db/cardinality :db.cardinality/one}))))))

(deftest-db test-alter-schema-cardinality-one-to-many conn
  (let [tempid (d/id-literal :db.part/db -1)
        es [[:db/add :db.part/db :db.install/attribute tempid]
            {:db/id tempid
             :db/ident :test/attr
             :db/valueType :db.type/long
             :db/cardinality :db.cardinality/one}]
        report (<? (d/<transact! conn es))
        db-after (:db-after report)
        eid (get-in report [:tempids tempid])]

    (is (= (get-in db-after [:symbolic-schema :test/attr :db/cardinality])
           :db.cardinality/one))

    ;; Add two values for the property. Observe that only one is preserved.
    (<? (d/<transact! conn [{:db/id 12345 :test/attr 111}]))
    (<? (d/<transact! conn [{:db/id 12345 :test/attr 222}]))
    (is (= [222]
           (<? (d/<q (d/db conn)
                     '[:find [?a ...] :in $ :where [12345 :test/attr ?a]]))))

    ;; Change it to a multi-valued property.
    (let [report (<? (d/<transact! conn [{:db/id eid
                                          :db/cardinality :db.cardinality/many
                                          :db.alter/_attribute :db.part/db}]))
          db-after (:db-after report)]

      (is (= eid (d/entid (d/db conn) :test/attr)))
      (is (= (get-in db-after [:symbolic-schema :test/attr :db/cardinality])
             :db.cardinality/many))

      (is (ds/multival? (.-schema (d/db conn)) eid))

      (is (= [222]
             (<? (d/<q (d/db conn)
                       '[:find [?a ...] :in $ :where [12345 :test/attr ?a]]))))
      (<? (d/<transact! conn [{:db/id 12345 :test/attr 333}]))
      (is (= [222 333]
             (<? (d/<q (d/db conn)
                       '[:find [?a ...] :in $ :where [12345 :test/attr ?a]]
                       {:order-by [[:a :asc]]})))))))

(deftest-db test-alter-schema-cardinality-many-to-one conn
  (let [prop-a (d/id-literal :db.part/db -1)
        prop-b (d/id-literal :db.part/db -2)
        prop-c (d/id-literal :db.part/db -3)
        es [[:db/add :db.part/db :db.install/attribute prop-a]
            [:db/add :db.part/db :db.install/attribute prop-b]
            [:db/add :db.part/db :db.install/attribute prop-c]
            {:db/id prop-a
             :db/ident :test/attra
             :db/valueType :db.type/long
             :db/cardinality :db.cardinality/many}
            {:db/id prop-b
             :db/ident :test/attrb
             :db/valueType :db.type/string
             :db/fulltext true
             :db/cardinality :db.cardinality/many}
            {:db/id prop-c
             :db/ident :test/attrc
             :db/valueType :db.type/long
             :db/cardinality :db.cardinality/many}]
        report (<? (d/<transact! conn es))
        db-after (:db-after report)
        e-a (get-in report [:tempids prop-a])
        e-b (get-in report [:tempids prop-b])
        e-c (get-in report [:tempids prop-c])]

    (is (= (get-in db-after [:symbolic-schema :test/attra :db/cardinality])
           :db.cardinality/many))
    (is (= (get-in db-after [:symbolic-schema :test/attrb :db/cardinality])
           :db.cardinality/many))

    ;; Add two values for one property, one for another, and none for the last.
    ;; Observe that only all are preserved.
    (<? (d/<transact! conn [{:db/id 12345 :test/attrb "foobar"}]))
    (<? (d/<transact! conn [{:db/id 12345 :test/attrc 222}]))
    (<? (d/<transact! conn [{:db/id 12345 :test/attrc 333}]))
    (is (= []
           (<? (d/<q (d/db conn)
                     '[:find [?a ...] :in $ :where [12345 :test/attra ?a]]))))
    (is (= ["foobar"]
           (<? (d/<q (d/db conn)
                     '[:find [?b ...] :in $ :where [12345 :test/attrb ?b]]))))
    (is (= [222 333]
           (<? (d/<q (d/db conn)
                     '[:find [?c ...] :in $ :where [12345 :test/attrc ?c]]))))

    ;; Change each to a single-valued property.
    ;; 'a' and 'b' should succeed, because they match the new cardinality
    ;; constraint. 'c' should fail, because it already has two values for 12345.
    (let [change
          (fn [eid attr]
            (go-pair
              (let [report (<? (d/<transact!
                                 conn
                                 [{:db/id eid
                                   :db/cardinality :db.cardinality/one
                                   :db.alter/_attribute :db.part/db}]))
                    db-after (:db-after report)]

                (is (= eid (d/entid (d/db conn) attr)))
                (is (= (get-in db-after [:symbolic-schema attr :db/cardinality])
                       :db.cardinality/one))

                (is (not (ds/multival? (.-schema (d/db conn)) eid))))))]

      (<? (change e-a :test/attra))
      (<? (change e-b :test/attrb))
      (is (thrown-with-msg?
            ExceptionInfo #"Can't alter :db/cardinality"
            (<? (change e-c :test/attrc)))))))
