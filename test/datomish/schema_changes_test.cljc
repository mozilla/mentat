;; This Source Code Form is subject to the terms of the Mozilla Public
;; License, v. 2.0. If a copy of the MPL was not distributed with this
;; file, You can obtain one at http://mozilla.org/MPL/2.0/.

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
