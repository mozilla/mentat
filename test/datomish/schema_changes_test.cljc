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
   [datomish.util :as util #?(:cljs :refer-macros :clj :refer) [raise cond-let]]
   [datomish.sqlite :as s]

   [datomish.datom :refer [datom]]

   [datomish.schema-changes :refer [datoms->schema-fragment]]

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
       {:db/ident :test/attr1
        :db/valueType :db.value/string
        :db/cardinalty :db.cardinality/one
        :db/unique :db.unique/identity}
       :test/attr2
       {:db/ident :test/attr2
        :db/valueType :db.value/integer
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
