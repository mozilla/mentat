;; This Source Code Form is subject to the terms of the Mozilla Public
;; License, v. 2.0. If a copy of the MPL was not distributed with this
;; file, You can obtain one at http://mozilla.org/MPL/2.0/.

(ns datomish.transact-test
  #?(:cljs
     (:require-macros
      [datomish.pair-chan :refer [go-pair <?]]
      [datomish.node-tempfile-macros :refer [with-tempfile]]
      [cljs.core.async.macros :as a :refer [go go-loop]]))
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
             [clojure.core.async :as a :refer [go go-loop <! >!]]])
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

(deftest-db test-overlapping-transacts conn
  (let [{tx0 :tx} (<? (d/<transact! conn test-schema))
        report0   (<? (d/<transact! conn [{:db/id (d/id-literal :db.part/user -1)
                                           :name  "Petr"}]))
        id0       (get (tempids report0) -1)
        n         5
        make-t    (fn [i]
                    ;; Be aware that a go block with a parking operation here
                    ;; can change the order of transaction evaluation, since the
                    ;; parking operation will be unparked non-deterministically.
                    (d/<transact! conn [{:db/id (d/id-literal :db.part/user -1)
                                         :name  "Petr"
                                         :email (str "@" i)}]))]

    ;; Wait for all transactions to complete.
    (<! (a/into []
                (a/merge
                  (map make-t (range n)))))

    ;; Transactions should be processed in order.  This is an awkward way to
    ;; express the expected data, but it's robust in the face of changing default
    ;; identities, transaction numbers, and values of n.
    (is (= (concat [[id0 :name "Petr" (+ 1 tx0) 1]
                    [id0 :email "@0" (+ 2 tx0) 1]]
                   (mapcat
                     #(-> [[id0 :email (str "@" %) (+ 3 % tx0) 0]
                           [id0 :email (str "@" (inc %)) (+ 3 % tx0) 1]])
                     (range 0 (dec n))))

           (filter #(not= :db/txInstant (second %)) (<? (<transactions-after (d/db conn) tx0)))))))

#_ (time (t/run-tests))
