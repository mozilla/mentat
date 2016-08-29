;; This Source Code Form is subject to the terms of the Mozilla Public
;; License, v. 2.0. If a copy of the MPL was not distributed with this
;; file, You can obtain one at http://mozilla.org/MPL/2.0/.

(ns datomish.query-test
  #?(:cljs
     (:require-macros
      [datomish.pair-chan :refer [go-pair <?]]
      [datomish.node-tempfile-macros :refer [with-tempfile]]
      [cljs.core.async.macros :as a :refer [go]]))
  (:require
   [datomish.api :as d]
   #?@(:clj [[datomish.jdbc-sqlite]
             [datomish.pair-chan :refer [go-pair <?]]
             [tempfile.core :refer [tempfile with-tempfile]]
             [datomish.test-macros :refer [deftest-async]]
             [clojure.test :as t :refer [is are deftest testing]]
             [clojure.core.async :refer [go <! >!]]])
   #?@(:cljs [[datomish.promise-sqlite]
              [datomish.pair-chan]
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

(def test-schema
  [{:db/id        (d/id-literal :db.part/user)
    :db/ident     :x
    :db/unique    :db.unique/identity
    :db/valueType :db.type/long
    :db.install/_attribute :db.part/db}
   ])

(deftest-async test-q
  (with-tempfile [t (tempfile)]
    (let [conn      (<? (d/<connect t))
          {tx0 :tx} (<? (d/<transact! conn test-schema))]
      (try
        (let [{tx1 :tx} (<? (d/<transact! conn [{:db/id 101 :x 505}]))]

          (is (= (<? (d/<q (d/db conn)
                           `[:find ?e ?a ?v ?tx :in $ :where
                             [?e ?a ?v ?tx]
                             [(> ?tx ~tx0)]
                             [(!= ?a ~(d/entid (d/db conn) :db/txInstant))] ;; TODO: map ident->entid for values.
                             ] {}))
                 [[101 (d/entid (d/db conn) :x) 505 tx1]]))) ;; TODO: map entid->ident on egress.
        (finally
          (<? (d/<close conn)))))))
