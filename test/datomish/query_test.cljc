;; Copyright 2016 Mozilla
;;
;; Licensed under the Apache License, Version 2.0 (the "License"); you may not use
;; this file except in compliance with the License. You may obtain a copy of the
;; License at http://www.apache.org/licenses/LICENSE-2.0
;; Unless required by applicable law or agreed to in writing, software distributed
;; under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
;; CONDITIONS OF ANY KIND, either express or implied. See the License for the
;; specific language governing permissions and limitations under the License.

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
   #?@(:cljs [[datomish.js-sqlite]
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
                           [:find '?e '?a '?v '?tx :in '$ :where
                            '[?e ?a ?v ?tx]
                            [(list '> '?tx tx0)]
                            [(list '!= '?a (d/entid (d/db conn) :db/txInstant))] ;; TODO: map ident->entid for values.
                            ] {}))
                 [[101 (d/entid (d/db conn) :x) 505 tx1]]))) ;; TODO: map entid->ident on egress.
        (finally
          (<? (d/<close conn)))))))
