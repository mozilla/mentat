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
              [cljs.core.async :as a :refer [<! >!]]])))

(defn <datoms [db]
  (go-pair
    (->>
      (<? (s/all-rows (:sqlite-connection db) ["SELECT e, a, v, tx FROM datoms ORDER BY tx ASC, e, a, v"]))
      (mapv #(vector (:e %) (:a %) (:v %) (:tx %) true)))))

(defn <transactions [db]
  (go-pair
    (->>
      (<? (s/all-rows (:sqlite-connection db) ["SELECT e, a, v, tx, added FROM transactions ORDER BY tx ASC, e, a, v, added"]))
      (mapv #(vector (:e %) (:a %) (:v %) (:tx %) (:added %))))))

(deftest-async test-add-one
  (with-tempfile [t (tempfile)]
    (let [c  (<? (s/<sqlite-connection t))
          db (<? (db/<with-sqlite-connection c))]
      (try
        (let [now        -1
              txInstant  (<? (db/<entid db :db/txInstant)) ;; TODO: convert entids to idents on egress.
              x          (<? (db/<entid db :x))            ;; TODO: convert entids to idents on egress.
              report     (<? (db/<transact! db [[:db/add 0 :x "valuex"]] nil now))
              current-tx (:current-tx report)]
          (is (= current-tx db/tx0))
          (is (= (<? (<datoms db))
                 [[0 x "valuex" db/tx0 true]
                  [db/tx0 txInstant now db/tx0 true]]))
          (is (= (<? (<transactions db))
                 [[0 x "valuex" db/tx0 1] ;; TODO: true, not 1.
                  [db/tx0 txInstant now db/tx0 1]])))
        (finally
          (<? (db/close db)))))))

(deftest-async test-add-two
  (with-tempfile [t (tempfile)]
    (let [c  (<? (s/<sqlite-connection t))
          db (<? (db/<with-sqlite-connection c))]
      (try
        (let [now        -1
              txInstant  (<? (db/<entid db :db/txInstant)) ;; TODO: convert entids to idents on egress.
              x          (<? (db/<entid db :x))            ;; TODO: convert entids to idents on egress.
              y          (<? (db/<entid db :y))            ;; TODO: convert entids to idents on egress.
              report     (<? (db/<transact! db [[:db/add 0 :x "valuex"] [:db/add 1 :y "valuey"]] nil now))
              current-tx (:current-tx report)]
          (is (= current-tx db/tx0))
          (is (= (<? (<datoms db))
                 [[0 x "valuex" db/tx0 true]
                  [1 y "valuey" db/tx0 true]
                  [db/tx0 txInstant now db/tx0 true]]))
          (is (= (<? (<transactions db))
                 [[0 x "valuex" db/tx0 1] ;; TODO: true, not 1.
                  [1 y "valuey" db/tx0 1]
                  [db/tx0 txInstant now db/tx0 1]])))
        (finally
          (<? (db/close db)))))))

;; TODO: test multipe :add and :retract of the same datom in the same transaction.
(deftest-async test-retract
  (with-tempfile [t (tempfile)]
    (let [c  (<? (s/<sqlite-connection t))
          db (<? (db/<with-sqlite-connection c))]
      (try
        (let [now       -1
              txInstant (<? (db/<entid db :db/txInstant)) ;; TODO: convert entids to idents on egress.
              x         (<? (db/<entid db :x))            ;; TODO: convert entids to idents on egress.
              ra        (<? (db/<transact! db [[:db/add     0 :x "valuex"]] nil now))
              rb        (<? (db/<transact! db [[:db/retract 0 :x "valuex"]] nil now))
              txa       (:current-tx ra)
              txb       (:current-tx rb)]
          (is (= (<? (<datoms db))
                 [[txa txInstant now txa true]
                  [txb txInstant now txb true]]))
          (is (= (<? (<transactions db))
                 [[0 x "valuex" txa 1] ;; TODO: true, not 1.
                  [txa txInstant -1 txa 1]
                  [0 x "valuex" txb 0]
                  [txb txInstant -1 txb 1]])))
        (finally
          (<? (db/close db)))))))
