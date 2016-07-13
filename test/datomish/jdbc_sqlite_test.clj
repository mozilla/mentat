;; This Source Code Form is subject to the terms of the Mozilla Public
;; License, v. 2.0. If a copy of the MPL was not distributed with this
;; file, You can obtain one at http://mozilla.org/MPL/2.0/.

(ns datomish.jdbc-sqlite-test
  (:require
   [datomish.sqlite :as s]
   [datomish.pair-chan :refer [go-pair <?]]
   [datomish.jdbc-sqlite :as j]
   [datomish.test-macros :refer [deftest-async]]
   [tempfile.core :refer [tempfile with-tempfile]]
   [clojure.core.async :as a :refer [<! >!]]
   [clojure.test :as t :refer [is are deftest testing]]))

(deftest-async test-all-rows
  (with-tempfile [t (tempfile)]
    (let [db (<? (j/open t))]
      (try
        (<? (s/execute! db ["CREATE TABLE test (a INTEGER)"]))
        (<? (s/execute! db ["INSERT INTO test VALUES (?)" 1]))
        (<? (s/execute! db ["INSERT INTO test VALUES (?)" 2]))
        (let [rows (<? (s/all-rows db ["SELECT * FROM test ORDER BY a ASC"]))]
          (is (= rows [{:a 1} {:a 2}])))
        (finally
          (<? (s/close db)))))))

(deftest-async test-in-transaction!
  (with-tempfile [t (tempfile)]
    (let [db (<? (j/open t))]
      (try
        (<? (s/execute! db ["CREATE TABLE ta (a INTEGER)"]))
        (<? (s/execute! db ["CREATE TABLE tb (b INTEGER)"]))
        (<? (s/execute! db ["INSERT INTO ta VALUES (?)" 1]))
        (let [[v e] (<! (s/in-transaction! db #(s/execute! db ["INSERT INTO tb VALUES (?)" 2])))]
          (is (not e)))
        (let [rows (<? (s/all-rows db ["SELECT * FROM ta ORDER BY a ASC"]))]
          (is (= rows [{:a 1}])))
        (let [rows (<? (s/all-rows db ["SELECT * FROM tb ORDER BY b ASC"]))]
          (is (= rows [{:b 2}])))
        (let [f #(go-pair
                   ;; The first succeeds ...
                   (<? (s/execute! db ["INSERT INTO ta VALUES (?)" 3]))
                   ;; ... but will get rolled back by the second failing.
                   (<? (s/execute! db ["INSERT INTO tb VALUES (?)" 4 "bad parameter"])))
              [v e] (<! (s/in-transaction! db f))]
          (is (some? e)))
        ;; No changes, since the transaction as a whole failed.
        (let [rows (<? (s/all-rows db ["SELECT * FROM ta ORDER BY a ASC"]))]
          (is (= rows [{:a 1}])))
        (let [rows (<? (s/all-rows db ["SELECT * FROM tb ORDER BY b ASC"]))]
          (is (= rows [{:b 2}])))
        (finally
          (<? (s/close db)))))))
