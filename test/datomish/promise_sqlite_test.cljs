;; Copyright 2016 Mozilla
;;
;; Licensed under the Apache License, Version 2.0 (the "License"); you may not use
;; this file except in compliance with the License. You may obtain a copy of the
;; License at http://www.apache.org/licenses/LICENSE-2.0
;; Unless required by applicable law or agreed to in writing, software distributed
;; under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
;; CONDITIONS OF ANY KIND, either express or implied. See the License for the
;; specific language governing permissions and limitations under the License.

(ns datomish.promise-sqlite-test
  (:require-macros
   [datomish.pair-chan :refer [go-pair <?]]
   [datomish.test-macros :refer [deftest-async]]
   [datomish.node-tempfile-macros :refer [with-tempfile]]
   [cljs.core.async.macros])
  (:require
   [datomish.node-tempfile :refer [tempfile]]
   [cljs.core.async :refer [<! >!]]
   [cljs.test :refer-macros [is are deftest testing async]]
   [datomish.pair-chan]
   [datomish.sqlite :as s]
   [datomish.js-sqlite :as ps]))

(deftest-async test-all-rows
  (with-tempfile [t (tempfile)]
    (let [db (<? (ps/open (.-name t)))]
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
    (let [db (<? (ps/open (.-name t)))]
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

(deftest-async test-long-strings
  ;; Make a string that's nicely over the 32K limit in node-sqlite3/#668.
  (let [s (str "be" (apply str (repeat 330000 "la")) "st")]
    (with-tempfile [t (tempfile)]
      (let [db (<? (ps/open (.-name t)))]
        (try
          (<? (s/execute! db ["CREATE TABLE strs (x INTEGER, a TEXT)"]))
          (<? (s/execute! db ["INSERT INTO strs VALUES (42, ?)" s]))

          ;; Test retrieval via binding comparison.
          (is (= (<? (s/all-rows db ["SELECT x FROM strs WHERE a = ?" s]))
                 [{:x 42}]))

          ;; Test computation.
          (is (= (<? (s/all-rows db ["SELECT length(a) AS yvr FROM strs"]))
                 [{:yvr 660004}]))
          (is (= (<? (s/all-rows db ["SELECT length(?) AS yvr FROM strs" s]))
                 [{:yvr 660004}]))

          ;; Test round-tripping.
          (is (= (<? (s/all-rows db ["SELECT x, a FROM strs"]))
                 [{:x 42, :a s}]))
          (is (= (<? (s/all-rows db ["SELECT ? AS yyz FROM strs" s]))
                 [{:yyz s}]))

          (finally
            (<? (s/close db))))
        ))))

(deftest test-tests-are-isolated
  (go-pair
    (let [conn-1 (<? (s/<sqlite-connection ""))
          conn-2 (<? (s/<sqlite-connection ""))]
      (is (not (= conn-1 conn-2)))
      (<? (s/execute! conn-1 ["CREATE TABLE foo (x INTEGER)"]))
      (<? (s/execute! conn-2 ["CREATE TABLE foo (x INTEGER)"]))
      (<? (s/execute! conn-1 ["INSERT INTO foo (x) VALUES (5)"]))
      (is (empty? (<? (s/all-rows conn-2 ["SELECT x FROM foo"]))))
      (s/close conn-1)
      (s/close conn-2))))
