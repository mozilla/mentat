;; Copyright 2016 Mozilla
;;
;; Licensed under the Apache License, Version 2.0 (the "License"); you may not use
;; this file except in compliance with the License. You may obtain a copy of the
;; License at http://www.apache.org/licenses/LICENSE-2.0
;; Unless required by applicable law or agreed to in writing, software distributed
;; under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
;; CONDITIONS OF ANY KIND, either express or implied. See the License for the
;; specific language governing permissions and limitations under the License.

(ns datomish.upgrade-test
  (:require
     [clojure.java.io :refer [copy]]
     [datomish.jdbc-sqlite :as jdbc]
     [datomish.sqlite :as s]
     [datomish.api :as d]
     [datomish.test-macros :refer [deftest-async]]
     [datomish.pair-chan :refer [go-pair <?]]
     [clojure.test :as t :refer [is are deftest testing]]))

(deftest-async test-upgrade-v1-connect
  ;; Copy our test DB to a temporary directory.
  ;; Open it with Datomish. Make sure nothing untoward happened.
  (let [out (java.io.File/createTempFile "someprefixstringv1" "db")]
    (copy (java.io.File. "test/v1.db") out)
    (let [path (.getAbsolutePath out)
          conn (<? (d/<connect path))]

      (is (= (d/entid (d/db conn) :db.schema/version) 36))
      (d/<close conn))))

(deftest-async test-upgrade-v1
  ;; Copy our test DB to a temporary directory.
  ;;
  ;; Open it with SQLite. Verify that v2 features are not present, and the
  ;; user_version is 1.
  ;;
  ;; Open it with Datomish. Verify that bootstrapped v2 features are present,
  ;; and the user_version is 2.
  (let [out (java.io.File/createTempFile "someprefixstringv1" "db")]
    (copy (java.io.File. "test/v1.db") out)
    (let [path (.getAbsolutePath out)
          sqlite (<? (jdbc/open path))]

      (is (= (<? (s/get-user-version sqlite))
             1))
      (is (= [{:idx 36}]
             (<? (s/all-rows sqlite ["SELECT idx FROM parts WHERE part = ':db.part/db'"]))))
      (is (empty? (<? (s/all-rows sqlite ["SELECT * FROM idents WHERE ident = ':db.schema/version'"]))))

      ;; This will automatically upgrade.
      (let [db (<? (datomish.db-factory/<db-with-sqlite-connection sqlite))]
        (is (= 2 (<? (s/get-user-version sqlite))))
        (is (= [{:idx 38}]
               (<? (s/all-rows sqlite ["SELECT idx FROM parts WHERE part = ':db.part/db'"]))))
        (is (= [{:entid 36}]
               (<? (s/all-rows sqlite ["SELECT entid FROM idents WHERE ident = ':db.schema/version'"]))))
        (is (= (d/entid db :db.schema/version) 36))
        (s/close sqlite)))))

