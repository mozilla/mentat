;; Copyright 2016 Mozilla
;;
;; Licensed under the Apache License, Version 2.0 (the "License"); you may not use
;; this file except in compliance with the License. You may obtain a copy of the
;; License at http://www.apache.org/licenses/LICENSE-2.0
;; Unless required by applicable law or agreed to in writing, software distributed
;; under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
;; CONDITIONS OF ANY KIND, either express or implied. See the License for the
;; specific language governing permissions and limitations under the License.

(ns datomish.sqlite-user-version-test
  #?(:cljs
     (:require-macros
      [datomish.pair-chan :refer [go-pair <?]]
      [datomish.node-tempfile-macros :refer [with-tempfile]]
      [cljs.core.async.macros :as a :refer [go]]))
  (:require
   [datomish.util :as util #?(:cljs :refer-macros :clj :refer) [raise raise-str cond-let]]
   [datomish.sqlite :as s]
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

(deftest-async test-all-rows
  (with-tempfile [t (tempfile)]
    (let [db (<? (s/<sqlite-connection t))]
      (try
        (<? (s/execute! db ["CREATE TABLE test (a INTEGER)"]))
        (<? (s/execute! db ["INSERT INTO test VALUES (?)" 1]))
        (<? (s/execute! db ["INSERT INTO test VALUES (?)" 2]))
        (let [rows (<? (s/all-rows db ["SELECT * FROM test ORDER BY a ASC"]))]
          (is (= rows [{:a 1} {:a 2}])))
        (finally
          (<? (s/close db)))))))
