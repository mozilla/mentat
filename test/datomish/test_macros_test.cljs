;; Copyright 2016 Mozilla
;;
;; Licensed under the Apache License, Version 2.0 (the "License"); you may not use
;; this file except in compliance with the License. You may obtain a copy of the
;; License at http://www.apache.org/licenses/LICENSE-2.0
;; Unless required by applicable law or agreed to in writing, software distributed
;; under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
;; CONDITIONS OF ANY KIND, either express or implied. See the License for the
;; specific language governing permissions and limitations under the License.

(ns datomish.test-macros-test
  (:require-macros
   [datomish.pair-chan :refer [go-pair]]
   [cljs.core.async.macros])
  (:require
   [datomish.test-macros :refer-macros [deftest-async]]
   [cljs.core.async]
   [cljs.test :refer-macros [is are deftest testing async]]))

(deftest sync-test
  (is (= 1 1)))

(deftest-async async-test
  (is (= 1 1)))
