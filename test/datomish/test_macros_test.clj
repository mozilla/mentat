;; This Source Code Form is subject to the terms of the Mozilla Public
;; License, v. 2.0. If a copy of the MPL was not distributed with this
;; file, You can obtain one at http://mozilla.org/MPL/2.0/.

(ns datomish.test-macros-test
  (:require
   [datomish.pair-chan :refer [go-pair]]
   [datomish.test-macros :refer [deftest-async]]
   [clojure.core.async :as a]
   [clojure.test :as t :refer [is are deftest testing]]))

(deftest sync-test
  (is (= 1 1)))

(deftest-async async-test
  (is (= 1 1)))
