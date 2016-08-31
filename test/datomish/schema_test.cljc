;; This Source Code Form is subject to the terms of the Mozilla Public
;; License, v. 2.0. If a copy of the MPL was not distributed with this
;; file, You can obtain one at http://mozilla.org/MPL/2.0/.

(ns datomish.schema-test
  (:require
   [datomish.schema :as schema]
   #?@(:clj [[datomish.test-macros :refer [deftest-async]]
             [clojure.test :as t :refer [is are deftest testing]]])
   #?@(:cljs [[datomish.test-macros :refer-macros [deftest-async]]
              [cljs.test :as t :refer-macros [is are deftest testing async]]])))

#?(:clj 
(deftest test-uuid-validation
  (is (not (schema/uuidish? "123e4567-e89b-12d3-a456-426655440000")))
  (is (schema/uuidish? (java.util.UUID/fromString "123e4567-e89b-12d3-a456-426655440000")))))

#?(:cljs
(deftest test-uuid-validation
  ;; Case-insensitive.
  (is (schema/uuidish? "123e4567-e89b-12d3-a456-426655440000"))
  (is (schema/uuidish? "123E4567-e89b-12d3-a456-426655440000"))))
