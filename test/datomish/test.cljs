(ns datomish.test
  (:require
   [doo.runner :refer-macros [doo-tests doo-all-tests]]
   [cljs.test :as t :refer-macros [is are deftest testing]]
   datomish.test-macros-test))

(doo-tests 'datomish.test-macros-test)
