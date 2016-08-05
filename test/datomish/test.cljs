(ns datomish.test
  (:require
   [doo.runner :refer-macros [doo-tests doo-all-tests]]
   [cljs.test :as t :refer-macros [is are deftest testing]]
   datomish.promise-sqlite-test
   datomish.db-test
   datomish.sqlite-user-version-test
   datomish.test.util
   datomish.test.transforms
   datomish.test.query
   datomish.test-macros-test))

(doo-tests
  'datomish.promise-sqlite-test
  'datomish.db-test
  'datomish.sqlite-user-version-test
  'datomish.test.util
  'datomish.test.transforms
  'datomish.test.query
  'datomish.test-macros-test)
