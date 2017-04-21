(ns datomish.test
  (:require
   [doo.runner :refer-macros [doo-tests doo-all-tests]]
   [cljs.test :as t :refer-macros [is are deftest testing]]
   datomish.schema-changes-test
   datomish.schema-management-test
   datomish.places.importer-test
   datomish.promise-sqlite-test
   datomish.db-test
   datomish.query-test
   datomish.sqlite-user-version-test
   datomish.tofinoish-test
   datomish.transact-test
   datomish.util-test
   datomish.test.transforms
   datomish.test.query
   datomish.test-macros-test
   ))

(doo-tests
  'datomish.schema-changes-test
  'datomish.schema-management-test
  'datomish.places.importer-test
  'datomish.promise-sqlite-test
  'datomish.db-test
  'datomish.query-test
  'datomish.sqlite-user-version-test
  'datomish.tofinoish-test
  'datomish.transact-test
  'datomish.util-test
  'datomish.test.transforms
  'datomish.test.query
  'datomish.test-macros-test)
