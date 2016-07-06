(ns datomish.test
  (:require
   [doo.runner :refer-macros [doo-tests]]
   [cljs.test    :as t :refer-macros [is are deftest testing]]
   datomish.test.core))

(doo-tests 'datomish.test.core)
