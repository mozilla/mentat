(ns datomish.test.core
  (:require
   [cljs.test    :as t :refer-macros [is are deftest testing]]
   [datomish.core :as d]))

(deftest test-core
  (is (= 1 1)))
