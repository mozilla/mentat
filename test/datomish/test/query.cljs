(ns datomish.test.query
  (:require
   [cljs.test    :as t :refer-macros [is are deftest testing]]
   [datomish.query :as dq]))

(deftest test-query
  (is (= 1 1)))
