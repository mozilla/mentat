(ns datomish.test.query
  (:require
     [datomish.query :as dq]
     #?(:clj  [clojure.test :as t :refer [is are deftest testing]])
     #?(:cljs [cljs.test :as t :refer-macros [is are deftest testing]])
     ))

(deftest test-query
  (is (= 1 1)))
