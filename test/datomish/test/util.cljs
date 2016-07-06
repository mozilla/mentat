(ns datomish.test.util
  (:require
     [cljs.test :as t :refer-macros [is are deftest testing]]
     [datomish.util :as util]))

(deftest test-var-translation
  (is (= :x (util/var->sql-var '?x)))
  (is (= :XX (util/var->sql-var '?XX))))

(deftest test-raise
  (let [caught
        (try
          (do
            (util/raise "succeed")
            "fail")
          (catch :default e e))]
    (is (= "succeed" (aget caught "message")))))
