(ns datomish.test.util
  (:require
     [datomish.util :as util]
     #?(:clj  [clojure.test :as t :refer [is are deftest testing]])
     #?(:cljs [cljs.test :as t :refer-macros [is are deftest testing]])
     ))

(deftest test-var-translation
  (is (= :x (util/var->sql-var '?x)))
  (is (= :XX (util/var->sql-var '?XX))))

#?(:cljs
     (deftest test-raise
       (let [caught
             (try
               (do
                 (util/raise "succeed")
                 "fail")
               (catch :default e e))]
         (is (= "succeed" (aget caught "message"))))))
