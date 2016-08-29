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
   (deftest test-integer?-js
     (is (integer? 0))
     (is (integer? 5))
     (is (integer? 50000000000))
     (is (not (integer? 5.1)))))

#?(:cljs
   (deftest test-raise
     (let [caught
           (try
             (do
               (util/raise "succeed" {:foo 1})
               "fail")
             (catch :default e e))]
       (is (= "succeed" (aget caught "message")))
       (is (= {:foo 1} (aget caught "data"))))))
