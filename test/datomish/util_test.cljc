(ns datomish.util-test
  #?(:cljs
     (:require-macros
      [datomish.util]
      [cljs.core.async.macros :refer [go go-loop]]))
  (:require
   [datomish.util :as util]
   [datomish.util.deque :as deque]
   [clojure.string :as str]
   #?@(:clj
       [[datomish.test-macros :refer [deftest-async]]
        [clojure.core.async :as a :refer [go go-loop <! >!]]
        [clojure.test :as t :refer [is are deftest testing]]])
   #?@(:cljs
       [[datomish.test-macros :refer-macros [deftest-async]]
        [cljs.core.async :as a :refer [<! >!]]
        [cljs.test :as t :refer-macros [is are deftest testing]]])))

(deftest test-var-translation
  (is (= :x (util/var->sql-var '?x)))
  (is (= :XX (util/var->sql-var '?XX))))

#?(:cljs
   (deftest test-integer?-js
     (is (integer? 0))
     (is (integer? 5))
     (is (integer? 50000000000))
     (is (integer? 5.00))             ; Because JS.
     (is (not (integer? 5.1)))))

#?(:clj
   (deftest test-integer?-clj
     (is (integer? 0))
     (is (integer? 5))
     (is (integer? 50000000000))
     (is (not (integer? 5.00)))
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

(deftest test-deque
  (let [d (deque/deque)]
    (is (= [] @d))
    (deque/enqueue! d 1)
    (is (= [1] @d))
    (deque/enqueue! d 2)
    (is (= [1 2] @d))
    (is (= 1 (deque/dequeue! d)))
    (is (= 2 (deque/dequeue! d)))
    (is (= [] @d))
    (is (= nil (deque/dequeue! d)))))
