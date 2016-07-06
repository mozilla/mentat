(ns datomish.test.transforms
  (:require
   [cljs.test    :as t :refer-macros [is are deftest testing]]
   [datomish.transforms :as transforms]))

(deftest test-attribute-transform-string
  (is (= "p/foo"
         (transforms/attribute-transform-string :p/foo))))

(deftest test-constant-transform-default
  ;; Keywords.
  (is (= "p/foo" (transforms/constant-transform-default :p/foo)))   ; For now.

  ;; Booleans.
  (is (= 1 (transforms/constant-transform-default true)))
  (is (= 0 (transforms/constant-transform-default false)))

  ;; Numbers and strings.
  (is (= 1 (transforms/constant-transform-default 1.0)))
  (is (= -1 (transforms/constant-transform-default -1)))
  (is (= 42 (transforms/constant-transform-default 42)))
  (is (= "" (transforms/constant-transform-default "")))
  (is (= "foo" (transforms/constant-transform-default "foo"))))
