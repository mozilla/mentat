(ns datomish.test.query
  (:require
     [datomish.query.context :as context]
     [datomish.query.source :as source]
     [datomish.query.transforms :as transforms]
     [datomish.query :as query]
     #?@(:clj
           [
          [honeysql.core :as sql :refer [param]]
          [clojure.test :as t :refer [is are deftest testing]]])
     #?@(:cljs
           [
          [honeysql.core :as sql :refer-macros [param]]
          [cljs.test :as t :refer-macros [is are deftest testing]]])
     ))

(defn- fgensym [s c]
  (symbol (str s c)))

(defn make-predictable-gensym []
  (let [counter (atom 0)]
    (fn
      ([]
       (fgensym "G__" (dec (swap! counter inc))))
      ([s]
       (fgensym s (dec (swap! counter inc)))))))

(defn mock-source [db]
  (source/map->Source
    {:table :datoms
     :columns [:e :a :v :tx :added]
     :attribute-transform transforms/attribute-transform-string
     :constant-transform transforms/constant-transform-default
     :table-alias (comp (make-predictable-gensym) name)
     :make-constraints nil}))

(defn- expand [find]
  (let [context (context/->Context (mock-source nil) nil nil)
        parsed (query/parse find)]
    (query/find->sql-clause context parsed)))

(deftest test-basic-join
  (is (= {:select '([:datoms1.v :timestampMicros] [:datoms0.e :page]),
          :modifiers [:distinct],
          :from '[[:datoms datoms0]
                  [:datoms datoms1]],
          :where (list
                   :and
                   [:= :datoms1.e :datoms0.tx]
                   [:= :datoms0.a "page/starred"]
                   [:= :datoms0.v 1]
                   [:= :datoms1.a "db/txInstant"]
                   [:not
                    (list :and (list :> :datoms1.e (sql/param :latest)))])}
         (expand
           '[:find ?timestampMicros ?page :in $ ?latest :where
             [?page :page/starred true ?t]
             [?t :db/txInstant ?timestampMicros]
             (not [(> ?t ?latest)])]))))

(deftest test-pattern-not-join
  (is (= '{:select ([:datoms1.v :timestampMicros] [:datoms0.e :page]),
           :modifiers [:distinct],
           :from [[:datoms datoms0]
                  [:datoms datoms1]],
           :where (:and
                     [:= :datoms1.e :datoms0.tx]
                     [:= :datoms0.a "page/starred"]
                     [:= :datoms0.v 1]
                     [:= :datoms1.a "db/txInstant"]
                     [:not
                      [:exists
                       {:select [1],
                        :from [[:datoms datoms2]],
                        :where (:and
                                  [:= :datoms2.a "foo/bar"]
                                  [:= :datoms0.e :datoms2.e])}]])}
         (expand
           '[:find ?timestampMicros ?page :in $ ?latest :where
             [?page :page/starred true ?t]
             [?t :db/txInstant ?timestampMicros]
             (not [?page :foo/bar _])]))))
