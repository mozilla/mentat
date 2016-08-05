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
  (source/map->DatomsSource
    {:table :datoms
     :fulltext-table :fulltext_values
     :fulltext-view :fulltext_datoms
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

;; Note that clause ordering is not directly correlated to the output: cross-bindings end up
;; at the front. The SQL engine will do its own analysis. See `clauses/expand-where-from-bindings`.
(deftest test-not-clause-ordering-preserved
  (is (= {:select '([:datoms1.v :timestampMicros] [:datoms0.e :page]),
          :modifiers [:distinct],
          :from '[[:datoms datoms0]
                  [:datoms datoms1]],
          :where (list
                   :and
                   [:= :datoms1.e :datoms0.tx]
                   [:= :datoms0.a "page/starred"]
                   [:= :datoms0.v 1]
                   [:not
                    (list :and (list :> :datoms0.tx (sql/param :latest)))]
                   [:= :datoms1.a "db/txInstant"])}
         (expand
           '[:find ?timestampMicros ?page :in $ ?latest :where
             [?page :page/starred true ?t]
             (not [(> ?t ?latest)])
             [?t :db/txInstant ?timestampMicros]]))))

(deftest test-pattern-not-join-ordering-preserved
  (is (= '{:select ([:datoms2.v :timestampMicros] [:datoms0.e :page]),
           :modifiers [:distinct],
           :from [[:datoms datoms0]
                  [:datoms datoms2]],
           :where (:and
                     [:= :datoms2.e :datoms0.tx]
                     [:= :datoms0.a "page/starred"]
                     [:= :datoms0.v 1]
                     [:not
                      [:exists
                       {:select [1],
                        :from [[:datoms datoms1]],
                        :where (:and
                                  [:= :datoms1.a "foo/bar"]
                                  [:= :datoms0.e :datoms1.e])}]]
                     [:= :datoms2.a "db/txInstant"]
                     )}
         (expand
           '[:find ?timestampMicros ?page :in $ ?latest :where
             [?page :page/starred true ?t]
             (not [?page :foo/bar _])
             [?t :db/txInstant ?timestampMicros]]))))

(deftest test-single-or
  (is (= '{:select ([:datoms1.e :page]),
           :modifiers [:distinct],
           :from ([:datoms datoms0] [:datoms datoms1] [:datoms datoms2]),
           :where (:and
                     [:= :datoms1.e :datoms0.e]
                     [:= :datoms1.e :datoms2.v]
                     [:= :datoms0.a "page/url"]
                     [:= :datoms0.v "http://example.com/"]
                     [:= :datoms1.a "page/title"]
                     [:= :datoms2.a "page/loves"])}
         (expand
           '[:find ?page :in $ ?latest :where
             [?page :page/url "http://example.com/"]
             [?page :page/title ?title]
             (or
               [?entity :page/loves ?page])]))))

(deftest test-simple-or
  (is (= '{:select ([:datoms1.e :page]),
           :modifiers [:distinct],
           :from ([:datoms datoms0] [:datoms datoms1] [:datoms datoms2]),
           :where (:and
                     [:= :datoms1.e :datoms0.e]
                     [:= :datoms1.e :datoms2.v]
                     [:= :datoms0.a "page/url"]
                     [:= :datoms0.v "http://example.com/"]
                     [:= :datoms1.a "page/title"]
                     (:or
                        [:= :datoms2.a "page/likes"]
                        [:= :datoms2.a "page/loves"]))}
         (expand
           '[:find ?page :in $ ?latest :where
             [?page :page/url "http://example.com/"]
             [?page :page/title ?title]
             (or
               [?entity :page/likes ?page]
               [?entity :page/loves ?page])]))))
