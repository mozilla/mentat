(ns datomish.test.query
  (:require
     [datomish.query.context :as context]
     [datomish.query.source :as source]
     [datomish.query.transforms :as transforms]
     [datomish.query :as query]
     [datomish.schema :as schema]
     #?@(:clj
           [
          [honeysql.core :as sql :refer [param]]
          [clojure.test :as t :refer [is are deftest testing]]])
     #?@(:cljs
           [
          [honeysql.core :as sql :refer-macros [param]]
          [cljs.test :as t :refer-macros [is are deftest testing]]])
     )
  #?(:clj
       (:import [clojure.lang ExceptionInfo])))

(defn- fgensym [s c]
  (symbol (str s c)))

(defn make-predictable-gensym []
  (let [counter (atom 0)]
    (fn
      ([]
       (fgensym "G__" (dec (swap! counter inc))))
      ([s]
       (fgensym s (dec (swap! counter inc)))))))

(def simple-schema
  {:db/txInstant {:db/ident :db/txInstant
                  :db/valueType :long
                  :db/cardinality :db.cardinality/one}
   :foo/int {:db/ident :foo/int
             :db/valueType :db.type/integer
             :db/cardinality :db.cardinality/one}
   :foo/str {:db/ident :foo/str
             :db/valueType :db.type/string
             :db/cardinality :db.cardinality/many}})

(defn mock-source [db schema]
  (source/map->DatomsSource
    {:table :datoms
     :fulltext-table :fulltext_values
     :fulltext-view :all_datoms
     :columns [:e :a :v :tx :added]
     :attribute-transform transforms/attribute-transform-string
     :constant-transform transforms/constant-transform-default
     :table-alias (comp (make-predictable-gensym) name)
     :schema (schema/map->Schema
               {:schema schema
                :rschema nil})
     :make-constraints nil}))

(defn- expand [find schema]
  (let [context (context/->Context (mock-source nil schema) nil nil)
        parsed (query/parse find)]
    (query/find->sql-clause context parsed)))

(defn- populate [find schema]
  (let [context (context/->Context (mock-source nil schema) nil nil)
        parsed (query/parse find)]
    (query/find-into-context context parsed)))

(deftest test-type-extraction
  (testing "Variable entity."
    (is (= (:known-types (:cc (populate '[:find ?e ?v :in $ :where [?e :foo/int ?v]] simple-schema)))
           {'?v :db.type/integer
            '?e :db.type/ref})))
  (testing "Numeric entid."
    (is (= (:known-types (:cc (populate '[:find ?v :in $ :where [6 :foo/int ?v]] simple-schema)))
           {'?v :db.type/integer})))
  (testing "Keyword entity."
    (is (= (:known-types (:cc (populate '[:find ?v :in $ :where [:my/thing :foo/int ?v]] simple-schema)))
           {'?v :db.type/integer}))))

(deftest test-value-constant-constraint-descends-into-not-and-or
  (testing "Elision of types inside a join."
    (is (= '{:select ([:datoms0.e :e]
                      [:datoms0.v :v]),
             :modifiers [:distinct],
             :from [[:datoms datoms0]],
             :where (:and
                       [:= :datoms0.a "foo/int"]
                       [:not
                        [:exists
                         {:select [1],
                          :from [[:all_datoms all_datoms1]],
                          :where (:and
                                    [:= :all_datoms1.e 15]
                                    [:= :datoms0.v :all_datoms1.v])}]])}
           (expand
             '[:find ?e ?v :in $ :where
               [?e :foo/int ?v]
               (not [15 ?a ?v])]
             simple-schema))))

  (testing "Type collisions inside :not."
    (is (thrown-with-msg?
          ExceptionInfo #"\?v already has type :db\.type\/integer"
          (expand
            '[:find ?e ?v :in $ :where
              [?e :foo/int ?v]
              (not [15 :foo/str ?v])]
            simple-schema))))
  (testing "Type collisions inside :or"
    (is (thrown-with-msg?
          ExceptionInfo #"\?v already has type :db\.type\/integer"
          (expand
            '[:find ?e ?v :in $ :where
              [?e :foo/int ?v]
              (or
                [15 :foo/str ?v]
                [10 :foo/int ?v])]
            simple-schema)))))

(deftest test-type-collision
  (let [find '[:find ?e ?v :in $
               :where
               [?e :foo/int ?v]
               [?x :foo/str ?v]]]
     (is (thrown-with-msg?
           ExceptionInfo #"\?v already has type :db\.type\/integer"
           (populate find simple-schema)))))

(deftest test-value-constant-constraint
  (is (= {:select '([:all_datoms0.e :foo]),
          :modifiers [:distinct],
          :from '[[:all_datoms all_datoms0]],
          :where (list :and
                       (list :or
                             [:= :all_datoms0.value_type_tag 0]
                             [:= :all_datoms0.value_type_tag 5])
                       [:= :all_datoms0.v 99])}
         (expand
           '[:find ?foo :in $ :where
             [?foo _ 99]]
           simple-schema))))

(deftest test-value-constant-constraint-elided-using-schema
  (testing "There's no need to produce value_type_tag constraints when the attribute is specified."
    (is
      (= '{:select ([:datoms1.v :timestampMicros] [:datoms0.e :page]),
           :modifiers [:distinct],
           :from [[:datoms datoms0]
                  [:datoms datoms1]],
           :where (:and
                     ;; We don't need a type check on the range of page/starred...
                     [:= :datoms0.a "page/starred"]
                     [:= :datoms0.v 1]
                     [:= :datoms1.a "db/txInstant"]
                     [:not
                      [:exists
                       {:select [1],
                        :from [[:datoms datoms2]],
                        :where (:and
                                  [:= :datoms2.a "foo/bar"]
                                  [:= :datoms0.e :datoms2.e])}]]
                     [:= :datoms0.tx :datoms1.e])}
         (expand
           '[:find ?timestampMicros ?page :in $ ?latest :where
             [?page :page/starred true ?t]
             [?t :db/txInstant ?timestampMicros]
             (not [?page :foo/bar _])]

           (merge
             simple-schema
             {:page/starred {:db/valueType :db.type/boolean
                             :db/ident :page/starred
                             :db/cardinality :db.cardinality/one}}))))))

(deftest test-basic-join
  (is (= {:select '([:datoms1.v :timestampMicros] [:datoms0.e :page]),
          :modifiers [:distinct],
          :from '[[:datoms datoms0]
                  [:datoms datoms1]],
          :where (list
                   :and
                   [:= :datoms0.a "page/starred"]
                   [:= :datoms0.value_type_tag 1]   ; boolean
                   [:= :datoms0.v 1]
                   [:= :datoms1.a "db/txInstant"]
                   [:not
                    (list :and (list :> :datoms0.tx (sql/param :latest)))]
                   [:= :datoms0.tx :datoms1.e])}
         (expand
           '[:find ?timestampMicros ?page :in $ ?latest :where
             [?page :page/starred true ?t]
             [?t :db/txInstant ?timestampMicros]
             (not [(> ?t ?latest)])]
           simple-schema))))

(deftest test-pattern-not-join
  (is (= '{:select ([:datoms1.v :timestampMicros] [:datoms0.e :page]),
           :modifiers [:distinct],
           :from [[:datoms datoms0]
                  [:datoms datoms1]],
           :where (:and
                     [:= :datoms0.a "page/starred"]
                     [:= :datoms0.value_type_tag 1]   ; boolean
                     [:= :datoms0.v 1]
                     [:= :datoms1.a "db/txInstant"]
                     [:not
                      [:exists
                       {:select [1],
                        :from [[:datoms datoms2]],
                        :where (:and
                                  [:= :datoms2.a "foo/bar"]
                                  [:= :datoms0.e :datoms2.e])}]]
                     [:= :datoms0.tx :datoms1.e])}
         (expand
           '[:find ?timestampMicros ?page :in $ ?latest :where
             [?page :page/starred true ?t]
             [?t :db/txInstant ?timestampMicros]
             (not [?page :foo/bar _])]
           simple-schema))))

;; Note that clause ordering is not directly correlated to the output: cross-bindings end up
;; at the front. The SQL engine will do its own analysis. See `clauses/expand-where-from-bindings`.
(deftest test-not-clause-ordering-preserved
  (is (= {:select '([:datoms1.v :timestampMicros] [:datoms0.e :page]),
          :modifiers [:distinct],
          :from '[[:datoms datoms0]
                  [:datoms datoms1]],
          :where (list
                   :and
                   [:= :datoms0.a "page/starred"]
                   [:= :datoms0.value_type_tag 1]   ; boolean
                   [:= :datoms0.v 1]
                   [:not
                    (list :and (list :> :datoms0.tx (sql/param :latest)))]
                   [:= :datoms1.a "db/txInstant"]
                   [:= :datoms0.tx :datoms1.e]
                   )}
         (expand
           '[:find ?timestampMicros ?page :in $ ?latest :where
             [?page :page/starred true ?t]
             (not [(> ?t ?latest)])
             [?t :db/txInstant ?timestampMicros]]
           simple-schema))))

(deftest test-pattern-not-join-ordering-preserved
  (is (= '{:select ([:datoms2.v :timestampMicros] [:datoms0.e :page]),
           :modifiers [:distinct],
           :from [[:datoms datoms0]
                  [:datoms datoms2]],
           :where (:and
                     [:= :datoms0.a "page/starred"]
                     [:= :datoms0.value_type_tag 1]   ; boolean
                     [:= :datoms0.v 1]
                     [:not
                      [:exists
                       {:select [1],
                        :from [[:datoms datoms1]],
                        :where (:and
                                  [:= :datoms1.a "foo/bar"]
                                  [:= :datoms0.e :datoms1.e])}]]
                     [:= :datoms2.a "db/txInstant"]
                     [:= :datoms0.tx :datoms2.e]
                     )}
         (expand
           '[:find ?timestampMicros ?page :in $ ?latest :where
             [?page :page/starred true ?t]
             (not [?page :foo/bar _])
             [?t :db/txInstant ?timestampMicros]]
           simple-schema))))

(deftest test-single-or
  (is (= '{:select ([:datoms0.e :page]),
           :modifiers [:distinct],
           :from ([:datoms datoms0] [:datoms datoms1] [:datoms datoms2]),
           :where (:and
                     [:= :datoms0.a "page/url"]
                     [:= :datoms0.value_type_tag 10]
                     [:= :datoms0.v "http://example.com/"]
                     [:= :datoms1.a "page/title"]
                     [:= :datoms2.a "page/loves"]
                     [:= :datoms0.e :datoms1.e]
                     [:= :datoms0.e :datoms2.v])}
         (expand
           '[:find ?page :in $ ?latest :where
             [?page :page/url "http://example.com/"]
             [?page :page/title ?title]
             (or
               [?entity :page/loves ?page])]
           simple-schema))))

(deftest test-simple-or
  (is (= '{:select ([:datoms0.e :page]),
           :modifiers [:distinct],
           :from ([:datoms datoms0] [:datoms datoms1] [:datoms datoms2]),
           :where (:and
                     [:= :datoms0.a "page/url"]
                     [:= :datoms0.value_type_tag 10]
                     [:= :datoms0.v "http://example.com/"]
                     [:= :datoms1.a "page/title"]
                     (:or
                        [:= :datoms2.a "page/likes"]
                        [:= :datoms2.a "page/loves"])
                     [:= :datoms0.e :datoms1.e]
                     [:= :datoms0.e :datoms2.v])}
         (expand
           '[:find ?page :in $ ?latest :where
             [?page :page/url "http://example.com/"]
             [?page :page/title ?title]
             (or
               [?entity :page/likes ?page]
               [?entity :page/loves ?page])]
           simple-schema))))

(deftest test-tag-projection
  (is (= '{:select ([:datoms0.e :page]
                    [:datoms1.v :thing]
                    [:datoms1.value_type_tag :_thing_type_tag]),
           :modifiers [:distinct],
           :from ([:datoms datoms0]
                  [:datoms datoms1]),
           :where (:and
                     [:= :datoms0.a "page/url"]
                     [:= :datoms0.value_type_tag 10]
                     [:= :datoms0.v "http://example.com/"]
                     (:or
                        [:= :datoms1.a "page/likes"]
                        [:= :datoms1.a "page/loves"])
                     [:= :datoms0.e :datoms1.e])}
         (expand
           '[:find ?page ?thing :in $ ?latest :where
             [?page :page/url "http://example.com/"]
             (or
               [?page :page/likes ?thing]
               [?page :page/loves ?thing])]
           simple-schema))))
