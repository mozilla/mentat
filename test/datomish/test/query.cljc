(ns datomish.test.query
  #?(:cljs
     (:require-macros
      [datomish.pair-chan :refer [go-pair <?]]
      [datomish.node-tempfile-macros :refer [with-tempfile]]
      [cljs.core.async.macros :as a :refer [go]]))
  (:require
     [datomish.query.cc :as cc]
     [datomish.query.context :as context]
     [datomish.query.source :as source]
     [datomish.query.transforms :as transforms]
     [datomish.query :as query]
     [datomish.db :as db]
     [datomish.schema :as schema]
     [datomish.transact :as transact]
     [datomish.api :as d]
     #?@(:clj
           [[datomish.pair-chan :refer [go-pair <?]]
            [datomish.jdbc-sqlite]
            [datomish.test-macros :refer [deftest-db]]
            [honeysql.core :as sql :refer [param]]
            [tempfile.core :refer [tempfile with-tempfile]]
            [clojure.test :as t :refer [is are deftest testing]]])
     #?@(:cljs
           [[datomish.js-sqlite]
            [datomish.test-macros :refer-macros [deftest-db]]
            [honeysql.core :as sql :refer-macros [param]]
            [datomish.node-tempfile :refer [tempfile]]
            [cljs.test :as t :refer-macros [is are deftest testing]]]))
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
  [{:db/id (d/id-literal :db.part/user)
    :db.install/_attribute :db.part/db
    :db/ident :db/txInstant
    :db/valueType :db.type/long
    :db/cardinality :db.cardinality/one}
   {:db/id (d/id-literal :db.part/user)
    :db.install/_attribute :db.part/db
    :db/ident :foo/bar
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/many}
   {:db/id (d/id-literal :db.part/user)
    :db.install/_attribute :db.part/db
    :db/ident :foo/int
    :db/valueType :db.type/long
    :db/cardinality :db.cardinality/one}
   {:db/id (d/id-literal :db.part/user)
    :db.install/_attribute :db.part/db
    :db/ident :foo/str
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/many}])

(def page-schema
  [{:db/id (d/id-literal :db.part/user)
    :db.install/_attribute :db.part/db
    :db/ident :page/loves
    :db/valueType :db.type/ref
    :db/cardinality :db.cardinality/many}
   {:db/id (d/id-literal :db.part/user)
    :db.install/_attribute :db.part/db
    :db/ident :page/likes
    :db/valueType :db.type/ref
    :db/cardinality :db.cardinality/many}
   {:db/id (d/id-literal :db.part/user)
    :db.install/_attribute :db.part/db
    :db/ident :page/url
    :db/valueType :db.type/string
    :db/unique :db.unique/identity
    :db/cardinality :db.cardinality/one}
   {:db/id (d/id-literal :db.part/user)
    :db.install/_attribute :db.part/db
    :db/ident :page/title
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one}
   {:db/id (d/id-literal :db.part/user)
    :db.install/_attribute :db.part/db
    :db/ident :page/save
    :db/valueType :db.type/ref
    :db/unique :db.unique/identity            ; A save uniquely identifies a page.
    :db/cardinality :db.cardinality/many}
   {:db/id (d/id-literal :db.part/user)
    :db.install/_attribute :db.part/db
    :db/ident :page/starred
    :db/valueType :db.type/boolean
    :db/cardinality :db.cardinality/one}])

(def aggregate-schema
  [{:db/id (d/id-literal :db.part/user)
    :db.install/_attribute :db.part/db
    :db/ident :page/url
    :db/valueType :db.type/string
    :db/unique :db.unique/identity
    :db/cardinality :db.cardinality/one}
   {:db/id (d/id-literal :db.part/user)
    :db.install/_attribute :db.part/db
    :db/ident :foo/points
    :db/valueType :db.type/long
    :db/cardinality :db.cardinality/many}
   {:db/id (d/id-literal :db.part/user)
    :db.install/_attribute :db.part/db
    :db/ident :foo/visitedAt
    :db/valueType :db.type/instant
    :db/cardinality :db.cardinality/many}])

(def save-schema
  [{:db/id (d/id-literal :db.part/user)
    :db.install/_attribute :db.part/db
    :db/cardinality :db.cardinality/one
    :db/valueType :db.type/string
    :db/fulltext true
    :db/ident :save/title}
   {:db/id (d/id-literal :db.part/user)
    :db.install/_attribute :db.part/db
    :db/cardinality :db.cardinality/one
    :db/valueType :db.type/string
    :db/fulltext true
    :db/ident :save/excerpt}
   {:db/id (d/id-literal :db.part/user)
    :db.install/_attribute :db.part/db
    :db/cardinality :db.cardinality/one
    :db/valueType :db.type/string
    :db/fulltext true
    :db/ident :save/content}])

(def schema-with-page
  (concat
    simple-schema
    page-schema))

(defn mock-source [db]
  (assoc (datomish.db/datoms-source db)
         :table-alias (comp (make-predictable-gensym) name)))

(defn conn->context [conn]
  (context/make-context (mock-source (d/db conn))))

(defn- expand [find conn]
  (let [context (conn->context conn)
        parsed (query/parse find)]
    (query/find->sql-clause context parsed)))

(defn- populate [find conn]
  (let [context (conn->context conn)
        parsed (query/parse find)]
    (query/find-into-context context parsed)))

(defn <initialize-with-schema [conn schema]
  (go-pair
    (let [tx (<? (d/<transact! conn schema))]
      (let [idents (map :db/ident schema)
            db (d/db conn)]
        (into {}
          (map (fn [ident]
                 [ident (d/entid db ident)])
               idents))))))

(deftest-db test-type-extraction conn
  ;; We expect to be able to look up the default types.
  (is (integer? (d/entid (d/db conn) :db.type/ref)))
  (is (integer? (d/entid (d/db conn) :db.type/long)))

  ;; Add our own schema.
  (<? (<initialize-with-schema conn simple-schema))
  (testing "Variable entity."
    (is (= (->
             (populate '[:find ?e ?v :in $ :where [?e :foo/int ?v]] conn)
             :cc :known-types)
           {'?v :db.type/long
            '?e :db.type/ref})))

  (testing "Numeric entid."
    (is (= (->
             (populate '[:find ?v :in $ :where [6 :foo/int ?v]] conn)
             :cc :known-types)
           {'?v :db.type/long})))

  (testing "Keyword entity."
    (is (= (->
             (populate '[:find ?v :in $ :where [:my/thing :foo/int ?v]] conn)
             :cc :known-types)
           {'?v :db.type/long}))))

(deftest-db test-value-constant-constraint-descends-into-not-and-or conn
  (let [attrs (<? (<initialize-with-schema conn simple-schema))]
    (testing "Elision of types inside a join."
      (is (= {:select '([:datoms0.e :e]
                        [:datoms0.v :v]),
               :modifiers [:distinct],
               :from [[:datoms 'datoms0]],
               :where (list :and
                         [:= :datoms0.a (:foo/int attrs)]
                         [:not
                          [:exists
                           {:select [1],
                            :from [[:all_datoms 'all_datoms1]],
                            :where (list :and
                                      [:= :all_datoms1.e 999]
                                      [:= :datoms0.v :all_datoms1.v])}]])}
             (expand
               '[:find ?e ?v :in $ :where
                 [?e :foo/int ?v]
                 (not [999 ?a ?v])]
               conn))))

    (testing "Type collisions inside :not."
      (is (thrown-with-msg?
            ExceptionInfo #"v already has type :db.type.long"
            (expand
              '[:find ?e ?v :in $ :where
                [?e :foo/int ?v]
                (not [999 :foo/str ?v])]
              conn))))

    (testing "Type collisions inside :or"
      (is (thrown-with-msg?
            ExceptionInfo #"v already has type :db.type.long"
            (expand
              '[:find ?e ?v :in $ :where
                [?e :foo/int ?v]
                (or
                  [999 :foo/str ?v]
                  [666 :foo/int ?v])]
              conn))))))

(deftest-db test-type-collision conn
  (<? (<initialize-with-schema conn simple-schema))
  (let [find '[:find ?e ?v :in $
               :where
               [?e :foo/int ?v]
               [?x :foo/str ?v]]]
     (is (thrown-with-msg?
           ExceptionInfo #"v already has type :db.type.long"
           (populate find conn)))))

(deftest-db test-value-constant-constraint conn
  (<? (<initialize-with-schema conn simple-schema))
  (is (= {:select '([:all_datoms0.e :foo]),
          :modifiers [:distinct],
          :from '[[:all_datoms all_datoms0]],
          :where (list :and
                       (list :or
                             [:= :all_datoms0.value_type_tag 0]
                             ;; In CLJS, this can also be an `instant`.
                             #?@(:cljs [[:= :all_datoms0.value_type_tag 4]])
                             [:= :all_datoms0.value_type_tag 5])
                       [:= :all_datoms0.v 99])}
         (expand
           '[:find ?foo :in $ :where
             [?foo _ 99]]
           conn))))

(deftest-db test-value-constant-constraint-elided-using-schema conn
  (let [attrs (<? (<initialize-with-schema conn schema-with-page))]
    (testing "Our attributes were interned."
      (is (integer? (d/entid (d/db conn) :foo/str)))
      (is (integer? (d/entid (d/db conn) :page/starred))))

    (testing "There's no need to produce value_type_tag constraints when the attribute is specified."
      (is
        (= {:select '([:datoms1.v :timestampMicros] [:datoms0.e :page]),
            :modifiers [:distinct],
            :from [[:datoms 'datoms0]
                   [:datoms 'datoms1]],
            :where (list :and
                       ;; We don't need a type check on the range of page/starred...
                       [:= :datoms0.a (:page/starred attrs)]
                       [:= :datoms0.v 1]
                       [:= :datoms1.a (:db/txInstant attrs)]
                       [:not
                        [:exists
                         {:select [1],
                          :from [[:datoms 'datoms2]],
                          :where (list :and
                                    [:= :datoms2.a (:foo/bar attrs)]
                                    [:= :datoms0.e :datoms2.e])}]]
                       [:= :datoms0.tx :datoms1.e])}
           (expand
             '[:find ?timestampMicros ?page :in $ ?latest :where
               [?page :page/starred true ?t]
               [?t :db/txInstant ?timestampMicros]
               (not [?page :foo/bar _])]

             conn))))))

(deftest-db test-basic-join conn
  ;; Note that we use a schema without :page/starred, so we
  ;; don't know what type it is.
  (let [attrs (<? (<initialize-with-schema conn simple-schema))]
    (is (= {:select '([:datoms1.v :timestampMicros] [:datoms0.e :page]),
            :modifiers [:distinct],
            :from '([:datoms datoms0]
                    [:datoms datoms1]),
            :where (list
                     :and
                     ;; Note that :page/starred is literal, because
                     ;; it's not present in the interned schema.
                     [:= :datoms0.a :page/starred]
                     [:= :datoms0.value_type_tag 1]   ; boolean
                     [:= :datoms0.v 1]
                     [:= :datoms1.a (:db/txInstant attrs)]
                     [:not
                      (list :and (list :> :datoms0.tx (sql/param :latest)))]
                     [:= :datoms0.tx :datoms1.e])}
           (expand
             '[:find ?timestampMicros ?page :in $ ?latest :where
               [?page :page/starred true ?t]
               [?t :db/txInstant ?timestampMicros]
               (not [(> ?t ?latest)])]
             conn)))))

(deftest-db test-pattern-not-join conn
  (let [attrs (<? (<initialize-with-schema conn simple-schema))]
    (is (= {:select '([:datoms1.v :timestampMicros] [:datoms0.e :page]),
            :modifiers [:distinct],
            :from [[:datoms 'datoms0]
                   [:datoms 'datoms1]],
            :where (list
                     :and
                     ;; Note that :page/starred is literal, because
                     ;; it's not present in the interned schema.
                     [:= :datoms0.a :page/starred]
                     [:= :datoms0.value_type_tag 1]   ; boolean
                     [:= :datoms0.v 1]
                     [:= :datoms1.a (:db/txInstant attrs)]
                     [:not
                      [:exists
                       {:select [1],
                        :from [[:datoms 'datoms2]],
                        :where (list :and
                                  [:= :datoms2.a (:foo/bar attrs)]
                                  [:= :datoms0.e :datoms2.e])}]]
                     [:= :datoms0.tx :datoms1.e])}
           (expand
             '[:find ?timestampMicros ?page :in $ ?latest :where
               [?page :page/starred true ?t]
               [?t :db/txInstant ?timestampMicros]
               (not [?page :foo/bar _])]
             conn)))))

;; Note that clause ordering is not directly correlated to the output: cross-bindings end up
;; at the front. The SQL engine will do its own analysis. See `clauses/expand-where-from-bindings`.
(deftest-db test-not-clause-ordering-preserved conn
  (let [attrs (<? (<initialize-with-schema conn schema-with-page))]
    (is (= {:select '([:datoms1.v :timestampMicros] [:datoms0.e :page]),
            :modifiers [:distinct],
            :from '[[:datoms datoms0]
                    [:datoms datoms1]],
            :where (list
                     :and
                     ;; We don't need a value tag constraint -- we know the range of the attribute.
                     [:= :datoms0.a (:page/starred attrs)]
                     [:= :datoms0.v 1]
                     [:not
                      (list :and (list :> :datoms0.tx (sql/param :latest)))]
                     [:= :datoms1.a (:db/txInstant attrs)]
                     [:= :datoms0.tx :datoms1.e]
                     )}
           (expand
             '[:find ?timestampMicros ?page :in $ ?latest :where
               [?page :page/starred true ?t]
               (not [(> ?t ?latest)])
               [?t :db/txInstant ?timestampMicros]]
             conn)))))

(deftest-db test-pattern-not-join-ordering-preserved conn
  (let [attrs (<? (<initialize-with-schema conn schema-with-page))]
    (is (= {:select '([:datoms2.v :timestampMicros] [:datoms0.e :page]),
            :modifiers [:distinct],
            :from [[:datoms 'datoms0]
                   [:datoms 'datoms2]],
            :where (list :and
                     ;; We don't need a value tag constraint -- we know the range of the attribute.
                      [:= :datoms0.a (:page/starred attrs)]
                      [:= :datoms0.v 1]
                      [:not
                       [:exists
                        {:select [1],
                         :from [[:datoms 'datoms1]],
                         :where (list :and
                                   [:= :datoms1.a (:foo/bar attrs)]
                                   [:= :datoms0.e :datoms1.e])}]]
                      [:= :datoms2.a (:db/txInstant attrs)]
                      [:= :datoms0.tx :datoms2.e])}
           (expand
             '[:find ?timestampMicros ?page :in $ ?latest :where
               [?page :page/starred true ?t]
               (not [?page :foo/bar _])
               [?t :db/txInstant ?timestampMicros]]
             conn)))))

(deftest-db test-single-or conn
  (let [attrs (<? (<initialize-with-schema conn schema-with-page))]
    (is (= {:select '([:datoms0.e :page]),
            :modifiers [:distinct],
            :from '([:datoms datoms0] [:datoms datoms1] [:datoms datoms2]),
            :where (list :and
                      [:= :datoms0.a (:page/url attrs)]
                      [:= :datoms0.v "http://example.com/"]
                      [:= :datoms1.a (:page/title attrs)]
                      [:= :datoms2.a (:page/loves attrs)]
                      [:= :datoms0.e :datoms1.e]
                      [:= :datoms0.e :datoms2.v])}
           (expand
             '[:find ?page :in $ ?latest :where
               [?page :page/url "http://example.com/"]
               [?page :page/title ?title]
               (or
                 [?entity :page/loves ?page])]
             conn)))))

(deftest-db test-simple-or conn
  (let [attrs (<? (<initialize-with-schema conn schema-with-page))]
    (is (= {:select '([:datoms0.e :page]),
            :modifiers [:distinct],
            :from '([:datoms datoms0] [:datoms datoms1] [:datoms datoms2]),
            :where (list :and
                      [:= :datoms0.a (:page/url attrs)]
                      [:= :datoms0.v "http://example.com/"]
                      [:= :datoms1.a (:page/title attrs)]
                      (list :or
                         [:= :datoms2.a (:page/likes attrs)]
                         [:= :datoms2.a (:page/loves attrs)])
                      [:= :datoms0.e :datoms1.e]
                      [:= :datoms0.e :datoms2.v])}
           (expand
             '[:find ?page :in $ ?latest :where
               [?page :page/url "http://example.com/"]
               [?page :page/title ?title]
               (or
                 [?entity :page/likes ?page]
                 [?entity :page/loves ?page])]
             conn)))))

(deftest-db test-complex-or conn
  (let [attrs (<? (<initialize-with-schema
                    conn
                    (concat save-schema schema-with-page)))]
    (is (= {:select '([:datoms0.e :page] [:datoms0.v :starred]),
            :modifiers [:distinct],
            :where (list :and
                         [:= :datoms0.a (:page/starred attrs)]
                         [:= :datoms0.e :orjoin1.page])
            :from
            [[:datoms 'datoms0]
             [{:union
               (list
                 ;; These first two will be merged together when
                 ;; we implement simple pattern alternation within
                 ;; complex `or`.
                 {:from '([:datoms datoms2]),
                  :select '([:datoms2.e :page]),
                  :where (list :and
                               [:= :datoms2.a (:page/url attrs)]
                               [:= :datoms0.e :datoms2.e]
                               [:= (sql/param :s) :datoms2.v])}
                 {:from '([:datoms datoms3]),
                  :select '([:datoms3.e :page]),
                  :where (list :and
                               [:= :datoms3.a (:page/title attrs)]
                               [:= :datoms0.e :datoms3.e]
                               [:= (sql/param :s) :datoms3.v])}

                 {:from '([:datoms datoms4]
                          [:fulltext_datoms fulltext_datoms5]
                          [:fulltext_datoms fulltext_datoms6]),
                  :select '([:datoms4.e :page]),
                  :where (list :and
                               [:= :datoms4.a (:page/save attrs)]
                               [:= :fulltext_datoms5.a (:save/excerpt attrs)]
                               [:= :fulltext_datoms6.a (:save/content attrs)]
                               [:= :datoms4.v :fulltext_datoms5.e]
                               [:= :datoms4.v :fulltext_datoms6.e]
                               [:= :fulltext_datoms5.v :fulltext_datoms6.v]
                               [:= :datoms0.e :datoms4.e]
                               [:= (sql/param :s) :fulltext_datoms5.v])})}
              'orjoin1]]}
           (expand
             '[:find ?page ?starred :in $ ?s :where
               [?page :page/starred ?starred]
               (or-join [?page]
                 [?page :page/url ?s]
                 [?page :page/title ?s]
                 (and [?page :page/save ?saved]
                      [?saved :save/excerpt ?s]
                      [?saved :save/content ?s]))]
             conn)))))

(defn tag-clauses [column input]
  (let [codes (cc/->tag-codes input)]
    (if (= 1 (count codes))
      [:= column (first codes)]
      (cons :or (map (fn [tag]
                       [:= column tag])
                     codes)))))

(deftest-db test-url-tag conn
  (let [attrs (<? (<initialize-with-schema conn schema-with-page))]
    (is (= {:select '([:all_datoms0.e :page]
                      [:datoms1.v :thing]),
            :modifiers [:distinct],
            :from '([:all_datoms all_datoms0]
                    [:datoms datoms1]),
            :where (list
                     :and
                     (tag-clauses :all_datoms0.value_type_tag "http://example.com/")
                     [:= :all_datoms0.v "http://example.com/"]
                     (list
                       :or
                       [:= :datoms1.a (:page/likes attrs)]
                       [:= :datoms1.a (:page/loves attrs)])
                     [:= :all_datoms0.e :datoms1.e])}
           (expand
             '[:find ?page ?thing :in $ ?latest :where
               [?page _ "http://example.com/"]
               (or
                 [?page :page/likes ?thing]
                 [?page :page/loves ?thing])]
             conn)))))

(deftest-db test-tag-projection conn
  (let [attrs (<? (<initialize-with-schema conn schema-with-page))]
    (is (= {:select '([:all_datoms0.e :page]
                      [:all_datoms0.v :thing]
                      [:all_datoms0.value_type_tag :_thing_type_tag]),
            :modifiers [:distinct],
            :from '([:all_datoms all_datoms0])}
           (expand
             '[:find ?page ?thing :in $ :where
               [?page _ ?thing]]
             conn)))))

(deftest-db test-aggregates conn
  (let [attrs (<? (<initialize-with-schema conn aggregate-schema))
        context
        (populate '[:find ?date (max ?v)
                    :with ?e
                    :in $ ?then
                    :where
                    [?e :foo/visitedAt ?date]
                    [(> ?date ?then)]
                    [?e :foo/points ?v]] conn)]

    (is (= (:group-by-vars context)
           ['?date '?e]))

    (is (= {:select '([:preag.date :date]
                      [:%max.preag.v :_max_v])
            :modifiers [:distinct]
            :group-by '(:date :e),
            :with {:preag
                   {:select '([:datoms0.v :date]
                               [:datoms1.v :v]
                               [:datoms0.e :e]),          ; Because we need to group on it.
                     :modifiers [:distinct],
                     :from '([:datoms datoms0] [:datoms datoms1]),
                     :where (list
                              :and
                              [:= :datoms0.a (:foo/visitedAt attrs)]
                              (list :> :datoms0.v (sql/param :then))
                              [:= :datoms1.a (:foo/points attrs)]
                              [:= :datoms0.e :datoms1.e])}}
            :from [:preag]}
           (query/context->sql-clause context)))))

(deftest-db test-get-else conn
  (let [attrs (<? (<initialize-with-schema conn page-schema))]
    (is (= {:select (list
                      [:datoms0.e :page]
                      [{:select [(sql/call
                                   :coalesce
                                   {:select [:v],
                                    :from [:datoms],
                                    :where [:and
                                            [:= 'a 65540]
                                            [:= 'e :datoms0.e]],
                                    :limit 1}
                                   "No title")],
                        :limit 1} :title]),
            :modifiers [:distinct],
            :from '([:datoms datoms0]),
            :where (list :and [:= :datoms0.a 65539])}
           (expand '[:find ?page ?title :in $
                     :where
                     [?page :page/url _]
                     [(get-else $ ?page :page/title "No title") ?title]]
                   conn)))))

(deftest-db test-limit-order conn
  (let [attrs (<? (<initialize-with-schema conn aggregate-schema))
        context
        (populate '[:find ?date (max ?v)
                    :with ?e
                    :in $ ?then
                    :where
                    [?e :foo/visitedAt ?date]
                    [(> ?date ?then)]
                    [?e :foo/points ?v]] conn)]
    (is
      (thrown-with-msg?
        ExceptionInfo #"Invalid limit \?x"
        (query/options-into-context context '?x [[:date :asc]])))
    (is
      (thrown-with-msg?
        ExceptionInfo #"Ordering expressions must be :asc or :desc"
        (query/context->sql-clause
          (query/options-into-context context 10 [[:date :upsidedown]]))))
    (is
      (thrown-with-msg?
        ExceptionInfo #"Ordering vars \#\{:nonexistent\} not a subset"
        (query/context->sql-clause
          (query/options-into-context context 10 [[:nonexistent :desc]]))))
    (is
      (=
        {:limit 10}
        (select-keys
          (query/context->sql-clause
            (query/options-into-context context 10 nil))
          [:order-by :limit]
          )))
    (is
      (=
        {:order-by [[:date :asc]]}
        (select-keys
          (query/context->sql-clause
            (query/options-into-context context nil [[:date :asc]]))
          [:order-by :limit]
          )))
    (is
      (=
        {:limit 10
         :order-by [[:date :asc]]}
        (select-keys
          (query/context->sql-clause
            (query/options-into-context context 10 [[:date :asc]]))
          [:order-by :limit]
          )))))

(deftest-db test-parsing-fulltext conn
  (let [attrs (<? (<initialize-with-schema conn save-schema))]
    (is (= {:select (list [:datoms1.e :save]),
            :modifiers [:distinct],
            :from (list [:fulltext_values 'fulltext_values0]
                        [:datoms 'datoms1]),
            :where (list :and
                         [:match :fulltext_values0.fulltext_values "something"]
                         [:= :datoms1.v :fulltext_values0.rowid]
                         [:= :datoms1.a (:save/title attrs)])}
           (expand {:find '[?save]
                    :in '[$]
                    :where [[(list 'fulltext
                                   '$
                                   :save/title
                                   "something")
                             '[[?save]]]]}
                  conn)))
    (is (= {:select (list [:datoms1.e :save]),
            :modifiers [:distinct],
            :from (list [:fulltext_values 'fulltext_values0]
                        [:datoms 'datoms1]),
            :where (list :and
                         [:match :fulltext_values0.fulltext_values "something"]
                         [:= :datoms1.v :fulltext_values0.rowid]
                         (list :or
                               [:= :datoms1.a (:save/title attrs)]
                               [:= :datoms1.a (:save/excerpt attrs)]))}
           (expand {:find '[?save]
                    :in '[$]
                    :where [[(list 'fulltext
                                   '$
                                   #{:save/title :save/excerpt}
                                   "something")
                             '[[?save]]]]}
                  conn)))))

(deftest-db test-find-specs-expansion conn
  (let [attrs (<? (<initialize-with-schema conn save-schema))]
    ;; Relation.
    (is (= {:select (list [:fulltext_datoms0.v :title])
            :modifiers [:distinct]
            :from (list [:fulltext_datoms 'fulltext_datoms0])
            :where (list :and [:= :fulltext_datoms0.a (:save/title attrs)])}
           (expand [:find '?title
                    :in '$
                    :where '[?save :save/title ?title]]
                   conn)))

    ;; Tuple. We expect only one result, and indeed we only take one.
    ;; No need for :distinct in this case!
    (is (= {:select (list [:fulltext_datoms0.v :title])
            :modifiers []
            :limit 1
            :from (list [:fulltext_datoms 'fulltext_datoms0])
            :where (list :and [:= :fulltext_datoms0.a (:save/title attrs)])}
           (expand [:find '[?title]
                    :in '$
                    :where '[?save :save/title ?title]]
                   conn)))

    ;; Scalar. As with the tuple form, we expect only one result.
    (is (= {:select (list [:fulltext_datoms0.v :title])
            :modifiers []
            :limit 1
            :from (list [:fulltext_datoms 'fulltext_datoms0])
            :where (list :and [:= :fulltext_datoms0.a (:save/title attrs)])}
           (expand [:find '?title '.
                    :in '$
                    :where '[?save :save/title ?title]]
                   conn)))

    ;; Collection.
    (is (= {:select (list [:fulltext_datoms0.v :title])
            :modifiers [:distinct]
            :from (list [:fulltext_datoms 'fulltext_datoms0])
            :where (list :and [:= :fulltext_datoms0.a (:save/title attrs)])}
           (expand [:find '[?title ...]
                    :in '$
                    :where '[?save :save/title ?title]]
                   conn)))))

(defn orderless=
  "Compare two arrays regardless of order."
  [a b]
  (= (set a) (set b)))

(deftest-db test-find-specs-empty-results conn
  (let [attrs (<? (<initialize-with-schema conn save-schema))]
    ;; Relation.
    (is (= []
           (<? (d/<q (d/db conn)
                     [:find '?title
                      :in '$
                      :where '[?save :save/title ?title]]))))

    ;; Tuple.
    (is (nil? (<? (d/<q (d/db conn)
                        [:find '[?title]
                         :in '$
                         :where '[?save :save/title ?title]]))))

    ;; Scalar.
    (is (nil? (<? (d/<q (d/db conn)
                        [:find '?title '.
                         :in '$
                         :where '[?save :save/title ?title]]))))

    ;; Collection.
    (is (= []
           (<? (d/<q (d/db conn)
                     [:find '[?title ...]
                      :in '$
                      :where '[?save :save/title ?title]]))))))

(deftest-db test-find-specs-result-shape conn
  (let [attrs (<? (<initialize-with-schema conn save-schema))]
    ;; Add some data.
    (<? (d/<transact! conn
                      [{:db/id (d/id-literal :db.part/user -1)
                        :save/title "Some page title"}
                       {:db/id (d/id-literal :db.part/user -2)
                        :save/title "A different page"}]))

    ;; Relation.
    (is (orderless=
          [["A different page"]["Some page title"]]
          (<? (d/<q (d/db conn)
                    [:find '?title
                     :in '$
                     :where '[?save :save/title ?title]]
                    {:order-by [[:title :asc]]}))))

    ;; Tuple. We expect only one result, and indeed we only take one.
    ;; No need for :distinct in this case!
    (let [result (<? (d/<q (d/db conn)
                           [:find '[?title]
                            :in '$
                            :where '[?save :save/title ?title]]
                           {:order-by [[:title :asc]]}))]
      (is (= ["A different page"] result)))

    ;; Scalar. As with the tuple form, we expect only one result.
    (let [result (<? (d/<q (d/db conn)
                           [:find '?title '.
                            :in '$
                            :where '[?save :save/title ?title]]
                           {:order-by [[:title :asc]]}))]
      (is (= "A different page" result)))

    ;; Collection.
    (is (orderless=
          ["Some page title" "A different page"]
          (<? (d/<q (d/db conn)
                    [:find '[?title ...]
                     :in '$
                     :where '[?save :save/title ?title]]
                    {:order-by [[:title :desc]]}))))))

(deftest-db test-tuple conn
  (let [attrs (<? (<initialize-with-schema conn save-schema))]
    (<? (d/<transact! conn
                      [{:db/id (d/id-literal :db.part/user -1)
                        :save/title "Some page title"
                        :save/excerpt "Some page excerpt"}
                       {:db/id (d/id-literal :db.part/user -2)
                        :save/title "A different page"
                        :save/excerpt "A different excerpt"}]))
    (let [result (<? (d/<q (d/db conn)
                           [:find '[?title ?excerpt]
                            :in '$
                            :where
                            '[?save :save/title ?title]
                            '[?save :save/excerpt ?excerpt]]))]
      (is (or (= ["Some page title" "Some page excerpt"] result)
              (= ["A different page" "A different excerpt"] result))))))
