;; This Source Code Form is subject to the terms of the Mozilla Public
;; License, v. 2.0. If a copy of the MPL was not distributed with this
;; file, You can obtain one at http://mozilla.org/MPL/2.0/.

(ns datomish.query
  (:require
   [datomish.query.clauses :as clauses]
   [datomish.query.context :as context]
   [datomish.query.projection :as projection]
   [datomish.query.transforms :as transforms]
   [datomish.util :as util #?(:cljs :refer-macros :clj :refer) [raise raise-str cond-let]]
   [datascript.parser :as dp
    #?@(:cljs
          [:refer [
            BindScalar
            Constant
            DefaultSrc
            Pattern
            Placeholder
            SrcVar
            Variable
            ]])]
   [clojure.string :as str]
   [honeysql.core :as sql]
   )
  #?(:clj
       (:import
          [datascript.parser
            BindScalar
            Constant
            DefaultSrc
            Pattern
            Placeholder
            SrcVar
            Variable
           ])))

;; Setting this to something else will make your output more readable,
;; but not automatically safe for use.
(def sql-quoting-style :ansi)

(defn- validated-order-by [projection order-by]
  (let [ordering-vars (set (map first order-by))
        projected-vars (set (map second projection))]

    (when-not (every? #{:desc :asc} (map second order-by))
      (raise-str "Ordering expressions must be :asc or :desc."))
    (when-not
      (clojure.set/subset? ordering-vars projected-vars)
      (raise "Ordering vars " ordering-vars " not a subset of projected vars " projected-vars
             {:projected projected-vars
              :ordering ordering-vars}))

    order-by))

(defn- limit-and-order [limit projection order-by]
  (when (or limit order-by)
    (util/assoc-if {}
                   :limit limit
                   :order-by (validated-order-by projection order-by))))

(defn context->sql-clause [context]
  (let [inner-projection (projection/sql-projection-for-relation context)
        inner
        (merge
          {:select inner-projection

           ;; Always SELECT DISTINCT, because Datalog is set-based.
           ;; TODO: determine from schema analysis whether we can avoid
           ;; the need to do this.
           :modifiers [:distinct]}
          (clauses/cc->partial-subquery (:cc context)))

        limit (:limit context)
        order-by (:order-by-vars context)]

    (if (:has-aggregates? context)
      (let [outer-projection (projection/sql-projection-for-aggregation context :preag)]
        ;; Validate the projected vars against the ordering clauses.
        (merge
          (limit-and-order limit outer-projection order-by)
          (when-not (empty? (:group-by-vars context))
            ;; We shouldn't need to account for types here, until we account for
            ;; `:or` clauses that bind from different attributes.
            {:group-by (map util/var->sql-var (:group-by-vars context))})
          {:select outer-projection
           :modifiers [:distinct]
           :from [:preag]
           :with {:preag inner}}))

      ;; Otherwise, validate against the inner.
      (merge
        (limit-and-order limit inner-projection order-by)
        inner))))

(defn context->sql-string [context args]
  (->
    context
    context->sql-clause
    (sql/format args :quoting sql-quoting-style)))

(defn- validate-with [with]
  (when-not (or (nil? with)
                (every? #(instance? Variable %1) with))
    (raise "Complex :with not supported." {:with with})))

(defn- validate-in [in]
  (when (nil? in)
    (raise ":in expression cannot be nil." {:binding in}))
  (when-not (= "$" (name (-> in first :variable :symbol)))
    (raise "Non-default sources not supported." {:binding in}))
  (when-not (every? (partial instance? BindScalar) (rest in))
    (raise "Non-scalar bindings not supported." {:binding in})))

(defn in->bindings
  "Take an `:in` list and return a bindings map suitable for use
   as external bindings in a CC."
  [in]
  (reduce
    (fn [m b]
      (or
        (when (instance? BindScalar b)
          (let [var (:variable b)]
            (when (instance? Variable var)
              (let [v (:symbol var)]
                (assoc m v [(sql/param (util/var->sql-var v))])))))
        m))
    {}
    in))

(defn options-into-context
  [context limit order-by]
  (when-not (or (and (integer? limit)
                     (pos? limit))
                (nil? limit))
    (raise "Invalid limit " limit {:limit limit}))
  (assoc context :limit limit :order-by-vars order-by))

(defn find-into-context
  "Take a parsed `find` expression and return a fully populated
  Context. You'll want this so you can get access to the
  projection, amongst other things."
  [context find]
  (let [{:keys [find in with where]} find]  ; Destructure the Datalog query.
    (validate-with with)
    (validate-in in)
    (let [external-bindings (in->bindings in)
          elements (:elements find)
          known-types {}
          group-by-vars (projection/extract-group-by-vars elements with)]
      (assoc context
             :elements elements
             :group-by-vars group-by-vars
             :has-aggregates? (not (nil? group-by-vars))
             :cc (clauses/patterns->cc (:default-source context) where known-types external-bindings)))))

(defn find->sql-clause
  "Take a parsed `find` expression and turn it into a structured SQL
   expression that can be formatted by honeysql."
  [context find]
  (->> find
       (find-into-context context)
       context->sql-clause))

(defn find->sql-string
  "Take a parsed `find` expression and turn it into SQL."
  [context find args]
  (->
    (find->sql-clause context find)
    (sql/format args :quoting sql-quoting-style)))

(defn parse
  "Parse a Datalog query array into a structured `find` expression."
  [q]
  (dp/parse-query q))

#_
(def sql-quoting-style nil)

#_
(datomish.query/find->sql-string
  (datomish.query.context/make-context (datomish.query.source/datoms-source nil))
  (datomish.query/parse
    '[:find ?timestampMicros ?page :in $ ?latest :where
      [?page :page/starred true ?t]
      [?t :db/txInstant ?timestampMicros]
      (not [(> ?t ?latest)]) ])
  {:latest 5})

#_
(datomish.query/find->sql-string
  (datomish.query.context/make-context (datomish.query.source/datoms-source nil))
  (datomish.query/parse
    '[:find ?page :in $ ?latest :where
      [?page :page/url "http://example.com/"]
      [(fulltext $ :page/title "Some title") [[?page ?title _ _]]]
      (or
        [?entity :page/likes ?page]
        [?entity :page/loves ?page])
      ])
  {})
