;; This Source Code Form is subject to the terms of the Mozilla Public
;; License, v. 2.0. If a copy of the MPL was not distributed with this
;; file, You can obtain one at http://mozilla.org/MPL/2.0/.

(ns datomish.query
  (:require
   [datomish.query.clauses :as clauses]
   [datomish.query.context :as context]
   [datomish.query.projection :as projection]
   [datomish.query.transforms :as transforms]
   [datomish.util :as util #?(:cljs :refer-macros :clj :refer) [raise-str cond-let]]
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

(defn context->sql-clause [context]
  (merge
    {:select (projection/sql-projection context)

     ;; Always SELECT DISTINCT, because Datalog is set-based.
     ;; TODO: determine from schema analysis whether we can avoid
     ;; the need to do this.
     :modifiers [:distinct]}
    (clauses/cc->partial-subquery (:cc context))))

(defn context->sql-string [context args]
  (->
    context
    context->sql-clause
    (sql/format args :quoting sql-quoting-style)))

(defn- validate-with [with]
  (when-not (nil? with)
    (raise-str "`with` not supported.")))

(defn- validate-in [in]
  (when (nil? in)
    (raise-str ":in expression cannot be nil."))
  (when-not (= "$" (name (-> in first :variable :symbol)))
    (raise-str "Non-default sources not supported."))
  (when-not (every? (partial instance? BindScalar) (rest in))
    (raise-str "Non-scalar bindings not supported.")))

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

(defn find-into-context
  "Take a parsed `find` expression and return a fully populated
  Context. You'll want this so you can get access to the
  projection, amongst other things."
  [context find]
  (let [{:keys [find in with where]} find]  ; Destructure the Datalog query.
    (validate-with with)
    (validate-in in)
    (let [external-bindings (in->bindings in)]
      (assoc context
             :elements (:elements find)
             :cc (clauses/patterns->cc (:default-source context) where external-bindings)))))

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

(comment
  (def sql-quoting-style nil)
  (datomish.query/find->sql-string
    (datomish.query.context/->Context (datomish.query.source/datoms-source nil) nil nil)
    (datomish.query/parse
      '[:find ?timestampMicros ?page :in $ ?latest :where
        [?page :page/starred true ?t]
        [?t :db/txInstant ?timestampMicros]
        (not [(> ?t ?latest)]) ])
    {:latest 5})
)
