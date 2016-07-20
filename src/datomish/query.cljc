;; This Source Code Form is subject to the terms of the Mozilla Public
;; License, v. 2.0. If a copy of the MPL was not distributed with this
;; file, You can obtain one at http://mozilla.org/MPL/2.0/.

(ns datomish.query
  (:require
   [datomish.util :as util #?(:cljs :refer-macros :clj :refer) [raise-str cond-let]]
   [datomish.transforms :as transforms]
   [datascript.parser :as dp
    #?@(:cljs [:refer [Pattern DefaultSrc Variable Constant Placeholder]])]
   [clojure.string :as str]
   [honeysql.core :as sql]
   )
  #?(:clj (:import [datascript.parser Pattern DefaultSrc Variable Constant Placeholder]))
  )

;; Setting this to something else will make your output more readable,
;; but not automatically safe for use.
(def sql-quoting-style :ansi)

(defn context->sql-clause [context]
  (merge
    {:select (sql-projection context)
     :from (:from context)}
    (if (empty? (:wheres context))
      {}
      {:where (cons :and (:wheres context))})))

(defn context->sql-string [context]
  (->
    context
    context->sql-clause
    (sql/format :quoting sql-quoting-style)))

(defn- validate-with [with]
  (when-not (nil? with)
    (raise-str "`with` not supported.")))

(defn- validate-in [in]
  (when-not (and (== 1 (count in))
                 (= "$" (name (-> in first :variable :symbol))))
    (raise-str "Complex `in` not supported: " in)))

(defn expand-find-into-context [context find]
  ;; There's some confusing use of 'where' and friends here. That's because
  ;; the parsed Datalog includes :where, and it's also input to honeysql's
  ;; SQL formatter.
  (let [{:keys [find in with where]} find]  ; Destructure the Datalog query.
    (validate-with with)
    (validate-in in)
    (apply-elements-to-context
      (expand-where-from-bindings
        (expand-patterns-into-context context where))    ; 'where' here is the Datalog :where clause.
      (:elements find))))

(defn find->sql-clause
  "Take a parsed `find` expression and turn it into a structured SQL
   expression that can be formatted by honeysql."
  [context find]
  ;; There's some confusing use of 'where' and friends here. That's because
  ;; the parsed Datalog includes :where, and it's also input to honeysql's
  ;; SQL formatter.
  (->> find
       (expand-find-into-context context)
       context->sql-clause))

(defn find->sql-string
  "Take a parsed `find` expression and turn it into SQL."
  [context find]
  (->>
    find
    (find->sql-clause context)
    (sql/format :quoting sql-quoting-style)))

(defn parse
  "Parse a Datalog query array into a structured `find` expression."
  [q]
  (dp/parse-query q))

(comment
  (datomish.query/find->sql-string
    (datomish.query/parse
      '[:find ?page :in $ :where [?page :page/starred true ?t] ])))

(comment
  (datomish.query/find->prepared-context
    (datomish.query/parse
      '[:find ?timestampMicros ?page
        :in $
        :where
        [?page :page/starred true ?t]
        [?t :db/txInstant ?timestampMicros]])))

(comment
  (pattern->sql
    (first
      (:where
        (datascript.parser/parse-query
          '[:find (max ?timestampMicros) (pull ?page [:page/url :page/title]) ?page
          :in $
          :where
          [?page :page/starred true ?t]
  (not-join [?fo]
  [(> ?fooo 5)]
  [?xpage :page/starred false]
  )
          [?t :db/txInstant ?timestampMicros]])))
    identity))

(cc->partial-subquery
  
  (require 'datomish.clauses)
  (in-ns 'datomish.clauses)
(patterns->cc (datomish.source/datoms-source nil)
  (:where
(datascript.parser/parse-query
  '[:find (max ?timestampMicros) (pull ?page [:page/url :page/title]) ?page
    :in $
    :where
    [?page :page/starred true ?t]
    (not-join [?page]
              [?page :page/starred false]
              )
          [?t :db/txInstant ?timestampMicros]])))

(Not->NotJoinClause (datomish.source/datoms-source nil)
#object[datomish.clauses$Not__GT_NotJoinClause 0x6d8aa02d "datomish.clauses$Not__GT_NotJoinClause@6d8aa02d"]
datomish.clauses=> #datascript.parser.Not{:source #datascript.parser.DefaultSrc{}, :vars [#datascript.parser.Variable{:symbol ?fooo}], :clauses [#datascript.parser.Pattern{:source #datascript.parser.DefaultSrc{}, :pattern [#datascript.parser.Variable{:symbol ?xpage} #datascript.parser.Constant{:value :page/starred} #datascript.parser.Constant{:value false}]}]})
