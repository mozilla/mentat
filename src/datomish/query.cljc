;; This Source Code Form is subject to the terms of the Mozilla Public
;; License, v. 2.0. If a copy of the MPL was not distributed with this
;; file, You can obtain one at http://mozilla.org/MPL/2.0/.

(ns datomish.query
  (:require
   [datomish.clauses :as clauses]
   [datomish.context :as context]
   [datomish.util :as util #?(:cljs :refer-macros :clj :refer) [raise-str cond-let]]
   [datomish.projection :as projection]
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
    {:select (projection/sql-projection context)}
    (clauses/cc->partial-subquery (:cc context))))

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
    (assoc context
           :elements (:elements find)
           :cc (clauses/patterns->cc (:default-source context) where))))

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
  (->
    (find->sql-clause context find)
    (sql/format :quoting sql-quoting-style)))

(defn parse
  "Parse a Datalog query array into a structured `find` expression."
  [q]
  (dp/parse-query q))

(comment
(def sql-quoting-style nil))
(comment
  (datomish.query/find->sql-string (datomish.context/->Context (datomish.source/datoms-source nil) nil nil)
    (datomish.query/parse
      '[:find ?timestampMicros ?page
        :in $
        :where
        [?page :page/starred true ?t]
        [?t :db/txInstant ?timestampMicros]
        (not [?page :page/deleted true]) ])))

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
