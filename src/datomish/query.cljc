;; This Source Code Form is subject to the terms of the Mozilla Public
;; License, v. 2.0. If a copy of the MPL was not distributed with this
;; file, You can obtain one at http://mozilla.org/MPL/2.0/.

(ns datomish.query
  (:require
   [datomish.util :as util :refer [raise var->sql-var]]
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

;;
;; Context.
;;
;; `attribute-transform` is a function from attribute to constant value. Used to
;; turn, e.g., :p/attribute into an interned integer.
;; `constant-transform` is a function from constant value to constant value. Used to
;; turn, e.g., the literal 'true' into 1.
;; `from` is a list of table pairs, suitable for passing to honeysql.
;; `:bindings` is a map from var to qualified columns.
;; `:wheres` is a list of fragments that can be joined by `:and`.
;;
(defrecord Context [from bindings wheres elements attribute-transform constant-transform])

(defn attribute-in-context [context attribute]
  ((:attribute-transform context) attribute))

(defn constant-in-context [context constant]
  ((:constant-transform context) constant))

(defn bind-column-to-var [context variable col]
  (let [var (:symbol variable)
        existing-bindings (get-in context [:bindings var])]
    (assoc-in context [:bindings var] (conj existing-bindings col))))

(defn constrain-column-to-constant [context col position value]
  (util/conj-in context [:wheres]
                [:= col (if (= :a position)
                          (attribute-in-context context value)
                          (constant-in-context context value))]))

(defn lookup-variable [context variable]
  (or (-> context :bindings variable first)
      (raise (str "Couldn't find variable " variable))))

(defn make-context []
  (->Context [] {} [] []
             transforms/attribute-transform-string
             transforms/constant-transform-default))

(defn apply-pattern-to-context
  "Transform a DataScript Pattern instance into the parts needed
   to build a SQL expression.

  @arg context A Context instance.
  @arg pattern The pattern instance.
  @return an augmented Context."
  [context pattern]
  (when-not (instance? Pattern pattern)
    (raise "Expected to be called with a Pattern instance."))
  (when-not (instance? DefaultSrc (:source pattern))
    (raise (str "Non-default sources are not supported in patterns. Pattern: "
                (print-str pattern))))

  (let [table :datoms
        alias (gensym (name table))
        places (map (fn [place col] [place col])
                    (:pattern pattern)
                    [:e :a :v :tx])]
    (reduce
      (fn [context
           [pattern-part                           ; ?x, :foo/bar, 42
            position]]                             ; :a
        (let [col (sql/qualify alias (name position))]    ; :datoms123.a
          (condp instance? pattern-part
            ;; Placeholders don't contribute any bindings, nor do
            ;; they constrain the query -- there's no need to produce
            ;; IS NOT NULL, because we don't store nulls in our schema.
            Placeholder
            context

            Variable
            (bind-column-to-var context pattern-part col)

            Constant
            (constrain-column-to-constant context col position (:value pattern-part))

            (raise (str "Unknown pattern part " (print-str pattern-part))))))

      ;; Record the new table mapping.
      (util/conj-in context [:from] [table alias])

      places)))

(defn- bindings->where
  "Take a bindings map like
    {?foo [:datoms12.e :datoms13.v :datoms14.e]}
  and produce a list of constraints expression like
    [[:= :datoms12.e :datoms13.v] [:= :datoms12.e :datoms14.e]]

  TODO: experiment; it might be the case that producing more
  pairwise equalities we get better or worse performance."
  [bindings]
  (mapcat (fn [[_ vs]]
            (when (> (count vs) 1)
              (let [root (first vs)]
                (map (fn [v] [:= root v]) (rest vs)))))
          bindings))

(defn expand-where-from-bindings
  "Take the bindings in the context and contribute
   additional where clauses. Calling this more than
   once will result in duplicate clauses."
  [context]
  (assoc context :wheres (concat (bindings->where (:bindings context))
                                 (:wheres context))))

(defn apply-elements-to-context [context elements]
  (assoc context :elements elements))

(defn patterns->context
  "Turn a sequence of patterns into a Context."
  [patterns]
  (reduce apply-pattern-to-context (make-context) patterns))

(defn sql-projection
  "Take a `find` clause's `:elements` list and turn it into a SQL
   projection clause, suitable for passing as a `:select` clause to
   honeysql.

   For example:

     [Variable{:symbol ?foo}, Variable{:symbol ?bar}]

   with bindings in the context:

     {?foo [:datoms12.e :datoms13.v], ?bar [:datoms13.e]}

   =>

     [[:datoms12.e :foo] [:datoms13.e :bar]]

   @param context A Context, containing elements.
   @return a sequence of pairs."
  [context]
  (let [elements (:elements context)]
    (when-not (every? #(instance? Variable %1) elements)
      (raise "Unable to :find non-variables."))
    (map (fn [elem]
           (let [var (:symbol elem)]
             [(lookup-variable context var) (var->sql-var var)]))
         elements)))

(defn row-transducer [context projection rf]
  ;; For now, we only support straight var lists, so
  ;; our transducer is trivial.
  (let [columns-in-order (map second projection)
        row-mapper (fn [row] (map columns-in-order row))]
    (fn
      ([] (rf))
      ([result] (rf result))
      ([result input]
       (rf result (row-mapper input))))))

(defn context->sql-clause [context]
  {:select (sql-projection context)
   :from (:from context)
   :where (if (empty? (:wheres context))
            nil
            (cons :and (:wheres context)))})

(defn- validate-with [with]
  (when-not (nil? with)
    (raise "`with` not supported.")))

(defn- validate-in [in]
  (when-not (and (== 1 (count in))
                 (= "$" (name (-> in first :variable :symbol))))
    (raise (str "Complex `in` not supported: " (print-str in)))))

(defn find->prepared-context [find]
  ;; There's some confusing use of 'where' and friends here. That's because
  ;; the parsed Datalog includes :where, and it's also input to honeysql's
  ;; SQL formatter.
  (let [{:keys [find in with where]} find]  ; Destructure the Datalog query.
    (validate-with with)
    (validate-in in)
    (apply-elements-to-context
      (expand-where-from-bindings
        (patterns->context where))    ; 'where' here is the Datalog :where clause.
      (:elements find))))

(defn find->sql-clause
  "Take a parsed `find` expression and turn it into a structured SQL
   expression that can be formatted by honeysql."
  [find]
  ;; There's some confusing use of 'where' and friends here. That's because
  ;; the parsed Datalog includes :where, and it's also input to honeysql's
  ;; SQL formatter.
  (context->sql-clause
    (find->prepared-context find)))

(defn find->sql-string
  "Take a parsed `find` expression and turn it into SQL."
  [find]
  (-> find find->sql-clause (sql/format :quoting sql-quoting-style)))

(defn parse
  "Parse a Datalog query array into a structured `find` expression."
  [q]
  (dp/parse-query q))

(comment
  (datomish.query/find->sql-string
    (datomish.query/parse
      '[:find ?page :in $ :where [?page :page/starred true ?t] ])))

(comment
  (datomish.query/find->sql-string
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
        (datomish.query/parse
          '[:find (max ?timestampMicros) (pull ?page [:page/url :page/title]) ?page
          :in $
          :where
          [?page :page/starred true ?t]
          [?t :db/txInstant ?timestampMicros]])))
    identity))
