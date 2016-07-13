;; This Source Code Form is subject to the terms of the Mozilla Public
;; License, v. 2.0. If a copy of the MPL was not distributed with this
;; file, You can obtain one at http://mozilla.org/MPL/2.0/.

(ns datomish.query
  (:require
     [datomish.util :as util :refer [raise var->sql-var]]
     [datomish.transforms :as transforms]
     [datascript.parser :as dp
      #?@(:cljs [:refer [Pattern DefaultSrc Variable Constant]])]
     [clojure.string :as str]
     [honeysql.core :as sql]
     )
  #?(:clj (:import [datascript.parser Pattern DefaultSrc Variable Constant]))
  )

;; Setting this to something else will make your output more readable,
;; but not automatically safe for use.
(def sql-quoting-style :ansi)

(defn pattern->parts
  "Transform a DataScript Pattern instance into the parts needed
   to build a SQL expression.

  @arg pattern The pattern instance.
  @arg attribute-transform A function from attribute to constant value. Used to
                           turn, e.g., :p/attribute into an interned integer.
  @arg constant-transform A function from constant value to constant value. Used to
                          turn, e.g., the literal 'true' into 1.
  @return A map, `{:from, :bindings, :where}`. `:from` is a list of table pairs,
          suitable for passing to honeysql. `:bindings` is a map from var to
          qualified columns. `:where` is a list of fragments that can be joined by
          `:and`."
  [pattern attribute-transform constant-transform]
  (when-not (instance? Pattern pattern)
    (raise "Expected to be called with a Pattern instance."))
  (when-not (instance? DefaultSrc (:source pattern))
    (raise (str "Non-default sources are not supported in patterns. Pattern: "
                (print-str pattern))))

  (let [table (keyword (name (gensym "eavt")))]

    (loop [places (:pattern pattern)
           columns [:e :a :v :t :added]
           bindings (transient {})         ; Maps from var to list of qualified columns.
           wheres (transient [])]          ; Fragments, each an expression.

      (if (empty? places)
        ;; We're done.
        {:from [:eavt table]
         :bindings (persistent! bindings)
         :where (persistent! wheres)}

        (let [pattern-part (first places)           ; ?x, :foo/bar, 42
              position (first columns)              ; :a
              col (sql/qualify table position)]     ; :eavt.a

          (condp instance? pattern-part
            Variable
            ;; We might get a pattern like this:
            ;;    [?x :foo/bar ?x]
            ;; so we look up existing bindings, collect more than one,
            ;; and will (outside this function) generate :=-relations
            ;; between each position.
            (let [var (:symbol pattern-part)
                  existing-bindings (get bindings pattern-part)]
              (recur (rest places) (rest columns)
                     (assoc! bindings var (conj existing-bindings col))
                     wheres))

            Constant
            (recur (rest places) (rest columns)
                   bindings
                   (conj! wheres
                          [:= col (if (= :a position)
                                    (attribute-transform (:value pattern-part))
                                    (constant-transform (:value pattern-part)))]))

            (raise (str "Unknown pattern part " (print-str pattern-part)))))))))

(defn bindings->where
  "Take a map like
    {?foo [:eavt12.e :eavt13.v :eavt14.e]}
  and produce a :where expression like
    (:and [:= :eavt12.e :eavt13.v] [:= :eavt12.e :eavt14.e])

  TODO: experiment; it might be the case that producing more
  pairwise equalities we get better or worse performance."
  [bindings]
  (let [clauses (mapcat (fn [[_ vs]]
                          (when (> (count vs) 1)
                            (let [root (first vs)]
                              (map (fn [v] [:= root v]) (rest vs)))))
                        bindings)]
    (when-not (empty? clauses)
      (cons :and clauses))))

(defn patterns->body [patterns]
  (let [clauses
        (map (fn [p]
               (pattern->parts p
                               transforms/attribute-transform-string
                               transforms/constant-transform-default))
             patterns)]
    {:from (map :from clauses)
     :where (cons :and (mapcat :where clauses))
     :bindings (apply merge-with concat (map :bindings clauses))}))

(defn elements->sql-projection
  "Take a `find` clause's `:elements` list and turn it into a SQL
   projection clause, suitable for passing as a `:select` clause to
   honeysql.

   Example:
     [Variable{:symbol ?foo}, Variable{:symbol ?bar}]
     {?foo [:eavt12.e :eavt13.v], ?bar [:eavt13.e]}
     =>
     [[:eavt12.e :foo] [:eavt13.e :bar]]

   @param elements The input clause.
   @param variable-lookup A function from symbol to column name.
   @return a sequence of pairs."
  [elements variable-lookup]
  (when-not (every? #(instance? Variable %1) elements)
    (raise "Unable to :find non-variables."))
  (map (fn [elem]
         (let [var (:symbol elem)]
           [(variable-lookup var) (var->sql-var var)]))
       elements))

(defn- validate-with [with]
  (when-not (nil? with)
    (raise "`with` not supported.")))

(defn- validate-in [in]
  (when-not (and (== 1 (count in))
                 (= "$" (name (-> in first :variable :symbol))))
    (raise (str "Complex `in` not supported: " (print-str in)))))

(defn find->sql-clause
  "Take a parsed `find` expression and turn it into a structured SQL
   expression that can be formatted by honeysql."
  [find]
  ;; There's some confusing use of 'where' and friends here. That's because
  ;; the parsed Datalog includes :where, and it's also input to honeysql's
  ;; SQL formatter.
  (let [{:keys [find in with where]} find]  ; Destructure the Datalog query.
    (validate-with with)
    (validate-in in)

    (let [{:keys [from where bindings]}     ; 'where' here is SQL.
          (patterns->body where)            ; 'where' here is the Datalog :where clause.
          variable-lookup #(or (first (%1 bindings))
                               (raise (str "Couldn't find variable " %1)))
          where-from-bindings (bindings->where bindings)]

      ;; Now we expand the :where clause to also include any
      ;; repeated variable usage, as noted in `bindings`.
      {:select (elements->sql-projection (:elements find) variable-lookup)
       :from from
       :where (if where-from-bindings
                (list :and where where-from-bindings)
                where)})))

(defn find->sql-string
  "Take a parsed `find` expression and turn it into SQL."
  [find]
  (-> find find->sql-clause (sql/format :quoting sql-quoting-style)))

(defn parse
  "Parse a Datalog query array into a structured `find` expression."
  [q]
  (dp/parse-query q))

(comment
  (find->sql-string
    (datomish.query/parse
      '[:find ?page
      :in $
      :where
      [?page :page/starred true ?t]
     ])))

(comment
  (find->sql-string
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
