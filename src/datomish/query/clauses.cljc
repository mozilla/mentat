;; This Source Code Form is subject to the terms of the Mozilla Public
;; License, v. 2.0. If a copy of the MPL was not distributed with this
;; file, You can obtain one at http://mozilla.org/MPL/2.0/.

(ns datomish.query.clauses
  (:require
   [datomish.query.cc :as cc]
   [datomish.query.functions :as functions]
   [datomish.query.source
    :refer [pattern->schema-value-type
            attribute-in-source
            constant-in-source
            source->from
            source->constraints]]
   [datomish.schema :as schema]
   [datomish.util :as util #?(:cljs :refer-macros :clj :refer) [raise raise-str cond-let]]
   [datascript.parser :as dp
    #?@(:cljs
        [:refer
         [
          Constant
          DefaultSrc
          Function
          Not
          Or
          Pattern
          Placeholder
          PlainSymbol
          Predicate
          Variable
          ]])]
   [honeysql.core :as sql]
   [clojure.string :as str]
   )
  #?(:clj
     (:import
      [datascript.parser
       Constant
       DefaultSrc
       Function
       Not
       Or
       Pattern
       Placeholder
       PlainSymbol
       Predicate
       Variable
       ])))

;; Pattern building is recursive, so we need forward declarations.
(declare
  Not->NotJoinClause not-join->where-fragment
  simple-or? simple-or->cc)

(defn- check-or-apply-value-type [cc value-type pattern-part]
  (if (nil? value-type)
    cc
    (condp instance? pattern-part
      Placeholder
      cc

      Variable
      (let [var-sym (:symbol pattern-part)]
        (if-let [existing-type (var-sym (:known-types cc))]
          (if (= existing-type value-type)
            cc
            (raise "Var " var-sym " already has type " existing-type "; this pattern wants " value-type
                   {:pattern pattern-part :value-type value-type}))
          (assoc-in cc [:known-types var-sym] value-type)))

      Constant
      (do
        (or (and (= :db.type/ref value-type)
                 (or (keyword? (:value pattern-part))   ; ident
                     (integer? (:value pattern-part)))) ; entid
            (schema/ensure-value-matches-type value-type (:value pattern-part)))
        cc))))

(defn- apply-pattern-clause-for-alias
  "This helper assumes that `cc` has already established a table association
   for the provided alias."
  [cc alias pattern]
  (let [pattern (:pattern pattern)
        columns (:columns (:source cc))
        places (map vector pattern columns)
        value-type (pattern->schema-value-type (:source cc) pattern)]   ; Optional; e.g., :db.type/string
    (reduce
      (fn [cc
           [pattern-part                           ; ?x, :foo/bar, 42
            position]]                             ; :a
        (let [cc (case position
                       ;; TODO: we should be able to constrain :e and :a to be
                       ;; entities... but the type checker expects that to be an int.
                       :v (check-or-apply-value-type cc value-type pattern-part)
                       :e (check-or-apply-value-type cc :db.type/ref pattern-part)
                       cc)]
          (condp instance? pattern-part
            ;; Placeholders don't contribute any bindings, nor do
            ;; they constrain the query -- there's no need to produce
            ;; IS NOT NULL, because we don't store nulls in our schema.
            Placeholder
            cc

            Variable
            (cc/bind-column-to-var cc pattern-part alias position)

            Constant
            (if (and (nil? value-type)
                     (= position :v))
              ;; If we don't know the type, but we have a constant, generate
              ;; a :wheres clause constraining the accompanying value_type_tag
              ;; column.
              (cc/constrain-value-column-to-constant cc alias (:value pattern-part))
              (cc/constrain-column-to-constant cc alias position (:value pattern-part)))

            (raise "Unknown pattern part." {:part pattern-part :clause pattern}))))

      cc
      places)))

(defn pattern->attribute [pattern]
  (second (:pattern pattern)))

;; Accumulates a pattern into the CC. Returns a new CC.
(defn apply-pattern-clause
  "Transform a DataScript Pattern instance into the parts needed
   to build a SQL expression.

  @arg cc A CC instance.
  @arg pattern The pattern instance.
  @return an augmented CC"
  [cc pattern]
  (when-not (instance? Pattern pattern)
    (raise-str "Expected to be called with a Pattern instance." pattern))
  (when-not (instance? DefaultSrc (:source pattern))
    (raise-str "Non-default sources are not supported in patterns. Pattern: " pattern))

  ;; TODO: look up the attribute in external bindings if it's a var. Perhaps we
  ;; already know what it is…
  (let [[table alias] (source->from
                        (:source cc)     ; e.g., [:datoms :datoms123]
                        (pattern->attribute pattern))]
    (apply-pattern-clause-for-alias

      ;; Record the new table mapping.
      (util/append-in cc [:from] [table alias])

      ;; Use the new alias for columns.
      alias
      pattern)))

(defn- plain-symbol->sql-predicate-symbol [fn]
  (when-not (instance? PlainSymbol fn)
    (raise-str "Predicate functions must be named by plain symbols." fn))
  (#{:> :>= :< :<= := :!=} (keyword (name (:symbol fn)))))

(defn apply-predicate-clause [cc predicate]
  (when-not (instance? Predicate predicate)
    (raise-str "Expected to be called with a Predicate instance." predicate))
  (let [f (plain-symbol->sql-predicate-symbol (:fn predicate))]
    (when-not f
      (raise-str "Unknown function " (:fn predicate)))

    (let [args (map (partial cc/argument->value cc) (:args predicate))]
      (util/append-in cc [:wheres] (cons f args)))))

(defn apply-not-clause [cc not]
  (when-not (instance? Not not)
    (raise "Expected to be called with a Not instance." {:clause not}))
  (when-not (instance? DefaultSrc (:source not))
    (raise "Non-default sources are not supported in `not` clauses." {:clause not}))

  ;; If our bindings are already available, great -- emit a :wheres
  ;; fragment, and include the external bindings so that they match up.
  ;; Otherwise, we need to delay. Right now we're lazy, so we just fail:
  ;; reorder your query yourself.
  ;;
  ;; Note that we don't extract and reuse any types established inside
  ;; the `not` clause: perhaps those won't make sense outside. But it's
  ;; a filter, so we push the external types _in_.
  (util/append-in cc
                  [:wheres]
                  (not-join->where-fragment
                    (Not->NotJoinClause (:source cc)
                                        (:known-types cc)
                                        (merge-with concat
                                                    (:external-bindings cc)
                                                    (:bindings cc))
                                        not))))

(defn apply-or-clause [cc orc]
  (when-not (instance? Or orc)
    (raise "Expected to be called with a Or instance." {:clause orc}))
  (when-not (instance? DefaultSrc (:source orc))
    (raise "Non-default sources are not supported in `or` clauses." {:clause orc}))

  ;; A simple `or` is something like:
  ;;
  ;; (or [?foo :foo/bar ?baz]
  ;;     [?foo :foo/noo ?baz])
  ;;
  ;; This can be converted into a single join and an `or` :where expression.
  ;;
  ;; Otherwise -- perhaps each leg of the `or` binds different variables, which
  ;; is acceptable for an `or-join` form -- we need to turn this into a joined
  ;; subquery.

  (if (simple-or? orc)
    (cc/merge-ccs cc (simple-or->cc (:source cc)
                                    (:known-types cc)
                                    (merge-with concat
                                                (:external-bindings cc)
                                                (:bindings cc))
                                    orc))

    ;; TODO: handle And within the Or patterns.
    (raise "Non-simple `or` clauses not yet supported." {:clause orc})))

(defn apply-function-clause [cc function]
  (or (functions/apply-sql-function cc function)
      (raise "Unknown function expression." {:clause function})))

;; We're keeping this simple for now: a straightforward type switch.
(defn apply-clause [cc it]
  (condp instance? it
    Or
    (apply-or-clause cc it)

    Not
    (apply-not-clause cc it)

    Predicate
    (apply-predicate-clause cc it)

    Pattern
    (apply-pattern-clause cc it)

    Function
    (apply-function-clause cc it)

    (raise "Unknown clause." {:clause it})))

(defn expand-pattern-clauses
  "Reduce a sequence of patterns into a CC."
  [cc patterns]
  (reduce apply-clause cc patterns))

(defn patterns->cc [source patterns known-types external-bindings]
  (cc/expand-where-from-bindings
    (expand-pattern-clauses
      (cc/map->ConjoiningClauses
        {:source source
         :from []
         :known-types (or known-types {})
         :extracted-types {}
         :external-bindings (or external-bindings {})
         :bindings {}
         :ctes {}
         :wheres []})
      patterns)))

(defn cc->partial-subquery
  "Build part of a honeysql query map from a CC: the `:from` and `:where` parts.
  This allows for reuse both in top-level query generation and also for
  subqueries and NOT EXISTS clauses."
  [cc]
  (merge
    {:from (:from cc)}
    (when-not (empty? (:ctes cc))
      {:with (:ctes cc)})
    (when-not (empty? (:wheres cc))
      {:where (cons :and (:wheres cc))})))


;; A `not-join` clause is a filter. It takes bindings from the enclosing query
;; and runs as a subquery with `NOT EXISTS`.
;; The only difference between `not` and `not-join` is that `not` computes
;; its varlist by recursively walking the provided patterns.
;; DataScript's parser does variable extraction for us, and also verifies
;; that a declared variable list is valid for the clauses given.
(defrecord NotJoinClause [unify-vars cc])

(defn Not->NotJoinClause [source known-types external-bindings not]
  (when-not (instance? DefaultSrc (:source not))
    (raise "Non-default sources are not supported in `not` clauses." {:clause not}))
  (map->NotJoinClause
    {:unify-vars (:vars not)
     :cc (patterns->cc source (:clauses not) known-types external-bindings)}))

(defn not-join->where-fragment [not-join]
  [:not
   (if (empty? (:bindings (:cc not-join)))
     ;; If the `not` doesn't establish any bindings, it means it only contains
     ;; expressions that constrain variables established outside itself.
     ;; We can just return an expression.
     (cons :and (:wheres (:cc not-join)))

     ;; If it does establish bindings, then it has to be a subquery.
     [:exists (merge {:select [1]} (cc->partial-subquery (:cc not-join)))])])


;; A simple Or clause is one in which each branch can be evaluated against
;; a single pattern match. That means that all the variables are in the same places.
;; We can produce a ConjoiningClauses in that case -- the :wheres will suffice
;; for alternation.

(defn validate-or-clause [orc]
  (when-not (instance? DefaultSrc (:source orc))
    (raise "Non-default sources are not supported in `or` clauses." {:clause orc}))
  (when-not (nil? (:required (:rule-vars orc)))
    (raise "We've never seen required rule-vars before." {:clause orc})))

(defn simple-or? [orc]
  (let [clauses (:clauses orc)]
    (and
      ;; Every pattern is a Pattern.
      (every? (partial instance? Pattern) clauses)

      (or
        (= 1 (count clauses))

        ;; Every pattern has the same source, and every place is either the
        ;; same var or a fixed value. We ignore placeholders for now.
        (let [template (first clauses)
              template-source (:source template)]
          (every? (fn [c]
                    (and (= (:source c) template-source)
                         (util/every-pair?
                           (fn [left right]
                             (condp instance? left
                               Variable (= left right)
                               Constant (instance? Constant right)

                               false))
                           (:pattern c) (:pattern template))))
                  (rest clauses)))))))

(defn simple-or->cc
  "The returned CC has not yet had bindings expanded."
  [source known-types external-bindings orc]
  (validate-or-clause orc)

  ;; We 'fork' a CC for each pattern, then union them together.
  ;; We need to build the first in order that the others use the same
  ;; column names and known types.
  (let [cc (cc/map->ConjoiningClauses
             {:source source
              :from []
              :known-types (or known-types {})
              :extracted-types {}
              :external-bindings (or external-bindings {})
              :bindings {}
              :wheres []})
        primary (apply-pattern-clause cc (first (:clauses orc)))
        remainder (rest (:clauses orc))]

    (if (empty? remainder)
      ;; That was easy.
      primary

      ;; Note that for a simple `or` clause, the same template is used for each,
      ;; so we can simply use the `extracted-types` bindings from `primary`.
      ;; A complex `or` is much harder to handle.
      (let [template (assoc primary :wheres [])
            alias (second (first (:from template)))
            ccs (map (partial apply-pattern-clause-for-alias template alias)
                     remainder)]

        ;; Because this is a simple clause, we know that the first pattern established
        ;; any necessary bindings.
        ;; Take any new :wheres from each CC and combine them with :or.
        (assoc primary
               :wheres
               [(cons :or
                     (reduce (fn [acc cc]
                               (let [w (:wheres cc)]
                                 (case (count w)
                                       0 acc
                                       1 (conj acc (first w))

                                       (conj acc (cons :and w)))))
                             []
                             (cons primary ccs)))])))))
