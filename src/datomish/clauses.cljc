;; This Source Code Form is subject to the terms of the Mozilla Public
;; License, v. 2.0. If a copy of the MPL was not distributed with this
;; file, You can obtain one at http://mozilla.org/MPL/2.0/.

(ns datomish.clauses
  (:require
     [datomish.source
      :refer [attribute-in-source
              constant-in-source
              source->from
              source->constraints]]
     [datomish.util :as util #?(:cljs :refer-macros :clj :refer) [raise-str cond-let]]
     [datascript.parser :as dp
      #?@(:cljs [:refer [Not Pattern DefaultSrc Variable Constant Placeholder]])]
     [honeysql.core :as sql]
     [clojure.string :as str]
     )
  #?(:clj
       (:import
          [datascript.parser Not Pattern DefaultSrc Variable Constant Placeholder])))

;; A CC is a collection of clauses that are combined with JOIN.
;; The topmost form in a query is a ConjoiningClauses.
;;
;; Ordinary pattern clauses turn into FROM parts and WHERE parts using :=.
;; Predicate clauses turn into the same, but with other functions.
;; Function clauses with bindings turn into:
;;   * Subqueries. Perhaps less efficient? Certainly clearer.
;;   * Projection expressions, if only used for output.
;;   * Inline expressions?
;; `not` turns into NOT EXISTS with WHERE clauses inside the subquery to
;; bind it to the outer variables.
;; `not-join` is similar, but with explicit binding.
;; `or` turns into a collection of UNIONs inside a subquery.
;; `or`'s documentation states that all clauses must include the same vars,
;; but that's an over-simplification: all clauses must refer to the external
;; unification vars.
;; The entire UNION-set is JOINed to any surrounding expressions per the `rule-vars`
;; clause, or the intersection of the vars in the two sides of the JOIN.

;; `from` is a list of [source alias] pairs, suitable for passing to honeysql.
;; `bindings` is a map from var to qualified columns.
;; `wheres` is a list of fragments that can be joined by `:and`.
(defrecord ConjoiningClauses [source from bindings wheres])
           
(defn bind-column-to-var [cc variable col]
  (let [var (:symbol variable)]
    (util/conj-in cc [:bindings var] col)))
 
(defn constrain-column-to-constant [cc col position value]
  (util/conj-in cc [:wheres]
                [:= col (if (= :a position)
                          (attribute-in-source (:source cc) value)
                          (constant-in-source (:source cc) value))]))

(declare Not->NotJoinClause not-join->where-fragment impose-external-bindings)

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

  (let [[table alias] (source->from (:source cc))   ; e.g., [:datoms :datoms123]
        places (map (fn [place col] [place col])
                    (:pattern pattern)
                    (:columns (:source cc)))]
    (reduce
      (fn [cc
           [pattern-part                           ; ?x, :foo/bar, 42
            position]]                             ; :a
        (let [col (sql/qualify alias (name position))]    ; :datoms123.a
          (condp instance? pattern-part
            ;; Placeholders don't contribute any bindings, nor do
            ;; they constrain the query -- there's no need to produce
            ;; IS NOT NULL, because we don't store nulls in our schema.
            Placeholder
            cc

            Variable
            (bind-column-to-var cc pattern-part col)

            Constant
            (constrain-column-to-constant cc col position (:value pattern-part))

            (raise-str "Unknown pattern part " pattern-part))))

      ;; Record the new table mapping.
      (util/conj-in cc [:from] [table alias])

      places)))
  
(defn apply-not-clause [cc not]
  (when-not (instance? Not not)
    (raise-str "Expected to be called with a Not instance." not))
  (when-not (instance? DefaultSrc (:source not))
    (raise-str "Non-default sources are not supported in patterns. Pattern: " not))
  
  (let [not-join-clause (Not->NotJoinClause (:source cc) not)]
    ;; If our bindings are already available, great -- emit a :wheres
    ;; fragment, and include the external bindings so that they match up.
    ;; Otherwise, we need to delay, and we do that now by failing.
    
    (let [seen (set (keys (:bindings cc)))
          to-unify (set (map :symbol (:unify-vars not-join-clause)))]
    (println "Seen " seen " need " to-unify)
      (if (clojure.set/subset? to-unify seen)
        (util/conj-in cc [:wheres] (not-join->where-fragment
                                     (impose-external-bindings not-join-clause (:bindings cc))))
        (raise-str "Haven't seen all the necessary vars for this `not` clause.")))))

(defn apply-clause [cc it]
  (if (instance? Not it)
    (apply-not-clause cc it)
    (apply-pattern-clause cc it)))

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
  "Take the bindings in the CC and contribute
   additional where clauses. Calling this more than
   once will result in duplicate clauses."
  [cc]
  (assoc cc :wheres (concat (bindings->where (:bindings cc))
                            (:wheres cc))))

(defn expand-pattern-clauses
  "Reduce a sequence of patterns into a CC."
  [cc patterns]
  (reduce apply-clause cc patterns))

(defn patterns->cc [source patterns]
  (expand-where-from-bindings
    (expand-pattern-clauses
      (->ConjoiningClauses source [] {} [])
      patterns)))

;; A `not-join` clause is a filter. It takes bindings from the enclosing query
;; and runs as a subquery with `NOT EXISTS`.
;; The only difference between `not` and `not-join` is that `not` computes
;; its varlist by recursively walking the provided patterns.
;; DataScript's parser does variable extraction for us, and also verifies
;; that a declared variable list is valid for the clauses given.
(defrecord NotJoinClause [unify-vars cc])

(defn make-not-join-clause [source unify-vars patterns]
  (->NotJoinClause unify-vars (patterns->cc source patterns)))

(defn Not->NotJoinClause [source not]
  (when-not (instance? DefaultSrc (:source not))
    (raise-str "Non-default sources are not supported in patterns. Pattern: "
           not))
  (make-not-join-clause source (:vars not) (:clauses not)))

;; This is so we can link the clause to the outside world.
(defn impose-external-constraints [not-join-clause wheres] 
  (util/concat-in not-join-clause [:cc :wheres] wheres))

(defn impose-external-bindings [not-join-clause bindings]
  (let [ours (:bindings (:cc not-join-clause))
        vars (clojure.set/intersection (set (keys bindings)) (set (keys ours)))
        pairings (map (fn [v] [:= (first (v bindings)) (first (v ours))]) vars)]
    (impose-external-constraints not-join-clause pairings)))
    
(defn cc->partial-subquery [cc]
  (merge
    {:from (:from cc)}
    (when-not (empty? (:wheres cc))
      {:where (cons :and (:wheres cc))})))

(defn not-join->where-fragment [not-join]
  [:not [:exists (merge {:select 1} (cc->partial-subquery (:cc not-join)))]])
