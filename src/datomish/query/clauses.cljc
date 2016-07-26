;; This Source Code Form is subject to the terms of the Mozilla Public
;; License, v. 2.0. If a copy of the MPL was not distributed with this
;; file, You can obtain one at http://mozilla.org/MPL/2.0/.

(ns datomish.query.clauses
  (:require
     [datomish.query.source
      :refer [attribute-in-source
              constant-in-source
              source->from
              source->constraints]]
     [datomish.util :as util #?(:cljs :refer-macros :clj :refer) [raise raise-str cond-let]]
     [datascript.parser :as dp
      #?@(:cljs
            [:refer
             [PlainSymbol Predicate Not Or Pattern DefaultSrc Variable Constant Placeholder]])]
     [honeysql.core :as sql]
     [clojure.string :as str]
     )
  #?(:clj
       (:import
          [datascript.parser
           PlainSymbol Predicate Not Or Pattern DefaultSrc Variable Constant Placeholder])))

;; A ConjoiningClauses (CC) is a collection of clauses that are combined with JOIN.
;; The topmost form in a query is a ConjoiningClauses.
;;
;;---------------------------------------------------------------------------------------
;; Done:
;; - Ordinary pattern clauses turn into FROM parts and WHERE parts using :=.
;; - Predicate clauses turn into the same, but with other functions.
;; - `not` turns into NOT EXISTS with WHERE clauses inside the subquery to
;;   bind it to the outer variables, or adds simple WHERE clauses to the outer
;;   clause.
;; - `not-join` is similar, but with explicit binding.
;;
;; Not yet done:
;; - Function clauses with bindings turn into:
;;   * Subqueries. Perhaps less efficient? Certainly clearer.
;;   * Projection expressions, if only used for output.
;;   * Inline expressions?
;; - `or` turns into a collection of UNIONs inside a subquery.
;;   `or`'s documentation states that all clauses must include the same vars,
;;   but that's an over-simplification: all clauses must refer to the external
;;   unification vars.
;;   The entire UNION-set is JOINed to any surrounding expressions per the `rule-vars`
;;   clause, or the intersection of the vars in the two sides of the JOIN.
;;---------------------------------------------------------------------------------------
;;
;; `from` is a list of [source alias] pairs, suitable for passing to honeysql.
;; `bindings` is a map from var to qualified columns.
;; `wheres` is a list of fragments that can be joined by `:and`.
(defrecord ConjoiningClauses [source from external-bindings bindings wheres])

(defn bind-column-to-var [cc variable col]
  (let [var (:symbol variable)]
    (util/conj-in cc [:bindings var] col)))

(defn constrain-column-to-constant [cc col position value]
  (util/conj-in cc [:wheres]
                [:= col (if (= :a position)
                          (attribute-in-source (:source cc) value)
                          (constant-in-source (:source cc) value))]))

(defn merge-ccs [left right]
  (assoc left
         :from (concat (:from left) (:from right))
         :bindings (merge-with concat (:bindings left) (:bindings right))
         :wheres (concat (:wheres left) (:wheres right))))

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

;; This is so we can link clauses to the outside world.
(defn impose-external-bindings [cc]
  (if (empty? (:external-bindings cc))
    cc
    (let [ours (:bindings cc)
          theirs (:external-bindings cc)
          vars (clojure.set/intersection (set (keys theirs)) (set (keys ours)))]
      (util/concat-in
        cc [:wheres]
        (map
          (fn [v]
            (let [external (first (v theirs))
                  internal (first (v ours))]
              (assert external)
              (assert internal)
              [:= external internal]))
          vars)))))

(defn expand-where-from-bindings
  "Take the bindings in the CC and contribute
   additional where clauses. Calling this more than
   once will result in duplicate clauses."
  [cc]
  (impose-external-bindings
    (assoc cc :wheres
           ;; Note that the order of clauses here means that cross-pattern var bindings
           ;; come first. That's OK: the SQL engine considers these altogether.
           (concat (bindings->where (:bindings cc))
                   (:wheres cc)))))

;; Pattern building is recursive, so we need forward declarations.
(declare
  Not->NotJoinClause not-join->where-fragment
  simple-or? simple-or->cc)

(defn- apply-pattern-clause-for-alias
  "This helper assumes that `cc` has already established a table association
   for the provided alias."
  [cc alias pattern]
  (let [places (map vector
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

      cc
      places)))

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

  (let [[table alias] (source->from (:source cc))]   ; e.g., [:datoms :datoms123]
    (apply-pattern-clause-for-alias

      ;; Record the new table mapping.
      (util/conj-in cc [:from] [table alias])

      ;; Use the new alias for columns.
      alias
      pattern)))

(defn- plain-symbol->sql-predicate-symbol [fn]
  (when-not (instance? PlainSymbol fn)
    (raise-str "Predicate functions must be named by plain symbols." fn))
  (#{:> :< :=} (keyword (name (:symbol fn)))))

(defn apply-predicate-clause [cc predicate]
  (when-not (instance? Predicate predicate)
    (raise-str "Expected to be called with a Predicate instance." predicate))
  (let [f (plain-symbol->sql-predicate-symbol (:fn predicate))]
    (when-not f
      (raise-str "Unknown function " (:fn predicate)))

    (let [args (map
                 (fn [arg]
                   (condp instance? arg
                     Placeholder
                     (raise-str "Can't use a placeholder in a predicate.")

                     Variable
                     (let [v (:symbol arg)
                           internal-bindings (v (:bindings cc))
                           external-bindings (v (:external-bindings cc))]
                       (or (first internal-bindings)
                           (first external-bindings)
                           (raise-str "No bindings yet for " v)))

                     Constant
                     (constant-in-source (:source cc) (:value arg))

                     (raise-str "Unknown predicate argument " arg)))

                 (:args predicate))]
      (util/conj-in cc [:wheres] (cons f args)))))

(defn apply-not-clause [cc not]
  (when-not (instance? Not not)
    (raise "Expected to be called with a Not instance." {:clause not}))
  (when-not (instance? DefaultSrc (:source not))
    (raise "Non-default sources are not supported in `not` clauses." {:clause not}))

  ;; If our bindings are already available, great -- emit a :wheres
  ;; fragment, and include the external bindings so that they match up.
  ;; Otherwise, we need to delay. Right now we're lazy, so we just fail:
  ;; reorder your query yourself.
  (util/conj-in cc [:wheres]
                (not-join->where-fragment
                  (Not->NotJoinClause (:source cc)
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
    (merge-ccs cc (simple-or->cc (:source cc)
                                 (merge-with concat
                                             (:external-bindings cc)
                                             (:bindings cc))
                                 orc))

    ;; TODO: handle And within the Or patterns.
    (raise "Non-simple `or` clauses not yet supported." {:clause orc})))

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

    (raise "Unknown clause." {:clause it})))

(defn expand-pattern-clauses
  "Reduce a sequence of patterns into a CC."
  [cc patterns]
  (reduce apply-clause cc patterns))

(defn patterns->cc [source patterns external-bindings]
  (expand-where-from-bindings
    (expand-pattern-clauses
      (map->ConjoiningClauses
        {:source source
         :from []
         :external-bindings (or external-bindings {})
         :bindings {}
         :wheres []})
      patterns)))

(defn cc->partial-subquery
  "Build part of a honeysql query map from a CC: the `:from` and `:where` parts.
  This allows for reuse both in top-level query generation and also for
  subqueries and NOT EXISTS clauses."
  [cc]
  (merge
    {:from (:from cc)}
    (when-not (empty? (:wheres cc))
      {:where (cons :and (:wheres cc))})))


;; A `not-join` clause is a filter. It takes bindings from the enclosing query
;; and runs as a subquery with `NOT EXISTS`.
;; The only difference between `not` and `not-join` is that `not` computes
;; its varlist by recursively walking the provided patterns.
;; DataScript's parser does variable extraction for us, and also verifies
;; that a declared variable list is valid for the clauses given.
(defrecord NotJoinClause [unify-vars cc])

(defn make-not-join-clause [source external-bindings unify-vars patterns]
  (->NotJoinClause unify-vars (patterns->cc source patterns external-bindings)))

(defn Not->NotJoinClause [source external-bindings not]
  (when-not (instance? DefaultSrc (:source not))
    (raise "Non-default sources are not supported in `not` clauses." {:clause not}))
  (make-not-join-clause source external-bindings (:vars not) (:clauses not)))

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
  [source external-bindings orc]
  (validate-or-clause orc)

  ;; We 'fork' a CC for each pattern, then union them together.
  ;; We need to build the first in order that the others use the same
  ;; column names.
  (let [cc (map->ConjoiningClauses
             {:source source
              :from []
              :external-bindings (or external-bindings {})
              :bindings {}
              :wheres []})
        primary (apply-pattern-clause cc (first (:clauses orc)))
        remainder (rest (:clauses orc))]

    (if (empty? remainder)
      ;; That was easy.
      primary

      (let [template (assoc primary :wheres [])
            alias (second (first (:from template)))
            ccs (map (partial apply-pattern-clause-for-alias template alias)
                     remainder)]

        ;; Because this is a simple clause, we know that the first pattern established
        ;; any necessary bindings.
        ;; Take any new :wheres from each CC and combine them with :or.
        (assoc primary :wheres
               [(cons :or
                     (reduce (fn [acc cc]
                               (let [w (:wheres cc)]
                                 (case (count w)
                                       0 acc
                                       1 (conj acc (first w))

                                       (conj acc (cons :and w)))))
                             []
                             (cons primary ccs)))])))))
