;; This Source Code Form is subject to the terms of the Mozilla Public
;; License, v. 2.0. If a copy of the MPL was not distributed with this
;; file, You can obtain one at http://mozilla.org/MPL/2.0/.

(ns datomish.query.cc
  (:require
     [datomish.query.source
      :refer [attribute-in-source
              constant-in-source]]
     [datomish.util :as util #?(:cljs :refer-macros :clj :refer) [raise raise-str cond-let]]
     [datascript.parser :as dp
      #?@(:cljs
            [:refer
             [
              Constant
              Placeholder
              Variable
              ]])])
  #?(:clj
       (:import
          [datascript.parser
           Constant
           Placeholder
           Variable
           ])))


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

(defn augment-cc [cc from bindings wheres]
  (assoc cc
         :from (concat (:from cc) from)
         :bindings (merge-with concat (:bindings cc) bindings)
         :wheres (concat (:wheres cc) wheres)))

(defn merge-ccs [left right]
  (augment-cc left (:from right) (:bindings right) (:wheres right)))

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
(defn- impose-external-bindings [cc]
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

(defn binding-for-symbol-or-throw [cc symbol]
  (let [internal-bindings (symbol (:bindings cc))
        external-bindings (symbol (:external-bindings cc))]
    (or (first internal-bindings)
        (first external-bindings)
        (raise-str "No bindings yet for " symbol))))

(defn argument->value
  "Take a value from an argument list and resolve it against the CC.
   Throws if the value can't be resolved (e.g., no binding is established)."
  [cc arg]
  (condp instance? arg
    Placeholder
    (raise-str "Can't use a placeholder in a predicate.")

    Variable
    (binding-for-symbol-or-throw cc (:symbol arg))

    Constant
    (constant-in-source (:source cc) (:value arg))

    (raise-str "Unknown predicate argument " arg)))
