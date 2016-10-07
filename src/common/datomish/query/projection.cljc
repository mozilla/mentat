;; This Source Code Form is subject to the terms of the Mozilla Public
;; License, v. 2.0. If a copy of the MPL was not distributed with this
;; file, You can obtain one at http://mozilla.org/MPL/2.0/.

(ns datomish.query.projection
  (:require
   [honeysql.core :as sql]
   [datomish.query.source :as source]
   [datomish.sqlite-schema :as ss]
   [datomish.util :as util #?(:cljs :refer-macros :clj :refer) [raise raise-str cond-let]]
   [datascript.parser :as dp
    #?@(:cljs [:refer
               [Aggregate
                Constant
                DefaultSrc
                FindRel FindColl FindTuple FindScalar
                Pattern
                Placeholder
                PlainSymbol
                Variable
                ]])]
   )
  #?(:clj (:import
             [datascript.parser
              Aggregate
              Constant
              DefaultSrc
              FindRel FindColl FindTuple FindScalar
              Pattern
              Placeholder
              PlainSymbol
              Variable
              ])))

(defn lookup-variable [cc variable]
  (or (-> cc :bindings variable first)
      (raise-str "Couldn't find variable " variable)))

(def aggregate-functions
  {:avg :avg
   :count :count
   :max :max
   :min :min
   :sum :total
   })

(defn- aggregate-symbols->projected-var [fn-symbol var-symbol]
  (keyword (str "_" (name fn-symbol) "_" (subs (name var-symbol) 1))))

(defn- aggregate->projected-var [elem]
  (aggregate-symbols->projected-var (:symbol (:fn elem))
                                    (:symbol (first (:args elem)))))

(defn simple-aggregate?
  "If `elem` is a simple aggregate -- symbolic function, one var arg --
   return the variable symbol."
  [elem]
  (when (instance? Aggregate elem)
    (let [{:keys [fn args]} elem]
      (when (and (instance? PlainSymbol fn)
                 (= 1 (count args)))
        (let [arg (first args)]
          (when (instance? Variable arg)
            (:symbol arg)))))))

(defn- aggregate->var [elem]
  (when (instance? Aggregate elem)
    (when-not (simple-aggregate? elem)
      (raise-str "Only know how to handle simple aggregates."))

    (:symbol (first (:args elem)))))

(defn- variable->var [elem]
  (when (instance? Variable elem)
    (:symbol elem)))

(defn- aggregate->projection [elem context lookup-fn]
  (when (instance? Aggregate elem)
    (when-not (simple-aggregate? elem)
      (raise-str "Only know how to handle simple aggregates."))

    (let [var-symbol (:symbol (first (:args elem)))
          fn-symbol (:symbol (:fn elem))
          lookup-var (lookup-fn var-symbol)
          aggregate-fn (get aggregate-functions (keyword fn-symbol))]

      (when-not aggregate-fn
        (raise-str "Unknown aggregate function " fn-symbol))

      (let [funcall-var (util/aggregate->sql-var aggregate-fn lookup-var)
            project-as (aggregate-symbols->projected-var fn-symbol var-symbol)]
        [[funcall-var project-as]]))))

(defn- type-projection
  "Produce a projection pair by looking up `var` in the provided
   `extracted-types`."
  [extracted-types var]
  (when-let [t (get extracted-types var)]
    [t (util/var->sql-type-var var)]))

(defn- aggregate-type-projection
  "Produce a passthrough projection pair for a type field
   in an inner query."
  [inner var]
  (let [type-var (util/var->sql-type-var var)]
    [(sql/qualify inner type-var) type-var]))

(defn- symbol->projection
  "Given a variable symbol, produce a projection pair.
   `lookup-fn` will be used to find a column. For a non-aggregate query,
   this will typically be a lookup into the CC's bindings. For an
   aggregate query it'll be a qualification of the same var into the
   subquery.
   `known-types` is a type map to decide whether to project a type tag.
   `type-projection-fn` is like `lookup-fn` but for type tag columns."
  [var lookup-fn known-types type-projection-fn]
  (let [lookup-var (lookup-fn var)
        projected-var (util/var->sql-var var)
        var-projection [lookup-var projected-var]]

    ;; If the type of a variable isn't explicitly known, we also select
    ;; its type column so we can transform it.
    (if-let [type-proj (when (not (contains? known-types var))
                         (type-projection-fn var))]
      [var-projection type-proj]
      [var-projection])))

(defn- variable->projection [elem lookup-fn known-types type-projection-fn]
  (when (instance? Variable elem)
    (symbol->projection (:symbol elem) lookup-fn known-types type-projection-fn)))

(defn sql-projection-for-relation
  "Take a `find` clause's `:elements` list and turn it into a SQL
   projection clause, suitable for passing as a `:select` clause to
   honeysql.

   For example:

     [Variable{:symbol ?foo}, Variable{:symbol ?bar}]

   with bindings in the context:

     {?foo [:datoms12.e :datoms13.v], ?bar [:datoms13.e]}

   =>

     [[:datoms12.e :foo] [:datoms13.e :bar]]

   Note that we also look at `:group-by-vars`, because we need to
   alias columns and apply `DISTINCT` to those columns in order to
   aggregate correctly.

   This function unpacks aggregate operations, instead selecting the var.

   @param context A Context, containing elements.
   @return a sequence of pairs."
  [context]
  (let [{:keys [group-by-vars elements cc]} context
        {:keys [known-types extracted-types]} cc]

    ;; The primary projections from the :find list.
    ;; Note that deduplication will be necessary, because we unpack aggregates.
    (let [projected-vars
          (map (fn [elem]
                 (or (aggregate->var elem)
                     (variable->var elem)
                     (raise "Only able to :find variables or aggregates."
                            {:elem elem})))
               elements)

          ;; If we have any GROUP BY requirements from :with, that aren't already
          ;; included in the above, project them now.
          additional-vars
          (clojure.set/difference
            (set group-by-vars)
            (set projected-vars))

          full-var-list
          (distinct (concat projected-vars additional-vars))

          type-proj-fn
          (partial type-projection extracted-types)

          lookup-fn
          (partial lookup-variable cc)]

      (mapcat (fn [var]
                (symbol->projection var lookup-fn known-types type-proj-fn))
              full-var-list))))

;; Like sql-projection-for-relation, but exposed for simpler
;; use (e.g., in handling complex `or` patterns).
(defn sql-projection-for-simple-variable-list [elements cc]
  {:pre [(every? (partial instance? Variable) elements)]}
  (let [{:keys [known-types extracted-types]} cc

        projected-vars
        (map variable->var elements)

        type-proj-fn
        (partial type-projection extracted-types)

        lookup-fn
        (partial lookup-variable cc)]

    (mapcat (fn [var]
              (symbol->projection var lookup-fn known-types type-proj-fn))
            projected-vars)))

(defn sql-projection-for-aggregation
  "Project an element list that contains aggregates. This expects a subquery
   aliased to `inner-table` which itself will project each var with the
   correct name."
  [context inner-table]
  (let [{:keys [group-by-vars elements cc]} context
        {:keys [known-types extracted-types]} cc
        lookup-fn (fn [var]
                    (sql/qualify inner-table (util/var->sql-var var)))
        type-proj-fn (partial aggregate-type-projection inner-table)]
    (mapcat (fn [elem]
              (or (variable->projection elem lookup-fn known-types type-proj-fn)
                  (aggregate->projection elem context lookup-fn)
                  (raise "Only able to :find variables or aggregates."
                         {:elem elem})))
            elements)))

(defn make-projectors-for-columns [elements known-types extracted-types]
  {:pre [(map? extracted-types)
         (map? known-types)]}
  (letfn [(variable->projector [elem known-types extracted-types tag-decoder]
          (when (instance? Variable elem)
            (let [var (:symbol elem)
                  projected-var (util/var->sql-var var)]

              (if-let [type (get known-types var)]
                ;; We know the type! We already know how to decode it.
                ;; TODO: most of these tags don't actually require calling through to <-tagged-SQLite.
                ;; TODO: optimize this without making it horrible.
                (let [decoder (tag-decoder (ss/->tag type))]
                  (fn [row]
                    (decoder (get row projected-var))))

                ;; We don't know the type. Find the type projection column
                ;; and use it to decode the value.
                (if (contains? extracted-types var)
                  (let [type-column (util/var->sql-type-var var)]
                    (fn [row]
                      (ss/<-tagged-SQLite
                        (get row type-column)
                        (get row projected-var))))

                  ;; We didn't extract a type and we don't know it in advance.
                  ;; Just pass through; the :col will look itself up in the row.
                  projected-var)))))

          ;; For now we assume numerics and that everything will shake out in the wash.
          (aggregate->projector [elem]
            (when (instance? Aggregate elem)
              (let [var (aggregate->projected-var elem)]
                (fn [row]
                  (get row var)))))]

    (let [tag-decoder (memoize
                        (fn [tag]
                          (partial ss/<-tagged-SQLite tag)))]
      (map (fn [elem]
             (or (variable->projector elem known-types extracted-types tag-decoder)
                 (aggregate->projector elem)))
           elements))))

(defn row-pair-transducer [context]
  (let [{:keys [find-spec elements cc]} context
        {:keys [source known-types extracted-types]} cc

        ;; We know the projection will fail above if these aren't simple variables or aggregates.
        projectors
        (make-projectors-for-columns elements known-types extracted-types)

        single-column-find-spec?
        (or (instance? FindScalar find-spec)
            (instance? FindColl find-spec))]

    (map
      (if single-column-find-spec?
        ;; We're only grabbing one result from each row.
        (let [projector (first projectors)]
          (when (second projectors)
            (raise "Single-column find spec used, but multiple projectors present."
                   {:elements elements
                    :projectors projectors
                    :find-spec find-spec}))

          (fn [[row err]]
            (if err
              [nil err]
              [(projector row) nil])))

        ;; Otherwise, collect each column into a sequence.
        (fn [[row err]]
          (if err
            [nil err]
            [(map (fn [projector] (projector row)) projectors) nil]))))))

(defn extract-group-by-vars
  "Take inputs to :find and, if any aggregates exist in `elements`,
  return the variable names upon which we should GROUP BY."
  [elements with]
  (when (some #(instance? Aggregate %1) elements)
    (loop [ignore #{}
           group-by (map :symbol with)
           e elements]

      (if-let [element (first e)]
        (if-let [aggregated-var (simple-aggregate? element)]
          (recur (conj ignore aggregated-var)
                 group-by
                 (rest e))
          (if (instance? Variable element)
            (let [var (:symbol element)]
              (recur ignore
                     (if (contains? ignore var)
                       group-by
                       (conj group-by var))
                     (rest e)))
            (raise-str "Unknown element." {:element element})))

        ;; Done. Remove any later vars we saw.
        (remove ignore group-by)))))
