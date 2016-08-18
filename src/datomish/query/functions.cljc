;; This Source Code Form is subject to the terms of the Mozilla Public
;; License, v. 2.0. If a copy of the MPL was not distributed with this
;; file, You can obtain one at http://mozilla.org/MPL/2.0/.

(ns datomish.query.functions
  (:require
     [honeysql.format :as fmt]
     [datomish.query.cc :as cc]
     [datomish.query.source :as source]
     [datomish.util :as util #?(:cljs :refer-macros :clj :refer) [raise raise-str cond-let]]
     [datascript.parser :as dp
      #?@(:cljs
            [:refer
             [
              BindColl
              BindScalar
              BindTuple
              BindIgnore
              Constant
              Function
              PlainSymbol
              SrcVar
              Variable
              ]])]
     [honeysql.core :as sql]
     [clojure.string :as str]
     )
  #?(:clj
       (:import
          [datascript.parser
           BindColl
           BindScalar
           BindTuple
           BindIgnore
           Constant
           Function
           PlainSymbol
           SrcVar
           Variable
           ])))

;; honeysql's MATCH handler doesn't work for sqlite. This does.
(defmethod fmt/fn-handler "match" [_ col val]
  (str (fmt/to-sql col) " MATCH " (fmt/to-sql val)))

(defn fulltext-attribute? [source attribute]
  ;; TODO: schema lookup.
  true)

(defn bind-coll->binding-vars [bind-coll]
  (:bindings (:binding bind-coll)))

(defn binding-placeholder-or-variable? [binding]
  (or
    ;; It's a placeholder...
    (instance? BindIgnore binding)

    ;; ... or it's a scalar binding to a variable.
    (and
      (instance? BindScalar binding)
      (instance? Variable (:variable binding)))))

(defn- validate-fulltext-clause [cc function]
  (let [bind-coll (:binding function)
        [src attr search] (:args function)]
    (when-not (and (instance? SrcVar src)
                   (= "$" (name (:symbol src))))
      (raise "Non-default sources not supported." {:arg src}))
    (when-not (instance? Constant attr)
      (raise "Non-constant fulltext attributes not supported." {:arg attr}))

    (when-not (fulltext-attribute? (:source cc) (:value attr))
      (raise-str "Attribute " (:value attr) " is not a fulltext-indexed attribute."))

    (when-not (and (instance? BindColl bind-coll)
                   (instance? BindTuple (:binding bind-coll))
                   (every? binding-placeholder-or-variable?
                           (bind-coll->binding-vars bind-coll)))

      (raise "Unexpected binding value." {:binding bind-coll}))))

(defn apply-fulltext-clause [cc function]
  (validate-fulltext-clause cc function)

  ;; A fulltext search string is either a constant string or a variable binding.
  ;; The search string and the attribute are used to generate a SQL MATCH expression:
  ;; table MATCH 'search string'
  ;; This is then joined against an ordinary pattern to yield entity, value, and tx.
  ;; We do not currently support scoring; the score value will always be 0.
  (let [[src attr search] (:args function)

        ;; Pull out the symbols for the binding array.
        [entity value tx score]
        (map (comp :symbol :variable)     ; This will nil-out placeholders.
             (get-in function [:binding :binding :bindings]))

        ;; Find the FTS table name and alias. We might have multiple fulltext
        ;; expressions so we will generate a query like
        ;;   SELECT ttt.a FROM t1 AS ttt WHERE ttt.t1 MATCH 'string'
        [fulltext-table fulltext-alias] (source/source->fulltext-from (:source cc))   ; [:t1 :ttt]
        match-column (sql/qualify fulltext-alias fulltext-table)                      ; :ttt.t1
        match-value (cc/argument->value cc search)

        [datom-table datom-alias] (source/source->non-fulltext-from (:source cc))

        ;; The following will end up being added to the CC.
        from [[fulltext-table fulltext-alias]
              [datom-table datom-alias]]

        extracted-types {}    ; TODO

        wheres [[:match match-column match-value]      ; The FTS match.

                ;; The fulltext rowid-to-datom correspondence.
                [:=
                 (sql/qualify datom-alias :v)
                 (sql/qualify fulltext-alias :rowid)]

                ;; The attribute itself must match.
                [:=
                 (sql/qualify datom-alias :a)
                 (source/attribute-in-source (:source cc) (:value attr))]]

        ;; Now compose any bindings for entity, value, tx, and score.
        ;; TODO: do we need to examine existing bindings to capture
        ;; wheres for any of these? We shouldn't, because the CC will
        ;; be internally cross-where'd when everything is done...
        bindings (into {}
                       (filter
                         (comp not nil? first)
                         [[entity [(sql/qualify datom-alias :e)]]
                          [value [match-column]]
                          [tx [(sql/qualify datom-alias :tx)]]

                          ;; Future: use matchinfo to compute a score
                          ;; if this is a variable rather than a placeholder.
                          [score [0]]]))]

    (cc/augment-cc cc from bindings extracted-types wheres)))

(def sql-functions
  ;; Future: versions of this that uses snippet() or matchinfo().
  {"fulltext" apply-fulltext-clause})

(defn apply-sql-function
  "Either returns an application of `function` to `cc`, or nil to
   encourage you to try a different application."
  [cc function]
  (when (and (instance? Function function)
             (instance? PlainSymbol (:fn function)))
    (when-let [apply-f (get sql-functions (name (:symbol (:fn function))))]
      (apply-f cc function))))

;; A fulltext expression parses to:
;;
;; Function ( :fn, :args )
;;
;; The args begin with a SrcVar, and then are attr and search.
;;
;; This binds a relation of [?entity ?value ?tx ?score]:
;;
;;   BindColl
;;     :binding BindTuple
;;       :bindings [BindScalar...]
;;
;; #datascript.parser.Function
;; {:fn #datascript.parser.PlainSymbol{:symbol fulltext},
;;  :args [#datascript.parser.SrcVar{:symbol $}
;;         #datascript.parser.Constant{:value :artist/name}
;;         #datascript.parser.Variable{:symbol ?search}],
;;  :binding #datascript.parser.BindColl
;;           {:binding #datascript.parser.BindTuple
;;                     {:bindings [
;;                       #datascript.parser.BindScalar{:variable #datascript.parser.Variable{:symbol ?entity}}
;;                       #datascript.parser.BindScalar{:variable #datascript.parser.Variable{:symbol ?name}}
;;                       #datascript.parser.BindScalar{:variable #datascript.parser.Variable{:symbol ?tx}}
;;                       #datascript.parser.BindScalar{:variable #datascript.parser.Variable{:symbol ?score}}]}}}
