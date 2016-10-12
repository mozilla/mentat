;; This Source Code Form is subject to the terms of the Mozilla Public
;; License, v. 2.0. If a copy of the MPL was not distributed with this
;; file, You can obtain one at http://mozilla.org/MPL/2.0/.

(ns datomish.query.functions
  (:require
     [honeysql.format :as fmt]
     [datomish.query.cc :as cc]
     [datomish.schema :as schema]
     [datomish.sqlite-schema :refer [->tag ->SQLite]]
     [datomish.query.source
      :as source
      :refer [attribute-in-source
              constant-in-source]]
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
    (when (and (instance? Constant attr)
               (not (fulltext-attribute? (:source cc) (:value attr))))
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

        ;; Note that DataScript's parser won't allow us to write a term like
        ;;
        ;;   [(fulltext $ _ "foo") [[?x]]]
        ;;
        ;; so we instead have a placeholder attribute. Sigh.
        ;; We also support sets of attributes, so you can write
        ;;
        ;;   [(fulltext $ #{:foo/bar :foo/baz} "Noo") [[?x]]]
        ;;
        ;; which involves some tomfoolery here.
        ;;
        ;; TODO: exclude any non-fulltext attributes. If the set shrinks to nothing,
        ;; fail the entire pattern.
        ;; https://github.com/mozilla/datomish/issues/56
        attr-constants (or
                         (and (instance? Constant attr)
                              (let [attr (:value attr)
                                    intern (partial source/attribute-in-source (:source cc))]
                                (when-not (= :any attr)
                                  (cond
                                    (set? attr)
                                    (map intern attr)

                                    (or (keyword? attr)
                                        (integer? attr))
                                    (list (intern attr))

                                    :else
                                    (raise-str "Unknown fulltext attribute " attr {:attr attr})))))

                         (and (instance? Variable attr)
                              (cc/binding-for-symbol-or-throw cc (:symbol attr)))

                         ;; nil, so it's seqable.
                         nil)

        ;; Pull out the symbols for the binding array.
        [entity value tx score]
        (map (comp :symbol :variable)     ; This will nil-out placeholders.
             (get-in function [:binding :binding :bindings]))

        ;; Find the FTS table name and alias. We might have multiple fulltext
        ;; expressions so we will generate a query like
        ;;   SELECT ttt.a FROM t1 AS ttt WHERE ttt.t1 MATCH 'string'
        [fulltext-table fulltext-alias] (source/source->fulltext-values (:source cc))   ; [:t1 :ttt]
        match-column (sql/qualify fulltext-alias fulltext-table)                        ; :ttt.t1
        match-value (cc/argument->value cc search)

        [datom-table datom-alias] (source/source->non-fulltext-from (:source cc))

        ;; The following will end up being added to the CC.
        from [[fulltext-table fulltext-alias]
              [datom-table datom-alias]]

        extracted-types {}                               ; TODO
        known-types     {entity :db.type/ref}            ; All entities are refs.

        wheres (concat
                 [[:match match-column match-value]      ; The FTS match.

                  ;; The fulltext rowid-to-datom correspondence.
                  [:=
                   (sql/qualify datom-alias :v)
                   (sql/qualify fulltext-alias :rowid)]]

                 ;; If known, the attribute itself must match.
                 (when (seq attr-constants)
                   (let [a (sql/qualify datom-alias :a)
                         fragments (map (fn [v] [:= a v])
                                        attr-constants)]
                     (if (seq (rest fragments))
                       [(cons :or fragments)]
                       fragments))))

        ;; Now compose any bindings for entity, value, tx, and score.
        ;; TODO: do we need to examine existing bindings to capture
        ;; wheres for any of these? We shouldn't, because the CC will
        ;; be internally cross-where'd when everything is done...
        ;; TODO: bind attribute?
        bindings (into {}
                       (filter
                         (comp not nil? first)
                         [[entity [(sql/qualify datom-alias :e)]]
                          [value [match-column]]
                          [tx [(sql/qualify datom-alias :tx)]]

                          ;; Future: use matchinfo to compute a score
                          ;; if this is a variable rather than a placeholder.
                          [score [0]]]))]

    (cc/augment-cc cc from bindings known-types extracted-types wheres)))

;; get-else is how Datalog handles optional attributes.
;;
;; It consists of:
;; * A bound entity
;; * A cardinality-one attribute
;; * A var to bind the value
;; * A default value.
;;
;; We model this as:
;; * A check against known bindings for the entity.
;; * A check against the schema for cardinality-one.
;; * Generating a COALESCE expression with a query inside the projection itself.
;;
;; Note that this will be messy for queries like:
;;
;;   [:find ?page ?title :in $
;;    :where [?page :page/url _]
;;           [(get-else ?page :page/title "<empty>") ?title]
;;           [_ :foo/quoted ?title]]
;;
;; or
;;           [(some-function ?title)]
;;
;; -- we aren't really establishing a binding, so the subquery will be
;; repeated. But this will do for now.
(defn apply-get-else-clause [cc function]
  (let [{:keys [source bindings external-bindings]} cc
        schema (:schema source)

        {:keys [args binding]} function
        [src e a default-val] args]

    (when-not (instance? BindScalar binding)
      (raise-str "Expected scalar binding."))
    (when-not (instance? Variable (:variable binding))
      (raise-str "Expected variable binding."))
    (when-not (instance? Constant a)
      (raise-str "Expected constant attribute."))
    (when-not (instance? Constant default-val)
      (raise-str "Expected constant default value."))
    (when-not (and (instance? SrcVar src)
                   (= "$" (name (:symbol src))))
      (raise "Non-default sources not supported." {:arg src}))

    (let [a (attribute-in-source source (:value a))
          a-type (get-in (:schema schema) [a :db/valueType])
          a-tag (->tag a-type)

          default-val (:value default-val)
          var (:variable binding)]

      ;; Schema check.
      (when-not (and (integer? a)
                     (not (datomish.schema/multival? schema a)))
        (raise-str "Attribute " a " is not cardinality-one."))

      ;; TODO: type-check the default value.

      (condp instance? e
        Variable
        (let [e (:symbol e)
              e-binding (cc/binding-for-symbol-or-throw cc e)]

          (let [[table _] (source/source->from source a)  ; We don't need to alias: single pattern.
                ;; These :limit values shouldn't be needed, but sqlite will
                ;; appreciate them.
                ;; Note that we don't extract type tags here: the attribute
                ;; must be known!
                subquery {:select
                          [(sql/call
                             :coalesce
                             {:select [:v]
                              :from [table]
                              :where [:and
                                      [:= 'a a]
                                      [:= 'e e-binding]]
                              :limit 1}
                             (->SQLite default-val))]
                          :limit 1}]
            (->
              (assoc-in cc [:known-types (:symbol var)] a-type)
              (util/append-in [:bindings (:symbol var)] subquery))))

        (raise-str "Can't handle entity" e)))))

(def sql-functions
  ;; Future: versions of this that uses snippet() or matchinfo().
  {"fulltext" apply-fulltext-clause
   "get-else" apply-get-else-clause})

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
