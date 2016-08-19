;; This Source Code Form is subject to the terms of the Mozilla Public
;; License, v. 2.0. If a copy of the MPL was not distributed with this
;; file, You can obtain one at http://mozilla.org/MPL/2.0/.

(ns datomish.query.projection
  (:require
   [datomish.query.source :as source]
   [datomish.sqlite-schema :as ss]
   [datomish.util :as util #?(:cljs :refer-macros :clj :refer) [raise-str cond-let]]
   [datascript.parser :as dp
    #?@(:cljs [:refer [Pattern DefaultSrc Variable Constant Placeholder]])]
   )
  #?(:clj (:import [datascript.parser Pattern DefaultSrc Variable Constant Placeholder]))
  )

(defn lookup-variable [cc variable]
  (or (-> cc :bindings variable first)
      (raise-str "Couldn't find variable " variable)))

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
  (let [elements (:elements context)
        cc (:cc context)
        known-types (:known-types cc)
        extracted-types (:extracted-types cc)]

    (when-not (every? #(instance? Variable %1) elements)
      (raise-str "Unable to :find non-variables."))

    ;; If the type of a variable isn't explicitly known, we also select
    ;; its type column so we can transform it.
    (mapcat (fn [elem]
              (let [var (:symbol elem)
                    lookup-var (lookup-variable cc var)
                    projected-var (util/var->sql-var var)
                    var-projection [lookup-var projected-var]]
                (if (or (contains? known-types var)
                        (not (contains? extracted-types var)))
                  [var-projection]
                  [var-projection [(get extracted-types var)
                                   (util/var->sql-type-var var)]])))
         elements)))

(defn make-projectors-for-columns [elements known-types extracted-types]
  {:pre [(map? extracted-types)
         (map? known-types)]}
  (map (fn [elem]
         (let [var (:symbol elem)
               projected-var (util/var->sql-var var)
               tag-decoder (memoize
                             (fn [tag]
                               (partial ss/<-tagged-SQLite tag)))]

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
               projected-var))))
       elements))

(defn row-pair-transducer [context]
  (let [{:keys [elements cc]} context
        {:keys [source known-types extracted-types]} cc

        ;; We know the projection will fail above if these aren't simple variables.
        projectors
        (make-projectors-for-columns elements known-types extracted-types)]

    (map
      (fn [[row err]]
        (if err
          [row err]
          [(map (fn [projector] (projector row)) projectors) nil])))))
