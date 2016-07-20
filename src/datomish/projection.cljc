;; This Source Code Form is subject to the terms of the Mozilla Public
;; License, v. 2.0. If a copy of the MPL was not distributed with this
;; file, You can obtain one at http://mozilla.org/MPL/2.0/.

(ns datomish.projection
  (:require
   [datomish.util :as util #?(:cljs :refer-macros :clj :refer) [raise-str cond-let]]
   [datascript.parser :as dp
    #?@(:cljs [:refer [Pattern DefaultSrc Variable Constant Placeholder]])]
   )
  #?(:clj (:import [datascript.parser Pattern DefaultSrc Variable Constant Placeholder]))
  )

(defn lookup-variable [cc variable]
  (println "Looking up " variable " in " (:bindings cc))
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
  (let [elements (:elements context)]
    (when-not (every? #(instance? Variable %1) elements)
      (raise-str "Unable to :find non-variables."))
    (map (fn [elem]
           (let [var (:symbol elem)]
             [(lookup-variable (:cc context) var) (util/var->sql-var var)]))
         elements)))

(defn row-pair-transducer [context projection]
  ;; For now, we only support straight var lists, so
  ;; our transducer is trivial.
  (let [columns-in-order (map second projection)]
    (map (fn [[row err]]
           (if err
             [row err]
             [(map row columns-in-order) nil])))))
