;; This Source Code Form is subject to the terms of the Mozilla Public
;; License, v. 2.0. If a copy of the MPL was not distributed with this
;; file, You can obtain one at http://mozilla.org/MPL/2.0/.

(ns datomish.util
  #?(:cljs (:require-macros datomish.util))
  (:require
   [clojure.string :as str]))

#?(:clj
   (defmacro raise [& fragments]
     (let [msgs (butlast fragments)
           data (last fragments)]
       `(throw (ex-info (str ~@(map (fn [m#] (if (string? m#) m# (list 'pr-str m#))) msgs)) ~data)))))

#?(:clj
   (defmacro cond-let [& clauses]
     (when-let [[test expr & rest] clauses]
       `(~(if (vector? test) 'if-let 'if) ~test
         ~expr
         (cond-let ~@rest)))))

(defn var->sql-var
  "Turns '?xyz into :xyz."
  [x]
  (if (and (symbol? x)
           (str/starts-with? (name x) "?"))
    (keyword (subs (name x) 1))
    (raise (str x " is not a Datalog var."))))

(defn conj-in
  "Associates a value into a sequence in a nested associative structure, where
  ks is a sequence of keys and v is the new value, and returns a new nested
  structure.
  If any levels do not exist, hash-maps will be created. If the destination
  sequence does not exist, a new one is created."
  {:static true}
  [m [k & ks] v]
  (if ks
    (assoc m k (conj-in (get m k) ks v))
    (assoc m k (conj (get m k) v))))

(defmacro while-let [binding & forms]
  `(loop []
     (when-let ~binding
       ~@forms
       (recur))))
