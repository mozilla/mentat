;; This Source Code Form is subject to the terms of the Mozilla Public
;; License, v. 2.0. If a copy of the MPL was not distributed with this
;; file, You can obtain one at http://mozilla.org/MPL/2.0/.

(ns datomish.util
  #?(:cljs (:require-macros datomish.util))
  (:require
   [clojure.string :as str]))

#?(:clj
   (defmacro raise-str
     "Like `raise`, but doesn't require a data argument."
     [& fragments]
     `(throw (ex-info (str ~@(map (fn [m#] (if (string? m#) m# (list 'pr-str m#))) fragments)) {}))))

#?(:clj
   (defmacro raise
     "The last argument must be a map."
     [& fragments]
     (let [msgs (butlast fragments)
           data (last fragments)]
       `(throw
          (ex-info
            (str ~@(map (fn [m#] (if (string? m#) m# (list 'pr-str m#))) msgs)) ~data)))))

#?(:clj
   (defmacro cond-let [& clauses]
     (when-let [[test expr & rest] clauses]
       `(~(if (vector? test) 'if-let 'if) ~test
         ~expr
         (cond-let ~@rest)))))

(defn var->sql-type-var
  "Turns '?xyz into :_xyz_type_tag."
  [x]
  (if (and (symbol? x)
           (str/starts-with? (name x) "?"))
    (keyword (str "_" (subs (name x) 1) "_type_tag"))
    (throw (ex-info (str x " is not a Datalog var.") {}))))

(defn var->sql-var
  "Turns '?xyz into :xyz."
  [x]
  (if (and (symbol? x)
           (str/starts-with? (name x) "?"))
    (keyword (subs (name x) 1))
    (throw (ex-info (str x " is not a Datalog var.") {}))))

(defn concat-in
  {:static true}
  [m [k & ks] vs]
  (if ks
    (assoc m k (concat-in (get m k) ks vs))
    (assoc m k (concat (get m k) vs))))

(defn append-in
  "Associates a value into a sequence in a nested associative structure, where
  ks is a sequence of keys and v is the new value, and returns a new nested
  structure.
  Always puts the value last.
  If any levels do not exist, hash-maps will be created. If the destination
  sequence does not exist, a new one is created."
  {:static true}
  [m path v]
  (concat-in m path [v]))

(defmacro while-let [binding & forms]
  `(loop []
     (when-let ~binding
       ~@forms
       (recur))))

(defn every-pair? [f xs ys]
  (or (and (empty? xs) (empty? ys))
      (and (not (empty? xs))
           (not (empty? ys))
           (f (first xs) (first ys))
           (recur f (rest xs) (rest ys)))))

(defn mapvals [f m]
  (into (empty m) (map #(vector (first %) (f (second %))) m)))
