;; This Source Code Form is subject to the terms of the Mozilla Public
;; License, v. 2.0. If a copy of the MPL was not distributed with this
;; file, You can obtain one at http://mozilla.org/MPL/2.0/.

(ns datomish.util
  #?(:cljs
     (:require-macros
      [datomish.util]
      [cljs.core.async.macros :refer [go go-loop]]))
  (:require
   [clojure.string :as str]
   [datomish.util.deque :as deque]
   #?@(:clj [[clojure.core.async :as a :refer [go go-loop <! >!]]])
   #?@(:cljs [[cljs.core.async :as a :refer [<! >!]]])))

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

(defn ensure-datalog-var [x]
  (or (and (symbol? x)
           (nil? (namespace x))
           (str/starts-with? (name x) "?"))
      (throw (ex-info (str x " is not a Datalog var.") {}))))

(defn var->sql-type-var
  "Turns '?xyz into :_xyz_type_tag."
  [x]
  (and
    (ensure-datalog-var x)
    (keyword (str "_" (subs (name x) 1) "_type_tag"))))

(defn var->sql-var
  "Turns '?xyz into :xyz."
  [x]
  (and
    (ensure-datalog-var x)
    (keyword (subs (name x) 1))))

(defn aggregate->sql-var
  "Turns (:max 'column) into :%max.column."
  [fn-kw x]
  (keyword (str "%" (name fn-kw) "." (name x))))

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

(defn assoc-if
  ([m k v]
   (if v
     (assoc m k v)
     m))
  ([m k v & kvs]
   (if kvs
     (let [[kk vv & remainder] kvs]
       (apply assoc-if
              (assoc-if m k v)
              kk vv remainder))
     (assoc-if m k v))))

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

(defn bottleneck
  "Combinator to limit an async function `af`'s concurrency to no more
  than one simultaneous evaluations.

  Similar to https://github.com/edw/async.combinators/blob/d50c213f5588b4ebc809942f22c8e03f0014dc83/src/async/combinators.clj#L4."
  [af]
  (let [inited     (atom false)
        token-chan (a/chan 1)
        ;; We need to maintain invocation argument order manually because the take! from token-chan
        ;; is non-deterministic.  That is, if multiple invocations are parked on take!, we have no
        ;; guarantee that the chronologically first invocation unparks first.
        ;;
        ;; We really want an unbounded buffer here, but that's deliberately hard to arrange using
        ;; core.async.  So we grow an unbounded deque instead.
        args-deque (deque/deque)]
    (fn [& args]
      (deque/enqueue! args-deque args)
      (go
        ;; Populate token pool; don't close channel after population.
        (when (compare-and-set! inited false true)
          (>! token-chan (gensym "bottleneck-token")))
        (let [token (<! token-chan)
              ;; There's a core.async bug requiring this atom.
              ;; See http://dev.clojure.org/jira/browse/ASYNC-180.
              result (atom :invalid)]
          (try
            (when-let [args (deque/dequeue! args-deque)]
              (reset! result (<! (apply af args))))
            (finally
              (>! token-chan token)))
          @result)))))
