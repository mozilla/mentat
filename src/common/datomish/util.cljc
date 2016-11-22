;; Copyright 2016 Mozilla
;;
;; Licensed under the Apache License, Version 2.0 (the "License"); you may not use
;; this file except in compliance with the License. You may obtain a copy of the
;; License at http://www.apache.org/licenses/LICENSE-2.0
;; Unless required by applicable law or agreed to in writing, software distributed
;; under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
;; CONDITIONS OF ANY KIND, either express or implied. See the License for the
;; specific language governing permissions and limitations under the License.

(ns datomish.util
  #?(:cljs
     (:require-macros
      [datomish.util]
      [cljs.core.async.macros :refer [go go-loop]]))
  (:require
   [clojure.string :as str]
   #?@(:clj [[clojure.core.async :as a :refer [go go-loop <! >!]]
             [clojure.core.async.impl.protocols]])
   #?@(:cljs [[cljs.core.async :as a :refer [<! >!]]
              [cljs.core.async.impl.protocols]])))

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

(defn dissoc-from
  "Given a map `m` and a key `k`, find the sub-map named by `k`
   and remove all of its keys in `vs`."
  [m k vs]
  (assoc m k (apply dissoc (get m k) vs)))

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

(defn unblocking-chan?
  "Returns true if the channel will never block. That is to say, puts
  into this channel will never cause the buffer to be full."
  [chan]
  (a/unblocking-buffer?
    ;; See http://dev.clojure.org/jira/browse/ASYNC-181.
    (#?(:cljs .-buf :clj .buf) chan)))

;; Modified from http://dev.clojure.org/jira/browse/ASYNC-23.
#?(:cljs
   (deftype UnlimitedBuffer [buf]
     cljs.core.async.impl.protocols/UnblockingBuffer

     cljs.core.async.impl.protocols/Buffer
     (full? [this]
       false)
     (remove! [this]
       (.pop buf))
     (add!* [this itm]
       (.unshift buf itm))
     (close-buf! [this])

     cljs.core/ICounted
     (-count [this]
       (.-length buf))))

#?(:clj
   (deftype UnlimitedBuffer [^java.util.LinkedList buf]
     clojure.core.async.impl.protocols/UnblockingBuffer

     clojure.core.async.impl.protocols/Buffer
     (full? [this]
       false)
     (remove! [this]
       (.removeLast buf))
     (add!* [this itm]
       (.addFirst buf itm))
     (close-buf! [this])

     clojure.lang.Counted
     (count [this]
       (.size buf))))

(defn unlimited-buffer []
  (UnlimitedBuffer. #?(:cljs (array) :clj (java.util.LinkedList.))))

(defn group-by-kv
  "Returns a map of the elements of coll keyed by the first element of
  the result of f on each element. The value at each key will be a
  vector of the second element of the result of f on the corresponding
  elements, in the order they appeared in coll."
  {:static true}
  [f coll]
  (persistent!
    (reduce
      (fn [ret x]
        (let [[k v] (f x)]
          (assoc! ret k (conj (get ret k []) v))))
      (transient {}) coll)))

(defn repeated-keys
  "Takes a seq of maps.
   Returns the set of keys that appear in more than one map."
  [maps]
  (if (not (seq (rest maps)))
    #{}
    ;; This is a perfect use case for transients, except that
    ;; you can't use them for intersection due to CLJ-700.
    ;; http://dev.clojure.org/jira/browse/CLJ-700
    (loop [overlapping #{}
           seen #{}
           key-sets (map (comp set keys) maps)]
      (if-let [ks (first key-sets)]
        (let [overlap (clojure.set/intersection seen ks)]
          (recur (clojure.set/union overlapping overlap)
                 (clojure.set/union seen ks)
                 (rest key-sets)))
        overlapping))))
