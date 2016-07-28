;; This Source Code Form is subject to the terms of the Mozilla Public
;; License, v. 2.0. If a copy of the MPL was not distributed with this
;; file, You can obtain one at http://mozilla.org/MPL/2.0/.

;; Purloined from DataScript.

(ns datomish.datom)

(declare hash-datom equiv-datom seq-datom val-at-datom nth-datom assoc-datom)

(deftype Datom [e a v tx added]
  #?@(:cljs
      [IHash
       (-hash [d] (or (.-__hash d)
                      (set! (.-__hash d) (hash-datom d))))
       IEquiv
       (-equiv [d o] (and (instance? Datom o) (equiv-datom d o)))

       ISeqable
       (-seq [d] (seq-datom d))

       ILookup
       (-lookup [d k] (val-at-datom d k nil))
       (-lookup [d k nf] (val-at-datom d k nf))

       IIndexed
       (-nth [this i] (nth-datom this i))
       (-nth [this i not-found] (nth-datom this i not-found))

       IAssociative
       (-assoc [d k v] (assoc-datom d k v))

       IPrintWithWriter
       (-pr-writer [d writer opts]
                   (pr-sequential-writer writer pr-writer
                                         "#datascript/Datom [" " " "]"
                                         opts [(.-e d) (.-a d) (.-v d) (.-tx d) (.-added d)]))
       ]
      :clj
      [Object
       (hashCode [d] (hash-datom d))

       clojure.lang.IHashEq
       (hasheq [d] (hash-datom d))

       clojure.lang.Seqable
       (seq [d] (seq-datom d))

       clojure.lang.IPersistentCollection
       (equiv [d o] (and (instance? Datom o) (equiv-datom d o)))
       (empty [d] (throw (UnsupportedOperationException. "empty is not supported on Datom")))
       (count [d] 5)
       (cons [d [k v]] (assoc-datom d k v))

       clojure.lang.Indexed
       (nth [this i]           (nth-datom this i))
       (nth [this i not-found] (nth-datom this i not-found))

       clojure.lang.ILookup
       (valAt [d k] (val-at-datom d k nil))
       (valAt [d k nf] (val-at-datom d k nf))

       clojure.lang.Associative
       (entryAt [d k] (some->> (val-at-datom d k nil) (clojure.lang.MapEntry k)))
       (containsKey [e k] (#{:e :a :v :tx :added} k))
       (assoc [d k v] (assoc-datom d k v))
       ]))

(defn ^Datom datom
  ([e a v tx]       (Datom. e a v tx true))
  ([e a v tx added] (Datom. e a v tx added)))

(defn datom? [x] (instance? Datom x))

(defn- hash-datom [^Datom d]
  (-> (hash (.-e d))
      (hash-combine (hash (.-a d)))
      (hash-combine (hash (.-v d)))))

(defn- equiv-datom [^Datom d ^Datom o]
  (and (= (.-e d) (.-e o))
       (= (.-a d) (.-a o))
       (= (.-v d) (.-v o))))

(defn- seq-datom [^Datom d]
  (list (.-e d) (.-a d) (.-v d) (.-tx d) (.-added d)))

;; keep it fast by duplicating for both keyword and string cases
;; instead of using sets or some other matching func
(defn- val-at-datom [^Datom d k not-found]
  (case k
    :e     (.-e d)        "e"     (.-e d)
    :a     (.-a d)        "a"     (.-a d)
    :v     (.-v d)        "v"     (.-v d)
    :tx    (.-tx d)       "tx"    (.-tx d)
    :added (.-added d)    "added" (.-added d)
    not-found))

(defn- nth-datom
  ([^Datom d ^long i]
   (case i
     0 (.-e d)
     1 (.-a d)
     2 (.-v d)
     3 (.-tx d)
     4 (.-added d)
     #?(:clj  (throw (IndexOutOfBoundsException.))
        :cljs (throw (js/Error. (str "Datom/-nth: Index out of bounds: " i))))))
  ([^Datom d ^long i not-found]
   (case i
     0 (.-e d)
     1 (.-a d)
     2 (.-v d)
     3 (.-tx d)
     4 (.-added d)
     not-found)))

(defn- ^Datom assoc-datom [^Datom d k v]
  (case k
    :e     (Datom. v       (.-a d) (.-v d) (.-tx d) (.-added d))
    :a     (Datom. (.-e d) v       (.-v d) (.-tx d) (.-added d))
    :v     (Datom. (.-e d) (.-a d) v       (.-tx d) (.-added d))
    :tx    (Datom. (.-e d) (.-a d) (.-v d) v        (.-added d))
    :added (Datom. (.-e d) (.-a d) (.-v d) (.-tx d) v)
    #?(:clj  (throw (IllegalArgumentException. (str "invalid key for #datascript/Datom: " k)))
       :cljs (throw (js/Error. (str "invalid key for #datascript/Datom: " k))))))

;; printing and reading

(defn ^Datom datom-from-reader [vec]
  (apply datom vec))

#?(:clj
   (defmethod print-method Datom [^Datom d, ^java.io.Writer w]
     (.write w (str "#datascript/Datom "))
     (binding [*out* w]
       (pr [(.-e d) (.-a d) (.-v d) (.-tx d) (.-added d)]))))
