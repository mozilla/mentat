(ns datomish.cljify)

(defn cljify
  "In node, equivalent to `(js->clj o :keywordize-keys true),
  but successfully passes Clojure Records through. This allows JS API
  callers to round-trip values they receive from ClojureScript APIs."
  [x]
  ;; This implementation is almost identical to js->clj, but it allows
  ;; us to hook into the recursion into sequences and objects, and it
  ;; passes through records.
  (if (record? x)
    x
    (cond
      (satisfies? IEncodeClojure x)
      (-js->clj x (apply array-map {:keywordize-keys true}))

      (seq? x)
      (doall (map cljify x))

      (coll? x)
      (into (empty x) (map cljify x))

      (array? x)
      (vec (map cljify x))

      (identical? (type x) js/Object)
      (into {} (for [k (js-keys x)]
                 [(keyword k) (cljify (aget x k))]))

      :else x)))
