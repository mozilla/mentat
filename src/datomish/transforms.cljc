(ns datomish.transforms)

#?(:clj
     (defn boolean? [x]
       (instance? Boolean x)))

(defn attribute-transform-string
  "Turns :p/foo into \"p/foo\". Adequate for testing, but this depends on the storage schema."
  [x]
  (str (namespace x) "/" (name x)))

(defn constant-transform-default [x]
  (if (boolean? x)
    (if x 1 0)
    (if (keyword? x)
      (attribute-transform-string x)
      x)))
