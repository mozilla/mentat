;; This Source Code Form is subject to the terms of the Mozilla Public
;; License, v. 2.0. If a copy of the MPL was not distributed with this
;; file, You can obtain one at http://mozilla.org/MPL/2.0/.

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
