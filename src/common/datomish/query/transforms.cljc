;; Copyright 2016 Mozilla
;;
;; Licensed under the Apache License, Version 2.0 (the "License"); you may not use
;; this file except in compliance with the License. You may obtain a copy of the
;; License at http://www.apache.org/licenses/LICENSE-2.0
;; Unless required by applicable law or agreed to in writing, software distributed
;; under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
;; CONDITIONS OF ANY KIND, either express or implied. See the License for the
;; specific language governing permissions and limitations under the License.

(ns datomish.query.transforms)

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
