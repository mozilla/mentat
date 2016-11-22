;; Copyright 2016 Mozilla
;;
;; Licensed under the Apache License, Version 2.0 (the "License"); you may not use
;; this file except in compliance with the License. You may obtain a copy of the
;; License at http://www.apache.org/licenses/LICENSE-2.0
;; Unless required by applicable law or agreed to in writing, software distributed
;; under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
;; CONDITIONS OF ANY KIND, either express or implied. See the License for the
;; specific language governing permissions and limitations under the License.

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
