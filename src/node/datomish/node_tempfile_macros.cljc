;; Copyright 2016 Mozilla
;;
;; Licensed under the Apache License, Version 2.0 (the "License"); you may not use
;; this file except in compliance with the License. You may obtain a copy of the
;; License at http://www.apache.org/licenses/LICENSE-2.0
;; Unless required by applicable law or agreed to in writing, software distributed
;; under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
;; CONDITIONS OF ANY KIND, either express or implied. See the License for the
;; specific language governing permissions and limitations under the License.

(ns datomish.node-tempfile-macros)

(defmacro with-tempfile
  "Uses a tempfile for some content and delete it immediately"
  [bindings & body]
  (cond
    (= (count bindings) 0) `(do ~@body)
    (symbol? (bindings 0)) `(let ~(subvec bindings 0 2)
                              (try
                                (with-tempfile ~(subvec bindings 2) ~@body)
                                (finally
                                  (.removeCallback ~(bindings 0))))) ;; See Node.js tmp module.
    :else (throw (java.lang.IllegalArgumentException.
                   "with-tempfile only allows Symbols in bindings"))))
