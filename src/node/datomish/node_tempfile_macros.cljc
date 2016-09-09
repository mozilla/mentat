;; This Source Code Form is subject to the terms of the Mozilla Public
;; License, v. 2.0. If a copy of the MPL was not distributed with this
;; file, You can obtain one at http://mozilla.org/MPL/2.0/.

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
