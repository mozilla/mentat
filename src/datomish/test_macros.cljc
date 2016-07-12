;; This Source Code Form is subject to the terms of the Mozilla Public
;; License, v. 2.0. If a copy of the MPL was not distributed with this
;; file, You can obtain one at http://mozilla.org/MPL/2.0/.

(ns datomish.test-macros
  (:refer-clojure :exclude [with-open])
  (:require
   [datomish.pair-chan]))

;; From https://github.com/plumatic/schema/blob/bf469889b730feb09448fd085be5828f28425b41/src/clj/schema/macros.clj#L10-L19.
(defn cljs-env?
  "Take the &env from a macro, and tell whether we are expanding into cljs."
  [env]
  (boolean (:ns env)))

(defmacro if-cljs
  "Return then if we are generating cljs code and else for Clojure code.
   https://groups.google.com/d/msg/clojurescript/iBY5HaQda4A/w1lAQi9_AwsJ"
  [then else]
  (if (cljs-env? &env) then else))

;; It's a huge pain to declare cross-environment macros.  This is awful, but making the namespace a
;; parameter appears to be *even worse*.
(defmacro deftest-async
  [name & body]
  `(if-cljs
     (cljs.test/deftest
       ~(with-meta name {:async true})
       (cljs.test/async done#
                        (->
                          (datomish.pair-chan/go-pair ~@body)
                          (cljs.core.async/take! (fn [v# e#]
                                                   (cljs.test/is (= e# nil))
                                                   (done#))))))
     (clojure.test/deftest
       ~(with-meta name {:async true})
       (let [[v# e#] (clojure.core.async/<!! (datomish.pair-chan/go-pair ~@body))]
         (clojure.test/is (= e# nil))))))

;; CLJS doesn't expose `with-open`, for reasons unknown.  Duplicate the definition of
;; `with-open` (and `assert-args`) here.
;; See https://github.com/clojure/clojure/blob/clojure-1.7.0/src/clj/clojure/core.clj#L3679-L3698.
(defmacro ^{:private true} assert-args
  [& pairs]
  `(do (when-not ~(first pairs)
         (throw (IllegalArgumentException.
                  (str (first ~'&form) " requires " ~(second pairs) " in " ~'*ns* ":" (:line (meta ~'&form))))))
       ~(let [more (nnext pairs)]
          (when more
            (list* `assert-args more)))))

(defmacro with-open
  "bindings => [name init ...]
  Evaluates body in a try expression with names bound to the values
  of the inits, and a finally clause that calls (.close name) on each
  name in reverse order."
  {:added "1.0"}
  [bindings & body]
  (assert-args
    (vector? bindings) "a vector for its binding"
    (even? (count bindings)) "an even number of forms in binding vector")
  (cond
    (= (count bindings) 0) `(do ~@body)
    (symbol? (bindings 0)) `(let ~(subvec bindings 0 2)
                              (try
                                (with-open ~(subvec bindings 2) ~@body)
                                (finally
                                  (. ~(bindings 0) close))))
    :else (throw (java.lang.IllegalArgumentException.
                   "with-open only allows Symbols in bindings"))))
