;; This Source Code Form is subject to the terms of the Mozilla Public
;; License, v. 2.0. If a copy of the MPL was not distributed with this
;; file, You can obtain one at http://mozilla.org/MPL/2.0/.

(ns datomish.test-macros
  #?(:cljs
     (:require-macros [datomish.test-macros]))
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
                          (cljs.core.async/take! (fn [[v# e#]]
                                                   (cljs.test/is (= e# nil)) ;; Can't synchronously fail.
                                                   (done#))))))
     (clojure.test/deftest
       ~(with-meta name {:async true})
       (let [[v# e#] (clojure.core.async/<!! (datomish.pair-chan/go-pair ~@body))]
         (when e# (throw e#)) ;; Assert nil just to be safe, even though we should always throw first.
         (clojure.test/is (= e# nil))))))
