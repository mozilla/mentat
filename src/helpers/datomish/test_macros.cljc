;; Copyright 2016 Mozilla
;;
;; Licensed under the Apache License, Version 2.0 (the "License"); you may not use
;; this file except in compliance with the License. You may obtain a copy of the
;; License at http://www.apache.org/licenses/LICENSE-2.0
;; Unless required by applicable law or agreed to in writing, software distributed
;; under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
;; CONDITIONS OF ANY KIND, either express or implied. See the License for the
;; specific language governing permissions and limitations under the License.

(ns datomish.test-macros
  #?(:cljs
     (:require-macros
      [datomish.test-macros]))
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

(defmacro deftest-db
  [n conn-var & body]
  `(deftest-async ~n
     (let [~conn-var (datomish.pair-chan/<? (datomish.api/<connect ""))]
       (try
         ~@body
         (finally
           (datomish.pair-chan/<? (datomish.api/<close ~conn-var)))))))
