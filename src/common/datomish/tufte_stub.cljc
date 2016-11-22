;; Copyright 2016 Mozilla
;;
;; Licensed under the Apache License, Version 2.0 (the "License"); you may not use
;; this file except in compliance with the License. You may obtain a copy of the
;; License at http://www.apache.org/licenses/LICENSE-2.0
;; Unless required by applicable law or agreed to in writing, software distributed
;; under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
;; CONDITIONS OF ANY KIND, either express or implied. See the License for the
;; specific language governing permissions and limitations under the License.

(ns datomish.tufte-stub)

;;; The real version of Tufte pulls in cljs.test, which pulls in
;;; pprint, which breaks the build in Firefox.

(defmacro p [name & forms]
  `(do ~@forms))

(defmacro profile [options & forms]
  `(do ~@forms))

(defn add-basic-println-handler! [args])
