;; This Source Code Form is subject to the terms of the Mozilla Public
;; License, v. 2.0. If a copy of the MPL was not distributed with this
;; file, You can obtain one at http://mozilla.org/MPL/2.0/.

(ns datomish.tufte-stub)

;;; The real version of Tufte pulls in cljs.test, which pulls in
;;; pprint, which breaks the build in Firefox.

(defmacro p [name & forms]
  `(do ~@forms))

(defmacro profile [options & forms]
  `(do ~@forms))
