;; This Source Code Form is subject to the terms of the Mozilla Public
;; License, v. 2.0. If a copy of the MPL was not distributed with this
;; file, You can obtain one at http://mozilla.org/MPL/2.0/.

(ns datomish.js-util)

(defn is-node? []
  (try
    (= "[object process]"
       (.toString (aget js/global "process")))
    (catch js/ReferenceError e
      false)
    (catch js/TypeError e
      false)))
