;; This Source Code Form is subject to the terms of the Mozilla Public
;; License, v. 2.0. If a copy of the MPL was not distributed with this
;; file, You can obtain one at http://mozilla.org/MPL/2.0/.

(ns datomish.js-sqlite
  (:require
   [datomish.sqlite :as s]
   [datomish.js-util :refer [is-node?]]
   [datomish.promise-sqlite :as promise-sqlite]))

(def open promise-sqlite/open)

(extend-protocol s/ISQLiteConnectionFactory
  string
  (<sqlite-connection [path]
    (open path))

  object ;; TODO: narrow this to the result of node-tempfile/tempfile.
  (<sqlite-connection [tempfile]
    (open (.-name tempfile))))
