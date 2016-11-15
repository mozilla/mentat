;; This Source Code Form is subject to the terms of the Mozilla Public
;; License, v. 2.0. If a copy of the MPL was not distributed with this
;; file, You can obtain one at http://mozilla.org/MPL/2.0/.

(ns datomish.sqlite-module)

(def sqlite (.import (aget js/Components "utils") "resource://gre/modules/Sqlite.jsm"))

(println "Browser code: sqlite is" (pr-str sqlite))
