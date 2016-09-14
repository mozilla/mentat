;; This Source Code Form is subject to the terms of the Mozilla Public
;; License, v. 2.0. If a copy of the MPL was not distributed with this
;; file, You can obtain one at http://mozilla.org/MPL/2.0/.

(ns datomish.core
  (:require [cljs.nodejs :as nodejs]))

(defn -main [& args])
(set! *main-cli-fn* -main)
(nodejs/enable-util-print!)
