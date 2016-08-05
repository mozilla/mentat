;; This Source Code Form is subject to the terms of the Mozilla Public
;; License, v. 2.0. If a copy of the MPL was not distributed with this
;; file, You can obtain one at http://mozilla.org/MPL/2.0/.

(ns datomish.api
  #?(:cljs
     (:require-macros
      [datomish.pair-chan :refer [go-pair <?]]
      [cljs.core.async.macros :as a :refer [go]]))
  (:require
   [datomish.db :as db]
   [datomish.db-factory :as db-factory]
   [datomish.sqlite :as sqlite]
   [datomish.transact :as transact]
   #?@(:clj [[datomish.pair-chan :refer [go-pair <?]]
             [clojure.core.async :refer [go <! >!]]])
   #?@(:cljs [[datomish.pair-chan]
              [cljs.core.async :as a :refer [<! >!]]])))

(defn <connect [uri]
  ;; Eventually, URI.  For now, just a plain path (no file://).
  (go-pair
    (->
      (sqlite/<sqlite-connection uri)
      (<?)

      (db-factory/<db-with-sqlite-connection)
      (<?)

      (transact/connection-with-db))))

(def <transact! transact/<transact!)

;; TODO: use Potemkin, or a subset of Potemkin that is CLJS friendly (like
;; https://github.com/ztellman/potemkin/issues/31) to improve this re-exporting process.
(def <close transact/close)

(def id-literal db/id-literal)

(def db transact/db)
