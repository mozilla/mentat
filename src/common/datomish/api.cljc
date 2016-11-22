;; Copyright 2016 Mozilla
;;
;; Licensed under the Apache License, Version 2.0 (the "License"); you may not use
;; this file except in compliance with the License. You may obtain a copy of the
;; License at http://www.apache.org/licenses/LICENSE-2.0
;; Unless required by applicable law or agreed to in writing, software distributed
;; under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
;; CONDITIONS OF ANY KIND, either express or implied. See the License for the
;; specific language governing permissions and limitations under the License.

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
    (let [conn (<? (sqlite/<sqlite-connection uri))
          db (<? (db-factory/<db-with-sqlite-connection conn))]
      (transact/connection-with-db db))))

(def <transact! transact/<transact!)

(def listen! transact/listen!)
(def listen-chan! transact/listen-chan!)
(def unlisten-chan! transact/unlisten-chan!)

;; TODO: use Potemkin, or a subset of Potemkin that is CLJS friendly (like
;; https://github.com/ztellman/potemkin/issues/31) to improve this re-exporting process.
(def <close transact/close)

(def id-literal db/id-literal)
(def id-literal? db/id-literal?)

(def lookup-ref db/lookup-ref)

(def db transact/db)

(def entid db/entid)

(def ident db/ident)

(def <q db/<?q)

(def schema db/schema)
