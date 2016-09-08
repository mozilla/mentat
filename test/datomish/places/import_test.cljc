;; This Source Code Form is subject to the terms of the Mozilla Public
;; License, v. 2.0. If a copy of the MPL was not distributed with this
;; file, You can obtain one at http://mozilla.org/MPL/2.0/.

(ns datomish.places.import-test
  #?(:cljs
     (:require-macros
      [datomish.pair-chan :refer [go-pair <?]]
      [datomish.node-tempfile-macros :refer [with-tempfile]]
      [cljs.core.async.macros :as a :refer [go]]))
  (:require
   [taoensso.tufte :as tufte
    #?(:cljs :refer-macros :clj :refer) [defnp p profiled profile]]
   [datomish.api :as d]
   [datomish.places.import :as pi]
   [datomish.util :as util #?(:cljs :refer-macros :clj :refer) [raise cond-let]]
   [datomish.sqlite :as s]
   #?@(:clj [[datomish.jdbc-sqlite]
             [datomish.pair-chan :refer [go-pair <?]]
             [tempfile.core :refer [tempfile with-tempfile]]
             [datomish.test-macros :refer [deftest-async]]
             [clojure.test :as t :refer [is are deftest testing]]
             [clojure.core.async :refer [go <! >!]]])
   #?@(:cljs [[datomish.js-sqlite]
              [datomish.pair-chan]
              [datomish.test-macros :refer-macros [deftest-async]]
              [datomish.node-tempfile :refer [tempfile]]
              [cljs.test :as t :refer-macros [is are deftest testing async]]
              [cljs.core.async :as a :refer [<! >!]]])))

#?(:cljs
   (def Throwable js/Error))

(tufte/add-basic-println-handler! {})

(deftest-async test-import
  (with-tempfile [t (tempfile)]
    (let [places (<? (s/<sqlite-connection "/tmp/places.sqlite"))
          conn (<? (d/<connect t))]
      (try
        (let [report    (profile {:dynamic? true} (<? (pi/import-places conn places)))]

          (is (= nil (count (:tx-data report)))))
        (finally
          (<? (d/<close conn)))))))

(deftest-async test-import-repeat
  ;; Repeated import is worst possible for the big joins to find datoms that already exist, because
  ;; *every* datom added in the first import will match in the second.
  (with-tempfile [t (tempfile)]
    (let [places (<? (s/<sqlite-connection "/tmp/places.sqlite"))
          conn (<? (d/<connect t))]
      (try
        (let [report0   (<? (pi/import-places conn places))
              report    (profile {:dynamic? true} (<? (pi/import-places conn places)))]

          (is (= nil (count (:tx-data report)))))

        (finally
          (<? (d/<close conn)))))))

#_
(defn <?? [pair-chan]
  (datomish.pair-chan/consume-pair (clojure.core.async/<!! pair-chan)))

#_ [
    (def places (<?? (s/<sqlite-connection "/tmp/places.sqlite")))
    (def conn (<?? (d/<connect "/tmp/testkb.sqlite")))
    (def tx0 (:tx (<?? (d/<transact! conn places-schema-fragment))))

    (tufte/add-basic-println-handler! {})
    (def report (profile {:dynamic? true} (<?? (pi/import conn places))))

    ;; Empty:
    ;; "Elapsed time: 5451.610551 msecs"
    ;; Reimport:
    ;; "Elapsed time: 25600.358881 msecs"

    ]
