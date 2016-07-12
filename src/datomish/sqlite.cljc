;; This Source Code Form is subject to the terms of the Mozilla Public
;; License, v. 2.0. If a copy of the MPL was not distributed with this
;; file, You can obtain one at http://mozilla.org/MPL/2.0/.

(ns datomish.sqlite
  #?(:cljs
     (:require-macros
      [datomish.pair-chan :refer [go-pair <?]]
      [cljs.core.async.macros :refer [go]]))
  #?(:clj
     (:require
      [datomish.pair-chan :refer [go-pair <?]]
      [clojure.core.async :refer [go <! >!]])
     :cljs
     (:require
      [datomish.pair-chan]
      [cljs.core.async :as a :refer [<! >!]])))

(defprotocol ISQLiteConnection
  (-execute!
    [db sql bindings]
    "Execute the given SQL string with the specified bindings. Returns a pair channel resolving
    to a query dependent `[result error]` pair.")

  (-each
    [db sql bindings row-cb]
    "Execute the given SQL string with the specified bindings, invoking the given `row-cb` callback
    function (if provided) with each returned row.  Each row will be presented to `row-cb` as a
    map-like object, such that `(:column-name row)` succeeds.  Returns a pair channel of `[result
    error]`, where `result` to the number of rows returned.")

  (close
    [db]
    "Close this SQLite connection. Returns a pair channel of [nil error]."))

(defn execute!
  [db [sql & bindings]]
  (-execute! db sql bindings))

(defn each-row
  [db [sql & bindings] row-cb]
  (-each db sql bindings row-cb))

(defn reduce-rows
  [db [sql & bindings] initial f]
  (let [acc (atom initial)]
    (go
      (let [[_ err] (<! (-each db sql bindings #(swap! acc f %)))]
        (if err
          [nil err]
          [@acc nil])))))

(defn all-rows
  [db [sql & bindings :as rest]]
  (reduce-rows db rest [] conj))

(defn in-transaction! [db chan-fn]
  (go
    (try
      (<? (execute! db ["BEGIN TRANSACTION"]))
      (let [[v e] (<! (chan-fn))]
        (if v
          (do
            (<? (execute! db ["COMMIT"]))
            [v nil])
          (do
            (<? (execute! db ["ROLLBACK TRANSACTION"]))
            [nil e])))
      (catch #?(:clj Exception :cljs js/Error) e
        [nil e]))))
