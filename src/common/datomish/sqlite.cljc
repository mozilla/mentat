;; Copyright 2016 Mozilla
;;
;; Licensed under the Apache License, Version 2.0 (the "License"); you may not use
;; this file except in compliance with the License. You may obtain a copy of the
;; License at http://www.apache.org/licenses/LICENSE-2.0
;; Unless required by applicable law or agreed to in writing, software distributed
;; under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
;; CONDITIONS OF ANY KIND, either express or implied. See the License for the
;; specific language governing permissions and limitations under the License.

(ns datomish.sqlite
  (:refer-clojure :exclude [format])
  #?(:cljs
     (:require-macros
      [datomish.pair-chan :refer [go-pair go-safely <?]]
      [cljs.core.async.macros :refer [go]]))
  #?(:clj
     (:require
      [honeysql.core]
      [datomish.pair-chan :refer [go-pair go-safely <?]]
      [clojure.core.async :refer [go <! >! chan put! take! close!]])
     :cljs
     (:require
      [honeysql.core]
      [datomish.pair-chan]
      [cljs.core.async :as a :refer [<! >! chan put! take! close!]])))

;; Setting this to something else will make your output more readable,
;; but not automatically safe for use.
(def sql-quoting-style :ansi)

(def log-sql? false)

(defn format [args]
  (honeysql.core/format args :quoting sql-quoting-style))

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

(defprotocol ISQLiteConnectionFactory
  (<sqlite-connection
    [path]
    "Open an ISQLiteConnection to `path`.  Returns a pair channel of `[sqlite-connection error]`"))

(defn execute!
  [db [sql & bindings]]
  (when log-sql?
    (println "Running SQL:" sql (pr-str bindings)))
  (-execute! db sql bindings))

(defn each-row
  [db [sql & bindings] row-cb]
  (-each db sql bindings row-cb))

(defn reduce-rows
  [db [sql & bindings] initial f]
  (let [acc (atom initial)]
    (go-pair
      (<? (-each db sql bindings #(swap! acc f %)))
      @acc)))

(defn <?all-rows
  "Takes a new channel, put!ing rows as [row err] pairs
   into it as they arrive from storage. Closes the channel
   when no more results exist. Consume with <?."
  [db [sql & bindings :as rest] chan]
  (go-safely [c chan]
             (let [result (<! (-each db sql bindings
                                     (fn [row]
                                       (put! c [row nil]))))]
               ;; We assume that a failure will result in the promise
               ;; channel being rejected and no further row callbacks
               ;; being called.
               (when (second result)
                 (put! c result))
               (close! c))))

(defn all-rows
  [db [sql & bindings :as rest]]
  (reduce-rows db rest [] conj))

(defn in-transaction! [db chan-fn]
  (go
    (try
      (<? (execute! db ["BEGIN EXCLUSIVE TRANSACTION"]))
      (let [[v e] (<! (chan-fn))]
        (if v
          (do
            (<? (execute! db ["COMMIT"]))
            [v nil])
          (do
            (<? (execute! db ["ROLLBACK TRANSACTION"]))
            [nil e])))
      (catch #?(:clj Throwable :cljs js/Error) e
        [nil e]))))

(defn get-user-version [db]
  (go-pair
    (let [row (first (<? (all-rows db ["PRAGMA user_version"])))]
      (or
        (:user_version row)
        0))))

(defn set-user-version [db version]
  (execute! db [(str "PRAGMA user_version = " version)]))
