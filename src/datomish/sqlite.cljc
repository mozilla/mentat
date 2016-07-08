;; This Source Code Form is subject to the terms of the Mozilla Public
;; License, v. 2.0. If a copy of the MPL was not distributed with this
;; file, You can obtain one at http://mozilla.org/MPL/2.0/.

(ns datomish.sqlite)

(defprotocol ISQLiteConnection
  (-each!
    [db sql bindings row-cb]
    "Execute the given SQL string with the specified bindings, invoking the given `row-cb` callback
    function (if provided) with each returned row.  Each row will be presented to `row-cb` as a
    map-like object, such that `(:column-name row)` succeeds.  Returns a Promise.")

  (in-transaction!
    [db f]
    "Invoke `f` in a transaction.  Commit the transaction if and only if `f` resolves to a value;
    otherwise, rollback the transaction.  Returns a Promise resolving to the value of `f` on
    success.")

  (close!
    [db]
    "Close this SQLite connection. Returns a Promise."))

(defn each!
  [db sql row-cb & {:keys [bindings]}]
  (-each! db sql bindings row-cb))
