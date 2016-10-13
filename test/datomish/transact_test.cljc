;; This Source Code Form is subject to the terms of the Mozilla Public
;; License, v. 2.0. If a copy of the MPL was not distributed with this
;; file, You can obtain one at http://mozilla.org/MPL/2.0/.

(ns datomish.transact-test
  #?(:cljs
     (:require-macros
      [datomish.pair-chan :refer [go-pair <?]]
      [datomish.node-tempfile-macros :refer [with-tempfile]]
      [cljs.core.async.macros :as a :refer [go go-loop]]))
  (:require
   [datomish.api :as d]
   [datomish.db.debug :refer [<datoms-after <datoms>= <transactions-after <shallow-entity <fulltext-values]]
   [datomish.util :as util #?(:cljs :refer-macros :clj :refer) [raise cond-let]]
   [datomish.schema :as ds]
   [datomish.simple-schema]
   [datomish.sqlite :as s]
   [datomish.sqlite-schema]
   [datomish.datom]
   #?@(:clj [[datomish.jdbc-sqlite]
             [datomish.pair-chan :refer [go-pair <?]]
             [tempfile.core :refer [tempfile with-tempfile]]
             [datomish.test-macros :refer [deftest-async deftest-db]]
             [clojure.test :as t :refer [is are deftest testing]]
             [clojure.core.async :as a :refer [go go-loop <! >!]]])
   #?@(:cljs [[datomish.js-sqlite]
              [datomish.pair-chan]
              [datomish.test-macros :refer-macros [deftest-async deftest-db]]
              [datomish.node-tempfile :refer [tempfile]]
              [cljs.test :as t :refer-macros [is are deftest testing async]]
              [cljs.core.async :as a :refer [<! >!]]]))
  #?(:clj
     (:import [clojure.lang ExceptionInfo]))
  #?(:clj
     (:import [datascript.db DB])))

#?(:cljs
   (def Throwable js/Error))

(defn- tempids [tx]
  (into {} (map (juxt (comp :idx first) second) (:tempids tx))))

(def test-schema
  [{:db/id        (d/id-literal :db.part/user)
    :db/ident     :x
    :db/unique    :db.unique/identity
    :db/valueType :db.type/long
    :db.install/_attribute :db.part/db}
   {:db/id        (d/id-literal :db.part/user)
    :db/ident     :name
    :db/unique    :db.unique/identity
    :db/valueType :db.type/string
    :db.install/_attribute :db.part/db}
   {:db/id          (d/id-literal :db.part/user)
    :db/ident       :y
    :db/cardinality :db.cardinality/many
    :db/valueType   :db.type/long
    :db.install/_attribute :db.part/db}
   {:db/id          (d/id-literal :db.part/user)
    :db/ident       :aka
    :db/cardinality :db.cardinality/many
    :db/valueType   :db.type/string
    :db.install/_attribute :db.part/db}
   {:db/id        (d/id-literal :db.part/user)
    :db/ident     :age
    :db/valueType :db.type/long
    :db.install/_attribute :db.part/db}
   {:db/id        (d/id-literal :db.part/user)
    :db/ident     :email
    :db/unique    :db.unique/identity
    :db/valueType :db.type/string
    :db.install/_attribute :db.part/db}
   {:db/id        (d/id-literal :db.part/user)
    :db/ident     :spouse
    :db/unique    :db.unique/value
    :db/valueType :db.type/string
    :db.install/_attribute :db.part/db}
   {:db/id          (d/id-literal :db.part/user)
    :db/ident       :friends
    :db/cardinality :db.cardinality/many
    :db/valueType   :db.type/ref
    :db.install/_attribute :db.part/db}
   ])

(deftest-db test-overlapping-transacts conn
  (let [{tx0 :tx} (<? (d/<transact! conn test-schema))
        report0   (<? (d/<transact! conn [{:db/id (d/id-literal :db.part/user -1)
                                           :name  "Petr"}]))
        id0       (get (tempids report0) -1)
        n         5
        make-t    (fn [i]
                    ;; Be aware that a go block with a parking operation here
                    ;; can change the order of transaction evaluation, since the
                    ;; parking operation will be unparked non-deterministically.
                    (d/<transact! conn [{:db/id (d/id-literal :db.part/user -1)
                                         :name  "Petr"
                                         :email (str "@" i)}]))]

    ;; Wait for all transactions to complete.
    (<! (a/into []
                (a/merge ;; pair-chan's never stop providing values; use take to force close.
                  (map #(a/take 1 (make-t %)) (range n)))))

    ;; Transactions should be processed in order.  This is an awkward way to
    ;; express the expected data, but it's robust in the face of changing default
    ;; identities, transaction numbers, and values of n.
    (is (= (concat [[id0 :name "Petr" (+ 1 tx0) 1]
                    [id0 :email "@0" (+ 2 tx0) 1]]
                   (mapcat
                     #(-> [[id0 :email (str "@" %) (+ 3 % tx0) 0]
                           [id0 :email (str "@" (inc %)) (+ 3 % tx0) 1]])
                     (range 0 (dec n))))

           (filter #(not= :db/txInstant (second %)) (<? (<transactions-after (d/db conn) tx0)))))))

(deftest-db test-listeners conn
  (let [{tx0 :tx} (<? (d/<transact! conn test-schema))

        c1  (a/chan (a/dropping-buffer 5))
        c2  (a/chan (a/dropping-buffer 5))]

    (testing "no listeners is okay"
      ;; So that we can upsert to concrete entids.
      (<? (d/<transact! conn [[:db/add 101 :name "Ivan"]
                              [:db/add 102 :name "Petr"]])))

    (testing "listeners are added, not accidentally notified of events before they were added"
      (d/listen-chan! conn c1)
      (d/listen-chan! conn c2)
      ;; This is not authoritative, because in an error situation a report may
      ;; be put! to a listener tap outside the expected flow.  We should witness
      ;; such an occurrence later in the test.
      (is (= nil (a/poll! c1)))
      (is (= nil (a/poll! c2))))

    (testing "unlistening to unrecognized key is ignored"
      (d/unlisten-chan! conn (a/chan)))

    (testing "listeners observe reports"
      (<? (d/<transact! conn [[:db/add (d/id-literal :db.part/user -1) :name "Ivan"]]))
      (is (= {-1 101}
             (tempids (<! c1))))
      (is (= {-1 101}
             (tempids (<! c2))))
      ;; Again, not authoritative.
      (is (= nil (a/poll! c1)))
      (is (= nil (a/poll! c2))))

    (testing "unlisten removes correct listener"
      (d/unlisten-chan! conn c1)

      (<? (d/<transact! conn [[:db/add (d/id-literal :db.part/user -2) :name "Petr"]]))
      (is (= {-2 102}
             (tempids (<! c2))))
      ;; Again, not authoritative.
      (is (= nil (a/poll! c1))))

    (testing "returning to no listeners is okay"
      (d/unlisten-chan! conn c2)

      (<? (d/<transact! conn [[:db/add (d/id-literal :db.part/user -1) :name "Petr"]]))

      ;; Again, not authoritative.
      (is (= nil (a/poll! c1)))
      (is (= nil (a/poll! c2)))

      ;; This should be authoritative, however.  We should be able to put! due
      ;; to the size of the buffer, and we should take! what we put!.
      (>! c1 :token-1)
      (is (= :token-1 (<! c1)))
      (>! c2 :token-1)
      (is (= :token-1 (<! c2))))

    (testing "complains about blocking channels"
      (is (thrown-with-msg?
            ExceptionInfo #"unblocking buffers"
            (d/listen-chan! conn (a/chan 1)))))
    ))

(deftest-db test-transact-in-listener conn
  (let [{tx0 :tx} (<? (d/<transact! conn test-schema))

        ;; So that we can see all transactions.
        lc  (a/chan (a/dropping-buffer 5))

        ;; A oneshot listener, to prevent infinite recursion.
        ofl (atom false)
        ol  (fn [report]
              (when (compare-and-set! ofl false true)
                ;; Asynchronously throw another transaction at the wall.  This
                ;; upserts to the earlier one.
                (d/<transact! conn [{:db/id (d/id-literal :db.part/user -1) :name "Ivan" :email "@1"}])))
        ]

    (testing "that we can invoke <transact! from within a listener"
      (d/listen-chan! conn lc)
      (d/listen! conn ol)

      ;; Transact once to get started, and so that we can upsert against concrete ids.
      (<? (d/<transact! conn [{:db/id 101 :name "Ivan"}]))
      (is (= (+ 1 tx0) (:tx (<! lc))))

      ;; The listener should have kicked off another transaction, but we can't
      ;; wait for it explicitly.  However, we can wait for the report to hit the
      ;; listening channel.
      (let [r (<! lc)]
        (is (= (+ 2 tx0) (:tx r)))
        (is (= {-1 101}
               (tempids r)))
        (is (= nil (a/poll! lc)))))))

#_ (time (t/run-tests))
