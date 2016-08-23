;; This Source Code Form is subject to the terms of the Mozilla Public
;; License, v. 2.0. If a copy of the MPL was not distributed with this
;; file, You can obtain one at http://mozilla.org/MPL/2.0/.

(ns datomish.tofinoish-test
  #?(:cljs
     (:require-macros
      [datomish.pair-chan :refer [go-pair <?]]
      [datomish.node-tempfile-macros :refer [with-tempfile]]
      [cljs.core.async.macros :as a :refer [go]]))
  (:require
   [datomish.api :as d]
   [datomish.util :as util]
   #?@(:clj [[datomish.jdbc-sqlite]
             [datomish.pair-chan :refer [go-pair <?]]
             [tempfile.core :refer [tempfile with-tempfile]]
             [datomish.test-macros :refer [deftest-async deftest-db]]
             [clojure.test :as t :refer [is are deftest testing]]
             [clojure.core.async :refer [go <! >!]]])
   #?@(:cljs [[datomish.promise-sqlite]
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


(def page-schema
  [{:db/id (d/id-literal :db.part/user)
    :db/ident              :page/url
    :db/valueType          :db.type/string          ; Because not all URLs are java.net.URIs. For JS we may want to use /uri.
    :db/cardinality        :db.cardinality/one
    :db/unique             :db.unique/identity
    :db/doc                "A page's URL."
    :db.install/_attribute :db.part/db}
   {:db/id (d/id-literal :db.part/user)
    :db/ident              :page/title
    :db/valueType          :db.type/string
    :db/cardinality        :db.cardinality/one      ; We supersede as we see new titles.
    :db/doc                "A page's title."
    :db.install/_attribute :db.part/db}
   {:db/id (d/id-literal :db.part/user)
    :db/ident              :page/starred
    :db/valueType          :db.type/boolean
    :db/cardinality        :db.cardinality/one
    :db/doc                "Whether the page is starred."
    :db.install/_attribute :db.part/db}
   {:db/id (d/id-literal :db.part/user)
    :db/ident              :page/visit
    :db/valueType          :db.type/ref
    :db/cardinality        :db.cardinality/many
    :db/doc                "A visit to the page."
    :db.install/_attribute :db.part/db}])

(def visit-schema
  [{:db/id (d/id-literal :db.part/user)
    :db/ident              :visit/visitAt
    :db/valueType          :db.type/instant
    :db/cardinality        :db.cardinality/many
    :db/doc                "The instant of the visit."
    :db.install/_attribute :db.part/db}])

(def session-schema
  [{:db/id (d/id-literal :db.part/user)
    :db/ident              :session/startedFromAncestor
    :db/valueType          :db.type/ref     ; To a session.
    :db/cardinality        :db.cardinality/one
    :db/doc                "The ancestor of a session."
    :db.install/_attribute :db.part/db}
   {:db/id (d/id-literal :db.part/user)
    :db/ident              :session/startedInScope
    :db/valueType          :db.type/string
    :db/cardinality        :db.cardinality/one
    :db/doc                "The parent scope of a session."
    :db.install/_attribute :db.part/db}
   {:db/id (d/id-literal :db.part/user)
    :db/ident              :session/startReason
    :db/valueType          :db.type/string    ; TODO: enum?
    :db/cardinality        :db.cardinality/many
    :db/doc                "The start reasons of a session."
    :db.install/_attribute :db.part/db}
   {:db/id (d/id-literal :db.part/user)
    :db/ident              :session/endReason
    :db/valueType          :db.type/string    ; TODO: enum?
    :db/cardinality        :db.cardinality/many
    :db/doc                "The end reasons of a session."
    :db.install/_attribute :db.part/db}
   {:db/id (d/id-literal :db.part/user)
    :db/ident              :event/session
    :db/valueType          :db.type/ref      ; To a session.
    :db/cardinality        :db.cardinality/one
    :db/doc                "The session in which a tx took place."
    :db.install/_attribute :db.part/db}])

(def tofino-schema (concat page-schema visit-schema session-schema))

(defn instant [x]
  #?(:cljs x)
  #?(:clj (.getTime x)))

(defn now []
  #?(:cljs (js/Date.))
  #?(:clj (java.util.Date.)))

;; Returns the session ID.
(defn <start-session [conn {:keys [ancestor scope reason]
                            :or {reason "none"}}]
  (let [id (d/id-literal :db.part/user -1)
        base {:db/id                  id
              :session/startedInScope (str scope)
              :session/startReason    reason}
        datoms
        (if ancestor
            [(assoc base :session/startedFromAncestor ancestor)
             {:db/id         :db/tx
              :event/session ancestor}]
            [base])]

    (go-pair
      (->
        (<? (d/<transact! conn datoms))
        :tempids
        (get id)))))

(defn <end-session [conn {:keys [session reason]
                          :or   {reason "none"}}]
  (d/<transact!
    conn
    [{:db/id         :db/tx
      :event/session session}                              ; So meta!
     {:db/id             session
      :session/endReason reason}]))

(defn <active-sessions [db]
  (d/<q
    db
    '[:find ?id ?reason ?ts :in $
      :where
      [?id :session/startReason ?reason ?tx]
      [?tx :db/txInstant ?ts]
      (not-join [?id]
        [?id :session/endReason _])]
    {}))

(defn <ended-sessions [db]
  (d/<q
    db
    '[:find ?id ?endReason ?ts :in $
      :where
      [?id :session/endReason ?endReason ?tx]
      [?tx :db/txInstant ?ts]]
    {}))

;; TODO: return ID?
(defn <add-visit [conn {:keys [url uri title session]}]
  (let [visit (d/id-literal :db.part/user -1)
        page (d/id-literal :db.part/user -2)]
    (d/<transact!
       conn
       [{:db/id        :db/tx
         :event/session session}
        {:db/id        visit
         :visit/visitAt (now)}
        {:db/id        page
         :page/url     (or uri url)
         :page/title   title
         :page/visit visit}])))

(defn- third [x]
  (nth x 2))

(defn <visited [db
               {:keys [limit]
                :or {limit 10}}]
  ;; Until we have aggregates -- (max lastVisited) -- we do this the hard
  ;; way, grouping and maxing manually.
  (let [descending-time (comp unchecked-negate instant third)]
    (go-pair
      (let [most-recent (fn [rows]
                          (first (sort-by descending-time rows)))
            row->map (fn [[uri title lastVisited]]
                       {:uri uri :title title :lastVisited lastVisited})
            raw (<?
                 (d/<q
                   db
                   '[:find ?uri ?title ?time :in $
                     :where
                     [?page :page/url ?uri]
                     [?page :page/title ?title]
                     [?page :page/visit ?visit]
                     [?visit :visit/visitAt ?time]]
                   {}))]
        (->>
          raw
          (sort-by first)
          (partition-by first)
          (map most-recent)
          (sort-by descending-time)
          (take limit)
          (map row->map))))))

(defn <find-title [db url]
  ;; Until we support [:find ?title . :in…] we crunch this by hand.
  (go-pair
    (first
      (first
        (<?
          (d/<q db
                '[:find ?title :in $ ?url
                  :where [?page :page/url ?url] [?page :page/title ?title]]
                {:url url}))))))

;; Ensure that we can grow the schema over time.
(deftest-db test-schema-evolution conn
  (<? (d/<transact! conn page-schema))
  (<? (d/<transact! conn tofino-schema)))

(deftest-db test-simple-sessions conn
  (<? (d/<transact! conn tofino-schema))

  ;; Start a session.
  (let [session (<? (<start-session conn {:ancestor nil :scope "foo"}))]
    (is (integer? session))

    ;; Now it's active.
    (let [active (<? (<active-sessions (d/db conn)))]
      (is (= 1 (count active)))
      (is (= (first (first active))
             session)))

    ;; There are no ended sessions yet.
    (is (empty? (<? (<ended-sessions (d/db conn)))))

    (let [earliest (instant (now))]
      (<? (<add-visit conn {:uri "http://example.org/"
                            :title "Example Philanthropy Old"
                            :session session}))
      (<? (<add-visit conn {:uri "http://example.com/"
                            :title "Example Commercial"
                            :session session}))
      (<? (<add-visit conn {:uri "http://example.org/"
                            :title "Example Philanthropy New"
                            :session session}))
      (let [latest (instant (now))
            visited (<? (<visited (d/db conn) {:limit 3}))]
        (is (= 2 (count visited)))
        (is (= "http://example.org/" (:uri (first visited))))
        (is (= "http://example.com/" (:uri (second visited))))
        (is (<= earliest (instant (:lastVisited (first visited))) latest))
        (is (<= earliest (instant (:lastVisited (second visited))) latest))
        (is (>= (instant (:lastVisited (first visited))) (instant (:lastVisited (second visited)))))))

    (is (= "Example Philanthropy New"
           (<? (<find-title (d/db conn) "http://example.org/"))))

    ;; If we end this one, then it's no longer active but is ended.
    (<? (<end-session conn {:session session}))
    (is (empty? (<? (<active-sessions (d/db conn)))))
    (is (= 1 (count (<? (<ended-sessions (d/db conn))))))))
