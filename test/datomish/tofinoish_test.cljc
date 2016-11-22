;; Copyright 2016 Mozilla
;;
;; Licensed under the Apache License, Version 2.0 (the "License"); you may not use
;; this file except in compliance with the License. You may obtain a copy of the
;; License at http://www.apache.org/licenses/LICENSE-2.0
;; Unless required by applicable law or agreed to in writing, software distributed
;; under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
;; CONDITIONS OF ANY KIND, either express or implied. See the License for the
;; specific language governing permissions and limitations under the License.

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

(def save-schema
  [{:db/id (d/id-literal :db.part/user)
    :db.install/_attribute :db.part/db
    :db/cardinality :db.cardinality/one
    :db/valueType :db.type/ref
    :db/ident :save/page}
   {:db/id (d/id-literal :db.part/user)
    :db.install/_attribute :db.part/db
    :db/cardinality :db.cardinality/one
    :db/valueType :db.type/instant
    :db/ident :save/savedAt}
   {:db/id (d/id-literal :db.part/user)
    :db.install/_attribute :db.part/db
    :db/cardinality :db.cardinality/one
    :db/valueType :db.type/string
    :db/fulltext true
    :db/ident :save/title}
   {:db/id (d/id-literal :db.part/user)
    :db.install/_attribute :db.part/db
    :db/cardinality :db.cardinality/one
    :db/valueType :db.type/string
    :db/fulltext true
    :db/ident :save/excerpt}
   {:db/id (d/id-literal :db.part/user)
    :db.install/_attribute :db.part/db
    :db/cardinality :db.cardinality/one
    :db/valueType :db.type/string
    :db/fulltext true
    :db/ident :save/content}
   {:db/id (d/id-literal :db.part/user)
    :db.install/_attribute :db.part/db
    :db/cardinality :db.cardinality/many
    :db/valueType :db.type/string
    :db/fulltext false
    :db/ident :save/unindexed}])

(def tofino-schema (concat page-schema visit-schema session-schema save-schema))

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
        [?id :session/endReason _])]))

(defn <ended-sessions [db]
  (d/<q
    db
    '[:find ?id ?endReason ?ts :in $
      :where
      [?id :session/endReason ?endReason ?tx]
      [?tx :db/txInstant ?ts]]))

(defn <star-page [conn {:keys [url uri title session]}]
  (let [page (d/id-literal :db.part/user -1)]
    (d/<transact!
      conn
      [{:db/id        :db/tx
        :event/session session}
        (merge
          (when title
            {:page/title title})
          {:db/id        page
           :page/url     (or uri url)
           :page/starred true})])))

(defn <starred-pages [db]
  (go-pair
    (->>
      (<?
        (d/<q
          db
          '[:find ?page ?uri ?title ?starredOn
            :in $
            :where
            [?page :page/starred true ?tx]
            [?tx :db/txInstant ?starredOn]
            [?page :page/url ?uri]
            [?page :page/title ?title]                   ; N.B., this means we will exclude pages with no title.
            ]))

      (map (fn [[page uri title starredOn]]
             {:page page :uri uri :title title :starredOn starredOn})))))

(defn <save-page [conn {:keys [url uri title session excerpt content]}]
  (let [save (d/id-literal :db.part/user -1)
        page (d/id-literal :db.part/user -2)]
    (d/<transact!
      conn
      [{:db/id        :db/tx
        :event/session session}
       {:db/id page
        :page/url (or uri url)}
       (merge
         {:db/id        save
          :save/savedAt (now)
          :save/page page}
         (when title
           {:save/title title})
         (when excerpt
           {:save/excerpt excerpt})
         (when content
           {:save/content content}))])))

(defn <saved-pages [db]
  (d/<q db
        '[:find ?page ?url ?title ?excerpt
          :in $
          :where
          [?save :save/page ?page]
          [?page :page/url ?url]
          [(get-else $ ?save :save/title "") ?title]
          [(get-else $ ?save :save/excerpt "") ?excerpt]]))

(defn <saved-pages-matching-string [db string]
  (d/<q db
        {:find '[?page ?url ?title ?excerpt]
         :in '[$]
         :where [[(list 'fulltext '$ :any string) '[[?save]]]
                 '[?save :save/page ?page]
                 '[?page :page/url ?url]
                 '[(get-else $ ?save :save/title "") ?title]
                 '[(get-else $ ?save :save/excerpt "") ?excerpt]]}))


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
        (merge
          (when title
            {:page/title title})
          {:db/id        page
           :page/url     (or uri url)
           :page/visit visit})])))

(defn- third [x]
  (nth x 2))

(defn <visited [db
               {:keys [limit since]
                :or {limit 10}}]
  (let [where
        (if since
          '[[?visit :visit/visitAt ?time]
           [(> ?time ?since)]
           [?page :page/visit ?visit]
           [?page :page/url ?uri]
           [(get-else $ ?page :page/title "") ?title]]

          '[[?page :page/visit ?visit]
           [?visit :visit/visitAt ?time]
           [?page :page/url ?uri]
           [(get-else $ ?page :page/title "") ?title]])]

    (go-pair
      (let [rows (<? (d/<q
                       db
                       {:find '[?uri ?title (max ?time)]
                        :in (if since '[$ ?since] '[$])
                        :where where}
                       {:limit limit
                        :order-by [[:_max_time :desc]]
                        :inputs {:since since}}))]
        (map (fn [[uri title lastVisited]]
               {:uri uri :title title :lastVisited lastVisited})
             rows)))))

(defn <find-title [db url]
  (d/<q db
        '[:find ?title . :in $ ?url
          :where
          [?page :page/url ?url]
          [?page :page/title ?title]]
        {:inputs {:url url}}))

;; Ensure that we can grow the schema over time.
(deftest-db test-schema-evolution conn
  (<? (d/<transact! conn page-schema))
  (<? (d/<transact! conn tofino-schema)))

(deftest-db test-starring conn
  (<? (d/<transact! conn tofino-schema))
  (let [session (<? (<start-session conn {:ancestor nil :scope "foo"}))
        earliest (instant (now))]
    (<? (<star-page conn {:uri "http://mozilla.org/"
                          :title "Mozilla"
                          :session session}))
    (let [[moz & starred] (<? (<starred-pages (d/db conn)))]
      (is (empty? starred))
      (is (= "Mozilla" (:title moz)))
      (is (<= earliest (:starredOn moz) (instant (now)))))))

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
        (is (<= earliest (:lastVisited (first visited)) latest))
        (is (<= earliest (:lastVisited (second visited)) latest))
        (is (>= (:lastVisited (first visited)) (:lastVisited (second visited))))))

    (is (= "Example Philanthropy New"
           (<? (<find-title (d/db conn) "http://example.org/"))))

    ;; Add a page with no title.
    (<? (<add-visit conn {:uri "http://notitle.example.org/"
                          :session session}))
    (is (nil? (<? (<find-title (d/db conn) "http://notitle.example.org/"))))
    (let [only-one (<? (<visited (d/db conn) {:limit 1}))]
      (is (= 1 (count only-one)))
      (is (= (select-keys (first only-one)
                          [:uri :title])
             {:uri "http://notitle.example.org/"
              :title ""})))

    ;; If we end this one, then it's no longer active but is ended.
    (<? (<end-session conn {:session session}))
    (is (empty? (<? (<active-sessions (d/db conn)))))
    (is (= 1 (count (<? (<ended-sessions (d/db conn))))))))

(deftest-db test-saved-pages conn
  (<? (d/<transact! conn tofino-schema))

  ;; Start a session.
  (let [session (<? (<start-session conn {:ancestor nil :scope "foo"}))]
    (<? (<save-page conn {:uri "http://example.com/apples/1"
                          :title "A page about apples."
                          :session session
                          :excerpt "This page tells you things about apples."
                          :content "<html><head><title>A page about apples.</title></head><body><p>Fruit content goes here.</p></body></html>"}))
    (<? (<save-page conn {:uri "http://example.com/apricots/1"
                          :title "A page about apricots."
                          :session session
                          :excerpt nil
                          :content "<html><head><title>A page about apricots.</title></head><body><p>Fruit content goes here.</p></body></html>"}))
    (<? (<save-page conn {:uri "http://example.com/bananas/2"
                          :title "A page about bananas"
                          :session session
                          :excerpt nil
                          :content nil}))

    (let [db (d/db conn)]
      ;; Fetch all.
      (let [all (sort-by first (<? (<saved-pages db)))]
        (is (= 3 (count all)))
        (let [[[apple-id apple-url apple-title apple-excerpt]
               [apricot-id apricot-url apricot-title apricot-excerpt]
               [banana-id banana-url banana-title banana-excerpt]]
              all]
          (is (= apple-url "http://example.com/apples/1"))
          (is (= apple-title "A page about apples."))
          (is (= apple-excerpt "This page tells you things about apples."))
          (is (= apricot-url "http://example.com/apricots/1"))
          (is (= apricot-title "A page about apricots."))
          (is (= apricot-excerpt ""))
          (is (= banana-url "http://example.com/bananas/2"))
          (is (= banana-title "A page about bananas"))
          (is (= banana-excerpt ""))))

      ;; Match against title.
      (let [this-page (sort-by first (<? (<saved-pages-matching-string db "about apricots")))]
        (is (= 1 (count this-page)))
        (let [[[apricot-id apricot-url apricot-title apricot-excerpt]]
              this-page]
          (is (= apricot-url "http://example.com/apricots/1"))
          (is (= apricot-title "A page about apricots."))
          (is (= apricot-excerpt ""))))

      ;; Match against excerpt.
      (let [this-page (sort-by first (<? (<saved-pages-matching-string db "This page")))]
        (is (= 1 (count this-page)))
        (let [[[apple-id apple-url apple-title apple-excerpt]]
              this-page]
          (is (= apple-url "http://example.com/apples/1"))
          (is (= apple-title "A page about apples."))
          (is (= apple-excerpt "This page tells you things about apples."))))

      ;; Match against content.
      (let [fruit-content (sort-by first (<? (<saved-pages-matching-string db "Fruit content")))]
        (is (= 2 (count fruit-content)))
        (let [[[apple-id apple-url apple-title apple-excerpt]
               [apricot-id apricot-url apricot-title apricot-excerpt]]
              fruit-content]
        (is (= apple-url "http://example.com/apples/1"))
        (is (= apple-title "A page about apples."))
        (is (= apple-excerpt "This page tells you things about apples."))
        (is (= apricot-url "http://example.com/apricots/1"))
        (is (= apricot-title "A page about apricots."))
        (is (= apricot-excerpt "")))))))

(deftest-db test-fulltext-set-attribute conn
  (<? (d/<transact! conn tofino-schema))
  (<? (d/<transact! conn
                    [{:db/id             999
                      :save/title "Whenever you want something"}
                     {:db/id             998
                      :save/excerpt "If there is something…"}
                     {:db/id             997
                      :save/unindexed "What something means…"}
                     {:db/id             996
                      :save/title "This is anything but."}
                     {:db/id             995
                      :save/content "There's something here that would match."}]))
  (let [results
        (<?
          (d/<q (d/db conn)
                [:find '[?save ...]
                 :in '$
                 :where [(list 'fulltext '$ #{:save/title :save/excerpt} "something")
                         '[[?save]]]]))]
    (is (= (set results)
           #{999 998}))))
