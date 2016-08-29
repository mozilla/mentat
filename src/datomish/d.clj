(ns datomish.d
  (:require [datomic.db :as db]))

(use '[datomic.api :only [q db] :as d])
(use 'clojure.pprint)

(def uri "datomic:free://localhost:4334//news")
(d/create-database uri)
(def conn (d/connect uri))

(def schema-tx (read-string (slurp "src/datomish/schema.edn")))
(println "schema-tx:")
(pprint schema-tx)


;; alter/attribute does not retract/assert install/attribute.
;; @(d/transact conn [{:db/id :news/baz
;;                     :db/cardinality :db.cardinality/one
;;                     :db.alter/_attribute :db.part/db}])

@(d/transact conn schema-tx)

;; add some data:
(def data-tx [{:news/title "Rain Tomorrow", :db/id #db/id[:db.part/user -1000001]}])

@(d/transact conn data-tx)

(def x
  @(d/transact conn [[:db/add #db/id[:db.part/user -1] :news/title "Rain Tomorrow 1" #db/id[:db.part/tx -1]]
                     [:db/add #db/id[:db.part/user -2] :news/title "Rain Tomorrow 2" #db/id[:db.part/tx -2]]
                     [:db/add #db/id[:db.part/tx -2] :news/title "Test"]]))


;; This drops the tx entirely!
(def x
  @(d/transact conn [[:db/add #db/id[:db.part/user -1] :news/title "Rain Tomorrow 3" 13194139534684]]))

(def x
  @(d/transact conn [[:db/add #db/id[:db.part/user -1] :news/title "Rain Tomorrow 3" 13194139534684]
                     {:db/id #db/id[:db.part/db -1] :db/ident :test/test2 :db.install/_partition :db.part/db}
                     [:db/add #db/id[:db.part/tx -2] :news/title "Test"]]))

(def x
  @(d/transact conn [[:db/add #db/id[:test/test2 -1] :news/title "Rain Tomorrow 5"]]))

;; [:db/add #db/id[:db.part/user -2] :news/title "Rain Tomorrow 2" #db/id[:db.part/tx -2]]
;; [:db/add #db/id[:db.part/tx -2] :news/title "Test"]]))




(def results (q '[:find ?n :where [?n :news/title]] (db conn)))
(println (count results))
(pprint results)
(pprint (first results))

(def id (ffirst results))
(def entity (-> conn db (d/entity id)))

;; display the entity map's keys
(pprint (keys entity))

;; display the value of the entity's community name
(println (:news/title entity))

;; @(d/transact conn [[:db/retract )
@(d/transact conn [[:db/add 17592186045427 :news/title "Test"]])

;; 1. Caused by datomic.impl.Exceptions$IllegalArgumentExceptionInfo
;; :db.error/datoms-conflict Two datoms in the same transaction
;; conflict {:d1 [17592186045427 :news/title "Test" 13194139534372
;;                true], :d2 [17592186045427 :news/title "Test2" 13194139534372
;;                            true]}

;; 1. Caused by datomic.impl.Exceptions$IllegalArgumentExceptionInfo
;; :db.error/not-an-entity Unable to resolve entity: [:news/foobar
;;                                                    "a"] in datom [[:news/foobar "a"] :news/foobar "b"]
;; {:db/error :db.error/not-an-entity}
;; @(d/transact conn [[:db/add #db/id[db.part/user -1] :news/foobar "a"]
;;                    [:db/add #db/id[db.part/user -2] :news/zot "a"]])

;; @(d/transact conn [[:db/add #db/id[db.part/user -1] :news/baz #db/id[db.part/user -2]]
;;                    [:db/add #db/id[db.part/user -2] :news/baz #db/id[db.part/user -1]]])
;; {:db-before datomic.db.Db@a0166706, :db-after datomic.db.Db@5d0d6a62, :tx-data [#datom[13194139534399 50 #inst "2016-07-20T04:45:19.553-00:00" 13194139534399 true] #datom[17592186045504 68 17592186045505 13194139534399 true] #datom[17592186045505 68 17592186045504 13194139534399 true]], :tempids {-9223350046622220289 17592186045504, -9223350046622220290 17592186045505}}

;; @(d/transact conn [[:db/add [:news/foobar "a"] :news/foobar "b"]])

;; @(d/transact conn [[:db/retract 17592186045505 68 17592186045504 13194139534399]])

;; 1. Caused by datomic.impl.Exceptions$IllegalArgumentExceptionInfo
;; :db.error/not-a-data-function Unable to resolve data function:
;; {:news/foobar "a", :news/zot "a", :news/title "test"}
;; {:db/error :db.error/not-a-data-function}

;; 1. Caused by datomic.impl.Exceptions$IllegalArgumentExceptionInfo
;; :db.error/entity-missing-db-id Missing :db/id
;; {:input {:news/foobar "a", :news/zot "a", :news/title "test"}, :db/error :db.error/entity-missing-db-id}
;; @(d/transact conn [{:news/foobar "a" :news/zot "a" :news/title "test"}])

;; ;; 1. Caused by datomic.impl.Exceptions$IllegalStateExceptionInfo
;; ;; :db.error/unique-conflict Unique conflict: :news/zot, value: a
;; ;; already held by: 17592186045489 asserted for: 17592186045483
;; ;; {:db/error :db.error/unique-conflict}
;; @(d/transact conn [{:db/id [:news/foobar "a"] :news/zot "a" :news/title "test"}])

;; ;; 1. Caused by datomic.impl.Exceptions$IllegalStateExceptionInfo
;; ;; :db.error/unique-conflict Unique conflict: :news/zot, value: b
;; ;; already held by: 17592186045492 asserted for: 17592186045483
;; @(d/transact conn [
;;                    {:db/id #db/id[db.part/user -1] :news/foobar "c" :news/zot "c"}])
;; ;; {:db/id #db/id[db.part/user -1] :news/zot "a"}                   ])

;; ;; 1. Caused by datomic.impl.Exceptions$IllegalStateExceptionInfo
;; ;; :db.error/unique-conflict Unique conflict: :news/foobar, value: b
;; ;; already held by: 17592186045478 asserted for: 1
;; ;; {:db/error :db.error/unique-conflict}

;; @(d/transact conn [[:db/add 17592186045478 :news/foobar "b"]])
;; @(d/transact conn [[:db/add 17592186045478 :news/foobar "a"]])

;; ;; Datomic accepts two different id-literals resolving to the same entid.
(def txx #db/id[db.part/tx])
(def x @(d/transact conn [[:db/add #db/id[db.part/user -1] :news/foobar "c"]
                          [:db/add #db/id[db.part/user -2] :news/zot "c"]
                          [:db/add txx :news/title "x"]
                          [:db/add #db/id[db.part/tx -5] :news/title "x"]
                          ]))

(def x @(d/transact conn [[:db/add #db/id[db.part/user -1] :news/baz #db/id[db.part/tx]]]))

;; ;; 1. Caused by datomic.impl.Exceptions$IllegalArgumentExceptionInfo
;; ;; :db.error/tempid-not-an-entity tempid used only as value in
;; ;; transaction
;; ;; {:db/error :db.error/tempid-not-an-entity}
;; @(d/transact conn [[:db/add #db/id[db.part/user -1] :news/baz #db/id[db.part/user -3]]
;;                    [:db/add #db/id[db.part/user -2] :news/baz #db/id[db.part/user -3]]])

;; ;; 2. Unhandled java.util.concurrent.ExecutionException
;; ;; java.lang.IndexOutOfBoundsException
;; @(d/transact conn [[:db/add #db/id[db.part/user -1] :db/ident :news/zot]
;;                    [:db/add #db/id[db.part/user -2] #db/id[db.part/user -1] "c"]])

(vec
  (q '[:find ?added ?a ?v ?tx
       :where
       [17592186045427 ?a ?v ?tx ?added]
       ;; [(>= ?e 17592186045427)]
       ;; [(tx-ids ?log ?t1 ?t2) [?tx ...]]
       ;; [(tx-data ?log ?tx) [[?e ?a ?v _ ?added]]]
       ]
     (d/db conn)))
;; [[true 63 "Test" 13194139534324]]

(vec
  (q '[:find ?e ?added ?a ?v ?tx
       :in ?log ?t1 ?t2
       :where
       [(tx-ids ?log ?t1 ?t2) [?tx ...]]
       [(tx-data ?log ?tx) [[?e ?a ?v _ ?added]]]
       [(= ?e 17592186045427)]
       ]
     (d/log conn) #inst "2013-08-01" #inst "2017-01-01"))

[[17592186045427 false 63 "Rain Tomorrow" 13194139534324] [17592186045427 true 63 "Test" 13194139534324] [17592186045427 false 63 "Test" 13194139534325] [17592186045427 true 63 "Rain Tomorrow" 13194139534322]]
;; [[17592186045427 false 63 "Rain Tomorrow" 13194139534324] [17592186045427 true 63 "Test" 13194139534324] [17592186045427 true 63 "Rain Tomorrow" 13194139534322]]

@(d/transact conn [[:db/retract 17592186045427 :news/title "Test"]])



(sort-by first (d/q '[:find ?a ?ident :where
                      [?e ?a ?ident]
                      [_ :db.install/attribute ?e]] (db/bootstrap-db)))

(def x (db/bootstrap-db))

(pprint (vec (map (juxt :e :a :v :tx :added) (filter #(= 13194139533312 (:tx %)) (d/datoms x :eavt)))))
(pprint (sort (set (map (juxt :tx) (d/datoms x :eavt)))))

(def tx0 13194139533312)
(def tx1 13194139533366)
(def tx2 13194139533368)
(def tx3 13194139533375)

(pprint
  (sort-by first (d/q '[:find ?e ?an ?v ?tx
                        :in $ ?tx
                        :where
                        [?e ?a ?v ?tx true]
                        [?a :db/ident ?an]
                        ] x tx3)))


;; (d/datoms x :eavt))))

;; 13194139533312
