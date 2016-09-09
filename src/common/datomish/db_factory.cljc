;; This Source Code Form is subject to the terms of the Mozilla Public
;; License, v. 2.0. If a copy of the MPL was not distributed with this
;; file, You can obtain one at http://mozilla.org/MPL/2.0/.

(ns datomish.db-factory
  #?(:cljs
     (:require-macros
      [datomish.pair-chan :refer [go-pair <?]]
      [cljs.core.async.macros :refer [go]]))
  (:require
   [datomish.db :as db]
   [datomish.transact :as transact]
   [datomish.transact.bootstrap :as bootstrap]
   [datomish.util :as util #?(:cljs :refer-macros :clj :refer) [raise raise-str cond-let]]
   [datomish.schema :as ds]
   [datomish.sqlite :as s]
   [datomish.sqlite-schema :as sqlite-schema]
   #?@(:clj [[datomish.pair-chan :refer [go-pair <?]]
             [clojure.core.async :as a :refer [chan go <! >!]]])
   #?@(:cljs [[datomish.pair-chan]
              [cljs.core.async :as a :refer [chan <! >!]]]))
  #?(:clj
     (:import
      [datomish.datom Datom])))

(defn <idents [sqlite-connection]
  "Read the ident map materialized view from the given SQLite store.
  Returns a map (keyword ident) -> (integer entid), like {:db/ident 0}."

  (go-pair
    (let [rows (<? (s/all-rows sqlite-connection ["SELECT ident, entid FROM idents"]))]
      (into {} (map (fn [row] [(sqlite-schema/<-SQLite :db.type/keyword (:ident row)) (:entid row)])) rows))))

(defn <parts [sqlite-connection]
  "Read the parts map materialized view from the given SQLite store.
  Returns a map (keyword part) -> {:start integer :idx integer}, like {:db.part/user {start: 0x100 idx: 0x101}}."

  (go-pair
    (let [rows (<? (s/all-rows sqlite-connection ["SELECT part, start, idx FROM parts"]))]
      (into {} (map (fn [row] [(sqlite-schema/<-SQLite :db.type/keyword (:part row)) (select-keys row [:start :idx])])) rows))))

(defn <symbolic-schema [sqlite-connection idents]
  "Read the schema map materialized view from the given SQLite store.
  Returns a map (keyword ident) -> (map (keyword attribute -> keyword value)), like
  {:db/ident {:db/cardinality :db.cardinality/one}}."

  (go-pair
    (let [ident-map  (clojure.set/map-invert idents)
          ref-tag    (sqlite-schema/->tag :db.type/ref)
          kw<-SQLite (partial sqlite-schema/<-SQLite :db.type/keyword)]
      (->>
        (s/all-rows sqlite-connection ["SELECT ident, attr, value, value_type_tag FROM schema"])
        (<?)

        (group-by (comp kw<-SQLite :ident))
        (map (fn [[ident rows]]
               [ident
                (into {} (map (fn [row]
                                (let [tag (:value_type_tag row)
                                      ;; We want a symbolic schema, but most of our values are
                                      ;; :db.type/ref attributes.  Map those entids back to idents.
                                      ;; This is ad-hoc since we haven't built a functional DB
                                      ;; instance yet.
                                      v   (if (= tag ref-tag) (get ident-map (:value row)) (:value row))]
                                  [(kw<-SQLite (:attr row))
                                   (sqlite-schema/<-tagged-SQLite tag v)])) rows))]))
        (into {})))))

(defn <initialize-connection [sqlite-connection]
  (go-pair
    ;; Some of these return values when set, and some don't, hence the craziness here.
    (<? (s/execute! sqlite-connection ["PRAGMA page_size=32768"]))
    (<? (s/all-rows sqlite-connection ["PRAGMA journal_mode=wal"]))
    (<? (s/all-rows sqlite-connection ["PRAGMA wal_autocheckpoint=32"]))
    (<? (s/all-rows sqlite-connection ["PRAGMA journal_size_limit=3145728"]))
    (<? (s/execute! sqlite-connection ["PRAGMA foreign_keys=ON"]))))

(defn <db-with-sqlite-connection
  [sqlite-connection]
  (go-pair
    (<? (<initialize-connection sqlite-connection))

    (when-not (= sqlite-schema/current-version (<? (sqlite-schema/<ensure-current-version sqlite-connection)))
      (raise "Could not ensure current SQLite schema version."))

    (let [db            (db/db sqlite-connection bootstrap/idents bootstrap/parts bootstrap/symbolic-schema)
          bootstrapped? (<? (db/<bootstrapped? db))]
      (when-not bootstrapped?
        ;; We need to bootstrap the DB.
        (let [fail-alter-ident (fn [old new] (if-not (= old new)
                                               (raise "Altering idents is not yet supported, got " new " altering existing ident " old
                                                      {:error :schema/alter-idents :old old :new new})
                                               new))
              fail-alter-attr  (fn [old new] (if-not (= old new)
                                               (raise "Altering schema attributes is not yet supported, got " new " altering existing schema attribute " old
                                                      {:error :schema/alter-schema :old old :new new})
                                               new))]
          (let [exec (partial s/execute! (:sqlite-connection db))
                part->vector (fn [[part {:keys [start idx]}]]
                               [(sqlite-schema/->SQLite part) start idx])]
            ;; TODO: allow inserting new parts.
            ;; TODO: think more carefully about allocating new parts and bitmasking part ranges.
            ;; TODO: install these using bootstrap assertions.  It's tricky because the part ranges are implicit.
            ;; TODO: chunk into 999/3 sections, for safety.
            (<? (exec
                  (cons (str "INSERT INTO parts VALUES "
                             (apply str (interpose ", " (repeat (count bootstrap/parts) "(?, ?, ?)"))))
                        (mapcat part->vector bootstrap/parts)))))

          (-> db
              ;; We use <with-internal rather than <transact! to apply the bootstrap transaction
              ;; data but to not follow the regular schema application process.  We can't apply the
              ;; schema changes, since the applied datoms would conflict with the bootstrapping
              ;; idents and schema.  (The bootstrapping idents and schema are required to be able to
              ;; write to the database conveniently; without them, we'd have to manually write
              ;; datoms to the store.  It's feasible but awkward.)  After bootstrapping, we read
              ;; back the idents and schema, just like when we re-open.
              (transact/<with-internal (bootstrap/tx-data) fail-alter-ident fail-alter-attr)
              (<?))))

      ;; We just bootstrapped, or we are returning to an already bootstrapped DB.
      (let [idents          (<? (<idents sqlite-connection))
            parts           (<? (<parts sqlite-connection))
            symbolic-schema (<? (<symbolic-schema sqlite-connection idents))]
        (when-not bootstrapped?
          (when (not (= idents bootstrap/idents))
            (raise "After bootstrapping database, expected new materialized idents and old bootstrapped idents to be identical"
                   {:error :bootstrap/bad-idents,
                    :new   idents :old bootstrap/idents
                    }))
          (when (not (= (dissoc parts :db.part/tx) (dissoc bootstrap/parts :db.part/tx))) ;; TODO: work around tx allocation.
            (raise "After bootstrapping database, expected new materialized parts and old bootstrapped parts to be identical (outside of db.part/tx)"
                   {:error :bootstrap/bad-parts,
                    :new   (dissoc parts :db.part/tx) :old (dissoc bootstrap/parts :db.part/tx)
                    }))
          (when (not (= symbolic-schema bootstrap/symbolic-schema))
            (raise "After bootstrapping database, expected new materialized symbolic schema and old bootstrapped symbolic schema to be identical"
                   {:error :bootstrap/bad-symbolic-schema,
                    :new   symbolic-schema :old bootstrap/symbolic-schema
                    })))
        (db/db sqlite-connection idents parts symbolic-schema)))))
