;; This Source Code Form is subject to the terms of the Mozilla Public
;; License, v. 2.0. If a copy of the MPL was not distributed with this
;; file, You can obtain one at http://mozilla.org/MPL/2.0/.

(ns datomish.schema-changes
  (:require
   [datomish.util :as util #?(:cljs :refer-macros :clj :refer) [raise raise-str cond-let]]))

(defn- is-install? [db [_ a & _]]
  (= a (get-in db [:idents :db.install/attribute])))

(defn datoms->schema-fragment
  "Turn [[:db.part/db :db.install/attribute e] [e :db/ident :attr]] into {:attr {:db/* v}}.

  From http://docs.datomic.com/schema.html, :db/ident, :db/valueType,
  and :db/cardinality are required.  For us, enforce that valueType and
  cardinality are present at schema validation time.

  This code is not pretty, so here's what it does:

  Input: a sequence of datoms, like [e :keyword-attr v _ added].
  1. Select [:db.part/db :db.install/attribute ... ].
  2. Fail if any are not (= added true)
  3. For each [ :db.part/db :db.install/attribute e ], collect
     {e {:db/* v}}, dropping the inner :db/ident key.
  4. Map e -> ident; fail if not possible.
  5. Return the map, with ident keys.

  This would be more pleasant with `q` and pull expressions."

  [datoms]
  {:pre [(sequential? datoms)]}

  (let [db-install? (fn [datom]
                      (= [:db.part/db :db.install/attribute] ((juxt :e :a) datom)))
        db-installs (filter db-install? datoms)]
    (if (empty? db-installs)
      {}
      (if-let [retracted (first (filter (comp not :added) db-installs))]
        (raise "Retracting a :db.install/attribute is not yet supported, got " retracted
               {:error :schema/db-install :op retracted})
        (let [by-e (group-by :e datoms)

              ;; TODO: pull entity from database, rather than expecting entire attribute to be in single transaction.
              installed-es (select-keys by-e (map :v db-installs))
              ;; select-keys ignores missing keys.  We don't want that.
              installed-es (merge (into {} (map (juxt :v (constantly {})) db-installs)) installed-es)

              db-*? (fn [datom]
                      (= "db" (namespace (:a datom))))]

          ;; Just the :db/* attribute-value pairs.
          (into {} (for [[e datoms] installed-es]
                     (let [->av (juxt :a :v)
                           ;; TODO: transduce!
                           db-avs (into {} (map ->av (filter db-*? datoms)))]
                       ;; TODO: get ident from existing datom, to allow [:db.part/db :db.install/attribute existing-id].
                       (if-let [ident (:db/ident db-avs)]
                         [ident (dissoc db-avs :db/ident)]
                         (raise ":db.install/attribute requires :db/ident, got " db-avs " for " e
                                {:error :schema/db-install :op db-avs}))))))))))
