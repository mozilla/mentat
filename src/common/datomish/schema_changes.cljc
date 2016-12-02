;; Copyright 2016 Mozilla
;;
;; Licensed under the Apache License, Version 2.0 (the "License"); you may not use
;; this file except in compliance with the License. You may obtain a copy of the
;; License at http://www.apache.org/licenses/LICENSE-2.0
;; Unless required by applicable law or agreed to in writing, software distributed
;; under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
;; CONDITIONS OF ANY KIND, either express or implied. See the License for the
;; specific language governing permissions and limitations under the License.

(ns datomish.schema-changes
  (:require
   [datomish.util :as util #?(:cljs :refer-macros :clj :refer) [raise raise-str cond-let]]))

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

  This would be more pleasant with `q` and pull expressions.

  Note that this function takes as input an existing map of {entid ident}.
  That's because it's possible for an ident to be established in a separate
  set of datoms -- we can't re-insert it without uniqueness constraint
  violations, so we just provide it here."

  [datoms existing-idents]
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
                       (if-let [ident (or (:db/ident db-avs)
                                          ;; The schema table wants a keyword, not an entid, and
                                          ;; we need to check the existing idents…
                                          (when (contains? existing-idents e)
                                            (if (keyword? e)
                                              e
                                              (get existing-idents e))))]
                         [ident (dissoc db-avs :db/ident)]
                         (raise ":db.install/attribute requires :db/ident, got " db-avs " for " e
                                {:error :schema/db-install :op db-avs}))))))))))
