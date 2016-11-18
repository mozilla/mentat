;; Copyright 2016 Mozilla
;;
;; Licensed under the Apache License, Version 2.0 (the "License"); you may not use
;; this file except in compliance with the License. You may obtain a copy of the
;; License at http://www.apache.org/licenses/LICENSE-2.0
;; Unless required by applicable law or agreed to in writing, software distributed
;; under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
;; CONDITIONS OF ANY KIND, either express or implied. See the License for the
;; specific language governing permissions and limitations under the License.

(ns datomish.schema-management
  #?(:cljs
     (:require-macros
      [datomish.pair-chan :refer [go-pair <?? <?]]))
  (:require
   [clojure.data :refer [diff]]
   #?@(:clj [[datomish.pair-chan :refer [go-pair <?? <?]]])
   [clojure.set]
   [datomish.api :as d]
   [datomish.schema]        ; For validation.
   [datomish.util :as util
    #?(:cljs :refer-macros :clj :refer) [raise raise-str cond-let]]))

;; For testing.
(def log (fn [& args]) #_ println)

;; This code implements the concept described in
;; https://github.com/mozilla/datomish/wiki/Proposal:-application-schema-coordination-and-versioning
;;
;; This is a high-level API: it's built on top of the schema alteration
;; primitives and plain ol' storage layer that Datomish exposes.
;;
;; Schema fragments are described by name and version number.
;; The existing schema in the store is diffed against incoming fragments.
;;
;; Validation:
;; - No attribute should be mentioned in a different fragment in the
;;   store and the input. Attributes cannot move between fragments.
;; - Fragments with the same name and version should be congruent,
;;   with the only acceptable changes being to add new attributes.
;; - Fragments with an increased version number can make changes to
;;   attributes:
;;   - Adding new attributes.
;;   - Rename existing attributes.
;;   - Altering cardinality, uniqueness, or indexing properties.
;;
;; The inputs to the diffing process are:
;; - The set of schema fragments.
;; - The existing database (and implicitly its active schema).
;; - A collection of attribute renames.
;; - A set of app- and fragment-scoped pre/post functions that will
;;   be run before and after schema changes are applied.
;;
;; The output of the diffing process, if validation succeeds, is a
;; set of operations to perform on the knowledge base. If there are
;; no version changes, no pre/post functions will be included.
;;
;; Potential outcomes:
;; - An attribute is mentioned in two incoming fragments: error.
;; - An attribute is in fragment A in the store and fragment B in input: error.
;; - An attribute changed between the store and input, but the version number is the same: error.
;; - An attribute is present in input, but not the store, and the version number is the same: add the attribute.
;; - An attribute is present in the store, but not input, and the version number is the same: do nothing.
;; - A fragment's version number is higher in the store than in the input:
;;   - If the input is a subset of the fragment in the store, then do nothing.
;;   - If the input differs, then error.
;; - A fragment's version number is higher in the input than in the store:
;;   - Run app 'pre' and 'post'.
;;   - Run this fragment's 'pre' and 'post'.
;;   - Alter the store to match. If altering fails due to a consistency error, roll back and error out.
;;
;; The core data format here, which we call a "managed schema fragment" is:
;;
;; {:name :org.mozilla.foo
;;  :version 4
;;  :attributes {:foo/bar {:db/valueType ...}}}
;;
;; This can be trivially expanded from the 'simple schema' format used by
;; JS callers:
;;
;; {"name": "org.mozilla.foo",
;;  "version": 4,
;;  "attributes": [
;;    {"name": "foo/bar",
;;     "type": ...}]}
;;
;; and it can be trivially collapsed into the format understood by the
;; transactor, which we call "schema datoms":
;;
;; [{:db/ident :org.mozilla.foo
;;   :db.schema/version 4}
;;  {:db/name :foo/bar
;;   :db.schema/_attribute :org.mozilla.foo
;;   :db/valueType ...}]

(defn- attribute->datoms [schema-fragment-id [attribute-name attribute-pairs]]
  (let [attribute-id (d/id-literal :db.part/user)]
    [(assoc
       attribute-pairs
       :db.install/_attribute :db.part/db
        ;; Point back to the fragment.
       :db.schema/_attribute schema-fragment-id
       :db/id attribute-id
       :db/ident attribute-name)]))

(defn managed-schema-fragment->datoms [{:keys [name version attributes]}]
  (let [fragment-id (d/id-literal :db.part/db)]
    (conj
      (mapcat (partial attribute->datoms fragment-id) attributes)
      {:db/id fragment-id
       :db/ident name
       :db.schema/version version})))

(defn <collect-schema-fragment-versions
  "Return a map, like {:org.mozilla.foo 5, :org.mozilla.core 2}."
  [db]
  (let [ident (partial d/ident db)]
    (go-pair
      (into {}
        (map
          (fn [[s v]] [(ident s) v])
          (<?
            (d/<q db '[:find ?s ?v
                       :in $
                       :where [?s :db.schema/version ?v]])))))))

(defn <collect-schema-fragment-attributes
  "Return a map, like {:foo/name :org.mozilla.foo}.
   Attributes that are not linked to a fragment will not be returned."
  [db]
  (let [ident (partial d/ident db)]
    (go-pair
      (into {}
        (map
          (fn [[a f]] [(ident a) (ident f)])
          (<?
            (d/<q db '[:find ?a ?f
                       :in $
                       :where [?f :db.schema/attribute ?a]])))))))

(defn db->symbolic-schema [db]
  (:symbolic-schema db))

(defn changed-attribute->datoms [schema-fragment-id attribute-name existing new-values]
  (let [differences (first (diff new-values existing))]
    (when-not (empty? differences)
      [(merge
         {:db/id (d/id-literal :db.part/user)
          :db/ident attribute-name
          ;; Point back to the fragment.
          :db.schema/_attribute schema-fragment-id
          :db.alter/_attribute :db.part/db}
         differences)])))

(defn changed-schema-fragment->datoms [schema-fragment-id existing-schema name attributes version]
  (conj
    (mapcat (fn [[attribute-name new-values]]
              (let [existing (get existing-schema attribute-name)]
                (if existing
                  (changed-attribute->datoms
                    schema-fragment-id
                    attribute-name
                    existing
                    new-values)
                  (attribute->datoms
                    schema-fragment-id [attribute-name new-values]))))
            attributes)
    {:db.schema/version version
     :db/ident name
     :db/id (d/id-literal :db.part/db)}))

(defn- prepare-schema-application-for-fragments
  "Given a non-empty collection of fragments known to be new or outdated,
   yield a migration sequence containing the necessary pre/post ops and
   transact bodies."
  [db
   symbolic-schema
   schema-fragment-versions
   schema-fragment-attributes
   {:keys [fragments pre post fragment-pre fragment-post] :as args}]

  (when-let
    [body
     (mapcat
       (fn [{:keys [name version attributes] :as fragment}]
         (let [existing-version (get schema-fragment-versions name)

               datoms
               [[:transact
                 (if existing-version
                   ;; It's a change.
                   ;; Transact the datoms to effect the change and
                   ;; bump the schema fragment version.
                   (changed-schema-fragment->datoms
                     (d/entid db name)
                     symbolic-schema
                     name
                     attributes
                     version)

                   ;; It's new! Just do it.
                   (managed-schema-fragment->datoms fragment))]]]

           ;; We optionally allow you to provide a `:none` migration here, which
           ;; is useful in the case where a vocabulary might have been added
           ;; outside of the schema management system.
           (concat
             (when-let [fragment-pre-for-this
                        (get-in fragment-pre [name (or existing-version :none)])]
               [[:call fragment-pre-for-this]])
             datoms
             (when-let [fragment-post-for-this
                        (get-in fragment-post [name (or existing-version :none)])]
               [[:call fragment-post-for-this]]))))

       fragments)]

    (concat
      (when pre [[:call pre]])
      body
      (when post [[:call post]]))))

(defn- <prepare-schema-application*
  [db {:keys [fragments pre post fragment-pre fragment-post] :as args}]
  {:pre [(map? (first fragments))]}
  (go-pair
    (let [symbolic-schema (db->symbolic-schema db)
          schema-fragment-versions (<? (<collect-schema-fragment-versions db))
          schema-fragment-attributes (<? (<collect-schema-fragment-attributes db))]

      ;; Filter out any incoming fragments that are already present with
      ;; the correct version. Err if any of the fragments are outdated,
      ;; or contain attributes that are already present elsewhere.
      (let [to-apply
            (filter
              (fn [{:keys [name version attributes]}]
                {:pre [(not (nil? name))
                       (integer? version)
                       (not (empty? attributes))]}

                ;; Make sure that every attribute in this fragment is either
                ;; already associate with its ident, or not associated with
                ;; anything. We do this before we even check the version.
                (doseq [attribute-name (keys attributes)]
                  (when-let [existing-fragment (get schema-fragment-attributes attribute-name)]
                    (when-not (= existing-fragment name)
                      (raise "Attribute " attribute-name
                             " already belongs to schema fragment "
                             existing-fragment ", not " name "."
                             {:error :schema/different-fragment
                              :existing existing-fragment
                              :fragment name}))))

                ;; Now we know that every attribute is either this fragment's or
                ;; not assigned to one.
                ;; Check our fragment version against the store.
                (let [existing-version (get schema-fragment-versions name)]
                  (log "Schema" name "at existing version" existing-version)
                  (or (nil? existing-version)
                      (> version existing-version)
                      (when (< version existing-version)
                        (raise "Existing version of " name " is " existing-version
                               ", which is later than requested " version "."
                               {:error :schema/outdated-version
                                :name name
                                :version version
                                :existing existing-version})))))
              fragments)]

        (if (empty? to-apply)
          (do
            (log "No fragments left to apply.")
            nil)
          (prepare-schema-application-for-fragments
            db
            symbolic-schema
            schema-fragment-versions
            schema-fragment-attributes
            (assoc args :fragments to-apply)))))))

(defn <prepare-schema-application
  "Take a database and a sequence of managed schema fragments,
   along with migration tools, and return a migration operation."
  [db {:keys [fragments pre post fragment-pre fragment-post] :as args}]
  (when-not (contains? args :fragments)
    (raise-str "Missing :fragments argument to <prepare-schema-application."))
  (if (empty? fragments)
    (go-pair nil)
    (do
      ;; Validate each fragment.
      (doseq [fragment fragments]
        (datomish.schema/validate-schema (:attributes fragment)))
      (let [repeated-attributes (util/repeated-keys (map :attributes fragments))]
        (when-not (empty? repeated-attributes)
          (raise "Attributes appear in more than one fragment: " repeated-attributes
                 {:error :schema/repeated-attributes
                  :repeated repeated-attributes}))

        ;; At this point we know we have schema fragments to apply,
        ;; and that they don't overlap. They might still cross fragment
        ;; boundaries when compared to the store, and they might still
        ;; be inconsistent, but we can proceed to the next step.
        (<prepare-schema-application* db args)))))

(defn- <schema-fragment-versions-match?
  "Quickly return true if every provided fragment matches the
   version in the store."
  [db fragments]
  (go-pair
    (let [schema-fragment-versions (<? (<collect-schema-fragment-versions db))]
      (every?
        (fn [{:keys [name version]}]
          (= (get schema-fragment-versions name)
             version))
        fragments))))

(defn <apply-schema-alteration
  "Take a database and a sequence of managed schema fragments,
  along with migration tools, and transact a migration operation.
  Throws and rolls back if any step of the operation fails. Returns
  nil if no work was done, or the last db-report otherwise."
  [conn args]
  (go-pair
    (if (or (empty? (:fragments args))
            (<? (<schema-fragment-versions-match?
                  (d/db conn)
                  (:fragments args))))
      (log "No schema work to do.")

      ;; Do the real fragment op computation inside the transaction.
      ;; This avoids a check-then-write race.
      (<?
        (d/<transact!
          conn
          (fn [db do-transact]
            (go-pair
              (let [last-report (atom {:db-after db})
                    ops (<? (<prepare-schema-application db args))]
                (doseq [[op op-arg] ops]
                  (case op
                        :transact
                        (reset! last-report
                                (<? (do-transact
                                      (:db-after @last-report)
                                      op-arg)))

                        :call
                        ;; We use <?? so that callers don't accidentally
                        ;; break us if they pass a function that returns
                        ;; nil rather than a *channel* that returns nil.
                        (when-let [new-report
                                   (<?? (op-arg (:db-after @last-report)
                                                do-transact))]
                          (when-not (:db-after new-report)
                            (raise "Function didn't return a valid report."
                                   {:error :schema/invalid-report
                                    :function op-arg
                                    :returned new-report}))

                          (reset! last-report new-report))))
                @last-report))))))))
