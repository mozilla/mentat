// Copyright 2016 Mozilla
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

#![allow(dead_code)]

//! This module implements the transaction application algorithm described at
//! https://github.com/mozilla/mentat/wiki/Transacting and its children pages.
//!
//! The implementation proceeds in four main stages, labeled "Pipeline stage 1" through "Pipeline
//! stage 4".  _Pipeline_ may be a misnomer, since the stages as written **cannot** be interleaved
//! in parallel.  That is, a single transacted entity cannot flow through all the stages without its
//! sibling entities.
//!
//! This unintuitive architectural decision was made because the second and third stages (resolving
//! lookup refs and tempids, respectively) operate _in bulk_ to minimize the number of expensive
//! SQLite queries by processing many in one SQLite invocation.  Pipeline stage 2 doesn't need to
//! operate like this: it is easy to handle each transacted entity independently of all the others
//! (and earlier, less efficient, implementations did this).  However, Pipeline stage 3 appears to
//! require processing multiple elements at the same time, since there can be arbitrarily complex
//! graph relationships between tempids.  Pipeline stage 4 (inserting elements into the SQL store)
//! could also be expressed as an independent operation per transacted entity, but there are
//! non-trivial uniqueness relationships inside a single transaction that need to enforced.
//! Therefore, some multi-entity processing is required, and a per-entity pipeline becomes less
//! attractive.
//!
//! A note on the types in the implementation.  The pipeline stages are strongly typed: each stage
//! accepts and produces a subset of the previous.  We hope this will reduce errors as data moves
//! through the system.  In contrast the Clojure implementation rewrote the fundamental entity type
//! in place and suffered bugs where particular code paths missed cases.
//!
//! The type hierarchy accepts `Entity` instances from the transaction parser and flows `Term`
//! instances through the term-rewriting transaction applier.  `Term` is a general `[:db/add e a v]`
//! with restrictions on the `e` and `v` components.  The hierarchy is expressed using `Result` to
//! model either/or, and layers of `Result` are stripped -- we might say the `Term` instances are
//! _lowered_ as they flow through the pipeline.  This type hierarchy could have been expressed by
//! combinatorially increasing `enum` cases, but this makes it difficult to handle the `e` and `v`
//! components symmetrically.  Hence, layers of `Result` type aliases.  Hopefully the explanatory
//! names -- `TermWithTempIdsAndLookupRefs`, anyone? -- and strongly typed stage functions will help
//! keep everything straight.

use std::borrow::Cow;
use std::collections::{
    BTreeMap,
    BTreeSet,
    VecDeque,
};

use db;
use db::{
    MentatStoring,
    PartitionMapping,
};
use edn::{
    NamespacedKeyword,
};
use entids;
use errors::{ErrorKind, Result};
use internal_types::{
    Either,
    KnownEntid,
    LookupRef,
    LookupRefOrTempId,
    TempIdHandle,
    TempIdMap,
    Term,
    TermWithTempIdsAndLookupRefs,
    TermWithTempIds,
    TermWithoutTempIds,
    replace_lookup_ref};

use mentat_core::{
    DateTime,
    Schema,
    UTC,
    attribute,
    intern_set,
};
use mentat_tx::entities as entmod;
use mentat_tx::entities::{
    Entity,
    OpType,
    TempId,
};
use mentat_tx_parser;
use metadata;
use rusqlite;
use schema::{
    SchemaBuilding,
    SchemaTypeChecking,
};
use types::{
    Attribute,
    AVPair,
    AVMap,
    Entid,
    PartitionMap,
    TypedValue,
    TxReport,
    ValueType,
};
use upsert_resolution::Generation;

/// A transaction on its way to being applied.
#[derive(Debug)]
pub struct Tx<'conn, 'a> {
    /// The storage to apply against.  In the future, this will be a Mentat connection.
    store: &'conn rusqlite::Connection, // TODO: db::MentatStoring,

    /// The partition map to allocate entids from.
    ///
    /// The partition map is volatile in the sense that every succesful transaction updates
    /// allocates at least one tx ID, so we own and modify our own partition map.
    partition_map: PartitionMap,

    /// The schema to update from the transaction entities.
    ///
    /// Transactions only update the schema infrequently, so we borrow this schema until we need to
    /// modify it.
    schema_for_mutation: Cow<'a, Schema>,

    /// The schema to use when interpreting the transaction entities.
    ///
    /// This schema is not updated, so we just borrow it.
    schema: &'a Schema,

    /// The transaction ID of the transaction.
    tx_id: Entid,

    /// The timestamp when the transaction began to be committed.
    tx_instant: DateTime<UTC>,
}

impl<'conn, 'a> Tx<'conn, 'a> {
    pub fn new(
        store: &'conn rusqlite::Connection,
        partition_map: PartitionMap,
        schema_for_mutation: &'a Schema,
        schema: &'a Schema,
        tx_id: Entid,
        tx_instant: DateTime<UTC>) -> Tx<'conn, 'a> {
        Tx {
            store: store,
            partition_map: partition_map,
            schema_for_mutation: Cow::Borrowed(schema_for_mutation),
            schema: schema,
            tx_id: tx_id,
            tx_instant: tx_instant,
        }
    }

    fn ensure_ident_exists(&self, e: &NamespacedKeyword) -> Result<KnownEntid> {
        let entid = self.schema.require_entid(e)?;
        Ok(KnownEntid(entid))
    }

    fn ensure_entid_exists(&self, e: Entid) -> Result<KnownEntid> {
        if self.partition_map.contains_entid(e) {
            Ok(KnownEntid(e))
        } else {
            bail!(ErrorKind::UnrecognizedEntid(e))
        }
    }

    /// Given a collection of tempids and the [a v] pairs that they might upsert to, resolve exactly
    /// which [a v] pairs do upsert to entids, and map each tempid that upserts to the upserted
    /// entid.  The keys of the resulting map are exactly those tempids that upserted.
    pub fn resolve_temp_id_avs<'b>(&self, temp_id_avs: &'b [(TempIdHandle, AVPair)]) -> Result<TempIdMap> {
        if temp_id_avs.is_empty() {
            return Ok(TempIdMap::default());
        }

        // Map [a v]->entid.
        let mut av_pairs: Vec<&AVPair> = vec![];
        for i in 0..temp_id_avs.len() {
            av_pairs.push(&temp_id_avs[i].1);
        }

        // Lookup in the store.
        let av_map: AVMap = self.store.resolve_avs(&av_pairs[..])?;

        // Map id->entid.
        let mut temp_id_map: TempIdMap = TempIdMap::default();
        for &(ref temp_id, ref av_pair) in temp_id_avs {
            if let Some(n) = av_map.get(&av_pair) {
                if let Some(&KnownEntid(previous_n)) = temp_id_map.get(&*temp_id) {
                    if *n != previous_n {
                        // Conflicting upsert!  TODO: collect conflicts and give more details on what failed this transaction.
                        bail!(ErrorKind::NotYetImplemented(format!("Conflicting upsert: tempid '{}' resolves to more than one entid: {:?}, {:?}", temp_id, previous_n, n))) // XXX
                    }
                }
                temp_id_map.insert(temp_id.clone(), KnownEntid(*n));
            }
        }

        Ok((temp_id_map))
    }

    /// Pipeline stage 1: convert `Entity` instances into `Term` instances, ready for term
    /// rewriting.
    ///
    /// The `Term` instances produce share interned TempId and LookupRef handles, and we return the
    /// interned handle sets so that consumers can ensure all handles are used appropriately.
    fn entities_into_terms_with_temp_ids_and_lookup_refs<I>(&self, entities: I) -> Result<(Vec<TermWithTempIdsAndLookupRefs>, intern_set::InternSet<TempId>, intern_set::InternSet<AVPair>)> where I: IntoIterator<Item=Entity> {
        let mut temp_ids: intern_set::InternSet<TempId> = intern_set::InternSet::new();
        let mut lookup_refs: intern_set::InternSet<AVPair> = intern_set::InternSet::new();

        let intern_lookup_ref = |lookup_refs: &mut intern_set::InternSet<AVPair>, lookup_ref: entmod::LookupRef| -> Result<LookupRef> {
            let lr_a: i64 = match lookup_ref.a {
                entmod::Entid::Entid(ref a) => *a,
                entmod::Entid::Ident(ref a) => self.schema.require_entid(&a)?,
            };
            let lr_attribute: &Attribute = self.schema.require_attribute_for_entid(lr_a)?;

            if lr_attribute.unique.is_none() {
                bail!(ErrorKind::NotYetImplemented(format!("Cannot resolve (lookup-ref {} {}) with attribute that is not :db/unique", lr_a, lookup_ref.v)))
            }

            let lr_typed_value: TypedValue = self.schema.to_typed_value(&lookup_ref.v, &lr_attribute)?;
            Ok(lookup_refs.intern((lr_a, lr_typed_value)))
        };

        // We want to handle entities in the order they're given to us, while also "exploding" some
        // entities into many.  We therefore push the initial entities onto the back of the deque,
        // take from the front of the deque, and explode onto the front as well.
        let mut deque: VecDeque<Entity> = VecDeque::default();
        deque.extend(entities);

        // Allocate private internal tempids reserved for Mentat.  Internal tempids just need to be
        // unique within one transaction; they should never escape a transaction.
        let mut mentat_id_count = 0;
        let mut allocate_mentat_id = move || {
            mentat_id_count += 1;
            entmod::EntidOrLookupRefOrTempId::TempId(TempId::Internal(mentat_id_count))
        };

        let mut terms: Vec<TermWithTempIdsAndLookupRefs> = Vec::with_capacity(deque.len());

        while let Some(entity) = deque.pop_front() {
            match entity {
                Entity::MapNotation(mut map_notation) => {
                    // :db/id is optional; if it's not given, we generate a special internal tempid
                    // to use for upserting.  This tempid will not be reported in the TxReport.
                    let db_id: entmod::EntidOrLookupRefOrTempId = mentat_tx_parser::remove_db_id(&mut map_notation)?.unwrap_or_else(&mut allocate_mentat_id);

                    // We're not nested, so :db/isComponent is not relevant.  We just explode the
                    // map notation.
                    for (a, v) in map_notation {
                        deque.push_front(Entity::AddOrRetract {
                            op: OpType::Add,
                            e: db_id.clone(),
                            a: a,
                            v: v,
                        });
                    }
                },

                Entity::AddOrRetract { op, e, a, v } => {
                    let a: i64 = match a {
                        entmod::Entid::Entid(ref a) => *a,
                        entmod::Entid::Ident(ref a) => self.schema.require_entid(&a)?,
                    };

                    let attribute: &Attribute = self.schema.require_attribute_for_entid(a)?;

                    let v = match v {
                        entmod::AtomOrLookupRefOrVectorOrMapNotation::Atom(v) => {
                            if attribute.value_type == ValueType::Ref && v.inner.is_text() {
                                Either::Right(LookupRefOrTempId::TempId(temp_ids.intern(v.inner.as_text().cloned().map(TempId::External).unwrap())))
                            } else {
                                // Here is where we do schema-aware typechecking: we either assert that
                                // the given value is in the attribute's value set, or (in limited
                                // cases) coerce the value into the attribute's value set.
                                let typed_value: TypedValue = self.schema.to_typed_value(&v.without_spans(), &attribute)?;
                                Either::Left(typed_value)
                            }
                        },

                        entmod::AtomOrLookupRefOrVectorOrMapNotation::LookupRef(lookup_ref) => {
                            if attribute.value_type != ValueType::Ref {
                                bail!(ErrorKind::NotYetImplemented(format!("Cannot resolve value lookup ref for attribute {} that is not :db/valueType :db.type/ref", a)))
                            }

                            Either::Right(LookupRefOrTempId::LookupRef(intern_lookup_ref(&mut lookup_refs, lookup_ref)?))
                        },

                        entmod::AtomOrLookupRefOrVectorOrMapNotation::Vector(vs) => {
                            if !attribute.multival {
                                bail!(ErrorKind::NotYetImplemented(format!("Cannot explode vector value for attribute {} that is not :db.cardinality :db.cardinality/many", a)));
                            }

                            for vv in vs {
                                deque.push_front(Entity::AddOrRetract {
                                    op: op.clone(),
                                    e: e.clone(),
                                    a: entmod::Entid::Entid(a),
                                    v: vv,
                                });
                            }
                            continue
                        },

                        entmod::AtomOrLookupRefOrVectorOrMapNotation::MapNotation(mut map_notation) => {
                            // TODO: consider handling this at the tx-parser level.  That would be
                            // more strict and expressive, but it would lead to splitting
                            // AddOrRetract, which proliferates types and code, or only handling
                            // nested maps rather than map values, like Datomic does.
                            if op != OpType::Add {
                                bail!(ErrorKind::NotYetImplemented(format!("Cannot explode nested map value in :db/retract for attribute {}", a)));
                            }

                            if attribute.value_type != ValueType::Ref {
                                bail!(ErrorKind::NotYetImplemented(format!("Cannot explode nested map value for attribute {} that is not :db/valueType :db.type/ref", a)))
                            }

                            // :db/id is optional; if it's not given, we generate a special internal tempid
                            // to use for upserting.  This tempid will not be reported in the TxReport.
                            let db_id: Option<entmod::EntidOrLookupRefOrTempId> = mentat_tx_parser::remove_db_id(&mut map_notation)?;
                            let mut dangling = db_id.is_none();
                            let db_id: entmod::EntidOrLookupRefOrTempId = db_id.unwrap_or_else(&mut allocate_mentat_id);

                            // We're nested, so we want to ensure we're not creating "dangling"
                            // entities that can't be reached.  If we're :db/isComponent, then this
                            // is not dangling.  Otherwise, the resulting map needs to have a
                            // :db/unique :db.unique/identity [a v] pair, so that it's reachable.
                            // Per http://docs.datomic.com/transactions.html: "Either the reference
                            // to the nested map must be a component attribute, or the nested map
                            // must include a unique attribute. This constraint prevents the
                            // accidental creation of easily-orphaned entities that have no identity
                            // or relation to other entities."
                            if attribute.component {
                                dangling = false;
                            }

                            for (inner_a, inner_v) in map_notation {
                                let inner_entid: i64 = match inner_a {
                                    entmod::Entid::Entid(ref a) => *a,
                                    entmod::Entid::Ident(ref a) => self.schema.require_entid(&a)?,
                                };

                                let inner_attribute: &Attribute = self.schema.require_attribute_for_entid(inner_entid)?;
                                if inner_attribute.unique == Some(attribute::Unique::Identity) {
                                    dangling = false;
                                }

                                deque.push_front(Entity::AddOrRetract {
                                    op: OpType::Add,
                                    e: db_id.clone(),
                                    a: entmod::Entid::Entid(inner_entid),
                                    v: inner_v,
                                });
                            }

                            if dangling {
                                bail!(ErrorKind::NotYetImplemented(format!("Cannot explode nested map value that would lead to dangling entity for attribute {}", a)));
                            }

                            // Similar, but not identical, to the expansion of the entity position e
                            // below.  This returns Either::Left(TypedValue) instances; that returns
                            // Either::Left(i64) instances.
                            match db_id {
                                entmod::EntidOrLookupRefOrTempId::Entid(e) => {
                                    let e = match e {
                                        entmod::Entid::Entid(ref e) => self.ensure_entid_exists(*e)?,
                                        entmod::Entid::Ident(ref e) => self.ensure_ident_exists(&e)?,
                                    };
                                    Either::Left(TypedValue::Ref(e.0))
                                },

                                entmod::EntidOrLookupRefOrTempId::TempId(e) => {
                                    Either::Right(LookupRefOrTempId::TempId(temp_ids.intern(e)))
                                },

                                entmod::EntidOrLookupRefOrTempId::LookupRef(lookup_ref) => {
                                    Either::Right(LookupRefOrTempId::LookupRef(intern_lookup_ref(&mut lookup_refs, lookup_ref)?))
                                },
                            }
                        },
                    };

                    let e = match e {
                        entmod::EntidOrLookupRefOrTempId::Entid(e) => {
                            let e = match e {
                                entmod::Entid::Entid(ref e) => self.ensure_entid_exists(*e)?,
                                entmod::Entid::Ident(ref e) => self.ensure_ident_exists(&e)?,
                            };
                            Either::Left(e)
                        },

                        entmod::EntidOrLookupRefOrTempId::TempId(e) => {
                            Either::Right(LookupRefOrTempId::TempId(temp_ids.intern(e)))
                        },

                        entmod::EntidOrLookupRefOrTempId::LookupRef(lookup_ref) => {
                            Either::Right(LookupRefOrTempId::LookupRef(intern_lookup_ref(&mut lookup_refs, lookup_ref)?))
                        },
                    };

                    terms.push(Term::AddOrRetract(op, e, a, v));
                },
            }
        };
        Ok((terms, temp_ids, lookup_refs))
    }

    /// Pipeline stage 2: rewrite `Term` instances with lookup refs into `Term` instances without
    /// lookup refs.
    ///
    /// The `Term` instances produce share interned TempId handles and have no LookupRef references.
    fn resolve_lookup_refs<I>(&self, lookup_ref_map: &AVMap, terms: I) -> Result<Vec<TermWithTempIds>> where I: IntoIterator<Item=TermWithTempIdsAndLookupRefs> {
        terms.into_iter().map(|term: TermWithTempIdsAndLookupRefs| -> Result<TermWithTempIds> {
            match term {
                Term::AddOrRetract(op, e, a, v) => {
                    let e = replace_lookup_ref(&lookup_ref_map, e, |x| KnownEntid(x))?;
                    let v = replace_lookup_ref(&lookup_ref_map, v, |x| TypedValue::Ref(x))?;
                    Ok(Term::AddOrRetract(op, e, a, v))
                },
            }
        }).collect::<Result<Vec<_>>>()
    }

    /// Transact the given `entities` against the store.
    ///
    /// This approach is explained in https://github.com/mozilla/mentat/wiki/Transacting.
    // TODO: move this to the transactor layer.
    pub fn transact_entities<I>(&mut self, entities: I) -> Result<TxReport> where I: IntoIterator<Item=Entity> {
        // TODO: push these into an internal transaction report?
        let mut tempids: BTreeMap<TempId, KnownEntid> = BTreeMap::default();

        // Pipeline stage 1: entities -> terms with tempids and lookup refs.
        let (terms_with_temp_ids_and_lookup_refs, tempid_set, lookup_ref_set) = self.entities_into_terms_with_temp_ids_and_lookup_refs(entities)?;

        // Pipeline stage 2: resolve lookup refs -> terms with tempids.
        let lookup_ref_avs: Vec<&(i64, TypedValue)> = lookup_ref_set.inner.iter().map(|rc| &**rc).collect();
        let lookup_ref_map: AVMap = self.store.resolve_avs(&lookup_ref_avs[..])?;

        let terms_with_temp_ids = self.resolve_lookup_refs(&lookup_ref_map, terms_with_temp_ids_and_lookup_refs)?;

        // Pipeline stage 3: upsert tempids -> terms without tempids or lookup refs.
        // Now we can collect upsert populations.
        let (mut generation, inert_terms) = Generation::from(terms_with_temp_ids, &self.schema)?;

        // And evolve them forward.
        while generation.can_evolve() {
            // Evolve further.
            let temp_id_map: TempIdMap = self.resolve_temp_id_avs(&generation.temp_id_avs()[..])?;
            generation = generation.evolve_one_step(&temp_id_map);

            // Report each tempid that resolves via upsert.
            for (tempid, entid) in temp_id_map {
                // Every tempid should be resolved at most once.  Prima facie, we might expect a
                // tempid to be resolved in two different generations.  However, that is not so: the
                // keys of temp_id_map are unique between generations.Suppose that id->e and id->e*
                // are two such mappings, resolved on subsequent evolutionary steps, and that `id`
                // is a key in the intersection of the two key sets. This can't happen: if `id` maps
                // to `e` via id->e, all instances of `id` have been evolved forward (replaced with
                // `e`) before we try to resolve the next set of `UpsertsE`.  That is, we'll never
                // successfully upsert the same tempid in more than one generation step.  (We might
                // upsert the same tempid to multiple entids via distinct `[a v]` pairs in a single
                // generation step; in this case, the transaction will fail.)
                let previous = tempids.insert((*tempid).clone(), entid);
                assert!(previous.is_none());
            }
        }

        // Allocate entids for tempids that didn't upsert.  BTreeSet rather than HashSet so this is deterministic.
        let unresolved_temp_ids: BTreeSet<TempIdHandle> = generation.temp_ids_in_allocations();

        // TODO: track partitions for temporary IDs.
        let entids = self.partition_map.allocate_entids(":db.part/user", unresolved_temp_ids.len());

        let temp_id_allocations: TempIdMap = unresolved_temp_ids.into_iter()
                                                                .zip(entids.map(|e| KnownEntid(e)))
                                                                .collect();

        let final_populations = generation.into_final_populations(&temp_id_allocations)?;

        // Report each tempid that is allocated.
        for (tempid, &entid) in &temp_id_allocations {
            // Every tempid should be allocated at most once.
            assert!(!tempids.contains_key(&**tempid));
            tempids.insert((**tempid).clone(), entid);
        }

        // Verify that every tempid we interned either resolved or has been allocated.
        assert_eq!(tempids.len(), tempid_set.inner.len());
        for tempid in &tempid_set.inner {
            assert!(tempids.contains_key(&**tempid));
        }

        // Any internal tempid has been allocated by the system and is a private implementation
        // detail; it shouldn't be exposed in the final transaction report.
        let tempids = tempids.into_iter().filter_map(|(tempid, e)| tempid.into_external().map(|s| (s, e.0))).collect();

        // A transaction might try to add or retract :db/ident assertions or other metadata mutating
        // assertions , but those assertions might not make it to the store.  If we see a possible
        // metadata mutation, we will figure out if any assertions made it through later.  This is
        // strictly an optimization: it would be correct to _always_ check what made it to the
        // store.
        let mut tx_might_update_metadata = false;

        let final_terms: Vec<TermWithoutTempIds> = [final_populations.resolved,
                                                    final_populations.allocated,
                                                    inert_terms.into_iter().map(|term| term.unwrap()).collect()].concat();

        { // TODO: Don't use this block to scope borrowing the schema; instead, extract a helper function.

        /// Assertions that are :db.cardinality/one and not :db.fulltext.
        let mut non_fts_one: Vec<db::ReducedEntity> = vec![];

        /// Assertions that are :db.cardinality/many and not :db.fulltext.
        let mut non_fts_many: Vec<db::ReducedEntity> = vec![];

        /// Assertions that are :db.cardinality/one and :db.fulltext.
        let mut fts_one: Vec<db::ReducedEntity> = vec![];

        /// Assertions that are :db.cardinality/many and :db.fulltext.
        let mut fts_many: Vec<db::ReducedEntity> = vec![];

        // We need to ensure that callers can't blindly transact entities that haven't been
        // allocated by this store.

        // Pipeline stage 4: final terms (after rewriting) -> DB insertions.
        // Collect into non_fts_*.
        // TODO: use something like Clojure's group_by to do this.
        for term in final_terms {
            match term {
                Term::AddOrRetract(op, e, a, v) => {
                    let attribute: &Attribute = self.schema.require_attribute_for_entid(a)?;
                    if entids::might_update_metadata(a) {
                        tx_might_update_metadata = true;
                    }

                    let added = op == OpType::Add;
                    let reduced = (e.0, a, attribute, v, added);
                    match (attribute.fulltext, attribute.multival) {
                        (false, true) => non_fts_many.push(reduced),
                        (false, false) => non_fts_one.push(reduced),
                        (true, false) => fts_one.push(reduced),
                        (true, true) => fts_many.push(reduced),
                    }
                },
            }
        }

        // Transact [:db/add :db/txInstant NOW :db/tx].
        // TODO: allow this to be present in the transaction data.
        non_fts_one.push((self.tx_id,
                          entids::DB_TX_INSTANT,
                          self.schema.require_attribute_for_entid(entids::DB_TX_INSTANT).unwrap(),
                          TypedValue::Instant(self.tx_instant),
                          true));

        if !non_fts_one.is_empty() {
            self.store.insert_non_fts_searches(&non_fts_one[..], db::SearchType::Inexact)?;
        }

        if !non_fts_many.is_empty() {
            self.store.insert_non_fts_searches(&non_fts_many[..], db::SearchType::Exact)?;
        }

        if !fts_one.is_empty() {
            self.store.insert_fts_searches(&fts_one[..], db::SearchType::Inexact)?;
        }

        if !fts_many.is_empty() {
            self.store.insert_fts_searches(&fts_many[..], db::SearchType::Exact)?;
        }

        self.store.commit_transaction(self.tx_id)?;
        }

        db::update_partition_map(self.store, &self.partition_map)?;

        if tx_might_update_metadata {
            // Extract changes to metadata from the store.
            let metadata_assertions = self.store.committed_metadata_assertions(self.tx_id)?;

            let mut new_schema = (*self.schema_for_mutation).clone(); // Clone the underlying Schema for modification.
            let metadata_report = metadata::update_schema_from_entid_quadruples(&mut new_schema, metadata_assertions)?;

            // We might not have made any changes to the schema, even though it looked like we
            // would.  This should not happen, even during bootstrapping: we mutate an empty
            // `Schema` in this case specifically to run the bootstrapped assertions through the
            // regular transactor code paths, updating the schema and materialized views uniformly.
            // But, belt-and-braces: handle it gracefully.
            if new_schema != *self.schema_for_mutation {
                let old_schema = (*self.schema_for_mutation).clone(); // Clone the original Schema for comparison.
                *self.schema_for_mutation.to_mut() = new_schema; // Store the new Schema.
                db::update_metadata(self.store, &old_schema, &*self.schema_for_mutation, &metadata_report)?;
            }
        }

        Ok(TxReport {
            tx_id: self.tx_id,
            tx_instant: self.tx_instant,
            tempids: tempids,
        })
    }
}

/// Transact the given `entities` against the given SQLite `conn`, using the given metadata.
///
/// This approach is explained in https://github.com/mozilla/mentat/wiki/Transacting.
// TODO: move this to the transactor layer.
pub fn transact<'conn, 'a, I>(
    conn: &'conn rusqlite::Connection,
    mut partition_map: PartitionMap,
    schema_for_mutation: &'a Schema,
    schema: &'a Schema,
    entities: I) -> Result<(TxReport, PartitionMap, Option<Schema>)> where I: IntoIterator<Item=Entity> {
    // Eventually, this function will be responsible for managing a SQLite transaction.  For
    // now, it's just about the tx details.

    let tx_instant = ::now(); // Label the transaction with the timestamp when we first see it: leading edge.
    let tx_id = partition_map.allocate_entid(":db.part/tx");

    conn.begin_transaction()?;

    let mut tx = Tx::new(conn, partition_map, schema_for_mutation, schema, tx_id, tx_instant);

    let report = tx.transact_entities(entities)?;

    // If the schema has moved on, return it.
    let next_schema = match tx.schema_for_mutation {
        Cow::Borrowed(_) => None,
        Cow::Owned(next_schema) => Some(next_schema),
    };
    Ok((report, tx.partition_map, next_schema))
}
