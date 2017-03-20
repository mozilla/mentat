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

use std;
use std::borrow::Cow;
use std::collections::{
    BTreeMap,
    BTreeSet,
};

use ::{to_namespaced_keyword};
use db;
use db::{
    MentatStoring,
    PartitionMapping,
};
use entids;
use errors::{ErrorKind, Result};
use internal_types::{
    LookupRefOrTempId,
    TempId,
    TempIdMap,
    Term,
    TermWithTempIdsAndLookupRefs,
    TermWithTempIds,
    TermWithoutTempIds,
    replace_lookup_ref};
use mentat_core::{
    intern_set,
    Schema,
};
use mentat_tx::entities as entmod;
use mentat_tx::entities::{Entity, OpType};
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

    /// The schema to use when interpreting the transaction entities.
    ///
    /// The schema is infrequently updated, so we borrow a schema until we need to modify it.
    schema: Cow<'a, Schema>,

    /// The transaction ID of the transaction.
    tx_id: Entid,

    /// The timestamp when the transaction began to be committed.
    ///
    /// This is milliseconds after the Unix epoch according to the transactor's local clock.
    // TODO: :db.type/instant.
    tx_instant: i64,
}

impl<'conn, 'a> Tx<'conn, 'a> {
    pub fn new(store: &'conn rusqlite::Connection, partition_map: PartitionMap, schema: &'a Schema, tx_id: Entid, tx_instant: i64) -> Tx<'conn, 'a> {
        Tx {
            store: store,
            partition_map: partition_map,
            schema: Cow::Borrowed(schema),
            tx_id: tx_id,
            tx_instant: tx_instant,
        }
    }

    /// Given a collection of tempids and the [a v] pairs that they might upsert to, resolve exactly
    /// which [a v] pairs do upsert to entids, and map each tempid that upserts to the upserted
    /// entid.  The keys of the resulting map are exactly those tempids that upserted.
    pub fn resolve_temp_id_avs<'b>(&self, temp_id_avs: &'b [(TempId, AVPair)]) -> Result<TempIdMap> {
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
                if let Some(previous_n) = temp_id_map.get(&*temp_id) {
                    if n != previous_n {
                        // Conflicting upsert!  TODO: collect conflicts and give more details on what failed this transaction.
                        bail!(ErrorKind::NotYetImplemented(format!("Conflicting upsert: tempid '{}' resolves to more than one entid: {:?}, {:?}", temp_id, previous_n, n))) // XXX
                    }
                }
                temp_id_map.insert(temp_id.clone(), *n);
            }
        }

        Ok((temp_id_map))
    }

    /// Pipeline stage 1: convert `Entity` instances into `Term` instances, ready for term
    /// rewriting.
    ///
    /// The `Term` instances produce share interned TempId and LookupRef handles, and we return the
    /// interned handle sets so that consumers can ensure all handles are used appropriately.
    fn entities_into_terms_with_temp_ids_and_lookup_refs<I>(&self, entities: I) -> Result<(Vec<TermWithTempIdsAndLookupRefs>, intern_set::InternSet<String>, intern_set::InternSet<AVPair>)> where I: IntoIterator<Item=Entity> {
        let mut temp_ids = intern_set::InternSet::new();
        // #183: We don't yet support lookup refs, so this isn't mutable.  Later, it'll be mutable.
        let lookup_refs = intern_set::InternSet::new();

        entities.into_iter()
            .map(|entity: Entity| -> Result<TermWithTempIdsAndLookupRefs> {
                match entity {
                    Entity::AddOrRetract { op, e, a, v } => {
                        let a: i64 = match a {
                            entmod::Entid::Entid(ref a) => *a,
                            entmod::Entid::Ident(ref a) => self.schema.require_entid(&a)?,
                        };

                        let attribute: &Attribute = self.schema.require_attribute_for_entid(a)?;

                        let e = match e {
                            entmod::EntidOrLookupRefOrTempId::Entid(e) => {
                                let e: i64 = match e {
                                    entmod::Entid::Entid(ref e) => *e,
                                    entmod::Entid::Ident(ref e) => self.schema.require_entid(&e)?,
                                };
                                std::result::Result::Ok(e)
                            },

                            entmod::EntidOrLookupRefOrTempId::TempId(e) => {
                                std::result::Result::Err(LookupRefOrTempId::TempId(temp_ids.intern(e)))
                            },

                            entmod::EntidOrLookupRefOrTempId::LookupRef(_) => {
                                // TODO: reference entity and initial input.
                                bail!(ErrorKind::NotYetImplemented(format!("Transacting lookup-refs is not yet implemented")))
                            },
                        };

                        let v = {
                            if attribute.value_type == ValueType::Ref && v.is_text() {
                                std::result::Result::Err(LookupRefOrTempId::TempId(temp_ids.intern(v.as_text().unwrap().clone())))
                            } else if attribute.value_type == ValueType::Ref && v.is_vector() && v.as_vector().unwrap().len() == 2 {
                                bail!(ErrorKind::NotYetImplemented(format!("Transacting lookup-refs is not yet implemented")))
                            } else {
                                // Here is where we do schema-aware typechecking: we either assert that
                                // the given value is in the attribute's value set, or (in limited
                                // cases) coerce the value into the attribute's value set.
                                let typed_value: TypedValue = self.schema.to_typed_value(&v, &attribute)?;

                                std::result::Result::Ok(typed_value)
                            }
                        };

                        Ok(Term::AddOrRetract(op, e, a, v))
                    },
                }
            })
            .collect::<Result<Vec<_>>>()
            .map(|terms| (terms, temp_ids, lookup_refs))
    }

    /// Pipeline stage 2: rewrite `Term` instances with lookup refs into `Term` instances without
    /// lookup refs.
    ///
    /// The `Term` instances produce share interned TempId handles and have no LookupRef references.
    fn resolve_lookup_refs<I>(&self, lookup_ref_map: &AVMap, terms: I) -> Result<Vec<TermWithTempIds>> where I: IntoIterator<Item=TermWithTempIdsAndLookupRefs> {
        terms.into_iter().map(|term: TermWithTempIdsAndLookupRefs| -> Result<TermWithTempIds> {
            match term {
                Term::AddOrRetract(op, e, a, v) => {
                    let e = replace_lookup_ref(&lookup_ref_map, e, |x| x)?;
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
        let mut tempids: BTreeMap<String, Entid> = BTreeMap::default();

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
        let unresolved_temp_ids: BTreeSet<TempId> = generation.temp_ids_in_allocations();

        // TODO: track partitions for temporary IDs.
        let entids = self.partition_map.allocate_entids(":db.part/user", unresolved_temp_ids.len());

        let temp_id_allocations: TempIdMap = unresolved_temp_ids.into_iter().zip(entids).collect();

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

        /// Assertions that are :db.cardinality/one and not :db.fulltext.
        let mut non_fts_one: Vec<db::ReducedEntity> = vec![];

        /// Assertions that are :db.cardinality/many and not :db.fulltext.
        let mut non_fts_many: Vec<db::ReducedEntity> = vec![];

        let final_terms: Vec<TermWithoutTempIds> = [final_populations.resolved,
                                                    final_populations.allocated,
                                                    inert_terms.into_iter().map(|term| term.unwrap()).collect()].concat();

        // Pipeline stage 4: final terms (after rewriting) -> DB insertions.
        // Collect into non_fts_*.
        // TODO: use something like Clojure's group_by to do this.
        for term in final_terms {
            match term {
                Term::AddOrRetract(op, e, a, v) => {
                    let attribute: &Attribute = self.schema.require_attribute_for_entid(a)?;
                    if attribute.fulltext {
                        bail!(ErrorKind::NotYetImplemented(format!("Transacting :db/fulltext entities is not yet implemented"))) // TODO: reference original input.  Difficult!
                    }

                    let added = op == OpType::Add;
                    if attribute.multival {
                        non_fts_many.push((e, a, attribute, v, added));
                    } else {
                        non_fts_one.push((e, a, attribute, v, added));
                    }
                },
            }
        }

        // Transact [:db/add :db/txInstant NOW :db/tx].
        // TODO: allow this to be present in the transaction data.
        non_fts_one.push((self.tx_id,
                          entids::DB_TX_INSTANT,
                          // TODO: extract this to a constant.
                          self.schema.require_attribute_for_entid(self.schema.require_entid(&to_namespaced_keyword(":db/txInstant").unwrap())?)?,
                          TypedValue::Long(self.tx_instant),
                          true));

        if !non_fts_one.is_empty() {
            self.store.insert_non_fts_searches(&non_fts_one[..], db::SearchType::Inexact)?;
        }

        if !non_fts_many.is_empty() {
            self.store.insert_non_fts_searches(&non_fts_many[..], db::SearchType::Exact)?;
        }

        self.store.commit_transaction(self.tx_id)?;

        // TODO: update idents and schema materialized views.
        db::update_partition_map(self.store, &self.partition_map)?;

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
pub fn transact<'conn, 'a, I>(conn: &'conn rusqlite::Connection, mut partition_map: PartitionMap, schema: &'a Schema, entities: I) -> Result<(TxReport, PartitionMap, Option<Schema>)> where I: IntoIterator<Item=Entity> {
    // Eventually, this function will be responsible for managing a SQLite transaction.  For
    // now, it's just about the tx details.

    let tx_instant = ::now(); // Label the transaction with the timestamp when we first see it: leading edge.
    let tx_id = partition_map.allocate_entid(":db.part/tx");

    conn.begin_transaction()?;

    let mut tx = Tx::new(conn, partition_map, schema, tx_id, tx_instant);

    let report = tx.transact_entities(entities)?;

    // If the schema has moved on, return it.
    let next_schema = match tx.schema {
        Cow::Borrowed(_) => None,
        Cow::Owned(next_schema) => Some(next_schema),
    };
    Ok((report, tx.partition_map, next_schema))
}
