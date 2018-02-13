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
use std::rc::{
    Rc,
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
    KnownEntidOr,
    LookupRef,
    LookupRefOrTempId,
    TempIdHandle,
    TempIdMap,
    Term,
    TermWithTempIds,
    TermWithTempIdsAndLookupRefs,
    TermWithoutTempIds,
    TypedValueOr,
    replace_lookup_ref,
};

use mentat_core::util::Either;

use mentat_core::{
    DateTime,
    KnownEntid,
    Schema,
    Utc,
    attribute,
    now,
};

use mentat_core::intern_set::InternSet;

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
    AttributeSet,
    AVPair,
    AVMap,
    Entid,
    PartitionMap,
    TypedValue,
    TxReport,
    ValueType,
};
use upsert_resolution::Generation;

use std::os::raw::c_char;
use std::os::raw::c_int;
use std::ffi::CString;

pub const ANDROID_LOG_DEBUG: i32 = 3;
#[cfg(all(target_os="android", not(test)))]
extern { pub fn __android_log_write(prio: c_int, tag: *const c_char, text: *const c_char) -> c_int; }

#[cfg(all(target_os="android", not(test)))]
pub fn d(message: &str) {
    let tag = "mentat_db::tx";
    let message = CString::new(message).unwrap();
    let message = message.as_ptr();
    let tag = CString::new(tag).unwrap();
    let tag = tag.as_ptr();
    unsafe { __android_log_write(ANDROID_LOG_DEBUG, tag, message) };
}

#[cfg(all(not(target_os="android")))]
pub fn d(message: &str) {
    let tag = "mentat_db::tx";
    println!("d: {}: {}", tag, message);
}

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
    tx_instant: Option<DateTime<Utc>>,
}

impl<'conn, 'a> Tx<'conn, 'a> {
    pub fn new(
        store: &'conn rusqlite::Connection,
        partition_map: PartitionMap,
        schema_for_mutation: &'a Schema,
        schema: &'a Schema,
        tx_id: Entid) -> Tx<'conn, 'a> {
        Tx {
            store: store,
            partition_map: partition_map,
            schema_for_mutation: Cow::Borrowed(schema_for_mutation),
            schema: schema,
            tx_id: tx_id,
            tx_instant: None,
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

        Ok(temp_id_map)
    }

    /// Pipeline stage 1: convert `Entity` instances into `Term` instances, ready for term
    /// rewriting.
    ///
    /// The `Term` instances produce share interned TempId and LookupRef handles, and we return the
    /// interned handle sets so that consumers can ensure all handles are used appropriately.
    fn entities_into_terms_with_temp_ids_and_lookup_refs<I>(&self, entities: I) -> Result<(Vec<TermWithTempIdsAndLookupRefs>, InternSet<TempId>, InternSet<AVPair>)> where I: IntoIterator<Item=Entity> {
        struct InProcess<'a> {
            partition_map: &'a PartitionMap,
            schema: &'a Schema,
            mentat_id_count: i64,
            tx_id: KnownEntid,
            temp_ids: InternSet<TempId>,
            lookup_refs: InternSet<AVPair>,
        }

        impl<'a> InProcess<'a> {
            fn with_schema_and_partition_map(schema: &'a Schema, partition_map: &'a PartitionMap, tx_id: KnownEntid) -> InProcess<'a> {
                InProcess {
                    partition_map,
                    schema,
                    mentat_id_count: 0,
                    tx_id,
                    temp_ids: InternSet::new(),
                    lookup_refs: InternSet::new(),
                }
            }

            fn ensure_entid_exists(&self, e: Entid) -> Result<KnownEntid> {
                if self.partition_map.contains_entid(e) {
                    Ok(KnownEntid(e))
                } else {
                    bail!(ErrorKind::UnrecognizedEntid(e))
                }
            }

            fn ensure_ident_exists(&self, e: &NamespacedKeyword) -> Result<KnownEntid> {
                self.schema.require_entid(e)
            }

            fn intern_lookup_ref(&mut self, lookup_ref: &entmod::LookupRef) -> Result<LookupRef> {
                let lr_a: i64 = match lookup_ref.a {
                    entmod::Entid::Entid(ref a) => *a,
                    entmod::Entid::Ident(ref a) => self.schema.require_entid(&a)?.into(),
                };
                let lr_attribute: &Attribute = self.schema.require_attribute_for_entid(lr_a)?;

                if lr_attribute.unique.is_none() {
                    bail!(ErrorKind::NotYetImplemented(format!("Cannot resolve (lookup-ref {} {}) with attribute that is not :db/unique", lr_a, lookup_ref.v)))
                }

                let lr_typed_value: TypedValue = self.schema.to_typed_value(&lookup_ref.v, lr_attribute.value_type)?;
                Ok(self.lookup_refs.intern((lr_a, lr_typed_value)))
            }

            fn intern_temp_id(&mut self, temp_id: TempId) -> Rc<TempId> {
                self.temp_ids.intern(temp_id)
            }

            /// Allocate private internal tempids reserved for Mentat.  Internal tempids just need to be
            /// unique within one transaction; they should never escape a transaction.
            fn allocate_mentat_id(&mut self) -> entmod::EntidOrLookupRefOrTempId {
                self.mentat_id_count += 1;
                entmod::EntidOrLookupRefOrTempId::TempId(TempId::Internal(self.mentat_id_count))
            }

            fn entity_e_into_term_e(&mut self, x: entmod::EntidOrLookupRefOrTempId) -> Result<KnownEntidOr<LookupRefOrTempId>> {
                match x {
                    entmod::EntidOrLookupRefOrTempId::Entid(e) => {
                        let e = match e {
                            entmod::Entid::Entid(ref e) => self.ensure_entid_exists(*e)?,
                            entmod::Entid::Ident(ref e) => self.ensure_ident_exists(&e)?,
                        };
                        Ok(Either::Left(e))
                    },

                    // Special case: current tx ID.
                    entmod::EntidOrLookupRefOrTempId::TempId(TempId::Tx) => {
                        Ok(Either::Left(self.tx_id))
                    },

                    entmod::EntidOrLookupRefOrTempId::TempId(e) => {
                        Ok(Either::Right(LookupRefOrTempId::TempId(self.intern_temp_id(e))))
                    },

                    entmod::EntidOrLookupRefOrTempId::LookupRef(ref lookup_ref) => {
                        Ok(Either::Right(LookupRefOrTempId::LookupRef(self.intern_lookup_ref(lookup_ref)?)))
                    },
                }
            }

            fn entity_a_into_term_a(&mut self, x: entmod::Entid) -> Result<Entid> {
                let a = match x {
                    entmod::Entid::Entid(ref a) => *a,
                    entmod::Entid::Ident(ref a) => self.schema.require_entid(&a)?.into(),
                };
                Ok(a)
            }

            fn entity_e_into_term_v(&mut self, x: entmod::EntidOrLookupRefOrTempId) -> Result<TypedValueOr<LookupRefOrTempId>> {
                self.entity_e_into_term_e(x).map(|r| r.map_left(|ke| TypedValue::Ref(ke.0)))
            }

            fn entity_v_into_term_e(&mut self, x: entmod::AtomOrLookupRefOrVectorOrMapNotation, backward_a: &entmod::Entid) -> Result<KnownEntidOr<LookupRefOrTempId>> {
                match backward_a.unreversed() {
                    None => {
                        bail!(ErrorKind::NotYetImplemented(format!("Cannot explode map notation value in :attr/_reversed notation for forward attribute")));
                    },
                    Some(forward_a) => {
                        let forward_a = self.entity_a_into_term_a(forward_a)?;
                        let forward_attribute = self.schema.require_attribute_for_entid(forward_a)?;
                        if forward_attribute.value_type != ValueType::Ref {
                            bail!(ErrorKind::NotYetImplemented(format!("Cannot use :attr/_reversed notation for attribute {} that is not :db/valueType :db.type/ref", forward_a)))
                        }

                        match x {
                            entmod::AtomOrLookupRefOrVectorOrMapNotation::Atom(ref v) => {
                                // Here is where we do schema-aware typechecking: we either assert
                                // that the given value is in the attribute's value set, or (in
                                // limited cases) coerce the value into the attribute's value set.
                                if let Some(text) = v.inner.as_text() {
                                    Ok(Either::Right(LookupRefOrTempId::TempId(self.intern_temp_id(TempId::External(text.clone())))))
                                } else {
                                    if let TypedValue::Ref(entid) = self.schema.to_typed_value(&v.clone().without_spans(), ValueType::Ref)? {
                                        Ok(Either::Left(KnownEntid(entid)))
                                    } else {
                                        // The given value is expected to be :db.type/ref, so this shouldn't happen.
                                        bail!(ErrorKind::NotYetImplemented(format!("Cannot use :attr/_reversed notation for attribute {} with value that is not :db.valueType :db.type/ref", forward_a)))
                                    }
                                }
                            },

                            entmod::AtomOrLookupRefOrVectorOrMapNotation::LookupRef(ref lookup_ref) =>
                                Ok(Either::Right(LookupRefOrTempId::LookupRef(self.intern_lookup_ref(lookup_ref)?))),

                            entmod::AtomOrLookupRefOrVectorOrMapNotation::Vector(_) =>
                                bail!(ErrorKind::NotYetImplemented(format!("Cannot explode vector value in :attr/_reversed notation for attribute {}", forward_a))),

                            entmod::AtomOrLookupRefOrVectorOrMapNotation::MapNotation(_) =>
                                bail!(ErrorKind::NotYetImplemented(format!("Cannot explode map notation value in :attr/_reversed notation for attribute {}", forward_a))),
                        }
                    },
                }
            }
        }

        let mut in_process = InProcess::with_schema_and_partition_map(&self.schema, &self.partition_map, KnownEntid(self.tx_id));

        // We want to handle entities in the order they're given to us, while also "exploding" some
        // entities into many.  We therefore push the initial entities onto the back of the deque,
        // take from the front of the deque, and explode onto the front as well.
        let mut deque: VecDeque<Entity> = VecDeque::default();
        deque.extend(entities);

        let mut terms: Vec<TermWithTempIdsAndLookupRefs> = Vec::with_capacity(deque.len());

        while let Some(entity) = deque.pop_front() {
            match entity {
                Entity::MapNotation(mut map_notation) => {
                    // :db/id is optional; if it's not given, we generate a special internal tempid
                    // to use for upserting.  This tempid will not be reported in the TxReport.
                    let db_id: entmod::EntidOrLookupRefOrTempId = mentat_tx_parser::remove_db_id(&mut map_notation)?.unwrap_or_else(|| in_process.allocate_mentat_id());

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
                    if let Some(reversed_a) = a.unreversed() {
                        let reversed_e = in_process.entity_v_into_term_e(v, &a)?;
                        let reversed_a = in_process.entity_a_into_term_a(reversed_a)?;
                        let reversed_v = in_process.entity_e_into_term_v(e)?;
                        terms.push(Term::AddOrRetract(OpType::Add, reversed_e, reversed_a, reversed_v));
                    } else {
                        let a = in_process.entity_a_into_term_a(a)?;
                        let attribute = self.schema.require_attribute_for_entid(a)?;

                        let v = match v {
                            entmod::AtomOrLookupRefOrVectorOrMapNotation::Atom(v) => {
                                if attribute.value_type == ValueType::Ref && v.inner.is_text() {
                                    Either::Right(LookupRefOrTempId::TempId(in_process.intern_temp_id(v.inner.as_text().cloned().map(TempId::External).unwrap())))
                                } else {
                                    // Here is where we do schema-aware typechecking: we either assert that
                                    // the given value is in the attribute's value set, or (in limited
                                    // cases) coerce the value into the attribute's value set.
                                    let typed_value: TypedValue = self.schema.to_typed_value(&v.without_spans(), attribute.value_type)?;
                                    Either::Left(typed_value)
                                }
                            },

                            entmod::AtomOrLookupRefOrVectorOrMapNotation::LookupRef(ref lookup_ref) => {
                                if attribute.value_type != ValueType::Ref {
                                    bail!(ErrorKind::NotYetImplemented(format!("Cannot resolve value lookup ref for attribute {} that is not :db/valueType :db.type/ref", a)))
                                }

                                Either::Right(LookupRefOrTempId::LookupRef(in_process.intern_lookup_ref(lookup_ref)?))
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
                                let db_id: entmod::EntidOrLookupRefOrTempId = db_id.unwrap_or_else(|| in_process.allocate_mentat_id());

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
                                    if let Some(reversed_a) = inner_a.unreversed() {
                                        // We definitely have a reference.  The reference might be
                                        // dangling (a bare entid, for example), but we don't yet
                                        // support nested maps and reverse notation simultaneously
                                        // (i.e., we don't accept {:reverse/_attribute {:nested map}})
                                        // so we don't need to check that the nested map reference isn't
                                        // dangling.
                                        dangling = false;

                                        let reversed_e = in_process.entity_v_into_term_e(inner_v, &inner_a)?;
                                        let reversed_a = in_process.entity_a_into_term_a(reversed_a)?;
                                        let reversed_v = in_process.entity_e_into_term_v(db_id.clone())?;
                                        terms.push(Term::AddOrRetract(OpType::Add, reversed_e, reversed_a, reversed_v));
                                    } else {
                                        let inner_a = in_process.entity_a_into_term_a(inner_a)?;
                                        let inner_attribute = self.schema.require_attribute_for_entid(inner_a)?;
                                        if inner_attribute.unique == Some(attribute::Unique::Identity) {
                                            dangling = false;
                                        }

                                        deque.push_front(Entity::AddOrRetract {
                                            op: OpType::Add,
                                            e: db_id.clone(),
                                            a: entmod::Entid::Entid(inner_a),
                                            v: inner_v,
                                        });
                                    }
                                }

                                if dangling {
                                    bail!(ErrorKind::NotYetImplemented(format!("Cannot explode nested map value that would lead to dangling entity for attribute {}", a)));
                                }

                                in_process.entity_e_into_term_v(db_id)?
                            },
                        };

                        let e = in_process.entity_e_into_term_e(e)?;
                        terms.push(Term::AddOrRetract(op, e, a, v));
                    }
                },
            }
        };
        Ok((terms, in_process.temp_ids, in_process.lookup_refs))
    }

    /// Pipeline stage 2: rewrite `Term` instances with lookup refs into `Term` instances without
    /// lookup refs.
    ///
    /// The `Term` instances produced share interned TempId handles and have no LookupRef references.
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
        // Pipeline stage 1: entities -> terms with tempids and lookup refs.
        let (terms_with_temp_ids_and_lookup_refs, tempid_set, lookup_ref_set) = self.entities_into_terms_with_temp_ids_and_lookup_refs(entities)?;

        // Pipeline stage 2: resolve lookup refs -> terms with tempids.
        let lookup_ref_avs: Vec<&(i64, TypedValue)> = lookup_ref_set.inner.iter().map(|rc| &**rc).collect();
        let lookup_ref_map: AVMap = self.store.resolve_avs(&lookup_ref_avs[..])?;

        let terms_with_temp_ids = self.resolve_lookup_refs(&lookup_ref_map, terms_with_temp_ids_and_lookup_refs)?;

        self.transact_simple_terms(terms_with_temp_ids, tempid_set)
    }

    pub fn transact_simple_terms<I>(&mut self, terms: I, tempid_set: InternSet<TempId>) -> Result<TxReport> where I: IntoIterator<Item=TermWithTempIds> {
        // TODO: push these into an internal transaction report?
        let mut tempids: BTreeMap<TempId, KnownEntid> = BTreeMap::default();

        // Pipeline stage 3: upsert tempids -> terms without tempids or lookup refs.
        // Now we can collect upsert populations.
        let (mut generation, inert_terms) = Generation::from(terms, &self.schema)?;

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

        let affected_attrs: AttributeSet = final_terms.iter().filter_map(|t| {
            match t {
                &Term::AddOrRetract(_, _, attrid, _) => Some(attrid),
            }
        }). collect();
        let tx_instant;

        { // TODO: Don't use this block to scope borrowing the schema; instead, extract a helper function.

        // Assertions that are :db.cardinality/one and not :db.fulltext.
        let mut non_fts_one: Vec<db::ReducedEntity> = vec![];

        // Assertions that are :db.cardinality/many and not :db.fulltext.
        let mut non_fts_many: Vec<db::ReducedEntity> = vec![];

        // Assertions that are :db.cardinality/one and :db.fulltext.
        let mut fts_one: Vec<db::ReducedEntity> = vec![];

        // Assertions that are :db.cardinality/many and :db.fulltext.
        let mut fts_many: Vec<db::ReducedEntity> = vec![];

        // We need to ensure that callers can't blindly transact entities that haven't been
        // allocated by this store.

        // Pipeline stage 4: final terms (after rewriting) -> DB insertions.
        // Collect into non_fts_*.
        // TODO: use something like Clojure's group_by to do this.
        for term in final_terms {
            match term {
                Term::AddOrRetract(op, KnownEntid(e), a, v) => {
                    let attribute: &Attribute = self.schema.require_attribute_for_entid(a)?;
                    if entids::might_update_metadata(a) {
                        tx_might_update_metadata = true;
                    }

                    let added = op == OpType::Add;

                    // We take the last encountered :db/txInstant value.
                    // If more than one is provided, the transactor will fail.
                    if added &&
                       e == self.tx_id &&
                       a == entids::DB_TX_INSTANT {
                        if let TypedValue::Instant(instant) = v {
                            if let Some(ts) = self.tx_instant {
                                if ts == instant {
                                    // Dupes are fine.
                                } else {
                                    bail!(ErrorKind::ConflictingDatoms);
                                }
                            } else {
                                self.tx_instant = Some(instant);
                            }
                            continue;
                        } else {
                            // The type error has been caught earlier.
                            unreachable!()
                        }
                    }

                    let reduced = (e, a, attribute, v, added);
                    match (attribute.fulltext, attribute.multival) {
                        (false, true) => non_fts_many.push(reduced),
                        (false, false) => non_fts_one.push(reduced),
                        (true, false) => fts_one.push(reduced),
                        (true, true) => fts_many.push(reduced),
                    }
                },
            }
        }

        tx_instant = self.tx_instant.unwrap_or_else(now);

        // Transact [:db/add :db/txInstant tx_instant :db/tx].
        non_fts_one.push((self.tx_id,
                          entids::DB_TX_INSTANT,
                          self.schema.require_attribute_for_entid(entids::DB_TX_INSTANT).unwrap(),
                          tx_instant.into(),
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
            tx_instant,
            tempids: tempids,
            changeset: affected_attrs,
        })
    }
}

/// Initialize a new Tx object with a new tx id and a tx instant. Kick off the SQLite conn, too.
fn start_tx<'conn, 'a>(conn: &'conn rusqlite::Connection,
                       mut partition_map: PartitionMap,
                       schema_for_mutation: &'a Schema,
                       schema: &'a Schema) -> Result<Tx<'conn, 'a>> {
    let tx_id = partition_map.allocate_entid(":db.part/tx");

    conn.begin_tx_application()?;

    Ok(Tx::new(conn, partition_map, schema_for_mutation, schema, tx_id))
}

fn conclude_tx(tx: Tx, report: TxReport) -> Result<(TxReport, PartitionMap, Option<Schema>)> {
    // If the schema has moved on, return it.
    let next_schema = match tx.schema_for_mutation {
        Cow::Borrowed(_) => None,
        Cow::Owned(next_schema) => Some(next_schema),
    };
    Ok((report, tx.partition_map, next_schema))
}

/// Transact the given `entities` against the given SQLite `conn`, using the given metadata.
/// If you want this work to occur inside a SQLite transaction, establish one on the connection
/// prior to calling this function.
///
/// This approach is explained in https://github.com/mozilla/mentat/wiki/Transacting.
// TODO: move this to the transactor layer.
pub fn transact<'conn, 'a, 'id, I>(conn: &'conn rusqlite::Connection,
                              partition_map: PartitionMap,
                              schema_for_mutation: &'a Schema,
                              schema: &'a Schema,
                              entities: I) -> Result<(TxReport, PartitionMap, Option<Schema>)>
    where I: IntoIterator<Item=Entity> {

    let mut tx = start_tx(conn, partition_map, schema_for_mutation, schema)?;
    let report = tx.transact_entities(entities)?;
    conclude_tx(tx, report)
}

/// Just like `transact`, but accepts lower-level inputs to allow bypassing the parser interface.
pub fn transact_terms<'conn, 'a, 'id, I>(conn: &'conn rusqlite::Connection,
                                    partition_map: PartitionMap,
                                    schema_for_mutation: &'a Schema,
                                    schema: &'a Schema,
                                    terms: I,
                                    tempid_set: InternSet<TempId>) -> Result<(TxReport, PartitionMap, Option<Schema>)>
    where I: IntoIterator<Item=TermWithTempIds> {
    let mut tx = start_tx(conn, partition_map, schema_for_mutation, schema)?;
    let report = tx.transact_simple_terms(terms, tempid_set)?;
    conclude_tx(tx, report)
}

pub struct TxObserver {
    notify_fn: Option<Box<FnMut(String, Vec<TxReport>)>>,
    attributes: AttributeSet,
    registration_time: Option<DateTime<Utc>>,
}

impl TxObserver {
    pub fn new<F>(attributes: AttributeSet, notify_fn: F) -> TxObserver where F: FnMut(String, Vec<TxReport>) + 'static {
        TxObserver {
            notify_fn: Some(Box::new(notify_fn)),
            attributes,
            registration_time: None,
        }
    }

    fn set_registered(&mut self) {
        self.registration_time = Some(now());
    }

    fn notify(&mut self, key: String, reports: &Vec<TxReport>) {
        let mut matching_reports: Vec<TxReport> = vec![];
        // if let Some(registration_time) = self.registration_time {
        //     if registration_time.le(&reports[0].tx_instant) {
                matching_reports = reports.iter().filter_map( |report| {
                    if self.attributes.intersection(&report.changeset).next().is_some(){
                        Some(report.clone())
                    } else {
                        None
                    }
                }).collect();
        //     }
        // }
        if !matching_reports.is_empty() {
            if let Some(ref mut notify_fn) = self.notify_fn {
                d(&format!("Notifying {:?} about tx {:?}", key, matching_reports));
                (notify_fn)(key, matching_reports);
            } else {
                d("no notify function specified for TxObserver");
            }
        } else {
            d(&format!("No matching reports for observer {:?} registered for updates on attributes {:?}: {:?}", key, reports, self.attributes));
        }
    }
}

#[derive(Default)]
pub struct TxObservationService {
    observers: BTreeMap<String, TxObserver>,
}

impl TxObservationService {
    // For testing purposes
    pub fn is_registered(&self, key: &String) -> bool {
        self.observers.contains_key(key)
    }

    pub fn register(&mut self, key: String, mut observer: TxObserver) {
        observer.set_registered();
        self.observers.insert(key.clone(), observer);
    }

    pub fn deregister(&mut self, key: &String) {
        self.observers.remove(key);
    }

    pub fn has_observers(&self) -> bool {
        !self.observers.is_empty()
    }

    pub fn transaction_did_commit(&mut self, reports: &Vec<TxReport>) {
        // notify all observers about their relevant transactions
        for (key, observer) in self.observers.iter_mut() {
            d(&format!("Notifying observer registered for key {:?}", key));
            observer.notify(key.clone(), reports);
        }
    }
}
