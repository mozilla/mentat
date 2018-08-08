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

use std::borrow::{
    Borrow,
};

use std::collections::{
    BTreeMap,
};

use std::fs::{
    File,
};

use std::io::{
    Read,
};

use std::path::{
    Path,
};

use std::sync::{
    Arc,
    Mutex,
};

use rusqlite;
use rusqlite::{
    TransactionBehavior,
};

use edn;
use edn::{
    InternSet,
};

pub use core_traits::{
    Entid,
    KnownEntid,
    StructuredMap,
    TypedValue,
    ValueType,
};

use mentat_core::{
    Attribute,
    HasSchema,
    Keyword,
    Schema,
    TxReport,
    ValueRc,
};

use mentat_db::cache::{
    InProgressCacheTransactWatcher,
    InProgressSQLiteAttributeCache,
    SQLiteAttributeCache,
};

use mentat_db::db;
use mentat_db::{
    transact,
    transact_terms,
    InProgressObserverTransactWatcher,
    PartitionMap,
    TransactableValue,
    TransactWatcher,
    TxObservationService,
    TxObserver,
};

use mentat_db::internal_types::TermWithTempIds;

use mentat_query_pull::{
    pull_attributes_for_entities,
    pull_attributes_for_entity,
};

use edn::entities::{
    TempId,
    OpType,
};

use entity_builder::{
    InProgressBuilder,
    TermBuilder,
};

use errors::{
    Result,
    MentatError,
};

use query::{
    Known,
    PreparedResult,
    QueryExplanation,
    QueryInputs,
    QueryOutput,
    lookup_value_for_attribute,
    lookup_values_for_attribute,
    q_explain,
    q_once,
    q_prepare,
    q_uncached,
};

/// Connection metadata required to query from, or apply transactions to, a Mentat store.
///
/// Owned data for the volatile parts (generation and partition map), and `Arc` for the infrequently
/// changing parts (schema) that we want to share across threads.
///
/// See https://github.com/mozilla/mentat/wiki/Thoughts:-modeling-db-conn-in-Rust.
pub struct Metadata {
    pub generation: u64,
    pub partition_map: PartitionMap,
    pub schema: Arc<Schema>,
    pub attribute_cache: SQLiteAttributeCache,
}

impl Metadata {
    // Intentionally not public.
    fn new(generation: u64, partition_map: PartitionMap, schema: Arc<Schema>, cache: SQLiteAttributeCache) -> Metadata {
        Metadata {
            generation: generation,
            partition_map: partition_map,
            schema: schema,
            attribute_cache: cache,
        }
    }
}

/// A mutable, safe reference to the current Mentat store.
pub struct Conn {
    /// `Mutex` since all reads and writes need to be exclusive.  Internally, owned data for the
    /// volatile parts (generation and partition map), and `Arc` for the infrequently changing parts
    /// (schema, cache) that we want to share across threads.  A consuming thread may use a shared
    /// reference after the `Conn`'s `Metadata` has moved on.
    ///
    /// The motivating case is multiple query threads taking references to the current schema to
    /// perform long-running queries while a single writer thread moves the metadata -- partition
    /// map and schema -- forward.
    ///
    /// We want the attribute cache to be isolated across transactions, updated within
    /// `InProgress` writes, and updated in the `Conn` on commit. To achieve this we
    /// store the cache itself in an `Arc` inside `SQLiteAttributeCache`, so that `.get_mut()`
    /// gives us copy-on-write semantics.
    /// We store that cached `Arc` here in a `Mutex`, so that the main copy can be carefully
    /// replaced on commit.
    metadata: Mutex<Metadata>,

    // TODO: maintain set of change listeners or handles to transaction report queues. #298.

    // TODO: maintain cache of query plans that could be shared across threads and invalidated when
    // the schema changes. #315.
    pub(crate) tx_observer_service: Mutex<TxObservationService>,
}

pub trait Queryable {
    fn q_explain<T>(&self, query: &str, inputs: T) -> Result<QueryExplanation>
        where T: Into<Option<QueryInputs>>;
    fn q_once<T>(&self, query: &str, inputs: T) -> Result<QueryOutput>
        where T: Into<Option<QueryInputs>>;
    fn q_prepare<T>(&self, query: &str, inputs: T) -> PreparedResult
        where T: Into<Option<QueryInputs>>;
    fn lookup_values_for_attribute<E>(&self, entity: E, attribute: &edn::Keyword) -> Result<Vec<TypedValue>>
        where E: Into<Entid>;
    fn lookup_value_for_attribute<E>(&self, entity: E, attribute: &edn::Keyword) -> Result<Option<TypedValue>>
        where E: Into<Entid>;
}

pub trait Pullable {
    fn pull_attributes_for_entities<E, A>(&self, entities: E, attributes: A) -> Result<BTreeMap<Entid, ValueRc<StructuredMap>>>
    where E: IntoIterator<Item=Entid>,
          A: IntoIterator<Item=Entid>;
    fn pull_attributes_for_entity<A>(&self, entity: Entid, attributes: A) -> Result<StructuredMap>
    where A: IntoIterator<Item=Entid>;
}

pub trait Syncable {
    fn sync(&mut self, server_uri: &String, user_uuid: &String) -> Result<()>;
}

/// Represents an in-progress, not yet committed, set of changes to the store.
/// Call `commit` to commit your changes, or `rollback` to discard them.
/// A transaction is held open until you do so.
/// Your changes will be implicitly dropped along with this struct.
pub struct InProgress<'a, 'c> {
    transaction: rusqlite::Transaction<'c>,
    mutex: &'a Mutex<Metadata>,
    generation: u64,
    partition_map: PartitionMap,
    pub(crate) schema: Schema,
    pub(crate) cache: InProgressSQLiteAttributeCache,
    use_caching: bool,
    tx_observer: &'a Mutex<TxObservationService>,
    tx_observer_watcher: InProgressObserverTransactWatcher,
}

/// Represents an in-progress set of reads to the store. Just like `InProgress`,
/// which is read-write, but only allows for reads.
pub struct InProgressRead<'a, 'c>(InProgress<'a, 'c>);

impl<'a, 'c> Queryable for InProgressRead<'a, 'c> {
    fn q_once<T>(&self, query: &str, inputs: T) -> Result<QueryOutput>
        where T: Into<Option<QueryInputs>> {
        self.0.q_once(query, inputs)
    }

    fn q_prepare<T>(&self, query: &str, inputs: T) -> PreparedResult
        where T: Into<Option<QueryInputs>> {
        self.0.q_prepare(query, inputs)
    }

    fn q_explain<T>(&self, query: &str, inputs: T) -> Result<QueryExplanation>
        where T: Into<Option<QueryInputs>> {
        self.0.q_explain(query, inputs)
    }

    fn lookup_values_for_attribute<E>(&self, entity: E, attribute: &edn::Keyword) -> Result<Vec<TypedValue>>
        where E: Into<Entid> {
        self.0.lookup_values_for_attribute(entity, attribute)
    }

    fn lookup_value_for_attribute<E>(&self, entity: E, attribute: &edn::Keyword) -> Result<Option<TypedValue>>
        where E: Into<Entid> {
        self.0.lookup_value_for_attribute(entity, attribute)
    }
}

impl<'a, 'c> Pullable for InProgressRead<'a, 'c> {
    fn pull_attributes_for_entities<E, A>(&self, entities: E, attributes: A) -> Result<BTreeMap<Entid, ValueRc<StructuredMap>>>
    where E: IntoIterator<Item=Entid>,
          A: IntoIterator<Item=Entid> {
        self.0.pull_attributes_for_entities(entities, attributes)
    }

    fn pull_attributes_for_entity<A>(&self, entity: Entid, attributes: A) -> Result<StructuredMap>
    where A: IntoIterator<Item=Entid> {
        self.0.pull_attributes_for_entity(entity, attributes)
    }
}

impl<'a, 'c> Queryable for InProgress<'a, 'c> {
    fn q_once<T>(&self, query: &str, inputs: T) -> Result<QueryOutput>
        where T: Into<Option<QueryInputs>> {

        if self.use_caching {
            let known = Known::new(&self.schema, Some(&self.cache));
            q_once(&*(self.transaction),
                   known,
                   query,
                   inputs)
        } else {
            q_uncached(&*(self.transaction),
                       &self.schema,
                       query,
                       inputs)
        }
    }

    fn q_prepare<T>(&self, query: &str, inputs: T) -> PreparedResult
        where T: Into<Option<QueryInputs>> {

        let known = Known::new(&self.schema, Some(&self.cache));
        q_prepare(&*(self.transaction),
                  known,
                  query,
                  inputs)
    }

    fn q_explain<T>(&self, query: &str, inputs: T) -> Result<QueryExplanation>
        where T: Into<Option<QueryInputs>> {

        let known = Known::new(&self.schema, Some(&self.cache));
        q_explain(&*(self.transaction),
                  known,
                  query,
                  inputs)
    }

    fn lookup_values_for_attribute<E>(&self, entity: E, attribute: &edn::Keyword) -> Result<Vec<TypedValue>>
        where E: Into<Entid> {
        let known = Known::new(&self.schema, Some(&self.cache));
        lookup_values_for_attribute(&*(self.transaction), known, entity, attribute)
    }

    fn lookup_value_for_attribute<E>(&self, entity: E, attribute: &edn::Keyword) -> Result<Option<TypedValue>>
        where E: Into<Entid> {
        let known = Known::new(&self.schema, Some(&self.cache));
        lookup_value_for_attribute(&*(self.transaction), known, entity, attribute)
    }
}

impl<'a, 'c> Pullable for InProgress<'a, 'c> {
    fn pull_attributes_for_entities<E, A>(&self, entities: E, attributes: A) -> Result<BTreeMap<Entid, ValueRc<StructuredMap>>>
    where E: IntoIterator<Item=Entid>,
          A: IntoIterator<Item=Entid> {
        pull_attributes_for_entities(&self.schema, &*(self.transaction), entities, attributes)
            .map_err(|e| e.into())
    }

    fn pull_attributes_for_entity<A>(&self, entity: Entid, attributes: A) -> Result<StructuredMap>
    where A: IntoIterator<Item=Entid> {
        pull_attributes_for_entity(&self.schema, &*(self.transaction), entity, attributes)
            .map_err(|e| e.into())
    }
}

impl<'a, 'c> HasSchema for InProgressRead<'a, 'c> {
    fn entid_for_type(&self, t: ValueType) -> Option<KnownEntid> {
        self.0.entid_for_type(t)
    }

    fn get_ident<T>(&self, x: T) -> Option<&Keyword> where T: Into<Entid> {
        self.0.get_ident(x)
    }

    fn get_entid(&self, x: &Keyword) -> Option<KnownEntid> {
        self.0.get_entid(x)
    }

    fn attribute_for_entid<T>(&self, x: T) -> Option<&Attribute> where T: Into<Entid> {
        self.0.attribute_for_entid(x)
    }

    fn attribute_for_ident(&self, ident: &Keyword) -> Option<(&Attribute, KnownEntid)> {
        self.0.attribute_for_ident(ident)
    }

    /// Return true if the provided entid identifies an attribute in this schema.
    fn is_attribute<T>(&self, x: T) -> bool where T: Into<Entid> {
        self.0.is_attribute(x)
    }

    /// Return true if the provided ident identifies an attribute in this schema.
    fn identifies_attribute(&self, x: &Keyword) -> bool {
        self.0.identifies_attribute(x)
    }

    fn component_attributes(&self) -> &[Entid] {
        self.0.component_attributes()
    }
}

impl<'a, 'c> HasSchema for InProgress<'a, 'c> {
    fn entid_for_type(&self, t: ValueType) -> Option<KnownEntid> {
        self.schema.entid_for_type(t)
    }

    fn get_ident<T>(&self, x: T) -> Option<&Keyword> where T: Into<Entid> {
        self.schema.get_ident(x)
    }

    fn get_entid(&self, x: &Keyword) -> Option<KnownEntid> {
        self.schema.get_entid(x)
    }

    fn attribute_for_entid<T>(&self, x: T) -> Option<&Attribute> where T: Into<Entid> {
        self.schema.attribute_for_entid(x)
    }

    fn attribute_for_ident(&self, ident: &Keyword) -> Option<(&Attribute, KnownEntid)> {
        self.schema.attribute_for_ident(ident)
    }

    /// Return true if the provided entid identifies an attribute in this schema.
    fn is_attribute<T>(&self, x: T) -> bool where T: Into<Entid> {
        self.schema.is_attribute(x)
    }

    /// Return true if the provided ident identifies an attribute in this schema.
    fn identifies_attribute(&self, x: &Keyword) -> bool {
        self.schema.identifies_attribute(x)
    }

    fn component_attributes(&self) -> &[Entid] {
        self.schema.component_attributes()
    }
}

impl<'a, 'c> InProgressRead<'a, 'c> {
    pub fn last_tx_id(&self) -> Entid {
        self.0.last_tx_id()
    }
}

impl<'a, 'c> InProgress<'a, 'c> {
    pub fn builder(self) -> InProgressBuilder<'a, 'c> {
        InProgressBuilder::new(self)
    }

    /// Choose whether to use in-memory caches for running queries.
    pub fn use_caching(&mut self, yesno: bool) {
        self.use_caching = yesno;
    }

    /// If you only have a reference to an `InProgress`, you can't use the easy builder.
    /// This exists so you can make your own.
    pub fn transact_builder(&mut self, builder: TermBuilder) -> Result<TxReport> {
        builder.build()
               .and_then(|(terms, _tempid_set)| {
                    self.transact_entities(terms)
               })
    }

    pub fn transact_terms<I>(&mut self, terms: I, tempid_set: InternSet<TempId>) -> Result<TxReport> where I: IntoIterator<Item=TermWithTempIds> {
        let w = InProgressTransactWatcher::new(
                &mut self.tx_observer_watcher,
                self.cache.transact_watcher());
        let (report, next_partition_map, next_schema, _watcher) =
            transact_terms(&self.transaction,
                           self.partition_map.clone(),
                           &self.schema,
                           &self.schema,
                           w,
                           terms,
                           tempid_set)?;
        self.partition_map = next_partition_map;
        if let Some(schema) = next_schema {
            self.schema = schema;
        }
        Ok(report)
    }

    pub fn transact_entities<I, V: TransactableValue>(&mut self, entities: I) -> Result<TxReport> where I: IntoIterator<Item=edn::entities::Entity<V>> {
        // We clone the partition map here, rather than trying to use a Cell or using a mutable
        // reference, for two reasons:
        // 1. `transact` allocates new IDs in partitions before and while doing work that might
        //    fail! We don't want to mutate this map on failure, so we can't just use &mut.
        // 2. Even if we could roll that back, we end up putting this `PartitionMap` into our
        //    `Metadata` on return. If we used `Cell` or other mechanisms, we'd be using
        //    `Default::default` in those situations to extract the partition map, and so there
        //    would still be some cost.
        let w = InProgressTransactWatcher::new(
                &mut self.tx_observer_watcher,
                self.cache.transact_watcher());
        let (report, next_partition_map, next_schema, _watcher) =
            transact(&self.transaction,
                     self.partition_map.clone(),
                     &self.schema,
                     &self.schema,
                     w,
                     entities)?;
        self.partition_map = next_partition_map;
        if let Some(schema) = next_schema {
            self.schema = schema;
        }
        Ok(report)
    }

    pub fn transact<B>(&mut self, transaction: B) -> Result<TxReport> where B: Borrow<str> {
        let entities = edn::parse::entities(transaction.borrow())?;
        self.transact_entities(entities)
    }

    pub fn import<P>(&mut self, path: P) -> Result<TxReport>
    where P: AsRef<Path> {
        let mut file = File::open(path)?;
        let mut text: String = String::new();
        file.read_to_string(&mut text)?;
        self.transact(text.as_str())
    }

    pub fn rollback(self) -> Result<()> {
        self.transaction.rollback().map_err(|e| e.into())
    }

    pub fn commit(self) -> Result<()> {
        // The mutex is taken during this entire method.
        let mut metadata = self.mutex.lock().unwrap();

        if self.generation != metadata.generation {
            // Somebody else wrote!
            // Retrying is tracked by https://github.com/mozilla/mentat/issues/357.
            // This should not occur -- an attempt to take a competing IMMEDIATE transaction
            // will fail with `SQLITE_BUSY`, causing this function to abort.
            bail!(MentatError::UnexpectedLostTransactRace);
        }

        // Commit the SQLite transaction while we hold the mutex.
        self.transaction.commit()?;

        metadata.generation += 1;
        metadata.partition_map = self.partition_map;

        // Update the conn's cache if we made any changes.
        self.cache.commit_to(&mut metadata.attribute_cache);

        if self.schema != *(metadata.schema) {
            metadata.schema = Arc::new(self.schema);

            // TODO: rebuild vocabularies and notify consumers that they've changed -- it's possible
            // that a change has arrived over the wire and invalidated some local module.
            // TODO: consider making vocabulary lookup lazy -- we won't need it much of the time.
        }

        let txes = self.tx_observer_watcher.txes;
        self.tx_observer.lock().unwrap().in_progress_did_commit(txes);

        Ok(())
    }

    pub fn cache(&mut self,
                 attribute: &Keyword,
                 cache_direction: CacheDirection,
                 cache_action: CacheAction) -> Result<()> {
        let attribute_entid: Entid = self.schema
                                         .attribute_for_ident(&attribute)
                                         .ok_or_else(|| MentatError::UnknownAttribute(attribute.to_string()))?.1.into();

        match cache_action {
            CacheAction::Register => {
                match cache_direction {
                    CacheDirection::Both => self.cache.register(&self.schema, &self.transaction, attribute_entid),
                    CacheDirection::Forward => self.cache.register_forward(&self.schema, &self.transaction, attribute_entid),
                    CacheDirection::Reverse => self.cache.register_reverse(&self.schema, &self.transaction, attribute_entid),
                }.map_err(|e| e.into())
            },
            CacheAction::Deregister => {
                self.cache.unregister(attribute_entid);
                Ok(())
            },
        }
    }

    pub fn last_tx_id(&self) -> Entid {
        self.partition_map[":db.part/tx"].next_entid() - 1
    }
}

struct InProgressTransactWatcher<'a, 'o> {
    cache_watcher: InProgressCacheTransactWatcher<'a>,
    observer_watcher: &'o mut InProgressObserverTransactWatcher,
    tx_id: Option<Entid>,
}

impl<'a, 'o> InProgressTransactWatcher<'a, 'o> {
    fn new(observer_watcher: &'o mut InProgressObserverTransactWatcher, cache_watcher: InProgressCacheTransactWatcher<'a>) -> Self {
        InProgressTransactWatcher {
            cache_watcher: cache_watcher,
            observer_watcher: observer_watcher,
            tx_id: None,
        }
    }
}

impl<'a, 'o> TransactWatcher for InProgressTransactWatcher<'a, 'o> {
    fn datom(&mut self, op: OpType, e: Entid, a: Entid, v: &TypedValue) {
        self.cache_watcher.datom(op.clone(), e.clone(), a.clone(), v);
        self.observer_watcher.datom(op.clone(), e.clone(), a.clone(), v);
    }

    fn done(&mut self, t: &Entid, schema: &Schema) -> ::mentat_db::errors::Result<()> {
        self.cache_watcher.done(t, schema)?;
        self.observer_watcher.done(t, schema)?;
        self.tx_id = Some(t.clone());
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CacheDirection {
    Forward,
    Reverse,
    Both,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CacheAction {
    Register,
    Deregister,
}

impl Conn {
    // Intentionally not public.
    fn new(partition_map: PartitionMap, schema: Schema) -> Conn {
        Conn {
            metadata: Mutex::new(Metadata::new(0, partition_map, Arc::new(schema), Default::default())),
            tx_observer_service: Mutex::new(TxObservationService::new()),
        }
    }

    /// Prepare the provided SQLite handle for use as a Mentat store. Creates tables but
    /// _does not_ write the bootstrap schema. This constructor should only be used by
    /// consumers that expect to populate raw transaction data themselves.

    pub(crate) fn empty(sqlite: &mut rusqlite::Connection) -> Result<Conn> {
        let (tx, db) = db::create_empty_current_version(sqlite)?;
        tx.commit()?;
        Ok(Conn::new(db.partition_map, db.schema))
    }


    pub fn connect(sqlite: &mut rusqlite::Connection) -> Result<Conn> {
        let db = db::ensure_current_version(sqlite)?;
        Ok(Conn::new(db.partition_map, db.schema))
    }

    /// Yield a clone of the current `Schema` instance.
    pub fn current_schema(&self) -> Arc<Schema> {
        // We always unwrap the mutex lock: if it's poisoned, this will propogate panics to all
        // accessing threads.  This is perhaps not reasonable; we expect the mutex to be held for
        // very short intervals, but a panic during a critical update section is possible, since the
        // lock encapsulates committing a SQL transaction.
        //
        // That being said, in the future we will provide an interface to take the mutex, providing
        // maximum flexibility for Mentat consumers.
        //
        // This approach might need to change when we support interrupting query threads (#297), and
        // will definitely need to change if we support interrupting transactor threads.
        //
        // Improving this is tracked by https://github.com/mozilla/mentat/issues/356.
        self.metadata.lock().unwrap().schema.clone()
    }

    pub fn current_cache(&self) -> SQLiteAttributeCache {
        self.metadata.lock().unwrap().attribute_cache.clone()
    }

    pub fn last_tx_id(&self) -> Entid {
        // The mutex is taken during this entire method.
        let metadata = self.metadata.lock().unwrap();

        metadata.partition_map[":db.part/tx"].next_entid() - 1
    }

    /// Query the Mentat store, using the given connection and the current metadata.
    pub fn q_once<T>(&self,
                     sqlite: &rusqlite::Connection,
                     query: &str,
                     inputs: T) -> Result<QueryOutput>
        where T: Into<Option<QueryInputs>> {

        // Doesn't clone, unlike `current_schema`.
        let metadata = self.metadata.lock().unwrap();
        let known = Known::new(&*metadata.schema, Some(&metadata.attribute_cache));
        q_once(sqlite,
               known,
               query,
               inputs)
    }

    /// Query the Mentat store, using the given connection and the current metadata,
    /// but without using the cache.
    pub fn q_uncached<T>(&self,
                         sqlite: &rusqlite::Connection,
                         query: &str,
                         inputs: T) -> Result<QueryOutput>
        where T: Into<Option<QueryInputs>> {

        let metadata = self.metadata.lock().unwrap();
        q_uncached(sqlite,
                   &*metadata.schema,        // Doesn't clone, unlike `current_schema`.
                   query,
                   inputs)
    }

    pub fn q_prepare<'sqlite, 'query, T>(&self,
                        sqlite: &'sqlite rusqlite::Connection,
                        query: &'query str,
                        inputs: T) -> PreparedResult<'sqlite>
        where T: Into<Option<QueryInputs>> {

        let metadata = self.metadata.lock().unwrap();
        let known = Known::new(&*metadata.schema, Some(&metadata.attribute_cache));
        q_prepare(sqlite,
                  known,
                  query,
                  inputs)
    }

    pub fn q_explain<T>(&self,
                        sqlite: &rusqlite::Connection,
                        query: &str,
                        inputs: T) -> Result<QueryExplanation>
        where T: Into<Option<QueryInputs>>
    {
        let metadata = self.metadata.lock().unwrap();
        let known = Known::new(&*metadata.schema, Some(&metadata.attribute_cache));
        q_explain(sqlite,
                  known,
                  query,
                  inputs)
    }

    pub fn pull_attributes_for_entities<E, A>(&self,
                                              sqlite: &rusqlite::Connection,
                                              entities: E,
                                              attributes: A) -> Result<BTreeMap<Entid, ValueRc<StructuredMap>>>
        where E: IntoIterator<Item=Entid>,
              A: IntoIterator<Item=Entid> {
        let metadata = self.metadata.lock().unwrap();
        let schema = &*metadata.schema;
        pull_attributes_for_entities(schema, sqlite, entities, attributes)
            .map_err(|e| e.into())
    }

    pub fn pull_attributes_for_entity<A>(&self,
                                         sqlite: &rusqlite::Connection,
                                         entity: Entid,
                                         attributes: A) -> Result<StructuredMap>
        where A: IntoIterator<Item=Entid> {
        let metadata = self.metadata.lock().unwrap();
        let schema = &*metadata.schema;
        pull_attributes_for_entity(schema, sqlite, entity, attributes)
            .map_err(|e| e.into())
    }

    pub fn lookup_values_for_attribute(&self,
                                       sqlite: &rusqlite::Connection,
                                       entity: Entid,
                                       attribute: &edn::Keyword) -> Result<Vec<TypedValue>> {
        let metadata = self.metadata.lock().unwrap();
        let known = Known::new(&*metadata.schema, Some(&metadata.attribute_cache));
        lookup_values_for_attribute(sqlite, known, entity, attribute)
    }

    pub fn lookup_value_for_attribute(&self,
                                      sqlite: &rusqlite::Connection,
                                      entity: Entid,
                                      attribute: &edn::Keyword) -> Result<Option<TypedValue>> {
        let metadata = self.metadata.lock().unwrap();
        let known = Known::new(&*metadata.schema, Some(&metadata.attribute_cache));
        lookup_value_for_attribute(sqlite, known, entity, attribute)
    }

    /// Take a SQLite transaction.
    fn begin_transaction_with_behavior<'m, 'conn>(&'m mut self, sqlite: &'conn mut rusqlite::Connection, behavior: TransactionBehavior) -> Result<InProgress<'m, 'conn>> {
        let tx = sqlite.transaction_with_behavior(behavior)?;
        let (current_generation, current_partition_map, current_schema, cache_cow) =
        {
            // The mutex is taken during this block.
            let ref current: Metadata = *self.metadata.lock().unwrap();
            (current.generation,
             // Expensive, but the partition map is updated after every committed transaction.
             current.partition_map.clone(),
             // Cheap.
             current.schema.clone(),
             current.attribute_cache.clone())
        };

        Ok(InProgress {
            mutex: &self.metadata,
            transaction: tx,
            generation: current_generation,
            partition_map: current_partition_map,
            schema: (*current_schema).clone(),
            cache: InProgressSQLiteAttributeCache::from_cache(cache_cow),
            use_caching: true,
            tx_observer: &self.tx_observer_service,
            tx_observer_watcher: InProgressObserverTransactWatcher::new(),
        })
    }

    // Helper to avoid passing connections around.
    // Make both args mutable so that we can't have parallel access.
    pub fn begin_read<'m, 'conn>(&'m mut self, sqlite: &'conn mut rusqlite::Connection) -> Result<InProgressRead<'m, 'conn>> {
        self.begin_transaction_with_behavior(sqlite, TransactionBehavior::Deferred)
            .map(InProgressRead)
    }

    pub fn begin_uncached_read<'m, 'conn>(&'m mut self, sqlite: &'conn mut rusqlite::Connection) -> Result<InProgressRead<'m, 'conn>> {
        self.begin_transaction_with_behavior(sqlite, TransactionBehavior::Deferred)
            .map(|mut ip| {
                ip.use_caching(false);
                InProgressRead(ip)
            })
    }

    /// IMMEDIATE means 'start the transaction now, but don't exclude readers'. It prevents other
    /// connections from taking immediate or exclusive transactions. This is appropriate for our
    /// writes and `InProgress`: it means we are ready to write whenever we want to, and nobody else
    /// can start a transaction that's not `DEFERRED`, but we don't need exclusivity yet.
    pub fn begin_transaction<'m, 'conn>(&'m mut self, sqlite: &'conn mut rusqlite::Connection) -> Result<InProgress<'m, 'conn>> {
        self.begin_transaction_with_behavior(sqlite, TransactionBehavior::Immediate)
    }

    /// Transact entities against the Mentat store, using the given connection and the current
    /// metadata.
    pub fn transact<B>(&mut self,
                    sqlite: &mut rusqlite::Connection,
                    transaction: B) -> Result<TxReport> where B: Borrow<str> {
        // Parse outside the SQL transaction. This is a tradeoff: we are limiting the scope of the
        // transaction, and indeed we don't even create a SQL transaction if the provided input is
        // invalid, but it means SQLite errors won't be found until the parse is complete, and if
        // there's a race for the database (don't do that!) we are less likely to win it.
        let entities = edn::parse::entities(transaction.borrow())?;

        let mut in_progress = self.begin_transaction(sqlite)?;
        let report = in_progress.transact_entities(entities)?;
        in_progress.commit()?;

        Ok(report)
    }

    /// Adds or removes the values of a given attribute to an in-memory cache.
    /// The attribute should be a namespaced string: e.g., `:foo/bar`.
    /// `cache_action` determines if the attribute should be added or removed from the cache.
    /// CacheAction::Add is idempotent - each attribute is only added once.
    /// CacheAction::Remove throws an error if the attribute does not currently exist in the cache.
    pub fn cache(&mut self,
                 sqlite: &mut rusqlite::Connection,
                 schema: &Schema,
                 attribute: &Keyword,
                 cache_direction: CacheDirection,
                 cache_action: CacheAction) -> Result<()> {
        let mut metadata = self.metadata.lock().unwrap();
        let attribute_entid: Entid;

        // Immutable borrow of metadata.
        {
            attribute_entid = metadata.schema
                                      .attribute_for_ident(&attribute)
                                      .ok_or_else(|| MentatError::UnknownAttribute(attribute.to_string()))?.1.into();
        }

        let cache = &mut metadata.attribute_cache;
        match cache_action {
            CacheAction::Register => {
                match cache_direction {
                    CacheDirection::Both => cache.register(schema, sqlite, attribute_entid),
                    CacheDirection::Forward => cache.register_forward(schema, sqlite, attribute_entid),
                    CacheDirection::Reverse => cache.register_reverse(schema, sqlite, attribute_entid),
                }.map_err(|e| e.into())
            },
            CacheAction::Deregister => {
                cache.unregister(attribute_entid);
                Ok(())
            },
        }
    }

    pub fn register_observer(&mut self, key: String, observer: Arc<TxObserver>) {
        self.tx_observer_service.lock().unwrap().register(key, observer);
    }

    pub fn unregister_observer(&mut self, key: &String) {
        self.tx_observer_service.lock().unwrap().deregister(key);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    extern crate time;

    use std::time::{
        Instant,
    };

    use core_traits::{
        Binding,
        TypedValue,
    };

    use mentat_core::{
        CachedAttributes,
    };

    use ::query::{
        Variable,
    };

    use ::{
        IntoResult,
        QueryInputs,
        QueryResults,
    };

    use mentat_db::USER0;

    #[test]
    fn test_transact_does_not_collide_existing_entids() {
        let mut sqlite = db::new_connection("").unwrap();
        let mut conn = Conn::connect(&mut sqlite).unwrap();

        // Let's find out the next ID that'll be allocated. We're going to try to collide with it
        // a bit later.
        let next = conn.metadata.lock().expect("metadata")
                       .partition_map[":db.part/user"].next_entid();
        let t = format!("[[:db/add {} :db.schema/attribute \"tempid\"]]", next + 1);

        match conn.transact(&mut sqlite, t.as_str()) {
            Err(MentatError::DbError(e)) => {
                assert_eq!(e.kind(), ::mentat_db::DbErrorKind::UnrecognizedEntid(next + 1));
            },
            x => panic!("expected db error, got {:?}", x),
        }

        // Transact two more tempids.
        let t = "[[:db/add \"one\" :db.schema/attribute \"more\"]]";
        let report = conn.transact(&mut sqlite, t)
                         .expect("transact succeeded");
        assert_eq!(report.tempids["more"], next);
        assert_eq!(report.tempids["one"], next + 1);
    }

    #[test]
    fn test_transact_does_not_collide_new_entids() {
        let mut sqlite = db::new_connection("").unwrap();
        let mut conn = Conn::connect(&mut sqlite).unwrap();

        // Let's find out the next ID that'll be allocated. We're going to try to collide with it.
        let next = conn.metadata.lock().expect("metadata").partition_map[":db.part/user"].next_entid();

        // If this were to be resolved, we'd get [:db/add 65537 :db.schema/attribute 65537], but
        // we should reject this, because the first ID was provided by the user!
        let t = format!("[[:db/add {} :db.schema/attribute \"tempid\"]]", next);

        match conn.transact(&mut sqlite, t.as_str()) {
            Err(MentatError::DbError(e)) => {
                // All this, despite this being the ID we were about to allocate!
                assert_eq!(e.kind(), ::mentat_db::DbErrorKind::UnrecognizedEntid(next));
            },
            x => panic!("expected db error, got {:?}", x),
        }

        // And if we subsequently transact in a way that allocates one ID, we _will_ use that one.
        // Note that `10` is a bootstrapped entid; we use it here as a known-good value.
        let t = "[[:db/add 10 :db.schema/attribute \"temp\"]]";
        let report = conn.transact(&mut sqlite, t)
                         .expect("transact succeeded");
        assert_eq!(report.tempids["temp"], next);
    }

    /// Return the entid that will be allocated to the next transacted tempid.
    fn get_next_entid(conn: &Conn) -> Entid {
        let partition_map = &conn.metadata.lock().unwrap().partition_map;
        partition_map.get(":db.part/user").unwrap().next_entid()
    }

    #[test]
    fn test_compound_transact() {
        let mut sqlite = db::new_connection("").unwrap();
        let mut conn = Conn::connect(&mut sqlite).unwrap();

        let tempid_offset = get_next_entid(&conn);

        let t = "[[:db/add \"one\" :db/ident :a/keyword1] \
                  [:db/add \"two\" :db/ident :a/keyword2]]";

        // This can refer to `t`, 'cos they occur in separate txes.
        let t2 = "[{:db.schema/attribute \"three\", :db/ident :a/keyword1}]";

        // Scoped borrow of `conn`.
        {
            let mut in_progress = conn.begin_transaction(&mut sqlite).expect("begun successfully");
            let report = in_progress.transact(t).expect("transacted successfully");
            let one = report.tempids.get("one").expect("found one").clone();
            let two = report.tempids.get("two").expect("found two").clone();
            assert!(one != two);
            assert!(one == tempid_offset || one == tempid_offset + 1);
            assert!(two == tempid_offset || two == tempid_offset + 1);

            println!("RES: {:?}", in_progress.q_once("[:find ?v :where [?x :db/ident ?v]]", None).unwrap());

            let during = in_progress.q_once("[:find ?x . :where [?x :db/ident :a/keyword1]]", None)
                                    .expect("query succeeded");
            assert_eq!(during.results, QueryResults::Scalar(Some(TypedValue::Ref(one).into())));

            let report = in_progress.transact(t2).expect("t2 succeeded");
            in_progress.commit().expect("commit succeeded");
            let three = report.tempids.get("three").expect("found three").clone();
            assert!(one != three);
            assert!(two != three);
        }

        // The DB part table changed.
        let tempid_offset_after = get_next_entid(&conn);
        assert_eq!(tempid_offset + 3, tempid_offset_after);
    }

    #[test]
    fn test_simple_prepared_query() {
        let mut c = db::new_connection("").expect("Couldn't open conn.");
        let mut conn = Conn::connect(&mut c).expect("Couldn't open DB.");
        conn.transact(&mut c, r#"[
            [:db/add "s" :db/ident :foo/boolean]
            [:db/add "s" :db/valueType :db.type/boolean]
            [:db/add "s" :db/cardinality :db.cardinality/one]
        ]"#).expect("successful transaction");

        let report = conn.transact(&mut c, r#"[
            [:db/add "u" :foo/boolean true]
            [:db/add "p" :foo/boolean false]
        ]"#).expect("successful transaction");
        let yes = report.tempids.get("u").expect("found it").clone();

        let vv = Variable::from_valid_name("?v");

        let values = QueryInputs::with_value_sequence(vec![(vv, true.into())]);

        let read = conn.begin_read(&mut c).expect("read");

        // N.B., you might choose to algebrize _without_ validating that the
        // types are known. In this query we know that `?v` must be a boolean,
        // and so we can kinda generate our own required input types!
        let mut prepared = read.q_prepare(r#"[:find [?x ...]
                                              :in ?v
                                              :where [?x :foo/boolean ?v]]"#,
                                          values).expect("prepare succeeded");

        let yeses = prepared.run(None).expect("result");
        assert_eq!(yeses.results, QueryResults::Coll(vec![TypedValue::Ref(yes).into()]));

        let yeses_again = prepared.run(None).expect("result");
        assert_eq!(yeses_again.results, QueryResults::Coll(vec![TypedValue::Ref(yes).into()]));
    }

    #[test]
    fn test_compound_rollback() {
        let mut sqlite = db::new_connection("").unwrap();
        let mut conn = Conn::connect(&mut sqlite).unwrap();

        let tempid_offset = get_next_entid(&conn);

        // Nothing in the store => USER0 should be our starting point.
        assert_eq!(tempid_offset, USER0);

        let t = "[[:db/add \"one\" :db/ident :a/keyword1] \
                  [:db/add \"two\" :db/ident :a/keyword2]]";

        // Scoped borrow of `sqlite`.
        {
            let mut in_progress = conn.begin_transaction(&mut sqlite).expect("begun successfully");
            let report = in_progress.transact(t).expect("transacted successfully");

            let one = report.tempids.get("one").expect("found it").clone();
            let two = report.tempids.get("two").expect("found it").clone();

            // The IDs are contiguous, starting at the previous part index.
            assert!(one != two);
            assert!(one == tempid_offset || one == tempid_offset + 1);
            assert!(two == tempid_offset || two == tempid_offset + 1);

            // Inside the InProgress we can see our changes.
            let during = in_progress.q_once("[:find ?x . :where [?x :db/ident :a/keyword1]]", None)
                                    .expect("query succeeded");

            assert_eq!(during.results, QueryResults::Scalar(Some(TypedValue::Ref(one).into())));

            // And we can do direct lookup, too.
            let kw = in_progress.lookup_value_for_attribute(one, &edn::Keyword::namespaced("db", "ident"))
                                .expect("lookup succeeded");
            assert_eq!(kw, Some(TypedValue::Keyword(edn::Keyword::namespaced("a", "keyword1").into())));

            in_progress.rollback()
                       .expect("rollback succeeded");
        }

        let after = conn.q_once(&mut sqlite, "[:find ?x . :where [?x :db/ident :a/keyword1]]", None)
                        .expect("query succeeded");
        assert_eq!(after.results, QueryResults::Scalar(None));

        // The DB part table is unchanged.
        let tempid_offset_after = get_next_entid(&conn);
        assert_eq!(tempid_offset, tempid_offset_after);
    }

    #[test]
    fn test_transact_errors() {
        let mut sqlite = db::new_connection("").unwrap();
        let mut conn = Conn::connect(&mut sqlite).unwrap();

        // Good: empty transaction.
        let report = conn.transact(&mut sqlite, "[]").unwrap();
        assert_eq!(report.tx_id, 0x10000000 + 1);

        // Bad EDN: missing closing ']'.
        let report = conn.transact(&mut sqlite, "[[:db/add \"t\" :db/ident :a/keyword]");
        match report.expect_err("expected transact to fail for bad edn") {
            MentatError::EdnParseError(_) => { },
            x => panic!("expected EDN parse error, got {:?}", x),
        }

        // Good EDN.
        let report = conn.transact(&mut sqlite, "[[:db/add \"t\" :db/ident :a/keyword]]").unwrap();
        assert_eq!(report.tx_id, 0x10000000 + 2);

        // Bad transaction data: missing leading :db/add.
        let report = conn.transact(&mut sqlite, "[[\"t\" :db/ident :b/keyword]]");
        match report.expect_err("expected transact error") {
            MentatError::EdnParseError(_) => { },
            x => panic!("expected EDN parse error, got {:?}", x),
        }

        // Good transaction data.
        let report = conn.transact(&mut sqlite, "[[:db/add \"u\" :db/ident :b/keyword]]").unwrap();
        assert_eq!(report.tx_id, 0x10000000 + 3);

        // Bad transaction based on state of store: conflicting upsert.
        let report = conn.transact(&mut sqlite, "[[:db/add \"u\" :db/ident :a/keyword]
                                                  [:db/add \"u\" :db/ident :b/keyword]]");
        match report.expect_err("expected transact error") {
            MentatError::DbError(e) => {
                match e.kind() {
                    ::mentat_db::DbErrorKind::SchemaConstraintViolation(_) => {},
                    _ => panic!("expected SchemaConstraintViolation"),
                }
            },
            x => panic!("expected db error, got {:?}", x),
        }
    }

    #[test]
    fn test_add_to_cache_failure_no_attribute() {
        let mut sqlite = db::new_connection("").unwrap();
        let mut conn = Conn::connect(&mut sqlite).unwrap();
        let _report = conn.transact(&mut sqlite, r#"[
            {  :db/ident       :foo/bar
               :db/valueType   :db.type/long },
            {  :db/ident       :foo/baz
               :db/valueType   :db.type/boolean }]"#).unwrap();

        let kw = kw!(:foo/bat);
        let schema = conn.current_schema();
        let res = conn.cache(&mut sqlite, &schema, &kw, CacheDirection::Forward, CacheAction::Register);
        match res.expect_err("expected cache to fail") {
            MentatError::UnknownAttribute(msg) => assert_eq!(msg, ":foo/bat"),
            x => panic!("expected UnknownAttribute error, got {:?}", x),
        }
    }

    // TODO expand tests to cover lookup_value_for_attribute comparing with and without caching
    #[test]
    fn test_lookup_attribute_with_caching() {

        let mut sqlite = db::new_connection("").unwrap();
        let mut conn = Conn::connect(&mut sqlite).unwrap();
        let _report = conn.transact(&mut sqlite, r#"[
            {  :db/ident       :foo/bar
               :db/valueType   :db.type/long },
            {  :db/ident       :foo/baz
               :db/valueType   :db.type/boolean }]"#).expect("transaction expected to succeed");

        {
            let mut in_progress = conn.begin_transaction(&mut sqlite).expect("transaction");
            for _ in 1..100 {
                let _report = in_progress.transact(r#"[
            {  :foo/bar        100
               :foo/baz        false },
            {  :foo/bar        200
               :foo/baz        true },
            {  :foo/bar        100
               :foo/baz        false },
            {  :foo/bar        300
               :foo/baz        true },
            {  :foo/bar        400
               :foo/baz        false },
            {  :foo/bar        500
               :foo/baz        true }]"#).expect("transaction expected to succeed");
            }
            in_progress.commit().expect("Committed");
        }

        let entities = conn.q_once(&sqlite, r#"[:find ?e . :where [?e :foo/bar 400]]"#, None).expect("Expected query to work").into_scalar().expect("expected rel results");
        let first = entities.expect("expected a result");
        let entid = match first {
            Binding::Scalar(TypedValue::Ref(entid)) => entid,
            x => panic!("expected Some(Ref), got {:?}", x),
        };

        let kw = kw!(:foo/bar);
        let start = Instant::now();
        let uncached_val = conn.lookup_value_for_attribute(&sqlite, entid, &kw).expect("Expected value on lookup");
        let finish = Instant::now();
        let uncached_elapsed_time = finish.duration_since(start);
        println!("Uncached time: {:?}", uncached_elapsed_time);

        let schema = conn.current_schema();
        conn.cache(&mut sqlite, &schema, &kw, CacheDirection::Forward, CacheAction::Register).expect("expected caching to work");

        for _ in 1..5 {
            let start = Instant::now();
            let cached_val = conn.lookup_value_for_attribute(&sqlite, entid, &kw).expect("Expected value on lookup");
            let finish = Instant::now();
            let cached_elapsed_time = finish.duration_since(start);
            assert_eq!(cached_val, uncached_val);

            println!("Cached time: {:?}", cached_elapsed_time);
            assert!(cached_elapsed_time < uncached_elapsed_time);
        }
    }

    #[test]
    fn test_cache_usage() {
        let mut sqlite = db::new_connection("").unwrap();
        let mut conn = Conn::connect(&mut sqlite).unwrap();

        let db_ident = (*conn.current_schema()).get_entid(&kw!(:db/ident)).expect("db_ident").0;
        let db_type = (*conn.current_schema()).get_entid(&kw!(:db/valueType)).expect("db_ident").0;
        println!("db/ident is {}", db_ident);
        println!("db/type is {}", db_type);
        let query = format!("[:find ?ident . :where [?e {} :db/doc][?e {} ?type][?type {} ?ident]]",
                            db_ident, db_type, db_ident);

        println!("Query is {}", query);

        assert!(!conn.current_cache().is_attribute_cached_forward(db_ident));

        {
            let mut ip = conn.begin_transaction(&mut sqlite).expect("began");

            let ident = ip.q_once(query.as_str(), None).into_scalar_result().expect("query");
            assert_eq!(ident, Some(TypedValue::typed_ns_keyword("db.type", "string").into()));

            let start = time::PreciseTime::now();
            ip.q_once(query.as_str(), None).into_scalar_result().expect("query");
            let end = time::PreciseTime::now();
            println!("Uncached took {}µs", start.to(end).num_microseconds().unwrap());

            ip.cache(&kw!(:db/ident), CacheDirection::Forward, CacheAction::Register).expect("registered");
            ip.cache(&kw!(:db/valueType), CacheDirection::Forward, CacheAction::Register).expect("registered");

            assert!(ip.cache.is_attribute_cached_forward(db_ident));

            let ident = ip.q_once(query.as_str(), None).into_scalar_result().expect("query");
            assert_eq!(ident, Some(TypedValue::typed_ns_keyword("db.type", "string").into()));

            let start = time::PreciseTime::now();
            ip.q_once(query.as_str(), None).into_scalar_result().expect("query");
            let end = time::PreciseTime::now();
            println!("Cached took {}µs", start.to(end).num_microseconds().unwrap());

            // If we roll back the change, our caching operations are also rolled back.
            ip.rollback().expect("rolled back");
        }

        assert!(!conn.current_cache().is_attribute_cached_forward(db_ident));

        {
            let mut ip = conn.begin_transaction(&mut sqlite).expect("began");

            let ident = ip.q_once(query.as_str(), None).into_scalar_result().expect("query");
            assert_eq!(ident, Some(TypedValue::typed_ns_keyword("db.type", "string").into()));
            ip.cache(&kw!(:db/ident), CacheDirection::Forward, CacheAction::Register).expect("registered");
            ip.cache(&kw!(:db/valueType), CacheDirection::Forward, CacheAction::Register).expect("registered");

            assert!(ip.cache.is_attribute_cached_forward(db_ident));

            ip.commit().expect("rolled back");
        }

        assert!(conn.current_cache().is_attribute_cached_forward(db_ident));
        assert!(conn.current_cache().is_attribute_cached_forward(db_type));
    }
}
