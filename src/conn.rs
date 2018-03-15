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

use mentat_core::{
    Attribute,
    Entid,
    HasSchema,
    KnownEntid,
    NamespacedKeyword,
    Schema,
    TypedValue,
    ValueType,
};

use mentat_core::intern_set::InternSet;

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
    TransactWatcher,
    TxObservationService,
    TxObserver,
    TxReport,
};

use mentat_db::internal_types::TermWithTempIds;

use mentat_tx;

use mentat_tx::entities::{
    TempId,
    OpType,
};

use mentat_tx_parser;

use mentat_tolstoy::Syncer;

use uuid::Uuid;

use entity_builder::{
    InProgressBuilder,
};

use errors::*;

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
    tx_observer_service: Mutex<TxObservationService>,
}

/// A convenience wrapper around a single SQLite connection and a Conn. This is suitable
/// for applications that don't require complex connection management.
pub struct Store {
    conn: Conn,
    sqlite: rusqlite::Connection,
}

impl Store {
    pub fn open_empty(path: &str) -> Result<Store> {
        if !path.is_empty() {
            if Path::new(path).exists() {
                bail!(ErrorKind::PathAlreadyExists(path.to_string()));
            }
        }

        let mut connection = ::new_connection(path)?;
        let conn = Conn::empty(&mut connection)?;
        Ok(Store {
            conn: conn,
            sqlite: connection,
        })
    }

    pub fn open(path: &str) -> Result<Store> {
        let mut connection = ::new_connection(path)?;
        let conn = Conn::connect(&mut connection)?;
        Ok(Store {
            conn: conn,
            sqlite: connection,
        })
    }
}

pub trait Queryable {
    fn q_explain<T>(&self, query: &str, inputs: T) -> Result<QueryExplanation>
        where T: Into<Option<QueryInputs>>;
    fn q_once<T>(&self, query: &str, inputs: T) -> Result<QueryOutput>
        where T: Into<Option<QueryInputs>>;
    fn q_prepare<T>(&self, query: &str, inputs: T) -> PreparedResult
        where T: Into<Option<QueryInputs>>;
    fn lookup_values_for_attribute<E>(&self, entity: E, attribute: &edn::NamespacedKeyword) -> Result<Vec<TypedValue>>
        where E: Into<Entid>;
    fn lookup_value_for_attribute<E>(&self, entity: E, attribute: &edn::NamespacedKeyword) -> Result<Option<TypedValue>>
        where E: Into<Entid>;
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
    schema: Schema,
    cache: InProgressSQLiteAttributeCache,
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

    fn lookup_values_for_attribute<E>(&self, entity: E, attribute: &edn::NamespacedKeyword) -> Result<Vec<TypedValue>>
        where E: Into<Entid> {
        self.0.lookup_values_for_attribute(entity, attribute)
    }

    fn lookup_value_for_attribute<E>(&self, entity: E, attribute: &edn::NamespacedKeyword) -> Result<Option<TypedValue>>
        where E: Into<Entid> {
        self.0.lookup_value_for_attribute(entity, attribute)
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

    fn lookup_values_for_attribute<E>(&self, entity: E, attribute: &edn::NamespacedKeyword) -> Result<Vec<TypedValue>>
        where E: Into<Entid> {
        let known = Known::new(&self.schema, Some(&self.cache));
        lookup_values_for_attribute(&*(self.transaction), known, entity, attribute)
    }

    fn lookup_value_for_attribute<E>(&self, entity: E, attribute: &edn::NamespacedKeyword) -> Result<Option<TypedValue>>
        where E: Into<Entid> {
        let known = Known::new(&self.schema, Some(&self.cache));
        lookup_value_for_attribute(&*(self.transaction), known, entity, attribute)
    }
}

impl<'a, 'c> HasSchema for InProgressRead<'a, 'c> {
    fn entid_for_type(&self, t: ValueType) -> Option<KnownEntid> {
        self.0.entid_for_type(t)
    }

    fn get_ident<T>(&self, x: T) -> Option<&NamespacedKeyword> where T: Into<Entid> {
        self.0.get_ident(x)
    }

    fn get_entid(&self, x: &NamespacedKeyword) -> Option<KnownEntid> {
        self.0.get_entid(x)
    }

    fn attribute_for_entid<T>(&self, x: T) -> Option<&Attribute> where T: Into<Entid> {
        self.0.attribute_for_entid(x)
    }

    fn attribute_for_ident(&self, ident: &NamespacedKeyword) -> Option<(&Attribute, KnownEntid)> {
        self.0.attribute_for_ident(ident)
    }

    /// Return true if the provided entid identifies an attribute in this schema.
    fn is_attribute<T>(&self, x: T) -> bool where T: Into<Entid> {
        self.0.is_attribute(x)
    }

    /// Return true if the provided ident identifies an attribute in this schema.
    fn identifies_attribute(&self, x: &NamespacedKeyword) -> bool {
        self.0.identifies_attribute(x)
    }
}

impl<'a, 'c> HasSchema for InProgress<'a, 'c> {
    fn entid_for_type(&self, t: ValueType) -> Option<KnownEntid> {
        self.schema.entid_for_type(t)
    }

    fn get_ident<T>(&self, x: T) -> Option<&NamespacedKeyword> where T: Into<Entid> {
        self.schema.get_ident(x)
    }

    fn get_entid(&self, x: &NamespacedKeyword) -> Option<KnownEntid> {
        self.schema.get_entid(x)
    }

    fn attribute_for_entid<T>(&self, x: T) -> Option<&Attribute> where T: Into<Entid> {
        self.schema.attribute_for_entid(x)
    }

    fn attribute_for_ident(&self, ident: &NamespacedKeyword) -> Option<(&Attribute, KnownEntid)> {
        self.schema.attribute_for_ident(ident)
    }

    /// Return true if the provided entid identifies an attribute in this schema.
    fn is_attribute<T>(&self, x: T) -> bool where T: Into<Entid> {
        self.schema.is_attribute(x)
    }

    /// Return true if the provided ident identifies an attribute in this schema.
    fn identifies_attribute(&self, x: &NamespacedKeyword) -> bool {
        self.schema.identifies_attribute(x)
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

    pub fn transact_entities<I>(&mut self, entities: I) -> Result<TxReport> where I: IntoIterator<Item=mentat_tx::entities::Entity> {
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

    pub fn transact(&mut self, transaction: &str) -> Result<TxReport> {
        let assertion_vector = edn::parse::value(transaction)?;
        let entities = mentat_tx_parser::Tx::parse(&assertion_vector)?;
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
            bail!("Lost the transact() race!");
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
                 attribute: &NamespacedKeyword,
                 cache_direction: CacheDirection,
                 cache_action: CacheAction) -> Result<()> {
        let attribute_entid: Entid = self.schema
                                         .attribute_for_ident(&attribute)
                                         .ok_or_else(|| ErrorKind::UnknownAttribute(attribute.to_string()))?.1.into();

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
}

struct InProgressTransactWatcher<'a, 'o> {
    cache_watcher: InProgressCacheTransactWatcher<'a>,
    observer_watcher: &'o mut InProgressObserverTransactWatcher,
}

impl<'a, 'o> InProgressTransactWatcher<'a, 'o> {
    fn new(observer_watcher: &'o mut InProgressObserverTransactWatcher, cache_watcher: InProgressCacheTransactWatcher<'a>) -> Self {
        InProgressTransactWatcher {
            cache_watcher: cache_watcher,
            observer_watcher: observer_watcher,
        }
    }
}

impl<'a, 'o> TransactWatcher for InProgressTransactWatcher<'a, 'o> {
    fn start(&mut self, t: &Entid) {
        self.cache_watcher.start(t);
        self.observer_watcher.start(t);
    }

    fn datom(&mut self, op: OpType, e: Entid, a: Entid, v: &TypedValue) {
        self.cache_watcher.datom(op.clone(), e.clone(), a.clone(), v);
        self.observer_watcher.datom(op.clone(), e.clone(), a.clone(), v);
    }

    fn done(&mut self, schema: &Schema) -> ::mentat_db::errors::Result<()> {
        self.cache_watcher.done(schema)?;
        self.observer_watcher.done(schema)?;
        Ok(())
    }
}

impl Store {
    /// Intended for use from tests.
    pub fn sqlite_mut(&mut self) -> &mut rusqlite::Connection {
        &mut self.sqlite
    }

    #[cfg(test)]
    pub fn is_registered_as_observer(&self, key: &String) -> bool {
        self.conn.tx_observer_service.lock().unwrap().is_registered(key)
    }
}

impl Store {
    pub fn dismantle(self) -> (rusqlite::Connection, Conn) {
        (self.sqlite, self.conn)
    }

    pub fn conn(&self) -> &Conn {
        &self.conn
    }

    pub fn begin_read<'m>(&'m mut self) -> Result<InProgressRead<'m, 'm>> {
        self.conn.begin_read(&mut self.sqlite)
    }

    pub fn begin_transaction<'m>(&'m mut self) -> Result<InProgress<'m, 'm>> {
        self.conn.begin_transaction(&mut self.sqlite)
    }

    pub fn cache(&mut self, attr: &NamespacedKeyword, direction: CacheDirection) -> Result<()> {
        let schema = &self.conn.current_schema();
        self.conn.cache(&mut self.sqlite,
                        schema,
                        attr,
                        direction,
                        CacheAction::Register)
    }

    pub fn register_observer(&mut self, key: String, observer: Arc<TxObserver>) {
        self.conn.register_observer(key, observer);
    }

    pub fn unregister_observer(&mut self, key: &String) {
        self.conn.unregister_observer(key);
    }
}

impl Queryable for Store {
    fn q_once<T>(&self, query: &str, inputs: T) -> Result<QueryOutput>
        where T: Into<Option<QueryInputs>> {
        self.conn.q_once(&self.sqlite, query, inputs)
    }

    fn q_prepare<T>(&self, query: &str, inputs: T) -> PreparedResult
        where T: Into<Option<QueryInputs>> {
        self.conn.q_prepare(&self.sqlite, query, inputs)
    }

    fn q_explain<T>(&self, query: &str, inputs: T) -> Result<QueryExplanation>
        where T: Into<Option<QueryInputs>> {
        self.conn.q_explain(&self.sqlite, query, inputs)
    }

    fn lookup_values_for_attribute<E>(&self, entity: E, attribute: &edn::NamespacedKeyword) -> Result<Vec<TypedValue>>
        where E: Into<Entid> {
        self.conn.lookup_values_for_attribute(&self.sqlite, entity.into(), attribute)
    }

    fn lookup_value_for_attribute<E>(&self, entity: E, attribute: &edn::NamespacedKeyword) -> Result<Option<TypedValue>>
        where E: Into<Entid> {
        self.conn.lookup_value_for_attribute(&self.sqlite, entity.into(), attribute)
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

impl Syncable for Store {
    fn sync(&mut self, server_uri: &String, user_uuid: &String) -> Result<()> {
        let uuid = Uuid::parse_str(&user_uuid)?;
        Ok(Syncer::flow(&mut self.sqlite, server_uri, &uuid)?)
    }
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
    fn empty(sqlite: &mut rusqlite::Connection) -> Result<Conn> {
        let (tx, db) = db::create_empty_current_version(sqlite)
            .chain_err(|| "Unable to initialize Mentat store")?;
        tx.commit()?;
        Ok(Conn::new(db.partition_map, db.schema))
    }


    pub fn connect(sqlite: &mut rusqlite::Connection) -> Result<Conn> {
        let db = db::ensure_current_version(sqlite)
            .chain_err(|| "Unable to initialize Mentat store")?;
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

    pub fn lookup_values_for_attribute(&self,
                                       sqlite: &rusqlite::Connection,
                                       entity: Entid,
                                       attribute: &edn::NamespacedKeyword) -> Result<Vec<TypedValue>> {
        let metadata = self.metadata.lock().unwrap();
        let known = Known::new(&*metadata.schema, Some(&metadata.attribute_cache));
        lookup_values_for_attribute(sqlite, known, entity, attribute)
    }

    pub fn lookup_value_for_attribute(&self,
                                      sqlite: &rusqlite::Connection,
                                      entity: Entid,
                                      attribute: &edn::NamespacedKeyword) -> Result<Option<TypedValue>> {
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
    pub fn transact(&mut self,
                    sqlite: &mut rusqlite::Connection,
                    transaction: &str) -> Result<TxReport> {
        // Parse outside the SQL transaction. This is a tradeoff: we are limiting the scope of the
        // transaction, and indeed we don't even create a SQL transaction if the provided input is
        // invalid, but it means SQLite errors won't be found until the parse is complete, and if
        // there's a race for the database (don't do that!) we are less likely to win it.
        let assertion_vector = edn::parse::value(transaction)?;
        let entities = mentat_tx_parser::Tx::parse(&assertion_vector)?;

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
                 attribute: &NamespacedKeyword,
                 cache_direction: CacheDirection,
                 cache_action: CacheAction) -> Result<()> {
        let mut metadata = self.metadata.lock().unwrap();
        let attribute_entid: Entid;

        // Immutable borrow of metadata.
        {
            attribute_entid = metadata.schema
                                      .attribute_for_ident(&attribute)
                                      .ok_or_else(|| ErrorKind::UnknownAttribute(attribute.to_string()))?.1.into();
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
    extern crate mentat_parser_utils;

    use std::collections::{
        BTreeSet,
    };
    use std::path::{
        PathBuf,
    };
    use std::sync::mpsc;
    use std::time::{
        Duration,
        Instant,
    };

    use mentat_core::{
        CachedAttributes,
        TypedValue,
    };

    use ::entity_builder::{
        BuildTerms,
    };

    use ::query::{
        PreparedQuery,
        Variable,
    };

    use ::{
        IntoResult,
        QueryInputs,
        QueryResults,
    };

    use ::vocabulary::{
        AttributeBuilder,
        Definition,
        VersionedStore,
    };

    use ::vocabulary::attribute::{
        Unique,
    };

    use mentat_db::USER0;

    #[test]
    fn test_transact_does_not_collide_existing_entids() {
        let mut sqlite = db::new_connection("").unwrap();
        let mut conn = Conn::connect(&mut sqlite).unwrap();

        // Let's find out the next ID that'll be allocated. We're going to try to collide with it
        // a bit later.
        let next = conn.metadata.lock().expect("metadata")
                       .partition_map[":db.part/user"].index;
        let t = format!("[[:db/add {} :db.schema/attribute \"tempid\"]]", next + 1);

        match conn.transact(&mut sqlite, t.as_str()).unwrap_err() {
            Error(ErrorKind::DbError(::mentat_db::errors::ErrorKind::UnrecognizedEntid(e)), _) => {
                assert_eq!(e, next + 1);
            },
            x => panic!("expected transact error, got {:?}", x),
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
        let next = conn.metadata.lock().expect("metadata").partition_map[":db.part/user"].index;

        // If this were to be resolved, we'd get [:db/add 65537 :db.schema/attribute 65537], but
        // we should reject this, because the first ID was provided by the user!
        let t = format!("[[:db/add {} :db.schema/attribute \"tempid\"]]", next);

        match conn.transact(&mut sqlite, t.as_str()).unwrap_err() {
            Error(ErrorKind::DbError(::mentat_db::errors::ErrorKind::UnrecognizedEntid(e)), _) => {
                // All this, despite this being the ID we were about to allocate!
                assert_eq!(e, next);
            },
            x => panic!("expected transact error, got {:?}", x),
        }

        // And if we subsequently transact in a way that allocates one ID, we _will_ use that one.
        // Note that `10` is a bootstrapped entid; we use it here as a known-good value.
        let t = "[[:db/add 10 :db.schema/attribute \"temp\"]]";
        let report = conn.transact(&mut sqlite, t)
                         .expect("transact succeeded");
        assert_eq!(report.tempids["temp"], next);
    }

    /// Return the entid that will be allocated to the next transacted tempid.
    fn get_next_entid(conn: &Conn) -> i64 {
        let partition_map = &conn.metadata.lock().unwrap().partition_map;
        partition_map.get(":db.part/user").unwrap().index
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

            let during = in_progress.q_once("[:find ?x . :where [?x :db/ident :a/keyword1]]", None)
                                    .expect("query succeeded");
            assert_eq!(during.results, QueryResults::Scalar(Some(TypedValue::Ref(one))));

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
        assert_eq!(yeses.results, QueryResults::Coll(vec![TypedValue::Ref(yes)]));

        let yeses_again = prepared.run(None).expect("result");
        assert_eq!(yeses_again.results, QueryResults::Coll(vec![TypedValue::Ref(yes)]));
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

            assert_eq!(during.results, QueryResults::Scalar(Some(TypedValue::Ref(one))));

            // And we can do direct lookup, too.
            let kw = in_progress.lookup_value_for_attribute(one, &edn::NamespacedKeyword::new("db", "ident"))
                                .expect("lookup succeeded");
            assert_eq!(kw, Some(TypedValue::Keyword(edn::NamespacedKeyword::new("a", "keyword1").into())));

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
        match report.unwrap_err() {
            Error(ErrorKind::EdnParseError(_), _) => { },
            x => panic!("expected EDN parse error, got {:?}", x),
        }

        // Good EDN.
        let report = conn.transact(&mut sqlite, "[[:db/add \"t\" :db/ident :a/keyword]]").unwrap();
        assert_eq!(report.tx_id, 0x10000000 + 2);

        // Bad transaction data: missing leading :db/add.
        let report = conn.transact(&mut sqlite, "[[\"t\" :db/ident :b/keyword]]");
        match report.unwrap_err() {
            Error(ErrorKind::TxParseError(::mentat_tx_parser::errors::ErrorKind::ParseError(_)), _) => { },
            x => panic!("expected EDN parse error, got {:?}", x),
        }

        // Good transaction data.
        let report = conn.transact(&mut sqlite, "[[:db/add \"u\" :db/ident :b/keyword]]").unwrap();
        assert_eq!(report.tx_id, 0x10000000 + 3);

        // Bad transaction based on state of store: conflicting upsert.
        let report = conn.transact(&mut sqlite, "[[:db/add \"u\" :db/ident :a/keyword]
                                                  [:db/add \"u\" :db/ident :b/keyword]]");
        match report.unwrap_err() {
            Error(ErrorKind::DbError(::mentat_db::errors::ErrorKind::NotYetImplemented(_)), _) => { },
            x => panic!("expected EDN parse error, got {:?}", x),
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
        match res.unwrap_err() {
            Error(ErrorKind::UnknownAttribute(msg), _) => assert_eq!(msg, ":foo/bat"),
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
            TypedValue::Ref(entid) => entid,
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
            assert_eq!(ident, Some(TypedValue::typed_ns_keyword("db.type", "string")));

            let start = time::PreciseTime::now();
            ip.q_once(query.as_str(), None).into_scalar_result().expect("query");
            let end = time::PreciseTime::now();
            println!("Uncached took {}µs", start.to(end).num_microseconds().unwrap());

            ip.cache(&kw!(:db/ident), CacheDirection::Forward, CacheAction::Register).expect("registered");
            ip.cache(&kw!(:db/valueType), CacheDirection::Forward, CacheAction::Register).expect("registered");

            assert!(ip.cache.is_attribute_cached_forward(db_ident));

            let ident = ip.q_once(query.as_str(), None).into_scalar_result().expect("query");
            assert_eq!(ident, Some(TypedValue::typed_ns_keyword("db.type", "string")));

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
            assert_eq!(ident, Some(TypedValue::typed_ns_keyword("db.type", "string")));
            ip.cache(&kw!(:db/ident), CacheDirection::Forward, CacheAction::Register).expect("registered");
            ip.cache(&kw!(:db/valueType), CacheDirection::Forward, CacheAction::Register).expect("registered");

            assert!(ip.cache.is_attribute_cached_forward(db_ident));

            ip.commit().expect("rolled back");
        }

        assert!(conn.current_cache().is_attribute_cached_forward(db_ident));
        assert!(conn.current_cache().is_attribute_cached_forward(db_type));
    }

    fn fixture_path(rest: &str) -> PathBuf {
        let fixtures = Path::new("fixtures/");
        fixtures.join(Path::new(rest))
    }

    #[test]
    fn test_prepared_query_with_cache() {
        let mut store = Store::open("").expect("opened");
        let mut in_progress = store.begin_transaction().expect("began");
        in_progress.import(fixture_path("cities.schema")).expect("transacted schema");
        in_progress.import(fixture_path("all_seattle.edn")).expect("transacted data");
        in_progress.cache(&kw!(:neighborhood/district), CacheDirection::Forward, CacheAction::Register).expect("cache done");
        in_progress.cache(&kw!(:district/name), CacheDirection::Forward, CacheAction::Register).expect("cache done");
        in_progress.cache(&kw!(:neighborhood/name), CacheDirection::Reverse, CacheAction::Register).expect("cache done");

        let query = r#"[:find ?district
                        :in ?hood
                        :where
                        [?neighborhood :neighborhood/name ?hood]
                        [?neighborhood :neighborhood/district ?d]
                        [?d :district/name ?district]]"#;
        let hood = "Beacon Hill";
        let inputs = QueryInputs::with_value_sequence(vec![(var!(?hood), TypedValue::typed_string(hood))]);
        let mut prepared = in_progress.q_prepare(query, inputs)
                                      .expect("prepared");
        match &prepared {
            &PreparedQuery::Constant { select: ref _select } => {},
            _ => panic!(),
        };


        let start = time::PreciseTime::now();
        let results = prepared.run(None).expect("results");
        let end = time::PreciseTime::now();
        println!("Prepared cache execution took {}µs", start.to(end).num_microseconds().unwrap());
        assert_eq!(results.into_rel().expect("result"),
                   vec![vec![TypedValue::typed_string("Greater Duwamish")]]);
    }

    trait StoreCache {
        fn get_entid_for_value(&self, attr: Entid, val: &TypedValue) -> Option<Entid>;
        fn is_attribute_cached_reverse(&self, attr: Entid) -> bool;
        fn is_attribute_cached_forward(&self, attr: Entid) -> bool;
    }

    impl StoreCache for Store {
        fn get_entid_for_value(&self, attr: Entid, val: &TypedValue) -> Option<Entid> {
            let cache = self.conn.current_cache();
            cache.get_entid_for_value(attr, val)
        }

        fn is_attribute_cached_forward(&self, attr: Entid) -> bool {
            self.conn.current_cache().is_attribute_cached_forward(attr)
        }

        fn is_attribute_cached_reverse(&self, attr: Entid) -> bool {
            self.conn.current_cache().is_attribute_cached_reverse(attr)
        }
    }

    #[test]
    fn test_cache_mutation() {
        let mut store = Store::open("").expect("opened");

        {
            let mut in_progress = store.begin_transaction().expect("begun");
            in_progress.transact(r#"[
                {  :db/ident       :foo/bar
                   :db/cardinality :db.cardinality/one
                   :db/index       true
                   :db/unique      :db.unique/identity
                   :db/valueType   :db.type/long },
                {  :db/ident       :foo/baz
                   :db/cardinality :db.cardinality/one
                   :db/valueType   :db.type/boolean }
                {  :db/ident       :foo/x
                   :db/cardinality :db.cardinality/many
                   :db/valueType   :db.type/long }]"#).expect("transact");

            // Cache one….
            in_progress.cache(&kw!(:foo/bar), CacheDirection::Reverse, CacheAction::Register).expect("cache done");
            in_progress.commit().expect("commit");
        }

        let foo_bar = store.conn.current_schema().get_entid(&kw!(:foo/bar)).expect("foo/bar").0;
        let foo_baz = store.conn.current_schema().get_entid(&kw!(:foo/baz)).expect("foo/baz").0;
        let foo_x = store.conn.current_schema().get_entid(&kw!(:foo/x)).expect("foo/x").0;

        // … and cache the others via the store.
        store.cache(&kw!(:foo/baz), CacheDirection::Both).expect("cache done");
        store.cache(&kw!(:foo/x), CacheDirection::Forward).expect("cache done");
        {
            assert!(store.is_attribute_cached_reverse(foo_bar));
            assert!(store.is_attribute_cached_forward(foo_baz));
            assert!(store.is_attribute_cached_reverse(foo_baz));
            assert!(store.is_attribute_cached_forward(foo_x));
        }

        // Add some data.
        {
            let mut in_progress = store.begin_transaction().expect("begun");

            {
                assert!(in_progress.cache.is_attribute_cached_reverse(foo_bar));
                assert!(in_progress.cache.is_attribute_cached_forward(foo_baz));
                assert!(in_progress.cache.is_attribute_cached_reverse(foo_baz));
                assert!(in_progress.cache.is_attribute_cached_forward(foo_x));

                assert!(in_progress.cache.overlay.is_attribute_cached_reverse(foo_bar));
                assert!(in_progress.cache.overlay.is_attribute_cached_forward(foo_baz));
                assert!(in_progress.cache.overlay.is_attribute_cached_reverse(foo_baz));
                assert!(in_progress.cache.overlay.is_attribute_cached_forward(foo_x));
            }

            in_progress.transact(r#"[
                {:foo/bar 15, :foo/baz false, :foo/x [1, 2, 3]}
                {:foo/bar 99, :foo/baz true}
                {:foo/bar -2, :foo/baz true}
                ]"#).expect("transact");

            // Data is in the cache.
            let first = in_progress.cache.get_entid_for_value(foo_bar, &TypedValue::Long(15)).expect("id");
            assert_eq!(in_progress.cache.get_value_for_entid(&in_progress.schema, foo_baz, first).expect("val"), &TypedValue::Boolean(false));

            // All three values for :foo/x.
            let all_three: BTreeSet<TypedValue> = in_progress.cache
                                                             .get_values_for_entid(&in_progress.schema, foo_x, first)
                                                             .expect("val")
                                                             .iter().cloned().collect();
            assert_eq!(all_three, vec![1, 2, 3].into_iter().map(TypedValue::Long).collect());

            in_progress.commit().expect("commit");
        }

        // Data is still in the cache.
        {
            let first = store.get_entid_for_value(foo_bar, &TypedValue::Long(15)).expect("id");
            let cache: SQLiteAttributeCache = store.conn.current_cache();
            assert_eq!(cache.get_value_for_entid(&store.conn.current_schema(), foo_baz, first).expect("val"), &TypedValue::Boolean(false));

            let all_three: BTreeSet<TypedValue> = cache.get_values_for_entid(&store.conn.current_schema(), foo_x, first)
                                                       .expect("val")
                                                       .iter().cloned().collect();
            assert_eq!(all_three, vec![1, 2, 3].into_iter().map(TypedValue::Long).collect());
        }

        // We can remove data and the cache reflects it, immediately and after commit.
        {
            let mut in_progress = store.begin_transaction().expect("began");
            let first = in_progress.cache.get_entid_for_value(foo_bar, &TypedValue::Long(15)).expect("id");
            in_progress.transact(format!("[[:db/retract {} :foo/x 2]]", first).as_str()).expect("transact");

            let only_two: BTreeSet<TypedValue> = in_progress.cache
                                                            .get_values_for_entid(&in_progress.schema, foo_x, first)
                                                            .expect("val")
                                                            .iter().cloned().collect();
            assert_eq!(only_two, vec![1, 3].into_iter().map(TypedValue::Long).collect());

            // Rollback: unchanged.
        }
        {
            let first = store.get_entid_for_value(foo_bar, &TypedValue::Long(15)).expect("id");
            let cache: SQLiteAttributeCache = store.conn.current_cache();
            assert_eq!(cache.get_value_for_entid(&store.conn.current_schema(), foo_baz, first).expect("val"), &TypedValue::Boolean(false));

            let all_three: BTreeSet<TypedValue> = cache.get_values_for_entid(&store.conn.current_schema(), foo_x, first)
                                                       .expect("val")
                                                       .iter().cloned().collect();
            assert_eq!(all_three, vec![1, 2, 3].into_iter().map(TypedValue::Long).collect());
        }

        // Try again, but this time commit.
        {
            let mut in_progress = store.begin_transaction().expect("began");
            let first = in_progress.cache.get_entid_for_value(foo_bar, &TypedValue::Long(15)).expect("id");
            in_progress.transact(format!("[[:db/retract {} :foo/x 2]]", first).as_str()).expect("transact");
            in_progress.commit().expect("committed");
        }
        {
            let first = store.get_entid_for_value(foo_bar, &TypedValue::Long(15)).expect("id");
            let cache: SQLiteAttributeCache = store.conn.current_cache();
            assert_eq!(cache.get_value_for_entid(&store.conn.current_schema(), foo_baz, first).expect("val"), &TypedValue::Boolean(false));

            let only_two: BTreeSet<TypedValue> = cache.get_values_for_entid(&store.conn.current_schema(), foo_x, first)
                                                      .expect("val")
                                                      .iter().cloned().collect();
            assert_eq!(only_two, vec![1, 3].into_iter().map(TypedValue::Long).collect());
        }
    }

    fn test_register_observer() {
        let mut conn = Store::open("").unwrap();

        let key = "Test Observer".to_string();
        let tx_observer = TxObserver::new(BTreeSet::new(), move |_obs_key, _batch| {});

        conn.register_observer(key.clone(), Arc::new(tx_observer));
        assert!(conn.is_registered_as_observer(&key));
    }

    #[test]
    fn test_deregister_observer() {
        let mut conn = Store::open("").unwrap();

        let key = "Test Observer".to_string();

        let tx_observer = TxObserver::new(BTreeSet::new(), move |_obs_key, _batch| {});

        conn.register_observer(key.clone(), Arc::new(tx_observer));
        assert!(conn.is_registered_as_observer(&key));

        conn.unregister_observer(&key);

        assert!(!conn.is_registered_as_observer(&key));
    }

    fn add_schema(conn: &mut Store) {
        // transact some schema
        let mut in_progress = conn.begin_transaction().expect("expected in progress");
        in_progress.ensure_vocabulary(&Definition {
            name: kw!(:todo/items),
            version: 1,
            attributes: vec![
                (kw!(:todo/uuid),
                AttributeBuilder::new()
                    .value_type(ValueType::Uuid)
                    .multival(false)
                    .unique(Unique::Value)
                    .index(true)
                    .build()),
                (kw!(:todo/name),
                AttributeBuilder::new()
                    .value_type(ValueType::String)
                    .multival(false)
                    .fulltext(true)
                    .build()),
                (kw!(:todo/completion_date),
                AttributeBuilder::new()
                    .value_type(ValueType::Instant)
                    .multival(false)
                    .build()),
                (kw!(:label/name),
                AttributeBuilder::new()
                    .value_type(ValueType::String)
                    .multival(false)
                    .unique(Unique::Value)
                    .fulltext(true)
                    .index(true)
                    .build()),
                (kw!(:label/color),
                AttributeBuilder::new()
                    .value_type(ValueType::String)
                    .multival(false)
                    .build()),
            ],
        }).expect("expected vocubulary");
        in_progress.commit().expect("Expected vocabulary committed");
    }

    #[derive(Default, Debug)]
    struct ObserverOutput {
        txids: Vec<i64>,
        changes: Vec<BTreeSet<i64>>,
        called_key: Option<String>,
    }

    #[test]
    fn test_observer_notified_on_registered_change() {
        let mut conn = Store::open("").unwrap();
        add_schema(&mut conn);

        let name_entid: Entid = conn.conn().current_schema().get_entid(&kw!(:todo/name)).expect("entid to exist for name").into();
        let date_entid: Entid = conn.conn().current_schema().get_entid(&kw!(:todo/completion_date)).expect("entid to exist for completion_date").into();
        let mut registered_attrs = BTreeSet::new();
        registered_attrs.insert(name_entid.clone());
        registered_attrs.insert(date_entid.clone());

        let key = "Test Observing".to_string();

        let output = Arc::new(Mutex::new(ObserverOutput::default()));

        let mut_output = Arc::downgrade(&output);
        let (tx, rx): (mpsc::Sender<()>, mpsc::Receiver<()>) = mpsc::channel();
        // because the TxObserver is in an Arc and is therefore Sync, we have to wrap the Sender in a Mutex to also
        // make it Sync.
        let thread_tx = Mutex::new(tx);
        let tx_observer = Arc::new(TxObserver::new(registered_attrs, move |obs_key, batch| {
            if let Some(out) = mut_output.upgrade() {
                let mut o = out.lock().unwrap();
                o.called_key = Some(obs_key.to_string());
                for (tx_id, changes) in batch.into_iter() {
                    o.txids.push(*tx_id);
                    o.changes.push(changes.clone());
                }
                o.txids.sort();
            }
            thread_tx.lock().unwrap().send(()).unwrap();
        }));

        conn.register_observer(key.clone(), Arc::clone(&tx_observer));
        assert!(conn.is_registered_as_observer(&key));

        let mut tx_ids = Vec::new();
        let mut changesets = Vec::new();
        let uuid_entid: Entid = conn.current_schema().get_entid(&kw!(:todo/uuid)).expect("entid to exist for name").into();
        {
            let mut in_progress = conn.begin_transaction().expect("expected transaction");
            for i in 0..3 {
                let mut changeset = BTreeSet::new();
                let name = format!("todo{}", i);
                let uuid = Uuid::new_v4();
                let mut builder = in_progress.builder().describe_tempid(&name);
                builder.add_kw(&kw!(:todo/uuid), TypedValue::Uuid(uuid)).expect("Expected added uuid");
                changeset.insert(uuid_entid.clone());
                builder.add_kw(&kw!(:todo/name), TypedValue::typed_string(&name)).expect("Expected added name");
                changeset.insert(name_entid.clone());
                if i % 2 == 0 {
                    builder.add_kw(&kw!(:todo/completion_date), TypedValue::current_instant()).expect("Expected added date");
                    changeset.insert(date_entid.clone());
                }
                let (ip, r) = builder.transact();
                let report = r.expect("expected a report");
                tx_ids.push(report.tx_id.clone());
                changesets.push(changeset);
                in_progress = ip;
            }
            let mut builder = in_progress.builder().describe_tempid("Label");
            builder.add_kw(&kw!(:label/name), TypedValue::typed_string("Label 1")).expect("Expected added name");
            builder.add_kw(&kw!(:label/color), TypedValue::typed_string("blue")).expect("Expected added color");
            builder.commit().expect("expect transaction to occur");
        }

        let delay = Duration::from_millis(100);
        let _ = rx.recv_timeout(delay);

        let out = Arc::try_unwrap(output).expect("unwrapped");
        let o = out.into_inner().expect("Expected an Output");
        assert_eq!(o.called_key, Some(key.clone()));
        assert_eq!(o.txids, tx_ids);
        assert_eq!(o.changes, changesets);
    }

    #[test]
    fn test_observer_not_notified_on_unregistered_change() {
        let mut conn = Store::open("").unwrap();
        add_schema(&mut conn);

        let name_entid: Entid = conn.conn().current_schema().get_entid(&kw!(:todo/name)).expect("entid to exist for name").into();
        let date_entid: Entid = conn.conn().current_schema().get_entid(&kw!(:todo/completion_date)).expect("entid to exist for completion_date").into();
        let mut registered_attrs = BTreeSet::new();
        registered_attrs.insert(name_entid.clone());
        registered_attrs.insert(date_entid.clone());

        let key = "Test Observing".to_string();

        let output = Arc::new(Mutex::new(ObserverOutput::default()));

        let mut_output = Arc::downgrade(&output);
        let (tx, rx): (mpsc::Sender<()>, mpsc::Receiver<()>) = mpsc::channel();
        let thread_tx = Mutex::new(tx);
        let tx_observer = Arc::new(TxObserver::new(registered_attrs, move |obs_key, batch| {
            if let Some(out) = mut_output.upgrade() {
                let mut o = out.lock().unwrap();
                o.called_key = Some(obs_key.to_string());
                for (tx_id, changes) in batch.into_iter() {
                    o.txids.push(*tx_id);
                    o.changes.push(changes.clone());
                }
                o.txids.sort();
            }
            thread_tx.lock().unwrap().send(()).unwrap();
        }));

        conn.register_observer(key.clone(), Arc::clone(&tx_observer));
        assert!(conn.is_registered_as_observer(&key));

        let tx_ids = Vec::<Entid>::new();
        let changesets = Vec::<BTreeSet<Entid>>::new();
        {
            let mut in_progress = conn.begin_transaction().expect("expected transaction");
            for i in 0..3 {
                let name = format!("label{}", i);
                let mut builder = in_progress.builder().describe_tempid(&name);
                builder.add_kw(&kw!(:label/name), TypedValue::typed_string(&name)).expect("Expected added name");
                builder.add_kw(&kw!(:label/color), TypedValue::typed_string("blue")).expect("Expected added color");
                let (ip, _) = builder.transact();
                in_progress = ip;
            }
        }

        let delay = Duration::from_millis(100);
        let _ = rx.recv_timeout(delay);

        let out = Arc::try_unwrap(output).expect("unwrapped");
        let o = out.into_inner().expect("Expected an Output");
        assert_eq!(o.called_key, None);
        assert_eq!(o.txids, tx_ids);
        assert_eq!(o.changes, changesets);
    }
}
