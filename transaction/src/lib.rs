// Copyright 2018 Mozilla
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

extern crate failure;
extern crate rusqlite;

extern crate edn;
extern crate public_traits;
#[macro_use]
extern crate core_traits;
extern crate db_traits;
extern crate mentat_core;
extern crate mentat_db;
extern crate mentat_query_algebrizer;
extern crate mentat_query_projector;
extern crate mentat_query_pull;
extern crate mentat_sql;

use std::sync::{
    Arc,
    Mutex,
};

use std::io::Read;

use std::borrow::Borrow;

use std::collections::BTreeMap;

use std::fs::{
    File,
};

use std::path::{
    Path,
};

use edn::{
    InternSet,
    Keyword,
};
use edn::entities::{
    TempId,
    OpType,
};

use core_traits::{
    Attribute,
    Entid,
    KnownEntid,
    StructuredMap,
    TypedValue,
    ValueType,
};

use public_traits::errors::{
    Result,
    MentatError,
};

use mentat_core::{
    HasSchema,
    Schema,
    TxReport,
    ValueRc,
};

use mentat_query_pull::{
    pull_attributes_for_entities,
    pull_attributes_for_entity,
};

use mentat_db::{
    transact,
    transact_terms,
    InProgressObserverTransactWatcher,
    PartitionMap,
    TransactableValue,
    TransactWatcher,
    TxObservationService,
};

use mentat_db::internal_types::TermWithTempIds;

use mentat_db::cache::{
    InProgressCacheTransactWatcher,
    InProgressSQLiteAttributeCache,
};

pub mod entity_builder;
pub mod metadata;
pub mod query;

pub use entity_builder::{
    InProgressBuilder,
    TermBuilder,
};

pub use metadata::{
    Metadata,
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

/// Represents an in-progress, not yet committed, set of changes to the store.
/// Call `commit` to commit your changes, or `rollback` to discard them.
/// A transaction is held open until you do so.
/// Your changes will be implicitly dropped along with this struct.
pub struct InProgress<'a, 'c> {
    pub transaction: rusqlite::Transaction<'c>,
    pub mutex: &'a Mutex<Metadata>,
    pub generation: u64,
    pub partition_map: PartitionMap,
    pub schema: Schema,
    pub cache: InProgressSQLiteAttributeCache,
    pub use_caching: bool,
    pub tx_observer: &'a Mutex<TxObservationService>,
    pub tx_observer_watcher: InProgressObserverTransactWatcher,
}

/// Represents an in-progress set of reads to the store. Just like `InProgress`,
/// which is read-write, but only allows for reads.
pub struct InProgressRead<'a, 'c> {
    pub in_progress: InProgress<'a, 'c>,
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

    pub fn savepoint(&self, name: &str) -> Result<()> {
        self.transaction.execute(&format!("SAVEPOINT {}", name), &[])?;
        Ok(())
    }

    pub fn rollback_savepoint(&self, name: &str) -> Result<()> {
        self.transaction.execute(&format!("ROLLBACK TO {}", name), &[])?;
        Ok(())
    }

    pub fn release_savepoint(&self, name: &str) -> Result<()> {
        self.transaction.execute(&format!("RELEASE {}", name), &[])?;
        Ok(())
    }
}

impl<'a, 'c> InProgressRead<'a, 'c> {
    pub fn last_tx_id(&self) -> Entid {
        self.in_progress.last_tx_id()
    }
}


impl<'a, 'c> Queryable for InProgressRead<'a, 'c> {
    fn q_once<T>(&self, query: &str, inputs: T) -> Result<QueryOutput>
        where T: Into<Option<QueryInputs>> {
        self.in_progress.q_once(query, inputs)
    }

    fn q_prepare<T>(&self, query: &str, inputs: T) -> PreparedResult
        where T: Into<Option<QueryInputs>> {
        self.in_progress.q_prepare(query, inputs)
    }

    fn q_explain<T>(&self, query: &str, inputs: T) -> Result<QueryExplanation>
        where T: Into<Option<QueryInputs>> {
        self.in_progress.q_explain(query, inputs)
    }

    fn lookup_values_for_attribute<E>(&self, entity: E, attribute: &edn::Keyword) -> Result<Vec<TypedValue>>
        where E: Into<Entid> {
        self.in_progress.lookup_values_for_attribute(entity, attribute)
    }

    fn lookup_value_for_attribute<E>(&self, entity: E, attribute: &edn::Keyword) -> Result<Option<TypedValue>>
        where E: Into<Entid> {
        self.in_progress.lookup_value_for_attribute(entity, attribute)
    }
}

impl<'a, 'c> Pullable for InProgressRead<'a, 'c> {
    fn pull_attributes_for_entities<E, A>(&self, entities: E, attributes: A) -> Result<BTreeMap<Entid, ValueRc<StructuredMap>>>
    where E: IntoIterator<Item=Entid>,
          A: IntoIterator<Item=Entid> {
        self.in_progress.pull_attributes_for_entities(entities, attributes)
    }

    fn pull_attributes_for_entity<A>(&self, entity: Entid, attributes: A) -> Result<StructuredMap>
    where A: IntoIterator<Item=Entid> {
        self.in_progress.pull_attributes_for_entity(entity, attributes)
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
        self.in_progress.entid_for_type(t)
    }

    fn get_ident<T>(&self, x: T) -> Option<&Keyword> where T: Into<Entid> {
        self.in_progress.get_ident(x)
    }

    fn get_entid(&self, x: &Keyword) -> Option<KnownEntid> {
        self.in_progress.get_entid(x)
    }

    fn attribute_for_entid<T>(&self, x: T) -> Option<&Attribute> where T: Into<Entid> {
        self.in_progress.attribute_for_entid(x)
    }

    fn attribute_for_ident(&self, ident: &Keyword) -> Option<(&Attribute, KnownEntid)> {
        self.in_progress.attribute_for_ident(ident)
    }

    /// Return true if the provided entid identifies an attribute in this schema.
    fn is_attribute<T>(&self, x: T) -> bool where T: Into<Entid> {
        self.in_progress.is_attribute(x)
    }

    /// Return true if the provided ident identifies an attribute in this schema.
    fn identifies_attribute(&self, x: &Keyword) -> bool {
        self.in_progress.identifies_attribute(x)
    }

    fn component_attributes(&self) -> &[Entid] {
        self.in_progress.component_attributes()
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

    fn done(&mut self, t: &Entid, schema: &Schema) -> ::db_traits::errors::Result<()> {
        self.cache_watcher.done(t, schema)?;
        self.observer_watcher.done(t, schema)?;
        self.tx_id = Some(t.clone());
        Ok(())
    }
}
