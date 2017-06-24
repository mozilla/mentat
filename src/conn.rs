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

use std::rc::Rc;
use std::sync::{Arc, Mutex};

use rusqlite;
use rusqlite::{
    TransactionBehavior,
};

use edn;

use mentat_core::{
    Schema,
};

use mentat_db::db;
use mentat_db::{
    transact,
    PartitionMap,
    TxReport,
};

use mentat_tx;

use mentat_tx_parser;

use errors::*;
use query::{
    q_once,
    QueryInputs,
    QueryResults,
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
}

impl Metadata {
    // Intentionally not public.
    fn new(generation: u64, partition_map: PartitionMap, schema: Arc<Schema>) -> Metadata {
        Metadata {
            generation: generation,
            partition_map: partition_map,
            schema: schema,
        }
    }
}

/// A mutable, safe reference to the current Mentat store.
pub struct Conn {
    /// `Mutex` since all reads and writes need to be exclusive.  Internally, owned data for the
    /// volatile parts (generation and partition map), and `Arc` for the infrequently changing parts
    /// (schema) that we want to share across threads.  A consuming thread may use a shared
    /// reference after the `Conn`'s `Metadata` has moved on.
    ///
    /// The motivating case is multiple query threads taking references to the current schema to
    /// perform long-running queries while a single writer thread moves the metadata -- partition
    /// map and schema -- forward.
    metadata: Mutex<Metadata>,

    // TODO: maintain set of change listeners or handles to transaction report queues. #298.

    // TODO: maintain cache of query plans that could be shared across threads and invalidated when
    // the schema changes. #315.
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
    last_report: Option<Rc<TxReport>>,
}

impl<'a, 'c> InProgress<'a, 'c> {
    pub fn transact_entities<I>(mut self, entities: I) -> Result<InProgress<'a, 'c>> where I: IntoIterator<Item=mentat_tx::entities::Entity> {
        let (report, next_partition_map, next_schema) = transact(&self.transaction, self.partition_map, &self.schema, &self.schema, entities)?;
        self.partition_map = next_partition_map;
        if let Some(schema) = next_schema {
            self.schema = schema;
        }
        self.last_report = Some(Rc::new(report));
        Ok(self)
    }

    /// Query the Mentat store, using the given connection and the current metadata.
    pub fn q_once<T>(&self,
                     query: &str,
                     inputs: T) -> Result<QueryResults>
        where T: Into<Option<QueryInputs>>
        {

        q_once(&*(self.transaction),
               &self.schema,
               query,
               inputs)
    }

    pub fn transact(self, transaction: &str) -> Result<InProgress<'a, 'c>> {
        let assertion_vector = edn::parse::value(transaction)?;
        let entities = mentat_tx_parser::Tx::parse(&assertion_vector)?;
        self.transact_entities(entities)
    }

    pub fn last_report(&self) -> Option<Rc<TxReport>> {
        self.last_report.clone()
    }

    pub fn rollback(mut self) -> Result<()> {
        self.last_report = None;
        self.transaction.rollback().map_err(|e| e.into())
    }

    pub fn commit(self) -> Result<Option<Rc<TxReport>>> {
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
        if self.schema != *(metadata.schema) {
            metadata.schema = Arc::new(self.schema);
        }

        Ok(self.last_report)
    }
}

impl Conn {
    // Intentionally not public.
    fn new(partition_map: PartitionMap, schema: Schema) -> Conn {
        Conn {
            metadata: Mutex::new(Metadata::new(0, partition_map, Arc::new(schema)))
        }
    }

    pub fn connect(sqlite: &mut rusqlite::Connection) -> Result<Conn> {
        let db = db::ensure_current_version(sqlite)
            .chain_err(|| "Unable to initialize Mentat store")?;
        Ok(Conn::new(db.partition_map, db.schema))
    }

    /// Yield the current `Schema` instance.
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

    /// Query the Mentat store, using the given connection and the current metadata.
    pub fn q_once<T>(&self,
                     sqlite: &rusqlite::Connection,
                     query: &str,
                     inputs: T) -> Result<QueryResults>
        where T: Into<Option<QueryInputs>>
        {

        q_once(sqlite,
               &*self.current_schema(),
               query,
               inputs)
    }

    pub fn begin_transaction<'m, 'conn>(&'m mut self, sqlite: &'conn mut rusqlite::Connection) -> Result<InProgress<'m, 'conn>> {
        let tx = sqlite.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let (current_generation, current_partition_map, current_schema) =
        {
            // The mutex is taken during this block.
            let ref current: Metadata = *self.metadata.lock().unwrap();
            (current.generation,
             // Expensive, but the partition map is updated after every committed transaction.
             current.partition_map.clone(),
             // Cheap.
             current.schema.clone())
        };

        Ok(InProgress {
            mutex: &self.metadata,
            transaction: tx,
            generation: current_generation,
            partition_map: current_partition_map,
            schema: (*current_schema).clone(),
            last_report: None,
        })
    }

    /// Transact entities against the Mentat store, using the given connection and the current
    /// metadata.
    pub fn transact(&mut self,
                    sqlite: &mut rusqlite::Connection,
                    transaction: &str) -> Result<TxReport> {

        let assertion_vector = edn::parse::value(transaction)?;
        let entities = mentat_tx_parser::Tx::parse(&assertion_vector)?;

        // We're about to write, so go straight ahead and get an IMMEDIATE transaction.
        let tx = sqlite.transaction_with_behavior(TransactionBehavior::Immediate)?;

        let (current_generation, current_partition_map, current_schema) =
        {
            // The mutex is taken during this block.
            let ref current: Metadata = *self.metadata.lock().unwrap();
            (current.generation,
             // Expensive, but the partition map is updated after every committed transaction.
             current.partition_map.clone(),
             // Cheap.
             current.schema.clone())
        };

        // The transaction is processed while the mutex is not held.
        let (report, next_partition_map, next_schema) = transact(&tx, current_partition_map, &*current_schema, &*current_schema, entities)?;

        {
            // The mutex is taken during this block.
            let mut metadata = self.metadata.lock().unwrap();

            if current_generation != metadata.generation {
                // Somebody else wrote!
                // Retrying is tracked by https://github.com/mozilla/mentat/issues/357.
                // This should not occur -- an attempt to take a competing IMMEDIATE transaction
                // will fail with `SQLITE_BUSY`, causing this function to abort.
                bail!("Lost the transact() race!");
            }

            // Commit the SQLite transaction while we hold the mutex.
            tx.commit()?;

            metadata.generation += 1;
            metadata.partition_map = next_partition_map;
            if let Some(next_schema) = next_schema {
                metadata.schema = Arc::new(next_schema);
            }
        }

        Ok(report)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    extern crate mentat_parser_utils;
    use mentat_core::Entid;

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

    #[test]
    fn test_compound_transact() {
        let mut sqlite = db::new_connection("").unwrap();
        let mut conn = Conn::connect(&mut sqlite).unwrap();
        let t = "[[:db/add \"one\" :db/ident :a/keyword1] \
                  [:db/add \"two\" :db/ident :a/keyword2]]";

        // This can refer to `t`, 'cos they occur in separate txes.
        let t2 = "[{:db.schema/attribute \"three\", :db/ident :a/keyword1}]";

        let in_progress = conn.begin_transaction(&mut sqlite).expect("begun successfully");
        let in_progress = in_progress.transact(t).expect("transacted successfully");
        let one = in_progress.last_report().unwrap().tempids.get("one").cloned();

        let report = in_progress.transact(t2)
                                .expect("t2 succeeded")
                                .commit()
                                .expect("commit succeeded");
        let mut three = report.unwrap().tempids.get("three").cloned();
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
}
