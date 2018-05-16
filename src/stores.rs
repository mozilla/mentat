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

use std::collections::{
    BTreeMap,
};

use std::path::{
    Path,
};

use std::sync::{
    Arc,
};

use rusqlite;

use edn;

use mentat_core::{
    Entid,
    Keyword,
    StructuredMap,
    TypedValue,
    ValueRc,
};
use mentat_db::{
    TxObserver,
    TxReport,
};

use mentat_tolstoy::Syncer;

use uuid::Uuid;

use conn::{
    CacheAction,
    CacheDirection,
    Conn,
    InProgress,
    InProgressRead,
    Pullable,
    Queryable,
    Syncable
};

use errors::*;

use query::{
    PreparedResult,
    QueryExplanation,
    QueryInputs,
    QueryOutput,
};

/// A convenience wrapper around a single SQLite connection and a Conn. This is suitable
/// for applications that don't require complex connection management.
pub struct Store {
    conn: Conn,
    sqlite: rusqlite::Connection,
}

impl Store {
    /// Open a store at the supplied path, ensuring that it includes the bootstrap schema.
    pub fn open(path: &str) -> Result<Store> {
        let mut connection = ::new_connection(path)?;
        let conn = Conn::connect(&mut connection)?;
        Ok(Store {
            conn: conn,
            sqlite: connection,
        })
    }

    /// Returns a totally blank store with no bootstrap schema. Use `open` instead.
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

    pub fn transact(&mut self, transaction: &str) -> Result<TxReport> {
        let mut ip = self.begin_transaction()?;
        let report = ip.transact(transaction)?;
        ip.commit()?;
        Ok(report)
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
    pub fn fork(&mut self, sqlite: rusqlite::Connection) -> Store {
        Store {
            conn: self.conn.clone(),
            sqlite: sqlite,
        }
    }

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

    pub fn cache(&mut self, attr: &Keyword, direction: CacheDirection) -> Result<()> {
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

    fn lookup_values_for_attribute<E>(&self, entity: E, attribute: &edn::Keyword) -> Result<Vec<TypedValue>>
        where E: Into<Entid> {
        self.conn.lookup_values_for_attribute(&self.sqlite, entity.into(), attribute)
    }

    fn lookup_value_for_attribute<E>(&self, entity: E, attribute: &edn::Keyword) -> Result<Option<TypedValue>>
        where E: Into<Entid> {
        self.conn.lookup_value_for_attribute(&self.sqlite, entity.into(), attribute)
    }
}

impl Pullable for Store {
    fn pull_attributes_for_entities<E, A>(&self, entities: E, attributes: A) -> Result<BTreeMap<Entid, ValueRc<StructuredMap>>>
    where E: IntoIterator<Item=Entid>,
          A: IntoIterator<Item=Entid> {
        self.conn.pull_attributes_for_entities(&self.sqlite, entities, attributes)
    }

    fn pull_attributes_for_entity<A>(&self, entity: Entid, attributes: A) -> Result<StructuredMap>
    where A: IntoIterator<Item=Entid> {
        self.conn.pull_attributes_for_entity(&self.sqlite, entity, attributes)
    }
}

impl Syncable for Store {
    fn sync(&mut self, server_uri: &String, user_uuid: &String) -> Result<()> {
        let uuid = Uuid::parse_str(&user_uuid)?;
        Ok(Syncer::flow(&mut self.sqlite, server_uri, &uuid)?)
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
    use std::sync::{
        Mutex,
    };
    use std::time::{
        Duration,
    };

    use mentat_db::cache::{
        SQLiteAttributeCache,
    };

    use mentat_core::{
        CachedAttributes,
        HasSchema,
        TypedValue,
        ValueType,
    };

    use ::entity_builder::{
        BuildTerms,
    };

    use ::query::{
        PreparedQuery,
    };

    use ::{
        QueryInputs,
    };

    use ::vocabulary::{
        AttributeBuilder,
        Definition,
        VersionedStore,
    };

    use ::vocabulary::attribute::{
        Unique,
    };

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
        let inputs = QueryInputs::with_value_sequence(vec![(var!(?hood), TypedValue::typed_string(hood).into())]);
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
                   vec![vec![TypedValue::typed_string("Greater Duwamish")]].into());
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
        in_progress.ensure_vocabulary(&Definition::new(
            kw!(:todo/items),
            1,
            vec![
                (kw!(:todo/uuid),
                AttributeBuilder::helpful()
                    .value_type(ValueType::Uuid)
                    .multival(false)
                    .unique(Unique::Value)
                    .index(true)
                    .build()),
                (kw!(:todo/name),
                AttributeBuilder::helpful()
                    .value_type(ValueType::String)
                    .multival(false)
                    .fulltext(true)
                    .build()),
                (kw!(:todo/completion_date),
                AttributeBuilder::helpful()
                    .value_type(ValueType::Instant)
                    .multival(false)
                    .build()),
                (kw!(:label/name),
                AttributeBuilder::helpful()
                    .value_type(ValueType::String)
                    .multival(false)
                    .unique(Unique::Value)
                    .fulltext(true)
                    .index(true)
                    .build()),
                (kw!(:label/color),
                AttributeBuilder::helpful()
                    .value_type(ValueType::String)
                    .multival(false)
                    .build()),
            ],
        )).expect("expected vocubulary");
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
        let db_tx_instant_entid: Entid = conn.conn().current_schema().get_entid(&kw!(:db/txInstant)).expect("entid to exist for :db/txInstant").into();
        let uuid_entid: Entid = conn.conn().current_schema().get_entid(&kw!(:todo/uuid)).expect("entid to exist for name").into();
        {
            let mut in_progress = conn.begin_transaction().expect("expected transaction");
            for i in 0..3 {
                let mut changeset = BTreeSet::new();
                changeset.insert(db_tx_instant_entid.clone());
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
