// Copyright 2016 Mozilla
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

use std::collections::BTreeMap;
use std::cmp::Ord;
use std::fmt::Debug;

use rusqlite;

use errors::*;
use mentat_core::{
    HasSchema,
    KnownEntid,
    NamespacedKeyword,
    Schema,
    TypedValue,
};

use query::{
    q_once,
    IntoResult,
    QueryInputs,
    Variable,
};

pub enum CacheAction {
    Add,
    Remove,
}

pub trait ValueProvider<K, V>: Sized {
    fn values_for_attribute<'sqlite, 'schema>(&mut self, sqlite: &'sqlite rusqlite::Connection, schema: &'schema Schema, attribute: &K) -> Result<V>;
}

pub trait Cacheable {
    type Key;
    type Value;
    type ValueProvider;

    fn new(value_provider: Self::ValueProvider) -> Self;
    fn contains_key(&self, key: &Self::Key) -> bool;
    fn add<'sqlite, 'schema>(&mut self,
                             sqlite: &'sqlite rusqlite::Connection,
                             schema: &'schema Schema,
                             key: Self::Key) -> Result<()>;
    fn remove(&mut self, key: &Self::Key) -> Result<Self::Value>;
    fn get(&mut self, key: &Self::Key) -> Result<&Self::Value>;
}

pub struct EagerCache<K, V, VP> where K: Ord, VP: ValueProvider<K, V> {
    cache: BTreeMap<K, V>,
    value_provider: VP,
}

impl<K, V, VP> Cacheable for EagerCache<K, V, VP> where K: Ord + Clone + Debug, V: Clone, VP: ValueProvider<K, V> {
    type Key = K;
    type Value = V;
    type ValueProvider = VP;

    fn new(value_provider: Self::ValueProvider) -> Self {
        EagerCache {
            cache: BTreeMap::new(),
            value_provider,
        }
    }

    fn contains_key(&self, key: &Self::Key) -> bool {
        self.cache.contains_key(key)
    }

    fn add<'sqlite, 'schema>(&mut self,
                                            sqlite: &'sqlite rusqlite::Connection,
                                            schema: &'schema Schema,
                                            key: Self::Key) -> Result<()> {
        // fetch results and add to cache
        let values = self.value_provider.values_for_attribute(sqlite, schema, &key)?;
        self.cache.insert(key.clone(), values);
        Ok(())
    }

    fn remove(&mut self, key: &Self::Key) -> Result<Self::Value> {
        let r = self.cache.remove(key).ok_or_else(||ErrorKind::CacheMiss(format!("{:?}", key)))?;
        Ok(r)
    }

    fn get(&mut self, key: &Self::Key) -> Result<&Self::Value> {
        Ok(self.cache.get(&key).ok_or_else(|| ErrorKind::CacheMiss(format!("{:?}", key)))?)
    }
}

pub struct AttributeCache {
    cache: EagerCache<NamespacedKeyword, BTreeMap<TypedValue, TypedValue>, AttributeValueProvider>,   // values keyed by attribute
}

impl AttributeCache {

    pub fn new() -> Self {
        AttributeCache {
            cache: EagerCache::new(AttributeValueProvider::default()),
        }
    }

    fn contains_key(&self, key: &NamespacedKeyword) -> bool {
        self.cache.contains_key(key)
    }

    pub fn add_to_cache<'sqlite, 'schema>(&mut self, sqlite: &'sqlite rusqlite::Connection, schema: &'schema Schema, key: NamespacedKeyword) -> Result<()> {
        self.cache.add( sqlite, schema, key)
    }

    pub fn remove_from_cache(&mut self, key: &NamespacedKeyword) -> Result<BTreeMap<TypedValue, TypedValue>> {
        self.cache.remove(key)
    }

    pub fn get<'sqlite, 'schema>(&mut self, key: &NamespacedKeyword) -> Result<&BTreeMap<TypedValue, TypedValue>> {
        self.cache.get( key)
    }

    pub fn get_for_entid(&mut self, key: NamespacedKeyword, entid: KnownEntid) -> Result<Option<&TypedValue>> {
        let entity: TypedValue = entid.into();
        let map = self.cache.get(&key)?;
        Ok(map.get(&entity))
    }
}

#[derive(Default)]
struct AttributeValueProvider{}

impl ValueProvider<NamespacedKeyword, BTreeMap<TypedValue, TypedValue>> for AttributeValueProvider {
    fn values_for_attribute<'sqlite, 'schema>(&mut self, sqlite: &'sqlite rusqlite::Connection, schema: &'schema Schema, attribute: &NamespacedKeyword) -> Result<BTreeMap<TypedValue, TypedValue>> {
        let entid = schema.get_entid(&attribute).ok_or_else(|| ErrorKind::UnknownAttribute(format!("{:?}", attribute)))?;
        Ok(q_once(sqlite,
                  schema,
                  r#"[:find ?entity ?value
                                          :in ?attribute
                                          :where [?entity ?attribute ?value]]"#,
                  QueryInputs::with_value_sequence(vec![(Variable::from_valid_name("?attribute"), TypedValue::Long(entid.0))]))
            .into_rel_result()?
            .iter()
            .fold(BTreeMap::new(), |mut map, row| {
                map.entry(row[0].clone()).or_insert(row[1].clone());
                map
            }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mentat_db::db;
    use conn::Conn;
    use mentat_db::TxReport;

    #[test]
    fn test_add_to_cache() {
        let mut sqlite = db::new_connection("").unwrap();
        let mut conn = Conn::connect(&mut sqlite).unwrap();
        let report = conn.transact(&mut sqlite, r#"[
            {  :db/ident       :foo/bar
               :db/valueType   :db.type/long },
            {  :db/ident       :foo/baz
               :db/valueType   :db.type/boolean }]"#).expect("transaction expected to succeed");
        let report = conn.transact(&mut sqlite, r#"[
            {  :foo/bar        100
               :foo/baz        false },
            {  :foo/bar        200
               :foo/baz        true }]"#).expect("transaction expected to succeed");
        let schema = conn.current_schema();
        let kw = NamespacedKeyword::new("foo", "bar");
        conn.attribute_cache().add_to_cache(&sqlite, &schema, kw.clone(), CacheType::Eager ).expect("No errors on add to cache");
        assert!(conn.attribute_cache().is_cached(&kw));
    }

    #[test]
    fn test_add_to_cache_wrong_attribute() {
        let mut sqlite = db::new_connection("").unwrap();
        let mut conn = Conn::connect(&mut sqlite).unwrap();
        let report = conn.transact(&mut sqlite, r#"[
            {  :db/ident       :foo/bar
               :db/valueType   :db.type/long },
            {  :db/ident       :foo/baz
               :db/valueType   :db.type/boolean }]"#).expect("transaction expected to succeed");
        let report = conn.transact(&mut sqlite, r#"[
            {  :foo/bar        100
               :foo/baz        false },
            {  :foo/bar        200
               :foo/baz        true }]"#).expect("transaction expected to succeed");
        let schema = conn.current_schema();
        let kw = NamespacedKeyword::new("foo", "bat");

        let res = conn.attribute_cache().add_to_cache(&sqlite, &schema,kw.clone(), CacheType::Eager);
        match res.unwrap_err() {
            Error(ErrorKind::UnknownAttribute(msg), _) => assert_eq!(msg, kw.to_string()),
            x => panic!("expected UnknownAttribute error, got {:?}", x),
        }
    }

    #[test]
    fn test_add_attribute_already_in_cache() {
        let mut sqlite = db::new_connection("").unwrap();
        let mut conn = Conn::connect(&mut sqlite).unwrap();
        let report = conn.transact(&mut sqlite, r#"[
            {  :db/ident       :foo/bar
               :db/valueType   :db.type/long },
            {  :db/ident       :foo/baz
               :db/valueType   :db.type/boolean }]"#).expect("transaction expected to succeed");
        let report = conn.transact(&mut sqlite, r#"[
            {  :foo/bar        100
               :foo/baz        false },
            {  :foo/bar        200
               :foo/baz        true }]"#).expect("transaction expected to succeed");
        let schema = conn.current_schema();

        let kw = NamespacedKeyword::new("foo", "bar");

        conn.attribute_cache().add_to_cache(&mut sqlite, &schema,kw.clone(), CacheType::Eager).expect("No errors on add to cache");
        assert!(conn.attribute_cache().is_cached(&kw));
        conn.attribute_cache().add_to_cache(&mut sqlite, &schema,kw.clone(), CacheType::Lazy).expect("No errors on add to cache");
        assert!(conn.attribute_cache().is_cached(&kw));
    }

    #[test]
    fn test_remove_from_cache() {
        let mut sqlite = db::new_connection("").unwrap();
        let mut conn = Conn::connect(&mut sqlite).unwrap();
        let report = conn.transact(&mut sqlite, r#"[
            {  :db/ident       :foo/bar
               :db/valueType   :db.type/long },
            {  :db/ident       :foo/baz
               :db/valueType   :db.type/boolean }]"#).expect("transaction expected to succeed");
        let report = conn.transact(&mut sqlite, r#"[
            {  :foo/bar        100
               :foo/baz        false },
            {  :foo/bar        200
               :foo/baz        true }]"#).expect("transaction expected to succeed");
        let schema = conn.current_schema();

        let kwr = NamespacedKeyword::new("foo", "bar");
        let kwz = NamespacedKeyword::new("foo", "baz");

        conn.attribute_cache().add_to_cache(&mut sqlite, &schema,kwr.clone(), CacheType::Eager).expect("No errors on add to cache");
        assert!(conn.attribute_cache().is_cached(&kwr));
        conn.attribute_cache().add_to_cache(&mut sqlite, &schema,kwz.clone(), CacheType::Lazy).expect("No errors on add to cache");
        assert!(conn.attribute_cache().is_cached(&kwz));

        // test that we can remove an item from cache
        conn.attribute_cache().remove_from_cache(&kwz).expect("No errors on remove from cache");
        assert!(!conn.attribute_cache().is_cached(&kwz));
    }

    #[test]
    fn test_remove_attribute_not_in_cache() {
        let mut sqlite = db::new_connection("").unwrap();
        let mut conn = Conn::connect(&mut sqlite).unwrap();
        let report = conn.transact(&mut sqlite, r#"[
            {  :db/ident       :foo/bar
               :db/valueType   :db.type/long },
            {  :db/ident       :foo/baz
               :db/valueType   :db.type/boolean }]"#).expect("transaction expected to succeed");
        let report = conn.transact(&mut sqlite, r#"[
            {  :foo/bar        100
               :foo/baz        false },
            {  :foo/bar        200
               :foo/baz        true }]"#).expect("transaction expected to succeed");
        let schema = conn.current_schema();

        let kw = NamespacedKeyword::new("foo", "baz");
        let res = conn.attribute_cache().remove_from_cache(&kw);
        match res.unwrap_err() {
            Error(ErrorKind::CacheMiss(msg), _) => assert_eq!(msg, kw.to_string()),
            x => panic!("expected CacheMiss error, got {:?}", x),
        }
    }
}


