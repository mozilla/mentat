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
    Register,
    Deregister,
}

pub trait ValueProvider<K, V>: Sized {
    fn fetch_values<'sqlite, 'schema>(&mut self, sqlite: &'sqlite rusqlite::Connection, schema: &'schema Schema) -> Result<BTreeMap<K, V>>;
}

pub trait Cacheable {
    type Key;
    type Value;
    type ValueProvider;

    fn new(value_provider: Self::ValueProvider) -> Self;
    fn cache_values<'sqlite, 'schema>(&mut self,
                             sqlite: &'sqlite rusqlite::Connection,
                             schema: &'schema Schema) -> Result<()>;
    fn update_values<'sqlite, 'schema>(&mut self,
                                       sqlite: &'sqlite rusqlite::Connection,
                                       schema: &'schema Schema) -> Result<()>;
    fn get(&self, key: &Self::Key) -> Option<&Self::Value>;
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

    fn cache_values<'sqlite, 'schema>(&mut self,
                                            sqlite: &'sqlite rusqlite::Connection,
                                            schema: &'schema Schema) -> Result<()> {
        // fetch results and add to cache
        self.cache = self.value_provider.fetch_values(sqlite, schema)?;
        Ok(())
    }

    fn update_values<'sqlite, 'schema>(&mut self,
                                      sqlite: &'sqlite rusqlite::Connection,
                                      schema: &'schema Schema) -> Result<()> {
        self.cache_values(sqlite, schema)
    }

    fn get(&self, key: &Self::Key) -> Option<&Self::Value> {
        self.cache.get(&key)
    }
}

pub struct AttributeCacher {
    cache: BTreeMap<NamespacedKeyword, EagerCache<TypedValue, Vec<TypedValue>, AttributeValueProvider>>,   // values keyed by attribute
}

impl AttributeCacher {

    pub fn new() -> Self {
        AttributeCacher {
            cache: BTreeMap::new(),
        }
    }

    pub fn register_attribute<'sqlite, 'schema>(&mut self, sqlite: &'sqlite rusqlite::Connection, schema: &'schema Schema, attribute: NamespacedKeyword) -> Result<()> {
        if schema.identifies_attribute(&attribute) {
            let value_provider = AttributeValueProvider{ attribute: attribute.clone() };
            let mut cacher = EagerCache::new(value_provider);
            cacher.cache_values(sqlite, schema)?;
            self.cache.insert(attribute, cacher);
            Ok(())
        } else {
            bail!(ErrorKind::UnknownAttribute(attribute.to_string()))
        }
    }

    pub fn deregister_attribute(&mut self, attribute: &NamespacedKeyword) -> Option<BTreeMap<TypedValue, Vec<TypedValue>>> {
        self.cache.remove(attribute).map(|m| m.cache )
    }

    pub fn get(&mut self, attribute: &NamespacedKeyword) -> Option<&BTreeMap<TypedValue, Vec<TypedValue>>> {
        self.cache.get( attribute ).map(|m| &m.cache )
    }

    pub fn get_for_entid(&mut self, attribute: NamespacedKeyword, entid: KnownEntid) -> Option<&Vec<TypedValue>> {
        if let Some(c) = self.cache.get(&attribute) {
            c.get(&entid.into())
        } else { None }
    }
}

struct AttributeValueProvider {
    attribute: NamespacedKeyword,
}

impl ValueProvider<TypedValue, Vec<TypedValue>> for AttributeValueProvider {
    fn fetch_values<'sqlite, 'schema>(&mut self, sqlite: &'sqlite rusqlite::Connection, schema: &'schema Schema) -> Result<BTreeMap<TypedValue, Vec<TypedValue>>> {
        Ok(q_once(sqlite,
                  schema,
                  r#"[:find ?entity ?value
                                          :in ?attribute
                                          :where [?entity ?a        ?value]
                                                 [?a      :db/ident ?attribute]]"#,
                  QueryInputs::with_value_sequence(vec![(Variable::from_valid_name("?attribute"), self.attribute.clone().into())]))
            .into_rel_result()?
            .into_iter()
            .fold(BTreeMap::new(), |mut map, row| {
                map.entry(row[0].clone()).or_insert(vec![]).push(row[1].clone());
                map
            }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mentat_db::db;
    use conn::Conn;

    fn assert_values_present_for_attribute(attribute_cache: &mut AttributeCacher, attribute: &NamespacedKeyword, values: Vec<Vec<TypedValue>>) {
        let cached_values: Vec<Vec<TypedValue>> = attribute_cache.get(&attribute)
            .expect("Expected cached values")
            .values()
            .cloned()
            .collect();
        assert_eq!(cached_values, values);
    }

    #[test]
    fn test_add_to_cache() {
        let mut sqlite = db::new_connection("").unwrap();
        let mut conn = Conn::connect(&mut sqlite).unwrap();
        let _report = conn.transact(&mut sqlite, r#"[
            {  :db/ident       :foo/bar
               :db/valueType   :db.type/long },
            {  :db/ident       :foo/baz
               :db/valueType   :db.type/boolean }]"#).expect("transaction expected to succeed");
        let _report = conn.transact(&mut sqlite, r#"[
            {  :foo/bar        100
               :foo/baz        false },
            {  :foo/bar        200
               :foo/baz        true }]"#).expect("transaction expected to succeed");
        let schema = conn.current_schema();
        let kw = NamespacedKeyword::new("foo", "bar");
        let mut attribute_cache = AttributeCacher::new();
        attribute_cache.register_attribute(&sqlite, &schema, kw.clone() ).expect("No errors on add to cache");
        assert_values_present_for_attribute(&mut attribute_cache, &kw, vec![vec![TypedValue::Long(100)], vec![TypedValue::Long(200)]]);
    }

    #[test]
    fn test_add_to_cache_wrong_attribute() {
        let mut sqlite = db::new_connection("").unwrap();
        let mut conn = Conn::connect(&mut sqlite).unwrap();
        let _report = conn.transact(&mut sqlite, r#"[
            {  :db/ident       :foo/bar
               :db/valueType   :db.type/long },
            {  :db/ident       :foo/baz
               :db/valueType   :db.type/boolean }]"#).expect("transaction expected to succeed");
        let _report = conn.transact(&mut sqlite, r#"[
            {  :foo/bar        100
               :foo/baz        false },
            {  :foo/bar        200
               :foo/baz        true }]"#).expect("transaction expected to succeed");
        let schema = conn.current_schema();
        let kw = NamespacedKeyword::new("foo", "bat");

        let mut attribute_cache = AttributeCacher::new();
        let res = attribute_cache.register_attribute(&sqlite, &schema,kw.clone());
        match res.unwrap_err() {
            Error(ErrorKind::UnknownAttribute(msg), _) => assert_eq!(msg, kw.to_string()),
            x => panic!("expected UnknownAttribute error, got {:?}", x),
        }
    }

    #[test]
    fn test_add_attribute_already_in_cache() {
        let mut sqlite = db::new_connection("").unwrap();
        let mut conn = Conn::connect(&mut sqlite).unwrap();
        let _report = conn.transact(&mut sqlite, r#"[
            {  :db/ident       :foo/bar
               :db/valueType   :db.type/long },
            {  :db/ident       :foo/baz
               :db/valueType   :db.type/boolean }]"#).expect("transaction expected to succeed");
        let _report = conn.transact(&mut sqlite, r#"[
            {  :foo/bar        100
               :foo/baz        false },
            {  :foo/bar        200
               :foo/baz        true }]"#).expect("transaction expected to succeed");
        let schema = conn.current_schema();

        let kw = NamespacedKeyword::new("foo", "bar");
        let mut attribute_cache = AttributeCacher::new();

        attribute_cache.register_attribute(&mut sqlite, &schema,kw.clone()).expect("No errors on add to cache");
        assert_values_present_for_attribute(&mut attribute_cache, &kw, vec![vec![TypedValue::Long(100)], vec![TypedValue::Long(200)]]);
        attribute_cache.register_attribute(&mut sqlite, &schema,kw.clone()).expect("No errors on add to cache");
        assert_values_present_for_attribute(&mut attribute_cache, &kw, vec![vec![TypedValue::Long(100)], vec![TypedValue::Long(200)]]);
    }

    #[test]
    fn test_remove_from_cache() {
        let mut sqlite = db::new_connection("").unwrap();
        let mut conn = Conn::connect(&mut sqlite).unwrap();
        let _report = conn.transact(&mut sqlite, r#"[
            {  :db/ident       :foo/bar
               :db/valueType   :db.type/long },
            {  :db/ident       :foo/baz
               :db/valueType   :db.type/boolean }]"#).expect("transaction expected to succeed");
        let _report = conn.transact(&mut sqlite, r#"[
            {  :foo/bar        100
               :foo/baz        false },
            {  :foo/bar        200
               :foo/baz        true }]"#).expect("transaction expected to succeed");
        let schema = conn.current_schema();

        let kwr = NamespacedKeyword::new("foo", "bar");
        let kwz = NamespacedKeyword::new("foo", "baz");

        let mut attribute_cache = AttributeCacher::new();

        attribute_cache.register_attribute(&mut sqlite, &schema,kwr.clone()).expect("No errors on add to cache");
        assert_values_present_for_attribute(&mut attribute_cache, &kwr, vec![vec![TypedValue::Long(100)], vec![TypedValue::Long(200)]]);
        attribute_cache.register_attribute(&mut sqlite, &schema,kwz.clone()).expect("No errors on add to cache");
        assert_values_present_for_attribute(&mut attribute_cache, &kwz, vec![vec![TypedValue::Boolean(false)], vec![TypedValue::Boolean(true)]]);

        // test that we can remove an item from cache
        attribute_cache.deregister_attribute(&kwz).expect("No errors on remove from cache");
        assert_eq!(attribute_cache.get(&kwz), None);
    }

    #[test]
    fn test_remove_attribute_not_in_cache() {
        let mut sqlite = db::new_connection("").unwrap();
        let mut conn = Conn::connect(&mut sqlite).unwrap();
        let _report = conn.transact(&mut sqlite, r#"[
            {  :db/ident       :foo/bar
               :db/valueType   :db.type/long },
            {  :db/ident       :foo/baz
               :db/valueType   :db.type/boolean }]"#).expect("transaction expected to succeed");
        let _report = conn.transact(&mut sqlite, r#"[
            {  :foo/bar        100
               :foo/baz        false },
            {  :foo/bar        200
               :foo/baz        true }]"#).expect("transaction expected to succeed");
        let mut attribute_cache = AttributeCacher::new();

        let kw = NamespacedKeyword::new("foo", "baz");
        assert_eq!(None, attribute_cache.deregister_attribute(&kw));
    }
}


