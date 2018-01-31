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
    lookup_value,
    q_once,
    IntoResult,
    QueryInputs,
    Variable,
};

pub enum CacheType {
    Lazy,
    Eager,
}

pub enum CacheAction {
    Add,
    Remove,
}

pub trait Cacheable<K, V> {
    fn new() -> Self;
    fn is_cached(&self, key: &K) -> bool;
    fn add_to_cache<'sqlite, 'schema>(&mut self, sqlite: &'sqlite rusqlite::Connection, schema: &'schema Schema, key: K, cache_type: CacheType) -> Result<()>;
    fn remove_from_cache(&mut self, key: &K) -> Result<()>;
    fn get<'sqlite, 'schema>(&mut self, sqlite: &'sqlite rusqlite::Connection, schema: &'schema Schema, key: K) -> Result<V>;
}

pub struct AttributeCache {
    eager_cache: BTreeMap<NamespacedKeyword, BTreeMap<TypedValue, TypedValue>>,   // values keyed by attribute
    lazy_cache: BTreeMap<NamespacedKeyword, BTreeMap<TypedValue, TypedValue>>,   // values keyed by attribute
}

impl Cacheable<NamespacedKeyword,BTreeMap<TypedValue, TypedValue>> for AttributeCache {
    fn new() -> Self {
        AttributeCache {
            eager_cache: BTreeMap::new(),
            lazy_cache: BTreeMap::new(),
        }
    }
    fn is_cached(&self, key: &NamespacedKeyword) -> bool {
        self.lazy_cache.contains_key(key) || self.eager_cache.contains_key(key)
    }

    fn add_to_cache<'sqlite, 'schema>(&mut self, sqlite: &'sqlite rusqlite::Connection, schema: &'schema Schema, key: NamespacedKeyword, cache_type: CacheType) -> Result<()> {
        // check to see if already in cache, return error if so.
        if self.is_cached(&key) {
            return Ok(());
        }
        match cache_type {
            CacheType::Lazy => self.lazy_cache.insert(key.clone(), BTreeMap::new()),
            CacheType::Eager => {
                // fetch results and add to cache
                let eager_values = self.values_for_attribute(sqlite, schema, &key)?;
                self.eager_cache.insert(key.clone(), eager_values)
            },
        };
        Ok(())
    }

    fn remove_from_cache(&mut self, key: &NamespacedKeyword) -> Result<()> {
        if !self.is_cached(key) {
            bail!(ErrorKind::CacheMiss(key.to_string()))
        }
        self.eager_cache.remove(key);
        self.lazy_cache.remove(key);
        Ok(())
    }

    fn get<'sqlite, 'schema>(&mut self, sqlite: &'sqlite rusqlite::Connection, schema: &'schema Schema, key: NamespacedKeyword) -> Result<BTreeMap<TypedValue, TypedValue>> {
        if !self.is_cached(&key) {
            bail!(ErrorKind::CacheMiss(key.to_string()))
        }
        if let Some(res) = self.eager_cache.get(&key) {
            Ok(res.clone())
        } else {
            let res = self.values_for_attribute(sqlite, schema, &key)?;
            self.lazy_cache.insert(key.clone(), res.clone());
            Ok(res)
        }
    }
}

impl AttributeCache {
    fn values_for_attribute<'sqlite, 'schema>(&self, sqlite: &'sqlite rusqlite::Connection, schema: &'schema Schema, attribute: &NamespacedKeyword) -> Result<BTreeMap<TypedValue, TypedValue>> {
        let entid = schema.get_entid(&attribute).ok_or_else(|| ErrorKind::UnknownAttribute(attribute.to_string()))?;
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

    pub fn get_for_entid<'sqlite, 'schema>(&mut self, sqlite: &'sqlite rusqlite::Connection, schema: &'schema Schema, key: NamespacedKeyword, entid: KnownEntid) -> Result<TypedValue> {
        if !self.is_cached(&key) {
            bail!(ErrorKind::CacheMiss(key.to_string()))
        }
        let entity: TypedValue = entid.into();
        if let Some(res) = self.eager_cache.get(&key) {
            let r = res.get(&entity).ok_or_else(|| ErrorKind::CacheMiss(String::new()))?;
            Ok(r.clone())
        } else {
            let mut map = self.lazy_cache.entry(key.clone()).or_insert(BTreeMap::new());
            let attr_entid = schema.get_entid(&key).ok_or_else(|| ErrorKind::UnknownAttribute(key.to_string()))?;
            let res = map.entry(entity)
                        .or_insert(lookup_value(sqlite, schema, entid, attr_entid)
                            .map_err(|_e|ErrorKind::CacheMiss(key.to_string()))?
                            .ok_or_else(||ErrorKind::CacheMiss(key.to_string()))?);
            Ok(res.clone())
        }
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


