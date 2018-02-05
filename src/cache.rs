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

use mentat_core::{
    Entid,
    KnownEntid,
    TypedValue,
};

use mentat_db::cache::{
    AttributeValueProvider,
    Cacheable,
    EagerCache,
};

use errors::{
    Result,
};

pub enum CacheAction {
    Register,
    Deregister,
}

pub struct AttributeCacher {
    cache: BTreeMap<Entid, EagerCache<Entid, Vec<TypedValue>, AttributeValueProvider>>,   // values keyed by attribute
}

impl AttributeCacher {

    pub fn new() -> Self {
        AttributeCacher {
            cache: BTreeMap::new(),
        }
    }

    pub fn register_attribute<'sqlite>(&mut self, sqlite: &'sqlite rusqlite::Connection, attribute: KnownEntid) -> Result<()> {
        let value_provider = AttributeValueProvider{ attribute: attribute.clone() };
        let mut cacher = EagerCache::new(value_provider);
        cacher.cache_values(sqlite)?;
        self.cache.insert(attribute.0, cacher);
        Ok(())
    }

    pub fn deregister_attribute(&mut self, attribute: &KnownEntid) -> Option<BTreeMap<Entid, Vec<TypedValue>>> {
        self.cache.remove(&attribute.0).map(|m| m.cache )
    }

    pub fn get(&mut self, attribute: &KnownEntid) -> Option<&BTreeMap<Entid, Vec<TypedValue>>> {
        self.cache.get( &attribute.0 ).map(|m| &m.cache )
    }

    pub fn get_for_entid(&mut self, attribute: &KnownEntid, entid: &KnownEntid) -> Option<&Vec<TypedValue>> {
        if let Some(c) = self.cache.get(&attribute.0) {
            c.get(&entid.0)
        } else { None }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mentat_core::HasSchema;
    use mentat_db::db;
    use mentat_db::types::TypedValue;

    use conn::Conn;

    fn populate_db() -> (Conn, rusqlite::Connection) {
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
        (conn, sqlite)
    }

    fn assert_values_present_for_attribute(attribute_cache: &mut AttributeCacher, attribute: &KnownEntid, values: Vec<Vec<TypedValue>>) {
        let cached_values: Vec<Vec<TypedValue>> = attribute_cache.get(&attribute)
            .expect("Expected cached values")
            .values()
            .cloned()
            .collect();
        assert_eq!(cached_values, values);
    }

    #[test]
    fn test_add_to_cache() {
        let (conn, sqlite) = populate_db();
        let schema = conn.current_schema();
        let mut attribute_cache = AttributeCacher::new();
        let kw = kw!(:foo/bar);
        let entid = schema.get_entid(&kw).expect("Expected entid for attribute");
        attribute_cache.register_attribute(&sqlite, entid.clone() ).expect("No errors on add to cache");
        assert_values_present_for_attribute(&mut attribute_cache, &entid, vec![vec![TypedValue::Long(100)], vec![TypedValue::Long(200)]]);
    }

    #[test]
    fn test_add_attribute_already_in_cache() {
        let (conn, mut sqlite) = populate_db();
        let schema = conn.current_schema();

        let kw = kw!(:foo/bar);
        let entid = schema.get_entid(&kw).expect("Expected entid for attribute");
        let mut attribute_cache = AttributeCacher::new();

        attribute_cache.register_attribute(&mut sqlite, entid.clone()).expect("No errors on add to cache");
        assert_values_present_for_attribute(&mut attribute_cache, &entid, vec![vec![TypedValue::Long(100)], vec![TypedValue::Long(200)]]);
        attribute_cache.register_attribute(&mut sqlite, entid.clone()).expect("No errors on add to cache");
        assert_values_present_for_attribute(&mut attribute_cache, &entid, vec![vec![TypedValue::Long(100)], vec![TypedValue::Long(200)]]);
    }

    #[test]
    fn test_remove_from_cache() {
        let (conn, mut sqlite) = populate_db();
        let schema = conn.current_schema();

        let kwr = kw!(:foo/bar);
        let entidr = schema.get_entid(&kwr).expect("Expected entid for attribute");
        let kwz = kw!(:foo/baz);
        let entidz = schema.get_entid(&kwz).expect("Expected entid for attribute");

        let mut attribute_cache = AttributeCacher::new();

        attribute_cache.register_attribute(&mut sqlite, entidr.clone()).expect("No errors on add to cache");
        assert_values_present_for_attribute(&mut attribute_cache, &entidr, vec![vec![TypedValue::Long(100)], vec![TypedValue::Long(200)]]);
        attribute_cache.register_attribute(&mut sqlite, entidz.clone()).expect("No errors on add to cache");
        assert_values_present_for_attribute(&mut attribute_cache, &entidz, vec![vec![TypedValue::Boolean(false)], vec![TypedValue::Boolean(true)]]);

        // test that we can remove an item from cache
        attribute_cache.deregister_attribute(&entidz).expect("No errors on remove from cache");
        assert_eq!(attribute_cache.get(&entidz), None);
    }

    #[test]
    fn test_remove_attribute_not_in_cache() {
        let (conn, _sqlite) = populate_db();
        let mut attribute_cache = AttributeCacher::new();

        let schema = conn.current_schema();
        let kw = kw!(:foo/baz);
        let entid = schema.get_entid(&kw).expect("Expected entid for attribute");
        assert_eq!(None, attribute_cache.deregister_attribute(&entid));
    }

    #[test]
    fn test_fetch_attribute_value_for_entid() {
        let (conn, mut sqlite) = populate_db();
        let schema = conn.current_schema();

        let entities = conn.q_once(&sqlite, r#"[:find ?e :where [?e :foo/bar 100]]"#, None).expect("Expected query to work").into_rel().expect("expected rel results");
        let first = entities.first().expect("expected a result");
        let entid = match first.first() {
            Some(&TypedValue::Ref(entid)) => entid,
            x => panic!("expected Some(Ref), got {:?}", x),
        };

        let kwr = kw!(:foo/bar);
        let attr_entid = schema.get_entid(&kwr).expect("Expected entid for attribute");

        let mut attribute_cache = AttributeCacher::new();

        attribute_cache.register_attribute(&mut sqlite, attr_entid.clone()).expect("No errors on add to cache");
        let val = attribute_cache.get_for_entid(&attr_entid, &KnownEntid(entid)).expect("Expected value");
        assert_eq!(*val, vec![TypedValue::Long(100)]);
    }
}


