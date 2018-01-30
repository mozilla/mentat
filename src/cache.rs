// Copyright 2016 Mozilla
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

use std::collections::HashMap;

use rusqlite;

use errors::*;
use mentat_core::{
    KnownEntid,
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


pub struct AttributeCache {
    eager_cache: HashMap<KnownEntid, HashMap<TypedValue, TypedValue>>,   // values keyed by attribute
    lazy_cache: HashMap<KnownEntid, HashMap<TypedValue, TypedValue>>,   // values keyed by attribute
    connection_cache: HashMap<KnownEntid, i64>,         // connections that have requested caching keyed by attribute
}

impl AttributeCache {

    pub fn new() -> AttributeCache {
        AttributeCache {
            eager_cache: HashMap::new(),
            lazy_cache: HashMap::new(),
            connection_cache: HashMap::new(),
        }
    }

    pub fn is_cached(&self, attribute: &KnownEntid) -> bool {
        self.lazy_cache.contains_key(attribute) || self.eager_cache.contains_key(attribute)
    }

    pub fn add_to_cache<'sqlite, 'schema>(&mut self, sqlite: &'sqlite rusqlite::Connection, schema: &'schema Schema, attribute: KnownEntid, lazy: bool) -> Result<()> {
        // check to see if already in cache, return error if so.
        if self.is_cached(&attribute) {
            return Ok(());
        }
        if lazy {
            self.lazy_cache.insert(attribute.clone(), HashMap::new());
        } else {
            // fetch results and add to cache
            let eager_values = self.values_for_attribute(sqlite, schema, &attribute)?;
            self.eager_cache.insert(attribute.clone(), eager_values);
        }
        *self.connection_cache.entry(attribute.clone()).or_insert(0) += 1;
        Ok(())
    }

    fn values_for_attribute<'sqlite, 'schema>(&self, sqlite: &'sqlite rusqlite::Connection, schema: &'schema Schema, attribute: &KnownEntid) -> Result<HashMap<TypedValue, TypedValue>> {
        Ok(q_once(sqlite,
               schema,
               r#"[:find ?entity ?value
                                          :in ?attribute
                                          :where [?entity ?attribute ?value]]"#,
               QueryInputs::with_value_sequence(vec![(Variable::from_valid_name("?attribute"), TypedValue::Long(attribute.0))]))
            .into_rel_result()?
            .iter()
            .fold(HashMap::new(), |mut map, row| {
                map.entry(row[0].clone()).or_insert(row[1].clone());
                map
            }))
    }

    pub fn remove_from_cache(&mut self, attribute: KnownEntid) -> Result<()> {
        if !self.is_cached(&attribute) {
            bail!(ErrorKind::CacheMiss(String::new()))
        }
        if let Some(x) = self.connection_cache.get_mut(&attribute) {
            *x -= 1;
            if *x == 0 {
                self.eager_cache.remove(&attribute);
                self.lazy_cache.remove(&attribute);
            }
        }
        Ok(())
    }

    pub fn fetch_attribute<'sqlite, 'schema>(&mut self, sqlite: &'sqlite rusqlite::Connection, schema: &'schema Schema, attribute: KnownEntid) -> Result<HashMap<TypedValue, TypedValue>> {
        if !self.is_cached(&attribute) {
            bail!(ErrorKind::CacheMiss(String::new()))
        }
        if let Some(res) = self.eager_cache.get(&attribute) {
            Ok(res.clone())
        } else {
            let res = self.values_for_attribute(sqlite, schema, &attribute)?;
            self.lazy_cache.insert(attribute.clone(), res.clone());
            Ok(res)
        }
    }

    pub fn fetch_attribute_for_entid<'sqlite, 'schema>(&mut self, sqlite: &'sqlite rusqlite::Connection, schema: &'schema Schema, attribute: KnownEntid, entid: KnownEntid) -> Result<TypedValue> {
        if !self.is_cached(&attribute) {
            bail!(ErrorKind::CacheMiss(String::new()))
        }
        let entity: TypedValue = entid.into();
        if let Some(res) = self.eager_cache.get(&attribute) {
            let r = res.get(&entity).ok_or_else(|| ErrorKind::CacheMiss(String::new()))?;
            Ok(r.clone())
        } else {
            let mut map = self.lazy_cache.entry(attribute).or_insert(HashMap::new());
            let res = map.entry(entity)
                        .or_insert(lookup_value(sqlite, schema, entid, attribute)
                            .map_err(|_e|ErrorKind::CacheMiss(String::new()))?
                            .ok_or_else(||ErrorKind::CacheMiss(String::new()))?);
            Ok(res.clone())
        }
    }
}


