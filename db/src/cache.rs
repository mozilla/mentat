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

use errors::{
    Result
};
use db::{
    TypedSQLValue,
};
use mentat_core::{
    Entid,
    TypedValue,
};

pub type CacheMap<K, V> = BTreeMap<K, V>;

pub trait ValueProvider<K, V>: Sized {
    fn fetch_values<'sqlite>(&mut self, sqlite: &'sqlite rusqlite::Connection) -> Result<CacheMap<K, V>>;
}

pub trait Cacheable {
    type Key;
    type Value;
    type ValueProvider;

    fn new(value_provider: Self::ValueProvider) -> Self;
    fn cache_values<'sqlite>(&mut self,
                                      sqlite: &'sqlite rusqlite::Connection) -> Result<()>;
    fn update_values<'sqlite>(&mut self,
                                       sqlite: &'sqlite rusqlite::Connection) -> Result<()>;
    fn get(&self, key: &Self::Key) -> Option<&Self::Value>;
}

#[derive(Clone)]
pub struct EagerCache<K, V, VP> where K: Ord, VP: ValueProvider<K, V> {
    pub cache: CacheMap<K, V>,
    value_provider: VP,
}

impl<K, V, VP> Cacheable for EagerCache<K, V, VP>
    where K: Ord + Clone + Debug + ::std::hash::Hash,
          V: Clone,
          VP: ValueProvider<K, V> {
    type Key = K;
    type Value = V;
    type ValueProvider = VP;

    fn new(value_provider: Self::ValueProvider) -> Self {
        EagerCache {
            cache: CacheMap::new(),
            value_provider,
        }
    }

    fn cache_values<'sqlite>(&mut self,
                                      sqlite: &'sqlite rusqlite::Connection) -> Result<()> {
        // fetch results and add to cache
        self.cache = self.value_provider.fetch_values(sqlite)?;
        Ok(())
    }

    fn update_values<'sqlite>(&mut self,
                                       sqlite: &'sqlite rusqlite::Connection) -> Result<()> {
        self.cache_values(sqlite)
    }

    fn get(&self, key: &Self::Key) -> Option<&Self::Value> {
        self.cache.get(&key)
    }
}

#[derive(Clone)]
pub struct AttributeValueProvider {
    pub attribute: Entid,
}

impl ValueProvider<Entid, Vec<TypedValue>> for AttributeValueProvider {
    fn fetch_values<'sqlite>(&mut self, sqlite: &'sqlite rusqlite::Connection) -> Result<CacheMap<Entid, Vec<TypedValue>>> {
        let sql = "SELECT e, v, value_type_tag FROM datoms WHERE a = ? ORDER BY e ASC";
        let mut stmt = sqlite.prepare(sql)?;
        let value_iter = stmt.query_map(&[&self.attribute], |row| {
            let entid: Entid = row.get(0);
            let value_type_tag: i32 = row.get(2);
            let value = TypedValue::from_sql_value_pair(row.get(1), value_type_tag).map(|x| x).unwrap();
            (entid, value)
        }).map_err(|e| e.into());
        value_iter.map(|v| {
            v.fold(CacheMap::new(), |mut map, row| {
                let _ = row.map(|r| {
                    map.entry(r.0).or_insert(vec![]).push(r.1);
                });
                map
            })
        })
    }
}
