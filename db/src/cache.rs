// Copyright 2016 Mozilla
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

use std::collections::{
    BTreeMap,
    BTreeSet,
};

use std::iter::Peekable;

use rusqlite;

use mentat_core::{
    CachedAttributes,
    Entid,
    HasSchema,
    Schema,
    TypedValue,
};

use db::{
    TypedSQLValue,
};

use errors::{
    ErrorKind,
    Result,
};

pub type Aev = (Entid, Entid, TypedValue);

fn row_to_aev(row: &rusqlite::Row) -> Aev {
    let a: Entid = row.get(0);
    let e: Entid = row.get(1);
    let value_type_tag: i32 = row.get(3);
    let v = TypedValue::from_sql_value_pair(row.get(2), value_type_tag).map(|x| x).unwrap();
    (a, e, v)
}

pub type CacheMap<K, V> = BTreeMap<K, V>;

pub struct AevRows<'conn> {
    rows: rusqlite::MappedRows<'conn, fn(&rusqlite::Row) -> Aev>,
}

/// Unwrap the Result from MappedRows. We could also use this opportunity to map_err it, but
/// for now it's convenient to avoid error handling.
impl<'conn> Iterator for AevRows<'conn> {
    type Item = Aev;
    fn next(&mut self) -> Option<Aev> {
        self.rows
            .next()
            .map(|row_result| row_result.expect("All database contents should be representable"))
    }
}

// The behavior of the cache is different for different kinds of attributes:
// - cardinality/one doesn't need a vec
// - unique/* should have a bijective mapping (reverse lookup)

trait CardinalityOneCache {
    fn clear(&mut self);
    fn set(&mut self, e: Entid, v: TypedValue);
    fn get(&self, e: Entid) -> Option<&TypedValue>;
}

trait CardinalityManyCache {
    fn clear(&mut self);
    fn acc(&mut self, e: Entid, v: TypedValue);
    fn set(&mut self, e: Entid, vs: Vec<TypedValue>);
    fn get(&self, e: Entid) -> Option<&Vec<TypedValue>>;
}

#[derive(Debug, Default)]
struct SingleValAttributeCache {
    attr: Entid,
    e_v: CacheMap<Entid, TypedValue>,
}

impl CardinalityOneCache for SingleValAttributeCache {
    fn clear(&mut self) {
        self.e_v.clear();
    }

    fn set(&mut self, e: Entid, v: TypedValue) {
        self.e_v.insert(e, v);
    }

    fn get(&self, e: Entid) -> Option<&TypedValue> {
        self.e_v.get(&e)
    }
}

#[derive(Debug, Default)]
struct MultiValAttributeCache {
    attr: Entid,
    e_vs: CacheMap<Entid, Vec<TypedValue>>,
}

impl CardinalityManyCache for MultiValAttributeCache {
    fn clear(&mut self) {
        self.e_vs.clear();
    }

    fn acc(&mut self, e: Entid, v: TypedValue) {
        self.e_vs.entry(e).or_insert(vec![]).push(v)
    }

    fn set(&mut self, e: Entid, vs: Vec<TypedValue>) {
        self.e_vs.insert(e, vs);
    }

    fn get(&self, e: Entid) -> Option<&Vec<TypedValue>> {
        self.e_vs.get(&e)
    }
}

#[derive(Debug, Default)]
struct UniqueReverseAttributeCache {
    attr: Entid,
    v_e: CacheMap<TypedValue, Entid>,
}

impl UniqueReverseAttributeCache {
    fn clear(&mut self) {
        self.v_e.clear();
    }

    fn set(&mut self, e: Entid, v: TypedValue) {
        self.v_e.insert(v, e);
    }

    fn get_e(&self, v: &TypedValue) -> Option<Entid> {
        self.v_e.get(v).cloned()
    }
}

#[derive(Debug, Default)]
struct NonUniqueReverseAttributeCache {
    attr: Entid,
    v_es: CacheMap<TypedValue, BTreeSet<Entid>>,
}

impl NonUniqueReverseAttributeCache {
    fn clear(&mut self) {
        self.v_es.clear();
    }

    fn acc(&mut self, e: Entid, v: TypedValue) {
        self.v_es.entry(v).or_insert(BTreeSet::new()).insert(e);
    }

    fn get_es(&self, v: &TypedValue) -> Option<&BTreeSet<Entid>> {
        self.v_es.get(v)
    }
}

#[derive(Debug, Default)]
pub struct AttributeCaches {
    reverse_cached_attributes: BTreeSet<Entid>,
    forward_cached_attributes: BTreeSet<Entid>,

    single_vals: BTreeMap<Entid, SingleValAttributeCache>,
    multi_vals: BTreeMap<Entid, MultiValAttributeCache>,
    unique_reverse: BTreeMap<Entid, UniqueReverseAttributeCache>,
    non_unique_reverse: BTreeMap<Entid, NonUniqueReverseAttributeCache>,
}

fn with_aev_iter<F, I>(a: Entid, iter: &mut Peekable<I>, mut f: F)
where I: Iterator<Item=Aev>,
      F: FnMut(Entid, TypedValue) {
    let check = Some(a);
    while iter.peek().map(|&(a, _, _)| a) == check {
        let (_, e, v) = iter.next().unwrap();
        f(e, v);
    }
}

fn accumulate_single_val_evs_forward<I, C>(a: Entid, f: &mut C, iter: &mut Peekable<I>) where I: Iterator<Item=Aev>, C: CardinalityOneCache {
    with_aev_iter(a, iter, |e, v| f.set(e, v))
}

fn accumulate_multi_val_evs_forward<I, C>(a: Entid, f: &mut C, iter: &mut Peekable<I>) where I: Iterator<Item=Aev>, C: CardinalityManyCache {
    with_aev_iter(a, iter, |e, v| f.acc(e, v))
}

fn accumulate_unique_evs_reverse<I>(a: Entid, r: &mut UniqueReverseAttributeCache, iter: &mut Peekable<I>) where I: Iterator<Item=Aev> {
    with_aev_iter(a, iter, |e, v| r.set(e, v))
}

fn accumulate_non_unique_evs_reverse<I>(a: Entid, r: &mut NonUniqueReverseAttributeCache, iter: &mut Peekable<I>) where I: Iterator<Item=Aev> {
    with_aev_iter(a, iter, |e, v| r.acc(e, v))
}

fn accumulate_single_val_unique_evs_both<I, C>(a: Entid, f: &mut C, r: &mut UniqueReverseAttributeCache, iter: &mut Peekable<I>) where I: Iterator<Item=Aev>, C: CardinalityOneCache {
    with_aev_iter(a, iter, |e, v| {
        f.set(e, v.clone());
        r.set(e, v);
    })
}

fn accumulate_multi_val_unique_evs_both<I, C>(a: Entid, f: &mut C, r: &mut UniqueReverseAttributeCache, iter: &mut Peekable<I>) where I: Iterator<Item=Aev>, C: CardinalityManyCache {
    with_aev_iter(a, iter, |e, v| {
        f.acc(e, v.clone());
        r.set(e, v);
    })
}

fn accumulate_single_val_non_unique_evs_both<I, C>(a: Entid, f: &mut C, r: &mut NonUniqueReverseAttributeCache, iter: &mut Peekable<I>) where I: Iterator<Item=Aev>, C: CardinalityOneCache {
    with_aev_iter(a, iter, |e, v| {
        f.set(e, v.clone());
        r.acc(e, v);
    })
}

fn accumulate_multi_val_non_unique_evs_both<I, C>(a: Entid, f: &mut C, r: &mut NonUniqueReverseAttributeCache, iter: &mut Peekable<I>) where I: Iterator<Item=Aev>, C: CardinalityManyCache {
    with_aev_iter(a, iter, |e, v| {
        f.acc(e, v.clone());
        r.acc(e, v);
    })
}

// TODO: if an entity or attribute is ever renumbered, the cache will need to be rebuilt.
impl AttributeCaches {
    //
    // These function names are brief and local.
    // f = forward; r = reverse; both = both forward and reverse.
    // s = single-val; m = multi-val.
    // u = unique; nu = non-unique.
    // c = cache.
    #[inline]
    fn fsc(&mut self, a: Entid) -> &mut SingleValAttributeCache {
        self.single_vals
            .entry(a)
            .or_insert_with(Default::default)
    }

    #[inline]
    fn fmc(&mut self, a: Entid) -> &mut MultiValAttributeCache {
        self.multi_vals
            .entry(a)
            .or_insert_with(Default::default)
    }

    #[inline]
    fn ruc(&mut self, a: Entid) -> &mut UniqueReverseAttributeCache {
        self.unique_reverse
            .entry(a)
            .or_insert_with(Default::default)
    }

    #[inline]
    fn rnuc(&mut self, a: Entid) -> &mut NonUniqueReverseAttributeCache {
        self.non_unique_reverse
            .entry(a)
            .or_insert_with(Default::default)
    }

    #[inline]
    fn both_s_u<'r>(&'r mut self, a: Entid) -> (&'r mut SingleValAttributeCache, &'r mut UniqueReverseAttributeCache) {
        (self.single_vals.entry(a).or_insert_with(Default::default),
         self.unique_reverse.entry(a).or_insert_with(Default::default))
    }

    #[inline]
    fn both_m_u<'r>(&'r mut self, a: Entid) -> (&'r mut MultiValAttributeCache, &'r mut UniqueReverseAttributeCache) {
        (self.multi_vals.entry(a).or_insert_with(Default::default),
         self.unique_reverse.entry(a).or_insert_with(Default::default))
    }

    #[inline]
    fn both_s_nu<'r>(&'r mut self, a: Entid) -> (&'r mut SingleValAttributeCache, &'r mut NonUniqueReverseAttributeCache) {
        (self.single_vals.entry(a).or_insert_with(Default::default),
         self.non_unique_reverse.entry(a).or_insert_with(Default::default))
    }

    #[inline]
    fn both_m_nu<'r>(&'r mut self, a: Entid) -> (&'r mut MultiValAttributeCache, &'r mut NonUniqueReverseAttributeCache) {
        (self.multi_vals.entry(a).or_insert_with(Default::default),
         self.non_unique_reverse.entry(a).or_insert_with(Default::default))
    }

    // Process rows in `iter` that all share an attribute with the first. Leaves the iterator
    // advanced to the first non-matching row.
    fn accumulate_evs<I>(&mut self, schema: &Schema, iter: &mut Peekable<I>, replace_a: bool) where I: Iterator<Item=Aev> {
        if let Some(&(a, _, _)) = iter.peek() {
            if let Some(attribute) = schema.attribute_for_entid(a) {
                let forward = self.is_attribute_cached_forward(a);
                let reverse = self.is_attribute_cached_reverse(a);
                let multi = attribute.multival;
                let unique = attribute.unique.is_some();
                match (forward, reverse, multi, unique) {
                    (true, true, true, true) => {
                        let (f, r) = self.both_m_u(a);
                        if replace_a {
                            f.clear();
                            r.clear();
                        }
                        accumulate_multi_val_unique_evs_both(a, f, r, iter);
                    },
                    (true, true, true, false) => {
                        let (f, r) = self.both_m_nu(a);
                        if replace_a {
                            f.clear();
                            r.clear();
                        }
                        accumulate_multi_val_non_unique_evs_both(a, f, r, iter);
                    },
                    (true, true, false, true) => {
                        let (f, r) = self.both_s_u(a);
                        if replace_a {
                            f.clear();
                            r.clear();
                        }
                        accumulate_single_val_unique_evs_both(a, f, r, iter);
                    },
                    (true, true, false, false) => {
                        let (f, r) = self.both_s_nu(a);
                        if replace_a {
                            f.clear();
                            r.clear();
                        }
                        accumulate_single_val_non_unique_evs_both(a, f, r, iter);
                    },
                    (true, false, true, _) => {
                        let f = self.fmc(a);
                        if replace_a {
                            f.clear();
                        }
                        accumulate_multi_val_evs_forward(a, f, iter)
                    },
                    (true, false, false, _) => {
                        let f = self.fsc(a);
                        if replace_a {
                            f.clear();
                        }
                        accumulate_single_val_evs_forward(a, f, iter)
                    },
                    (false, true, _, true) => {
                        let r = self.ruc(a);
                        if replace_a {
                            r.clear();
                        }
                        accumulate_unique_evs_reverse(a, r, iter);
                    },
                    (false, true, _, false) => {
                        let r = self.rnuc(a);
                        if replace_a {
                            r.clear();
                        }
                        accumulate_non_unique_evs_reverse(a, r, iter);
                    },
                    (false, false, _, _) => {
                        unreachable!();           // Must be cached in at least one direction!
                    },
                }
            }
        }
    }

    fn add_to_cache<I>(&mut self, schema: &Schema, mut iter: Peekable<I>, replace_a: bool) -> Result<()> where I: Iterator<Item=Aev> {
        while iter.peek().is_some() {
            self.accumulate_evs(schema, &mut iter, replace_a);
        }
        Ok(())
    }

    fn clear_cache(&mut self) {
        self.single_vals.clear();
        self.multi_vals.clear();
        self.unique_reverse.clear();
        self.non_unique_reverse.clear();
    }

    fn unregister_all_attributes(&mut self) {
        self.reverse_cached_attributes.clear();
        self.forward_cached_attributes.clear();
        self.clear_cache();
    }

    pub fn unregister_attribute<U>(&mut self, attribute: U)
    where U: Into<Entid> {
        let a = attribute.into();
        self.reverse_cached_attributes.remove(&a);
        self.forward_cached_attributes.remove(&a);
        self.single_vals.remove(&a);
        self.multi_vals.remove(&a);
        self.unique_reverse.remove(&a);
        self.non_unique_reverse.remove(&a);
    }
}

impl CachedAttributes for AttributeCaches {
    fn get_values_for_entid(&self, schema: &Schema, attribute: Entid, entid: Entid) -> Option<&Vec<TypedValue>> {
        self.values_pairs(schema, attribute)
            .and_then(|c| c.get(&entid))
    }

    fn get_value_for_entid(&self, schema: &Schema, attribute: Entid, entid: Entid) -> Option<&TypedValue> {
        self.value_pairs(schema, attribute)
            .and_then(|c| c.get(&entid))
    }

    fn is_attribute_cached_reverse(&self, attribute: Entid) -> bool {
        self.reverse_cached_attributes.contains(&attribute)
    }

    fn is_attribute_cached_forward(&self, attribute: Entid) -> bool {
        self.forward_cached_attributes.contains(&attribute)
    }

    fn get_entid_for_value(&self, attribute: Entid, value: &TypedValue) -> Option<Entid> {
        if self.is_attribute_cached_reverse(attribute) {
            self.unique_reverse.get(&attribute).and_then(|c| c.get_e(value))
        } else {
            None
        }
    }

    fn get_entids_for_value(&self, attribute: Entid, value: &TypedValue) -> Option<&BTreeSet<Entid>> {
        if self.is_attribute_cached_reverse(attribute) {
            self.non_unique_reverse.get(&attribute).and_then(|c| c.get_es(value))
        } else {
            None
        }
    }
}

impl AttributeCaches {
    fn values_pairs<U>(&self, schema: &Schema, attribute: U) -> Option<&BTreeMap<Entid, Vec<TypedValue>>>
    where U: Into<Entid> {
        let attribute = attribute.into();
        schema.attribute_for_entid(attribute)
              .and_then(|attr|
                if attr.multival {
                    self.multi_vals
                        .get(&attribute)
                        .map(|c| &c.e_vs)
                } else {
                    None
                })
    }

    fn value_pairs<U>(&self, schema: &Schema, attribute: U) -> Option<&CacheMap<Entid, TypedValue>>
    where U: Into<Entid> {
        let attribute = attribute.into();
        schema.attribute_for_entid(attribute)
              .and_then(|attr|
                if attr.multival {
                    None
                } else {
                    self.single_vals
                        .get(&attribute)
                        .map(|c| &c.e_v)
                })
    }
}

#[derive(Debug, Default)]
pub struct SQLiteAttributeCache {
    inner: AttributeCaches,
}

impl SQLiteAttributeCache {
    pub fn register_forward<U>(&mut self, schema: &Schema, sqlite: &rusqlite::Connection, attribute: U) -> Result<()>
    where U: Into<Entid> {
        let a = attribute.into();

        // The attribute must exist!
        let _ = schema.attribute_for_entid(a).ok_or_else(|| ErrorKind::UnknownAttribute(a))?;
        self.inner.forward_cached_attributes.insert(a);
        self.repopulate(schema, sqlite, a)
    }

    pub fn register_reverse<U>(&mut self, schema: &Schema, sqlite: &rusqlite::Connection, attribute: U) -> Result<()>
    where U: Into<Entid> {
        let a = attribute.into();

        // The attribute must exist!
        let _ = schema.attribute_for_entid(a).ok_or_else(|| ErrorKind::UnknownAttribute(a))?;

        self.inner.reverse_cached_attributes.insert(a);
        self.repopulate(schema, sqlite, a)
    }

    pub fn register<U>(&mut self, schema: &Schema, sqlite: &rusqlite::Connection, attribute: U) -> Result<()>
    where U: Into<Entid> {
        let a = attribute.into();

        // TODO: reverse-index unique by default?

        self.inner.forward_cached_attributes.insert(a);
        self.inner.reverse_cached_attributes.insert(a);
        self.repopulate(schema, sqlite, a)
    }

    fn repopulate(&mut self, schema: &Schema, sqlite: &rusqlite::Connection, attribute: Entid) -> Result<()> {
        let sql = "SELECT a, e, v, value_type_tag FROM datoms WHERE a = ? ORDER BY a ASC, e ASC";
        let args: Vec<&rusqlite::types::ToSql> = vec![&attribute];
        let mut stmt = sqlite.prepare(sql)?;
        let rows = stmt.query_map(&args, row_to_aev as fn(&rusqlite::Row) -> Aev)?;
        let aevs = AevRows {
            rows: rows,
        };
        self.inner.add_to_cache(schema, aevs.peekable(), true)?;
        Ok(())
    }

    pub fn unregister<U>(&mut self, attribute: U)
    where U: Into<Entid> {
        self.inner.unregister_attribute(attribute);
    }

    pub fn unregister_all(&mut self) {
        self.inner.unregister_all_attributes();
    }
}

impl CachedAttributes for SQLiteAttributeCache {
    fn get_values_for_entid(&self, schema: &Schema, attribute: Entid, entid: Entid) -> Option<&Vec<TypedValue>> {
        self.inner.get_values_for_entid(schema, attribute, entid)
    }

    fn get_value_for_entid(&self, schema: &Schema, attribute: Entid, entid: Entid) -> Option<&TypedValue> {
        self.inner.get_value_for_entid(schema, attribute, entid)
    }

    fn is_attribute_cached_reverse(&self, attribute: Entid) -> bool {
        self.inner.is_attribute_cached_reverse(attribute)
    }

    fn is_attribute_cached_forward(&self, attribute: Entid) -> bool {
        self.inner.is_attribute_cached_forward(attribute)
    }

    fn get_entids_for_value(&self, attribute: Entid, value: &TypedValue) -> Option<&BTreeSet<Entid>> {
        self.inner.get_entids_for_value(attribute, value)
    }

    fn get_entid_for_value(&self, attribute: Entid, value: &TypedValue) -> Option<Entid> {
        self.inner.get_entid_for_value(attribute, value)
    }
}

impl SQLiteAttributeCache {
    /// Intended for use from tests.
    pub fn values_pairs<U>(&self, schema: &Schema, attribute: U) -> Option<&BTreeMap<Entid, Vec<TypedValue>>>
    where U: Into<Entid> {
        self.inner.values_pairs(schema, attribute)
    }

    /// Intended for use from tests.
    pub fn value_pairs<U>(&self, schema: &Schema, attribute: U) -> Option<&BTreeMap<Entid, TypedValue>>
    where U: Into<Entid> {
        self.inner.value_pairs(schema, attribute)
    }
}
