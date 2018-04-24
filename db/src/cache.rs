// Copyright 2018 Mozilla
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

///! An implementation of attribute caching.
///! Attribute caching means storing the entities and values for a given attribute, in the current
///! state of the world, in one or both directions (forward or reverse).
///!
///! One might use a reverse cache to implement fast in-memory lookup of unique identities. One
///! might use a forward cache to implement fast in-memory lookup of common properties.
///!
///! These caches are specialized wrappers around maps. We have four: single/multi forward, and
///! unique/non-unique reverse. There are traits to provide access.
///!
///! A little tower of functions allows for multiple caches to be updated when provided with a
///! single SQLite cursor over `(a, e, v)`s, sorted appropriately.
///!
///! Much of the complexity in this module is to support copy-on-write.
///!
///! When a transaction begins, we expect:
///!
///! - Existing references to the `Conn`'s attribute cache to still be valid.
///! - Isolation to be preserved: that cache will return the same answers until the transaction
///!   commits and a fresh reference to the cache is obtained.
///! - Assertions and retractions within the transaction to be reflected in the cache.
///!   - Retractions apply first, then assertions.
///! - No writes = limited memory allocation for the cache.
///! - New attributes can be cached, and existing attributes uncached, during the transaction.
///!   These changes are isolated, too.
///!
///! All of this means that we need a decent copy-on-write layer that can represent retractions.
///!
///! This is `InProgressSQLiteAttributeCache`. It listens for committed transactions, and handles
///! changes to the cached attribute set, maintaining a reference back to the stable cache. When
///! necessary it copies and modifies. Retractions are modeled via a `None` option.
///!
///! When we're done, we take each of the four caches, and each cached attribute that changed, and
///! absorbe them back into the stable cache. This uses `Arc::make_mut`, so if nobody is looking at
///! the old cache, we modify it in place.
///!
///! Most of the tests for this module are actually in `conn.rs`, where we can set up transactions
///! and test the external API.

use std::collections::{
    BTreeMap,
    BTreeSet,
    HashSet,
};

use std::collections::btree_map::{
    Entry,
};

use std::collections::btree_map::Entry::{
    Occupied,
    Vacant,
};

use std::iter::{
    once,
};

use std::mem;

use std::sync::Arc;

use std::iter::Peekable;

use num;

use rusqlite;

use mentat_core::{
    Binding,
    CachedAttributes,
    Entid,
    HasSchema,
    Schema,
    TypedValue,
    UpdateableCache,
    ValueRc,
};

use mentat_core::util::{
    Either,
};

use mentat_sql::{
    QueryBuilder,
    SQLiteQueryBuilder,
    SQLQuery,
};

use mentat_tx::entities::{
    OpType,
};

use db::{
    TypedSQLValue,
};

use errors::{
    ErrorKind,
    Result,
};

use watcher::{
    TransactWatcher,
};

// Right now we use BTreeMap, because we expect few cached attributes.
pub type CacheMap<K, V> = BTreeMap<K, V>;

trait Remove<T> where T: PartialEq {
    fn remove_every(&mut self, item: &T) -> usize;
}

impl<T> Remove<T> for Vec<T> where T: PartialEq {
    /// Remove all occurrences from a vector in-place, by equality.
    /// Eventually replace with unstable feature: #40062.
    fn remove_every(&mut self, item: &T) -> usize {
        let mut removed = 0;
        let range = num::range_step_inclusive(self.len() as isize - 1, 0, -1);
        for i in range {
            if self.get(i as usize).map_or(false, |x| x == item) {
                self.remove(i as usize);
                removed += 1;
            }
        }
        removed
    }
}

trait Absorb {
    fn absorb(&mut self, other: Self);
}

impl<K, V> Absorb for CacheMap<K, Option<V>> where K: Ord {
    fn absorb(&mut self, other: Self) {
        for (e, v) in other.into_iter() {
            match v {
                None => {
                    // It was deleted. Remove it from our map.
                    self.remove(&e);
                },
                s @ Some(_) => {
                    self.insert(e, s);
                },
            }
        }
    }
}

trait ExtendByAbsorbing {
    /// Just like `extend`, but rather than replacing our value with the other, the other is
    /// absorbed into ours.
    fn extend_by_absorbing(&mut self, other: Self);
}

impl<K, V> ExtendByAbsorbing for BTreeMap<K, V> where K: Ord, V: Absorb {
    fn extend_by_absorbing(&mut self, other: Self) {
        for (k, v) in other.into_iter() {
            match self.entry(k) {
                Occupied(mut entry) => {
                    entry.get_mut().absorb(v);
                },
                Vacant(entry) => {
                    entry.insert(v);
                },
            }
        }
    }
}

// Can't currently put doc tests on traits, so here it is.
#[test]
fn test_vec_remove_item() {
    let mut v = vec![1, 2, 3, 4, 5, 4, 3];
    v.remove_every(&3);
    assert_eq!(v, vec![1, 2, 4, 5, 4]);
    v.remove_every(&4);
    assert_eq!(v, vec![1, 2, 5]);
}

//
// The basics of attribute caching.
//

pub type Aev = (Entid, Entid, TypedValue);

pub struct AevFactory {
    // Our own simple string-interning system.
    strings: HashSet<ValueRc<String>>,
}

impl AevFactory {
    fn new() -> AevFactory {
        AevFactory {
            strings: Default::default(),
        }
    }

    fn intern(&mut self, v: TypedValue) -> TypedValue {
        match v {
            TypedValue::String(rc) => {
                let existing = self.strings.get(&rc).cloned().map(TypedValue::String);
                if let Some(existing) = existing {
                    return existing;
                }
                self.strings.insert(rc.clone());
                return TypedValue::String(rc);
            },
            t => t,
        }
    }

    fn row_to_aev(&mut self, row: &rusqlite::Row) -> Aev {
        let a: Entid = row.get(0);
        let e: Entid = row.get(1);
        let value_type_tag: i32 = row.get(3);
        let v = TypedValue::from_sql_value_pair(row.get(2), value_type_tag).map(|x| x).unwrap();
        (a, e, self.intern(v))
    }
}

pub struct AevRows<'conn, F> {
    rows: rusqlite::MappedRows<'conn, F>,
}

/// Unwrap the Result from MappedRows. We could also use this opportunity to map_err it, but
/// for now it's convenient to avoid error handling.
impl<'conn, F> Iterator for AevRows<'conn, F> where F: FnMut(&rusqlite::Row) -> Aev {
    type Item = Aev;
    fn next(&mut self) -> Option<Aev> {
        self.rows
            .next()
            .map(|row_result| row_result.expect("All database contents should be representable"))
    }
}

// The behavior of the cache is different for different kinds of attributes:
// - cardinality/one doesn't need a vec
// - unique/* should ideally have a bijective mapping (reverse lookup)

pub trait AttributeCache {
    fn has_e(&self, e: Entid) -> bool;
    fn binding_for_e(&self, e: Entid) -> Option<Binding>;
}

trait RemoveFromCache {
    fn remove(&mut self, e: Entid, v: &TypedValue);
}

trait ClearCache {
    fn clear(&mut self);
}

trait CardinalityOneCache: RemoveFromCache + ClearCache {
    fn set(&mut self, e: Entid, v: TypedValue);
    fn get(&self, e: Entid) -> Option<&TypedValue>;
}

trait CardinalityManyCache: RemoveFromCache + ClearCache {
    fn acc(&mut self, e: Entid, v: TypedValue);
    fn set(&mut self, e: Entid, vs: Vec<TypedValue>);
    fn get(&self, e: Entid) -> Option<&Vec<TypedValue>>;
}

#[derive(Clone, Debug, Default)]
struct SingleValAttributeCache {
    attr: Entid,
    e_v: CacheMap<Entid, Option<TypedValue>>,
}

impl Absorb for SingleValAttributeCache {
    fn absorb(&mut self, other: Self) {
        assert_eq!(self.attr, other.attr);
        self.e_v.absorb(other.e_v);
    }
}

impl AttributeCache for SingleValAttributeCache {
    fn binding_for_e(&self, e: Entid) -> Option<Binding> {
        self.get(e).map(|v| v.clone().into())
    }

    fn has_e(&self, e: Entid) -> bool {
        self.e_v.contains_key(&e)
    }
}

impl ClearCache for SingleValAttributeCache {
    fn clear(&mut self) {
        self.e_v.clear();
    }
}

impl RemoveFromCache for SingleValAttributeCache {
    // We never directly remove from the cache unless we're InProgress. In that case, we
    // want to leave a sentinel in place.
    fn remove(&mut self, e: Entid, v: &TypedValue) {
        match self.e_v.entry(e) {
            Occupied(mut entry) => {
                let removed = entry.insert(None);
                match removed {
                    None => {},                     // Already removed.
                    Some(ref r) if r == v => {},    // We removed it!
                    r => {
                        eprintln!("Cache inconsistency: should be ({}, {:?}), was ({}, {:?}).",
                                  e, v, e, r);
                    }
                }
            },
            Vacant(entry) => {
                entry.insert(None);
            },
        }
    }
}

impl CardinalityOneCache for SingleValAttributeCache {
    fn set(&mut self, e: Entid, v: TypedValue) {
        self.e_v.insert(e, Some(v));
    }

    fn get(&self, e: Entid) -> Option<&TypedValue> {
        self.e_v.get(&e).and_then(|m| m.as_ref())
    }
}

#[derive(Clone, Debug, Default)]
struct MultiValAttributeCache {
    attr: Entid,
    e_vs: CacheMap<Entid, Vec<TypedValue>>,
}

impl Absorb for MultiValAttributeCache {
    fn absorb(&mut self, other: Self) {
        assert_eq!(self.attr, other.attr);
        for (e, vs) in other.e_vs.into_iter() {
            if vs.is_empty() {
                self.e_vs.remove(&e);
            } else {
                // We always override with a whole vector, so let's just overwrite.
                self.e_vs.insert(e, vs);
            }
        }
    }
}

impl AttributeCache for MultiValAttributeCache {
    fn binding_for_e(&self, e: Entid) -> Option<Binding> {
        self.e_vs.get(&e).map(|vs| {
            let bindings = vs.iter().cloned().map(|v| v.into()).collect();
            Binding::Vec(ValueRc::new(bindings))
        })
    }

    fn has_e(&self, e: Entid) -> bool {
        self.e_vs.contains_key(&e)
    }
}

impl ClearCache for MultiValAttributeCache {
    fn clear(&mut self) {
        self.e_vs.clear();
    }
}

impl RemoveFromCache for MultiValAttributeCache {
    fn remove(&mut self, e: Entid, v: &TypedValue) {
        if let Some(vec) = self.e_vs.get_mut(&e) {
            let removed = vec.remove_every(v);
            if removed == 0 {
                eprintln!("Cache inconsistency: tried to remove ({}, {:?}), was not present.", e, v);
            }
        } else {
            eprintln!("Cache inconsistency: tried to remove ({}, {:?}), was empty.", e, v);
        }
    }
}

impl CardinalityManyCache for MultiValAttributeCache {
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

#[derive(Clone, Debug, Default)]
struct UniqueReverseAttributeCache {
    attr: Entid,
    v_e: CacheMap<TypedValue, Option<Entid>>,
}

impl Absorb for UniqueReverseAttributeCache {
    fn absorb(&mut self, other: Self) {
        assert_eq!(self.attr, other.attr);
        self.v_e.absorb(other.v_e);
    }
}

impl ClearCache for UniqueReverseAttributeCache {
    fn clear(&mut self) {
        self.v_e.clear();
    }
}

impl RemoveFromCache for UniqueReverseAttributeCache {
    fn remove(&mut self, e: Entid, v: &TypedValue) {
        match self.v_e.entry(v.clone()) {           // Future: better entry API!
            Occupied(mut entry) => {
                let removed = entry.insert(None);
                match removed {
                    None => {},                     // Already removed.
                    Some(r) if r == e => {},        // We removed it!
                    r => {
                        eprintln!("Cache inconsistency: should be ({}, {:?}), was ({}, {:?}).", e, v, e, r);
                    }
                }
            },
            Vacant(entry) => {
                // It didn't already exist.
                entry.insert(None);
            },
        }
    }
}

impl UniqueReverseAttributeCache {
    fn set(&mut self, e: Entid, v: TypedValue) {
        self.v_e.insert(v, Some(e));
    }

    fn get_e(&self, v: &TypedValue) -> Option<Entid> {
        self.v_e.get(v).and_then(|o| o.clone())
    }

    fn lookup(&self, v: &TypedValue) -> Option<Option<Entid>> {
        self.v_e.get(v).cloned()
    }
}

#[derive(Clone, Debug, Default)]
struct NonUniqueReverseAttributeCache {
    attr: Entid,
    v_es: CacheMap<TypedValue, BTreeSet<Entid>>,
}

impl Absorb for NonUniqueReverseAttributeCache {
    fn absorb(&mut self, other: Self) {
        assert_eq!(self.attr, other.attr);
        for (v, es) in other.v_es.into_iter() {
            if es.is_empty() {
                self.v_es.remove(&v);
            } else {
                // We always override with a whole vector, so let's just overwrite.
                self.v_es.insert(v, es);
            }
        }
    }
}

impl ClearCache for NonUniqueReverseAttributeCache {
    fn clear(&mut self) {
        self.v_es.clear();
    }
}

impl RemoveFromCache for NonUniqueReverseAttributeCache {
    fn remove(&mut self, e: Entid, v: &TypedValue) {
        if let Some(vec) = self.v_es.get_mut(&v) {
            let removed = vec.remove(&e);
            if !removed {
                eprintln!("Cache inconsistency: tried to remove ({}, {:?}), was not present.", e, v);
            }
        } else {
            eprintln!("Cache inconsistency: tried to remove ({}, {:?}), was empty.", e, v);
        }
    }
}

impl NonUniqueReverseAttributeCache {
    fn acc(&mut self, e: Entid, v: TypedValue) {
        self.v_es.entry(v).or_insert(BTreeSet::new()).insert(e);
    }

    fn get_es(&self, v: &TypedValue) -> Option<&BTreeSet<Entid>> {
        self.v_es.get(v)
    }
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

fn accumulate_removal_one<I, C>(a: Entid, c: &mut C, iter: &mut Peekable<I>) where I: Iterator<Item=Aev>, C: RemoveFromCache {
    with_aev_iter(a, iter, |e, v| {
        c.remove(e, &v);
    })
}

fn accumulate_removal_both<I, F, R>(a: Entid, f: &mut F, r: &mut R, iter: &mut Peekable<I>)
where I: Iterator<Item=Aev>, F: RemoveFromCache, R: RemoveFromCache {
    with_aev_iter(a, iter, |e, v| {
        f.remove(e, &v);
        r.remove(e, &v);
    })
}


//
// Collect four different kinds of cache together, and track what we're storing.
//

#[derive(Copy, Clone, Eq, PartialEq)]
enum AccumulationBehavior {
    Add { replacing: bool },
    Remove,
}

impl AccumulationBehavior {
    fn is_replacing(&self) -> bool {
        match self {
            &AccumulationBehavior::Add { replacing } => replacing,
            _ => false,
        }
    }
}

#[derive(Clone, Debug, Default)]
pub struct AttributeCaches {
    reverse_cached_attributes: BTreeSet<Entid>,
    forward_cached_attributes: BTreeSet<Entid>,

    single_vals: BTreeMap<Entid, SingleValAttributeCache>,
    multi_vals: BTreeMap<Entid, MultiValAttributeCache>,
    unique_reverse: BTreeMap<Entid, UniqueReverseAttributeCache>,
    non_unique_reverse: BTreeMap<Entid, NonUniqueReverseAttributeCache>,
}

// TODO: if an entity or attribute is ever renumbered, the cache will need to be rebuilt.
impl AttributeCaches {
    //
    // These function names are brief and local.
    // f = forward; r = reverse; both = both forward and reverse.
    // s = single-val; m = multi-val.
    // u = unique; nu = non-unique.
    // c = cache.
    // Note that each of these optionally copies the entry from a fallback cache for copy-on-write.
    #[inline]
    fn fsc(&mut self, a: Entid, fallback: Option<&AttributeCaches>) -> &mut SingleValAttributeCache {
        self.single_vals
            .entry(a)
            .or_insert_with(|| fallback.and_then(|c| c.single_vals.get(&a).cloned())
                                       .unwrap_or_else(Default::default))
    }

    #[inline]
    fn fmc(&mut self, a: Entid, fallback: Option<&AttributeCaches>) -> &mut MultiValAttributeCache {
        self.multi_vals
            .entry(a)
            .or_insert_with(|| fallback.and_then(|c| c.multi_vals.get(&a).cloned())
                                       .unwrap_or_else(Default::default))
    }

    #[inline]
    fn ruc(&mut self, a: Entid, fallback: Option<&AttributeCaches>) -> &mut UniqueReverseAttributeCache {
        self.unique_reverse
            .entry(a)
            .or_insert_with(|| fallback.and_then(|c| c.unique_reverse.get(&a).cloned())
                                       .unwrap_or_else(Default::default))
    }

    #[inline]
    fn rnuc(&mut self, a: Entid, fallback: Option<&AttributeCaches>) -> &mut NonUniqueReverseAttributeCache {
        self.non_unique_reverse
            .entry(a)
            .or_insert_with(|| fallback.and_then(|c| c.non_unique_reverse.get(&a).cloned())
                                       .unwrap_or_else(Default::default))
    }

    #[inline]
    fn both_s_u<'r>(&'r mut self, a: Entid, forward_fallback: Option<&AttributeCaches>, reverse_fallback: Option<&AttributeCaches>) -> (&'r mut SingleValAttributeCache, &'r mut UniqueReverseAttributeCache) {
        (self.single_vals
             .entry(a)
             .or_insert_with(|| forward_fallback.and_then(|c| c.single_vals.get(&a).cloned())
                                                .unwrap_or_else(Default::default)),
         self.unique_reverse
             .entry(a)
             .or_insert_with(|| reverse_fallback.and_then(|c| c.unique_reverse.get(&a).cloned())
                                                .unwrap_or_else(Default::default)))
    }

    #[inline]
    fn both_m_u<'r>(&'r mut self, a: Entid, forward_fallback: Option<&AttributeCaches>, reverse_fallback: Option<&AttributeCaches>) -> (&'r mut MultiValAttributeCache, &'r mut UniqueReverseAttributeCache) {
        (self.multi_vals
             .entry(a)
             .or_insert_with(|| forward_fallback.and_then(|c| c.multi_vals.get(&a).cloned())
                                                .unwrap_or_else(Default::default)),
         self.unique_reverse
             .entry(a)
             .or_insert_with(|| reverse_fallback.and_then(|c| c.unique_reverse.get(&a).cloned())
                                                .unwrap_or_else(Default::default)))
    }

    #[inline]
    fn both_s_nu<'r>(&'r mut self, a: Entid, forward_fallback: Option<&AttributeCaches>, reverse_fallback: Option<&AttributeCaches>) -> (&'r mut SingleValAttributeCache, &'r mut NonUniqueReverseAttributeCache) {
        (self.single_vals
             .entry(a)
             .or_insert_with(|| forward_fallback.and_then(|c| c.single_vals.get(&a).cloned())
                                                .unwrap_or_else(Default::default)),
         self.non_unique_reverse
             .entry(a)
            .or_insert_with(|| reverse_fallback.and_then(|c| c.non_unique_reverse.get(&a).cloned())
                                               .unwrap_or_else(Default::default)))
    }

    #[inline]
    fn both_m_nu<'r>(&'r mut self, a: Entid, forward_fallback: Option<&AttributeCaches>, reverse_fallback: Option<&AttributeCaches>) -> (&'r mut MultiValAttributeCache, &'r mut NonUniqueReverseAttributeCache) {
        (self.multi_vals
             .entry(a)
             .or_insert_with(|| forward_fallback.and_then(|c| c.multi_vals.get(&a).cloned())
                                                .unwrap_or_else(Default::default)),
         self.non_unique_reverse
             .entry(a)
             .or_insert_with(|| reverse_fallback.and_then(|c| c.non_unique_reverse.get(&a).cloned())
                                                .unwrap_or_else(Default::default)))
    }

    // Process rows in `iter` that all share an attribute with the first. Leaves the iterator
    // advanced to the first non-matching row.
    fn accumulate_evs<I>(&mut self,
                         fallback: Option<&AttributeCaches>,
                         schema: &Schema,
                         iter: &mut Peekable<I>,
                         behavior: AccumulationBehavior) where I: Iterator<Item=Aev> {
        if let Some(&(a, _, _)) = iter.peek() {
            if let Some(attribute) = schema.attribute_for_entid(a) {
                let fallback_cached_forward = fallback.map_or(false, |c| c.is_attribute_cached_forward(a));
                let fallback_cached_reverse = fallback.map_or(false, |c| c.is_attribute_cached_reverse(a));
                let now_cached_forward = self.is_attribute_cached_forward(a);
                let now_cached_reverse = self.is_attribute_cached_reverse(a);

                let replace_a = behavior.is_replacing();
                let copy_forward_if_missing = now_cached_forward && fallback_cached_forward && !replace_a;
                let copy_reverse_if_missing = now_cached_reverse && fallback_cached_reverse && !replace_a;

                let forward_fallback = if copy_forward_if_missing {
                    fallback
                } else {
                    None
                };
                let reverse_fallback = if copy_reverse_if_missing {
                    fallback
                } else {
                    None
                };

                let multi = attribute.multival;
                let unique = attribute.unique.is_some();
                match (now_cached_forward, now_cached_reverse, multi, unique) {
                    (true, true, true, true) => {
                        let (f, r) = self.both_m_u(a, forward_fallback, reverse_fallback);
                        match behavior {
                            AccumulationBehavior::Add { replacing } => {
                                if replacing {
                                    f.clear();
                                    r.clear();
                                }
                                accumulate_multi_val_unique_evs_both(a, f, r, iter);
                            },
                            AccumulationBehavior::Remove => accumulate_removal_both(a, f, r, iter),
                        }
                    },
                    (true, true, true, false) => {
                        let (f, r) = self.both_m_nu(a, forward_fallback, reverse_fallback);
                        match behavior {
                            AccumulationBehavior::Add { replacing } => {
                                if replacing {
                                    f.clear();
                                    r.clear();
                                }
                                accumulate_multi_val_non_unique_evs_both(a, f, r, iter);
                            },
                            AccumulationBehavior::Remove => accumulate_removal_both(a, f, r, iter),
                        }
                    },
                    (true, true, false, true) => {
                        let (f, r) = self.both_s_u(a, forward_fallback, reverse_fallback);
                        match behavior {
                            AccumulationBehavior::Add { replacing } => {
                                if replacing {
                                    f.clear();
                                    r.clear();
                                }
                                accumulate_single_val_unique_evs_both(a, f, r, iter);
                            },
                            AccumulationBehavior::Remove => accumulate_removal_both(a, f, r, iter),
                        }
                    },
                    (true, true, false, false) => {
                        let (f, r) = self.both_s_nu(a, forward_fallback, reverse_fallback);
                        match behavior {
                            AccumulationBehavior::Add { replacing } => {
                                if replacing {
                                    f.clear();
                                    r.clear();
                                }
                                accumulate_single_val_non_unique_evs_both(a, f, r, iter);
                            },
                            AccumulationBehavior::Remove => accumulate_removal_both(a, f, r, iter),
                        }
                    },
                    (true, false, true, _) => {
                        let f = self.fmc(a, forward_fallback);
                        match behavior {
                            AccumulationBehavior::Add { replacing } => {
                                if replacing {
                                    f.clear();
                                }
                                accumulate_multi_val_evs_forward(a, f, iter);
                            },
                            AccumulationBehavior::Remove => accumulate_removal_one(a, f, iter),
                        }
                    },
                    (true, false, false, _) => {
                        let f = self.fsc(a, forward_fallback);
                        match behavior {
                            AccumulationBehavior::Add { replacing } => {
                                if replacing {
                                    f.clear();
                                }
                                accumulate_single_val_evs_forward(a, f, iter)
                            },
                            AccumulationBehavior::Remove => accumulate_removal_one(a, f, iter),
                        }
                    },
                    (false, true, _, true) => {
                        let r = self.ruc(a, reverse_fallback);
                        match behavior {
                            AccumulationBehavior::Add { replacing } => {
                                if replacing {
                                    r.clear();
                                }
                                accumulate_unique_evs_reverse(a, r, iter);
                            },
                            AccumulationBehavior::Remove => accumulate_removal_one(a, r, iter),
                        }
                    },
                    (false, true, _, false) => {
                        let r = self.rnuc(a, reverse_fallback);
                        match behavior {
                            AccumulationBehavior::Add { replacing } => {
                                if replacing {
                                    r.clear();
                                }
                                accumulate_non_unique_evs_reverse(a, r, iter);
                            },
                            AccumulationBehavior::Remove => accumulate_removal_one(a, r, iter),
                        }
                    },
                    (false, false, _, _) => {
                        unreachable!();           // Must be cached in at least one direction!
                    },
                }
            }
        }
    }

    fn accumulate_into_cache<I>(&mut self, fallback: Option<&AttributeCaches>, schema: &Schema, mut iter: Peekable<I>, behavior: AccumulationBehavior) -> Result<()> where I: Iterator<Item=Aev> {
        while iter.peek().is_some() {
            self.accumulate_evs(fallback, schema, &mut iter, behavior);
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

// We need this block for fallback.
impl AttributeCaches {
    fn get_entid_for_value_if_present(&self, attribute: Entid, value: &TypedValue) -> Option<Option<Entid>> {
        if self.is_attribute_cached_reverse(attribute) {
            self.unique_reverse
                .get(&attribute)
                .and_then(|c| c.lookup(value))
        } else {
            None
        }
    }

    fn get_value_for_entid_if_present(&self, schema: &Schema, attribute: Entid, entid: Entid) -> Option<Option<&TypedValue>> {
        if let Some(&Some(ref tv)) = self.value_pairs(schema, attribute)
                                         .and_then(|c| c.get(&entid)) {
            Some(Some(tv))
        } else {
            None
        }
    }
}

/// SQL stuff.
impl AttributeCaches {
    fn repopulate(&mut self,
                  schema: &Schema,
                  sqlite: &rusqlite::Connection,
                  attribute: Entid) -> Result<()> {
        let is_fulltext = schema.attribute_for_entid(attribute).map_or(false, |s| s.fulltext);
        let table = if is_fulltext { "fulltext_datoms" } else { "datoms" };
        let sql = format!("SELECT a, e, v, value_type_tag FROM {} WHERE a = ? ORDER BY a ASC, e ASC", table);
        let args: Vec<&rusqlite::types::ToSql> = vec![&attribute];
        let mut stmt = sqlite.prepare(&sql)?;
        let replacing = true;
        self.repopulate_from_aevt(schema, &mut stmt, args, replacing)
    }

    fn repopulate_from_aevt<'a, 's, 'c, 'v>(&'a mut self,
                                            schema: &'s Schema,
                                            statement: &'c mut rusqlite::Statement,
                                            args: Vec<&'v rusqlite::types::ToSql>,
                                            replacing: bool) -> Result<()> {
        let mut aev_factory = AevFactory::new();
        let rows = statement.query_map(&args, |row| aev_factory.row_to_aev(row))?;
        let aevs = AevRows {
            rows: rows,
        };
        self.accumulate_into_cache(None, schema, aevs.peekable(), AccumulationBehavior::Add { replacing })?;
        Ok(())
    }
}

// Borrowed from mentat_query_sql.
macro_rules! interpose {
    ( $name: pat, $across: expr, $body: block, $inter: block ) => {
        interpose_iter!($name, $across.iter(), $body, $inter)
    }
}

// Borrowed from mentat_query_sql.
macro_rules! interpose_iter {
    ( $name: pat, $across: expr, $body: block, $inter: block ) => {
        let mut seq = $across;
        if let Some($name) = seq.next() {
            $body;
            for $name in seq {
                $inter;
                $body;
            }
        }
    }
}

#[derive(Clone)]
pub enum AttributeSpec {
    All,
    Specified {
        // These are assumed to not include duplicates.
        fts: Vec<Entid>,
        non_fts: Vec<Entid>,
    },
}

impl AttributeSpec {
    pub fn all() -> AttributeSpec {
        AttributeSpec::All
    }

    pub fn specified(attrs: &BTreeSet<Entid>, schema: &Schema) -> AttributeSpec {
        let mut fts = Vec::with_capacity(attrs.len());
        let mut non_fts = Vec::with_capacity(attrs.len());
        for attr in attrs.iter() {
            if let Some(a) = schema.attribute_for_entid(*attr) {
                if a.fulltext {
                    fts.push(*attr);
                } else {
                    non_fts.push(*attr);
                }
            }
        }

        AttributeSpec::Specified { fts, non_fts }
    }
}

impl AttributeCaches {
    fn populate_cache_for_entities_and_attributes<'s, 'c>(&mut self,
                                                          schema: &'s Schema,
                                                          sqlite: &'c rusqlite::Connection,
                                                          attrs: AttributeSpec,
                                                          entities: &Vec<Entid>) -> Result<()> {

        // Mark the attributes as cached as we go. We do this because we're going in through the
        // back door here, and the usual caching API won't have taken care of this for us.
        let mut qb = SQLiteQueryBuilder::new();
        qb.push_sql("SELECT a, e, v, value_type_tag FROM ");
        match attrs {
            AttributeSpec::All => {
                qb.push_sql("all_datoms WHERE e IN (");
                interpose!(item, entities,
                           { qb.push_sql(&item.to_string()) },
                           { qb.push_sql(", ") });
                qb.push_sql(") ORDER BY a ASC, e ASC");

                self.forward_cached_attributes.extend(schema.attribute_map.keys());
            },
            AttributeSpec::Specified { fts, non_fts } => {
                let has_fts = !fts.is_empty();
                let has_non_fts = !non_fts.is_empty();

                if !has_fts && !has_non_fts {
                    // Nothing to do.
                    return Ok(());
                }

                if has_non_fts {
                    qb.push_sql("datoms WHERE e IN (");
                    interpose!(item, entities,
                               { qb.push_sql(&item.to_string()) },
                               { qb.push_sql(", ") });
                    qb.push_sql(") AND a IN (");
                    interpose!(item, non_fts,
                               { qb.push_sql(&item.to_string()) },
                               { qb.push_sql(", ") });
                    qb.push_sql(")");

                    self.forward_cached_attributes.extend(non_fts.iter());
                }

                if has_fts && has_non_fts {
                    // Both.
                    qb.push_sql(" UNION ALL SELECT a, e, v, value_type_tag FROM ");
                }

                if has_fts {
                    qb.push_sql("fulltext_datoms WHERE e IN (");
                    interpose!(item, entities,
                               { qb.push_sql(&item.to_string()) },
                               { qb.push_sql(", ") });
                    qb.push_sql(") AND a IN (");
                    interpose!(item, fts,
                               { qb.push_sql(&item.to_string()) },
                               { qb.push_sql(", ") });
                    qb.push_sql(")");

                    self.forward_cached_attributes.extend(fts.iter());
                }
                qb.push_sql(" ORDER BY a ASC, e ASC");
            },
        };

        let SQLQuery { sql, args } = qb.finish();
        assert!(args.is_empty());                       // TODO: we know there are never args, but we'd like to run this query 'properly'.
        let mut stmt = sqlite.prepare(sql.as_str())?;
        let replacing = false;
        self.repopulate_from_aevt(schema, &mut stmt, vec![], replacing)
    }

    pub fn forward_attribute_cache_for_attribute<'a, 's>(&'a self, schema: &'s Schema, a: Entid) -> Option<&'a AttributeCache> {
        if !self.forward_cached_attributes.contains(&a) {
            return None;
        }
        schema.attribute_for_entid(a)
              .and_then(|attr|
                if attr.multival {
                    self.multi_vals.get(&a).map(|v| v as &AttributeCache)
                } else {
                    self.single_vals.get(&a).map(|v| v as &AttributeCache)
                })
    }

    // Ensure that `entities` is unique.
    pub fn extend_cache_for_entities_and_attributes<'s, 'c>(&mut self,
                                                            schema: &'s Schema,
                                                            sqlite: &'c rusqlite::Connection,
                                                            mut attrs: AttributeSpec,
                                                            entities: &Vec<Entid>) -> Result<()> {
        // TODO: Exclude any entities for which every attribute is known.
        // TODO: initialize from an existing (complete) AttributeCache.

        // Exclude any attributes for which every entity's value is already known.
        match &mut attrs {
            &mut AttributeSpec::All => {
                // If we're caching all attributes, there's nothing we can exclude.
            },
            &mut AttributeSpec::Specified { ref mut non_fts, ref mut fts } => {
                // Remove any attributes for which all entities are present in the cache (even
                // as a 'miss').
                let exclude_missing = |vec: &mut Vec<Entid>| {
                    vec.retain(|a| {
                        if let Some(attr) = schema.attribute_for_entid(*a) {
                            if !self.forward_cached_attributes.contains(a) {
                                // The attribute isn't cached at all. Do the work for all entities.
                                return true;
                            }

                            // Return true if there are any entities missing for this attribute.
                            if attr.multival {
                                self.multi_vals
                                    .get(&a)
                                    .map(|cache| entities.iter().any(|e| !cache.has_e(*e)))
                                    .unwrap_or(true)
                            } else {
                                self.single_vals
                                    .get(&a)
                                    .map(|cache| entities.iter().any(|e| !cache.has_e(*e)))
                                    .unwrap_or(true)
                            }
                        } else {
                            // Unknown attribute.
                            false
                        }
                    });
                };
                exclude_missing(non_fts);
                exclude_missing(fts);
            },
        }

        self.populate_cache_for_entities_and_attributes(schema, sqlite, attrs, entities)
    }

    /// Ensure that `entities` is unique.
    pub fn make_cache_for_entities_and_attributes<'s, 'c>(schema: &'s Schema,
                                                          sqlite: &'c rusqlite::Connection,
                                                          attrs: AttributeSpec,
                                                          entities: &Vec<Entid>) -> Result<AttributeCaches> {
        let mut cache = AttributeCaches::default();
        cache.populate_cache_for_entities_and_attributes(schema, sqlite, attrs, entities)?;
        Ok(cache)
    }
}


impl CachedAttributes for AttributeCaches {
    fn get_values_for_entid(&self, schema: &Schema, attribute: Entid, entid: Entid) -> Option<&Vec<TypedValue>> {
        self.values_pairs(schema, attribute)
            .and_then(|c| c.get(&entid))
    }

    fn get_value_for_entid(&self, schema: &Schema, attribute: Entid, entid: Entid) -> Option<&TypedValue> {
        if let Some(&Some(ref tv)) = self.value_pairs(schema, attribute)
                                         .and_then(|c| c.get(&entid)) {
            Some(tv)
        } else {
            None
        }
    }

    fn has_cached_attributes(&self) -> bool {
        !self.reverse_cached_attributes.is_empty() ||
        !self.forward_cached_attributes.is_empty()
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

impl UpdateableCache for AttributeCaches {
    type Error = ::errors::Error;
    fn update<I>(&mut self, schema: &Schema, retractions: I, assertions: I) -> ::std::result::Result<(), Self::Error>
    where I: Iterator<Item=(Entid, Entid, TypedValue)> {
        self.update_with_fallback(None, schema, retractions, assertions)
    }
}

impl AttributeCaches {
    fn update_with_fallback<I>(&mut self, fallback: Option<&AttributeCaches>, schema: &Schema, retractions: I, assertions: I) -> ::std::result::Result<(), ::errors::Error>
    where I: Iterator<Item=(Entid, Entid, TypedValue)> {
        let r_aevs = retractions.peekable();
        self.accumulate_into_cache(fallback, schema, r_aevs, AccumulationBehavior::Remove)?;

        let aevs = assertions.peekable();
        self.accumulate_into_cache(fallback, schema, aevs, AccumulationBehavior::Add { replacing: false })
    }

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

    fn value_pairs<U>(&self, schema: &Schema, attribute: U) -> Option<&CacheMap<Entid, Option<TypedValue>>>
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

impl Absorb for AttributeCaches {
    // Replace or insert attribute-cache pairs from `other` into `self`.
    // Fold in any in-place deletions.
    fn absorb(&mut self, other: Self) {
        self.forward_cached_attributes.extend(other.forward_cached_attributes);
        self.reverse_cached_attributes.extend(other.reverse_cached_attributes);

        self.single_vals.extend_by_absorbing(other.single_vals);
        self.multi_vals.extend_by_absorbing(other.multi_vals);
        self.unique_reverse.extend_by_absorbing(other.unique_reverse);
        self.non_unique_reverse.extend_by_absorbing(other.non_unique_reverse);
    }
}

#[derive(Clone, Debug, Default)]
pub struct SQLiteAttributeCache {
    inner: Arc<AttributeCaches>,
}

impl SQLiteAttributeCache {
    fn make_mut<'s>(&'s mut self) -> &'s mut AttributeCaches {
        Arc::make_mut(&mut self.inner)
    }

    fn make_override(&self) -> AttributeCaches {
        let mut new = AttributeCaches::default();
        new.forward_cached_attributes = self.inner.forward_cached_attributes.clone();
        new.reverse_cached_attributes = self.inner.reverse_cached_attributes.clone();
        new
    }

    pub fn register_forward<U>(&mut self, schema: &Schema, sqlite: &rusqlite::Connection, attribute: U) -> Result<()>
    where U: Into<Entid> {
        let a = attribute.into();

        // The attribute must exist!
        let _ = schema.attribute_for_entid(a).ok_or_else(|| ErrorKind::UnknownAttribute(a))?;
        let caches = self.make_mut();
        caches.forward_cached_attributes.insert(a);
        caches.repopulate(schema, sqlite, a)
    }

    pub fn register_reverse<U>(&mut self, schema: &Schema, sqlite: &rusqlite::Connection, attribute: U) -> Result<()>
    where U: Into<Entid> {
        let a = attribute.into();

        // The attribute must exist!
        let _ = schema.attribute_for_entid(a).ok_or_else(|| ErrorKind::UnknownAttribute(a))?;

        let caches = self.make_mut();
        caches.reverse_cached_attributes.insert(a);
        caches.repopulate(schema, sqlite, a)
    }

    pub fn register<U>(&mut self, schema: &Schema, sqlite: &rusqlite::Connection, attribute: U) -> Result<()>
    where U: Into<Entid> {
        let a = attribute.into();

        // TODO: reverse-index unique by default?

        let caches = self.make_mut();
        caches.forward_cached_attributes.insert(a);
        caches.reverse_cached_attributes.insert(a);
        caches.repopulate(schema, sqlite, a)
    }

    pub fn unregister<U>(&mut self, attribute: U)
    where U: Into<Entid> {
        self.make_mut().unregister_attribute(attribute);
    }

    pub fn unregister_all(&mut self) {
        self.make_mut().unregister_all_attributes();
    }
}

impl UpdateableCache for SQLiteAttributeCache {
    type Error = ::errors::Error;
    fn update<I>(&mut self, schema: &Schema, retractions: I, assertions: I) -> ::std::result::Result<(), Self::Error>
    where I: Iterator<Item=(Entid, Entid, TypedValue)> {
        self.make_mut().update(schema, retractions, assertions)
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

    fn has_cached_attributes(&self) -> bool {
        !self.inner.forward_cached_attributes.is_empty() ||
        !self.inner.reverse_cached_attributes.is_empty()
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
    pub fn value_pairs<U>(&self, schema: &Schema, attribute: U) -> Option<&BTreeMap<Entid, Option<TypedValue>>>
    where U: Into<Entid> {
        self.inner.value_pairs(schema, attribute)
    }
}

/// We maintain a diff on top of the `inner` -- existing -- cache.
/// That involves tracking unregisterings and registerings.
#[derive(Debug, Default)]
pub struct InProgressSQLiteAttributeCache {
    inner: Arc<AttributeCaches>,
    pub overlay: AttributeCaches,
    unregistered_forward: BTreeSet<Entid>,
    unregistered_reverse: BTreeSet<Entid>,
}

impl InProgressSQLiteAttributeCache {
    pub fn from_cache(inner: SQLiteAttributeCache) -> InProgressSQLiteAttributeCache {
        let overlay = inner.make_override();
        InProgressSQLiteAttributeCache {
            inner: inner.inner,
            overlay: overlay,
            unregistered_forward: Default::default(),
            unregistered_reverse: Default::default(),
        }
    }

    pub fn register_forward<U>(&mut self, schema: &Schema, sqlite: &rusqlite::Connection, attribute: U) -> Result<()>
    where U: Into<Entid> {
        let a = attribute.into();

        // The attribute must exist!
        let _ = schema.attribute_for_entid(a).ok_or_else(|| ErrorKind::UnknownAttribute(a))?;

        if self.is_attribute_cached_forward(a) {
            return Ok(());
        }

        self.unregistered_forward.remove(&a);
        self.overlay.forward_cached_attributes.insert(a);
        self.overlay.repopulate(schema, sqlite, a)
    }

    pub fn register_reverse<U>(&mut self, schema: &Schema, sqlite: &rusqlite::Connection, attribute: U) -> Result<()>
    where U: Into<Entid> {
        let a = attribute.into();

        // The attribute must exist!
        let _ = schema.attribute_for_entid(a).ok_or_else(|| ErrorKind::UnknownAttribute(a))?;

        if self.is_attribute_cached_reverse(a) {
            return Ok(());
        }

        self.unregistered_reverse.remove(&a);
        self.overlay.reverse_cached_attributes.insert(a);
        self.overlay.repopulate(schema, sqlite, a)
    }

    pub fn register<U>(&mut self, schema: &Schema, sqlite: &rusqlite::Connection, attribute: U) -> Result<()>
    where U: Into<Entid> {
        let a = attribute.into();

        // The attribute must exist!
        let _ = schema.attribute_for_entid(a).ok_or_else(|| ErrorKind::UnknownAttribute(a))?;

        // TODO: reverse-index unique by default?
        let reverse_done = self.is_attribute_cached_reverse(a);
        let forward_done = self.is_attribute_cached_forward(a);

        if forward_done && reverse_done {
            return Ok(());
        }

        self.unregistered_forward.remove(&a);
        self.unregistered_reverse.remove(&a);
        if !reverse_done {
            self.overlay.reverse_cached_attributes.insert(a);
        }
        if !forward_done {
            self.overlay.forward_cached_attributes.insert(a);
        }

        self.overlay.repopulate(schema, sqlite, a)
    }


    pub fn unregister<U>(&mut self, attribute: U)
    where U: Into<Entid> {
        let a = attribute.into();
        self.overlay.unregister_attribute(a);
        self.unregistered_forward.insert(a);
        self.unregistered_reverse.insert(a);
    }

    pub fn unregister_all(&mut self) {
        self.overlay.unregister_all_attributes();
        self.unregistered_forward.extend(self.inner.forward_cached_attributes.iter().cloned());
        self.unregistered_reverse.extend(self.inner.reverse_cached_attributes.iter().cloned());
    }
}

impl UpdateableCache for InProgressSQLiteAttributeCache {
    type Error = ::errors::Error;
    fn update<I>(&mut self, schema: &Schema, retractions: I, assertions: I) -> ::std::result::Result<(), Self::Error>
    where I: Iterator<Item=(Entid, Entid, TypedValue)> {
        self.overlay.update_with_fallback(Some(&self.inner), schema, retractions, assertions)
    }
}

impl CachedAttributes for InProgressSQLiteAttributeCache {
    fn get_values_for_entid(&self, schema: &Schema, attribute: Entid, entid: Entid) -> Option<&Vec<TypedValue>> {
        if self.unregistered_forward.contains(&attribute) {
            None
        } else {
            // If it was present in `inner` but the values were deleted, there will be an empty
            // array in `overlay` -- `Some(vec![])` -- and we won't fall through.
            // We can safely use `or_else`.
            self.overlay
                .get_values_for_entid(schema, attribute, entid)
                .or_else(|| self.inner.get_values_for_entid(schema, attribute, entid))
        }
    }

    fn get_value_for_entid(&self, schema: &Schema, attribute: Entid, entid: Entid) -> Option<&TypedValue> {
        if self.unregistered_forward.contains(&attribute) {
            None
        } else {
            // If it was present in `inner` but the value was deleted, there will be `Some(None)`
            // in `overlay`, and we won't fall through.
            // We can safely use `or_else`.
            match self.overlay.get_value_for_entid_if_present(schema, attribute, entid) {
                Some(present) => present,
                None => self.inner.get_value_for_entid(schema, attribute, entid),
            }
        }
    }

    fn is_attribute_cached_reverse(&self, attribute: Entid) -> bool {
        !self.unregistered_reverse.contains(&attribute) &&
        (self.inner.reverse_cached_attributes.contains(&attribute) ||
         self.overlay.reverse_cached_attributes.contains(&attribute))
    }

    fn is_attribute_cached_forward(&self, attribute: Entid) -> bool {
        !self.unregistered_forward.contains(&attribute) &&
        (self.inner.forward_cached_attributes.contains(&attribute) ||
         self.overlay.forward_cached_attributes.contains(&attribute))
    }

    fn has_cached_attributes(&self) -> bool {
        // If we've added any, we're definitely not empty.
        if self.overlay.has_cached_attributes() {
            return true;
        }

        // If we haven't removed any, pass through to inner.
        if self.unregistered_forward.is_empty() &&
           self.unregistered_reverse.is_empty() {
            return self.inner.has_cached_attributes();
        }

        // Otherwise, we need to check whether we've removed anything that was cached.
        if self.inner
               .forward_cached_attributes
               .iter()
               .filter(|a| !self.unregistered_forward.contains(a))
               .next()
               .is_some() {
            return true;
        }

        self.inner
            .reverse_cached_attributes
            .iter()
            .filter(|a| !self.unregistered_reverse.contains(a))
            .next()
            .is_some()
    }

    fn get_entids_for_value(&self, attribute: Entid, value: &TypedValue) -> Option<&BTreeSet<Entid>> {
        if self.unregistered_reverse.contains(&attribute) {
            None
        } else {
            self.overlay
                .get_entids_for_value(attribute, value)
                .or_else(|| self.inner.get_entids_for_value(attribute, value))
        }
    }

    fn get_entid_for_value(&self, attribute: Entid, value: &TypedValue) -> Option<Entid> {
        if self.unregistered_reverse.contains(&attribute) {
            None
        } else {
            // If it was present in `inner` but the value was deleted, there will be `Some(None)`
            // in `overlay`, and we won't fall through.
            // We can safely use `or_else`.
            match self.overlay.get_entid_for_value_if_present(attribute, value) {
                Some(present) => present,
                None => self.inner.get_entid_for_value(attribute, value),
            }
        }
    }
}

impl InProgressSQLiteAttributeCache {
    /// Intended for use from tests.
    pub fn values_pairs<U>(&self, schema: &Schema, attribute: U) -> Option<&BTreeMap<Entid, Vec<TypedValue>>>
    where U: Into<Entid> {
        let a = attribute.into();
        self.overlay.values_pairs(schema, a)
                    .or_else(|| self.inner.values_pairs(schema, a))
    }

    /// Intended for use from tests.
    pub fn value_pairs<U>(&self, schema: &Schema, attribute: U) -> Option<&BTreeMap<Entid, Option<TypedValue>>>
    where U: Into<Entid> {
        let a = attribute.into();
        self.overlay
            .value_pairs(schema, a)
            .or_else(|| self.inner.value_pairs(schema, a))
    }

    pub fn commit_to(self, destination: &mut SQLiteAttributeCache) {
        // If the destination is empty, great: just take `overlay`.
        if !destination.has_cached_attributes() {
            destination.inner = Arc::new(self.overlay);
            return;
        }

        // If we have exclusive write access to the destination cache, update it in place.
        // Because the `Conn` also contains an `Arc`, this will ordinarily never be the case.
        // In order to hit this code block, we need to eliminate our reference… do so by dropping
        // our copy of the `Arc`.
        ::std::mem::drop(self.inner);
        if let Some(dest) = Arc::get_mut(&mut destination.inner) {
            // Yeah, we unregister in both directions. The only way
            // `unregistered_forward` won't be the same as `unregistered_reverse` is if
            // our `overlay` added one direction back in.
            for unregistered in self.unregistered_forward.union(&self.unregistered_reverse) {
                dest.unregister_attribute(*unregistered);
            }

            // Now replace each attribute's entry with `overlay`.
            dest.absorb(self.overlay);
            return;
        }

        // If we don't, populate `self.overlay` with whatever we didn't overwrite,
        // and then shim it into `destination.`
        // We haven't implemented this because it does not currently occur.
        // TODO: do this! Then do this:
        // destination.inner = Arc::new(self.overlay);
        unimplemented!();
    }
}

pub struct InProgressCacheTransactWatcher<'a> {
    // A transaction might involve attributes that we cache. Track those values here so that
    // we can update the cache after we commit the transaction.
    collected_assertions: BTreeMap<Entid, Either<(), Vec<(Entid, TypedValue)>>>,
    collected_retractions: BTreeMap<Entid, Either<(), Vec<(Entid, TypedValue)>>>,
    cache: &'a mut InProgressSQLiteAttributeCache,
    active: bool,
}

impl<'a> InProgressCacheTransactWatcher<'a> {
    fn new(cache: &'a mut InProgressSQLiteAttributeCache) -> InProgressCacheTransactWatcher<'a> {
        let mut w = InProgressCacheTransactWatcher {
            collected_assertions: Default::default(),
            collected_retractions: Default::default(),
            cache: cache,
            active: true,
        };

        // This won't change during a transact.
        w.active = w.cache.has_cached_attributes();
        w
    }
}

impl<'a> TransactWatcher for InProgressCacheTransactWatcher<'a> {
    fn datom(&mut self, op: OpType, e: Entid, a: Entid, v: &TypedValue) {
        if !self.active {
            return;
        }

        let target = if op == OpType::Add {
            &mut self.collected_assertions
        } else {
            &mut self.collected_retractions
        };
        match target.entry(a) {
            Entry::Vacant(entry) => {
                let is_cached = self.cache.is_attribute_cached_forward(a) ||
                                self.cache.is_attribute_cached_reverse(a);
                if is_cached {
                    entry.insert(Either::Right(vec![(e, v.clone())]));
                } else {
                    entry.insert(Either::Left(()));
                }
            },
            Entry::Occupied(mut entry) => {
                match entry.get_mut() {
                    &mut Either::Left(_) => {
                        // Nothing to do.
                    },
                    &mut Either::Right(ref mut vec) => {
                        vec.push((e, v.clone()));
                    },
                }
            },
        }
    }

    fn done(&mut self, _t: &Entid, schema: &Schema) -> Result<()> {
        // Oh, I wish we had impl trait. Without it we have a six-line type signature if we
        // try to break this out as a helper function.
        let collected_retractions = mem::replace(&mut self.collected_retractions, Default::default());
        let collected_assertions = mem::replace(&mut self.collected_assertions, Default::default());
        let mut intermediate_expansion =
            once(collected_retractions)
                .chain(once(collected_assertions))
                .into_iter()
                .map(move |tree| tree.into_iter()
                                     .filter_map(move |(a, evs)| {
                                        match evs {
                                            // Drop the empty placeholders.
                                            Either::Left(_) => None,
                                            Either::Right(vec) => Some((a, vec)),
                                        }
                                     })
                                     .flat_map(move |(a, evs)| {
                                        // Flatten into a vec of (a, e, v).
                                        evs.into_iter().map(move |(e, v)| (a, e, v))
                                     }));
        let retractions = intermediate_expansion.next().unwrap();
        let assertions = intermediate_expansion.next().unwrap();
        self.cache.update(schema, retractions, assertions)
    }
}

impl InProgressSQLiteAttributeCache {
    pub fn transact_watcher<'a>(&'a mut self) -> InProgressCacheTransactWatcher<'a> {
        InProgressCacheTransactWatcher::new(self)
    }
}
