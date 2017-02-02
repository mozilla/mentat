// Copyright 2016 Mozilla
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

#![allow(unused_imports)]

use std::collections::{BTreeSet, BTreeMap, LinkedList};
use std::cmp::{Ordering, Ord, PartialOrd};
use std::fmt::{Display, Formatter};

use symbols;
use num::bigint::{BigInt, ToBigInt, ParseBigIntError};
use ordered_float::OrderedFloat;

/// Value represents one of the allowed values in an EDN string.
#[derive(PartialEq, Eq, Hash, Clone, Debug)]
pub enum Value {
    Nil,
    Boolean(bool),
    Integer(i64),
    BigInteger(BigInt),
    // https://users.rust-lang.org/t/hashmap-key-cant-be-float-number-type-why/7892
    Float(OrderedFloat<f64>),
    Text(String),
    PlainSymbol(symbols::PlainSymbol),
    NamespacedSymbol(symbols::NamespacedSymbol),
    Keyword(symbols::Keyword),
    NamespacedKeyword(symbols::NamespacedKeyword),
    Vector(Vec<Value>),
    // We're using a LinkedList here instead of a Vec or VecDeque because the
    // LinkedList is faster for appending (which we do a lot of).
    // See https://github.com/mozilla/mentat/issues/231
    List(LinkedList<Value>),
    // We're using BTree{Set, Map} rather than Hash{Set, Map} because the BTree variants
    // implement Hash. The Hash variants don't in order to preserve O(n) hashing
    // time, which is hard given recursive data structures.
    // See https://internals.rust-lang.org/t/implementing-hash-for-hashset-hashmap/3817/1
    Set(BTreeSet<Value>),
    Map(BTreeMap<Value, Value>),
}

use self::Value::*;

impl Display for Value {
    fn fmt(&self, f: &mut Formatter) -> ::std::fmt::Result {
        match *self {
            Keyword(ref v) => v.fmt(f),
            NamespacedKeyword(ref v) => v.fmt(f),
            PlainSymbol(ref v) => v.fmt(f),
            NamespacedSymbol(ref v) => v.fmt(f),

            Integer(v) => write!(f, "{}", v),
            BigInteger(ref v) => write!(f, "{}N", v),
            Float(OrderedFloat(v)) => write!(f, "{}", v),       // TODO: make sure float syntax is correct.
                                                                // TODO: NaN.
            Text(ref v) => write!(f, "{}", v),                  // TODO: EDN escaping.
            Vector(ref v) => {
                try!(write!(f, "["));
                for x in v {
                    try!(write!(f, " {}", x));
                }
                write!(f, " ]")
            }
            _ =>
                write!(f, "{}",
                       match *self {
                           Nil => "null",
                           Boolean(b) => if b { "true" } else { "false" },
                           _ => unimplemented!(),
                       }),
        }
    }
}

#[test]
fn test_value_from() {
    assert_eq!(Value::from(42f64), Value::Float(OrderedFloat::from(42f64)));
    assert_eq!(Value::from_bigint("42").unwrap(), Value::BigInteger(42.to_bigint().unwrap()));
}

#[test]
fn test_print_edn() {
    assert_eq!("[ 1 2 [ 3.1 ] [ ] :five :six/seven eight nine/ten true ]",
               Value::Vector(vec!(Value::Integer(1),
               Value::Integer(2),
               Value::Vector(vec!(Value::Float(OrderedFloat(3.1)))),
               Value::Vector(vec!()),
               Value::Keyword(symbols::Keyword::new("five")),
               Value::NamespacedKeyword(symbols::NamespacedKeyword::new("six", "seven")),
               Value::PlainSymbol(symbols::PlainSymbol::new("eight")),
               Value::NamespacedSymbol(symbols::NamespacedSymbol::new("nine", "ten")),
               Value::Boolean(true))).to_string());
}

/// Creates `is_$TYPE` helper functions for Value, like
/// `is_big_integer()` or `is_text()`.
macro_rules! def_is {
    ($name: ident, $pat: pat) => {
        pub fn $name(&self) -> bool {
            match *self { $pat => true, _ => false }
        }
    }
}

/// Creates `as_$TYPE` helper functions for Value, like `as_big_integer()`,
/// which returns the underlying value representing this Value wrapped
/// in an Option, like `<Option<&BigInt>`.
macro_rules! def_as {
    ($name: ident, $kind: path, $t: ty) => {
        pub fn $name(&self) -> Option<&$t> {
            match *self { $kind(ref v) => Some(v), _ => None }
        }
    }
}

impl Value {
    def_is!(is_nil, Nil);
    def_is!(is_boolean, Boolean(_));
    def_is!(is_integer, Integer(_));
    def_is!(is_big_integer, BigInteger(_));
    def_is!(is_float, Float(_));
    def_is!(is_text, Text(_));
    def_is!(is_symbol, PlainSymbol(_));
    def_is!(is_namespaced_symbol, NamespacedSymbol(_));
    def_is!(is_keyword, Keyword(_));
    def_is!(is_namespaced_keyword, NamespacedKeyword(_));
    def_is!(is_vector, Vector(_));
    def_is!(is_list, List(_));
    def_is!(is_set, Set(_));
    def_is!(is_map, Map(_));

    /// `as_nil` does not use the macro as it does not have an underlying
    /// value, and returns `Option<()>`.
    pub fn as_nil(&self) -> Option<()> {
        match *self { Nil => Some(()), _ => None }
    }

    def_as!(as_boolean, Boolean, bool);
    def_as!(as_integer, Integer, i64);
    def_as!(as_big_integer, BigInteger, BigInt);
    def_as!(as_float, Float, OrderedFloat<f64>);
    def_as!(as_text, Text, String);
    def_as!(as_symbol, PlainSymbol, symbols::PlainSymbol);
    def_as!(as_namespaced_symbol, NamespacedSymbol, symbols::NamespacedSymbol);
    def_as!(as_keyword, Keyword, symbols::Keyword);
    def_as!(as_namespaced_keyword, NamespacedKeyword, symbols::NamespacedKeyword);
    def_as!(as_vector, Vector, Vec<Value>);
    def_as!(as_list, List, LinkedList<Value>);
    def_as!(as_set, Set, BTreeSet<Value>);
    def_as!(as_map, Map, BTreeMap<Value, Value>);

    pub fn from_bigint(src: &str) -> Option<Value> {
        src.parse::<BigInt>().map(Value::BigInteger).ok()
    }
}

impl From<f64> for Value {
    fn from(src: f64) -> Value {
        Value::Float(OrderedFloat::from(src))
    }
}

impl PartialOrd for Value {
    fn partial_cmp(&self, other: &Value) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

// TODO: Check we follow the equality rules at the bottom of https://github.com/edn-format/edn
impl Ord for Value {
    fn cmp(&self, other: &Value) -> Ordering {

        let ord_order = to_ord(self).cmp(&to_ord(other));
        match *self {
            Nil             => match *other { Nil             => Ordering::Equal, _ => ord_order },
            Boolean(bs)     => match *other { Boolean(bo)     => bo.cmp(&bs), _ => ord_order },
            BigInteger(ref bs) => match *other { BigInteger(ref bo) => bo.cmp(&bs), _ => ord_order },
            Integer(is)     => match *other { Integer(io)     => io.cmp(&is), _ => ord_order },
            Float(ref fs)   => match *other { Float(ref fo)   => fo.cmp(&fs), _ => ord_order },
            Text(ref ts)    => match *other { Text(ref to)    => to.cmp(&ts), _ => ord_order },
            PlainSymbol(ref ss)  => match *other { PlainSymbol(ref so)  => so.cmp(&ss), _ => ord_order },
            NamespacedSymbol(ref ss)
                => match *other { NamespacedSymbol(ref so)    => so.cmp(&ss), _ => ord_order },
            Keyword(ref ks) => match *other { Keyword(ref ko) => ko.cmp(&ks), _ => ord_order },
            NamespacedKeyword(ref ks)
                => match *other { NamespacedKeyword(ref ko)   => ko.cmp(&ks), _ => ord_order },
            Vector(ref vs)  => match *other { Vector(ref vo)  => vo.cmp(&vs), _ => ord_order },
            List(ref ls)    => match *other { List(ref lo)    => lo.cmp(&ls), _ => ord_order },
            Set(ref ss)     => match *other { Set(ref so)     => so.cmp(&ss), _ => ord_order },
            Map(ref ms)     => match *other { Map(ref mo)     => mo.cmp(&ms), _ => ord_order },
        }
    }
}

fn to_ord(value: &Value) -> i32 {
    match *value {
        Nil => 0,
        Boolean(_) => 1,
        Integer(_) => 2,
        BigInteger(_) => 3,
        Float(_) => 4,
        Text(_) => 5,
        PlainSymbol(_) => 6,
        NamespacedSymbol(_) => 7,
        Keyword(_) => 8,
        NamespacedKeyword(_) => 9,
        Vector(_) => 10,
        List(_) => 11,
        Set(_) => 12,
        Map(_) => 13,
    }
}

/// Converts `name` into a plain or namespaced value symbol, depending on
/// whether or not `namespace` is given.
///
/// # Examples
///
/// ```
/// # use edn::types::to_symbol;
/// # use edn::types::Value;
/// # use edn::symbols;
/// let value = to_symbol("foo", "bar");
/// assert_eq!(value, Value::NamespacedSymbol(symbols::NamespacedSymbol::new("foo", "bar")));
///
/// let value = to_symbol(None, "baz");
/// assert_eq!(value, Value::PlainSymbol(symbols::PlainSymbol::new("baz")));
/// ```
pub fn to_symbol<'a, T: Into<Option<&'a str>>>(namespace: T, name: &str) -> Value {
    namespace.into().map_or_else(
        || Value::PlainSymbol(symbols::PlainSymbol::new(name)),
        |ns| Value::NamespacedSymbol(symbols::NamespacedSymbol::new(ns, name)))
}

/// Converts `name` into a plain or namespaced value keyword, depending on
/// whether or not `namespace` is given.
///
/// # Examples
///
/// ```
/// # use edn::types::to_keyword;
/// # use edn::types::Value;
/// # use edn::symbols;
/// let value = to_keyword("foo", "bar");
/// assert_eq!(value, Value::NamespacedKeyword(symbols::NamespacedKeyword::new("foo", "bar")));
///
/// let value = to_keyword(None, "baz");
/// assert_eq!(value, Value::Keyword(symbols::Keyword::new("baz")));
/// ```
pub fn to_keyword<'a, T: Into<Option<&'a str>>>(namespace: T, name: &str) -> Value {
    namespace.into().map_or_else(
        || Value::Keyword(symbols::Keyword::new(name)),
        |ns| Value::NamespacedKeyword(symbols::NamespacedKeyword::new(ns, name)))
}