// Copyright 2016 Mozilla
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

use std::collections::{BTreeSet, BTreeMap, LinkedList};
use std::cmp::{Ordering, Ord, PartialOrd};
use std::fmt::{Display, Formatter};
use std::f64;

use symbols;
use num::BigInt;
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
    // TODO: Make sure float syntax is correct, handle NaN and escaping.
    // See https://github.com/mozilla/mentat/issues/232
    fn fmt(&self, f: &mut Formatter) -> ::std::fmt::Result {
        match *self {
            Nil => write!(f, "nil"),
            Boolean(v) => write!(f, "{}", v),
            Integer(v) => write!(f, "{}", v),
            BigInteger(ref v) => write!(f, "{}N", v),
            Float(ref v) => {
                // TODO: make sure float syntax is correct.
                if *v == OrderedFloat(f64::INFINITY) {
                    write!(f, "#f {}", "+Infinity")
                } else if *v == OrderedFloat(f64::NEG_INFINITY) {
                    write!(f, "#f {}", "-Infinity")
                } else if *v == OrderedFloat(f64::NAN) {
                    write!(f, "#f {}", "NaN")
                } else {
                    write!(f, "{}", v)
                }
            }
            // TODO: EDN escaping.
            Text(ref v) => write!(f, "{}", v),
            PlainSymbol(ref v) => v.fmt(f),
            NamespacedSymbol(ref v) => v.fmt(f),
            Keyword(ref v) => v.fmt(f),
            NamespacedKeyword(ref v) => v.fmt(f),
            Vector(ref v) => {
                write!(f, "[")?;
                for x in v {
                    write!(f, " {}", x)?;
                }
                write!(f, " ]")
            }
            List(ref v) => {
                write!(f, "(")?;
                for x in v {
                    write!(f, " {}", x)?;
                }
                write!(f, " )")
            }
            Set(ref v) => {
                write!(f, "#{{")?;
                for x in v {
                    write!(f, " {}", x)?;
                }
                write!(f, " }}")
            }
            Map(ref v) => {
                write!(f, "{{")?;
                for (key, val) in v {
                    write!(f, " :{} {}", key, val)?;
                }
                write!(f, " }}")
            }
        }
    }
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

    pub fn from_symbol<'a, T: Into<Option<&'a str>>>(namespace: T, name: &str) -> Value {
        to_symbol(namespace, name)
    }

    pub fn from_keyword<'a, T: Into<Option<&'a str>>>(namespace: T, name: &str) -> Value {
        to_keyword(namespace, name)
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

impl Ord for Value {
    fn cmp(&self, other: &Value) -> Ordering {
        match (self, other) {
            (&Nil, &Nil) => Ordering::Equal,
            (&Boolean(a), &Boolean(b)) => b.cmp(&a),
            (&Integer(a), &Integer(b)) => b.cmp(&a),
            (&BigInteger(ref a), &BigInteger(ref b)) => b.cmp(a),
            (&Float(ref a), &Float(ref b)) => b.cmp(a),
            (&Text(ref a), &Text(ref b)) => b.cmp(a),
            (&PlainSymbol(ref a), &PlainSymbol(ref b)) => b.cmp(a),
            (&NamespacedSymbol(ref a), &NamespacedSymbol(ref b)) => b.cmp(a),
            (&Keyword(ref a), &Keyword(ref b)) => b.cmp(a),
            (&NamespacedKeyword(ref a), &NamespacedKeyword(ref b)) => b.cmp(a),
            (&Vector(ref a), &Vector(ref b)) => b.cmp(a),
            (&List(ref a), &List(ref b)) => b.cmp(a),
            (&Set(ref a), &Set(ref b)) => b.cmp(a),
            (&Map(ref a), &Map(ref b)) => b.cmp(a),
            _ => to_ord(self).cmp(&to_ord(other))
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

#[cfg(test)]
mod test {
    extern crate ordered_float;
    extern crate num;

    use super::*;

    use std::collections::{BTreeSet, BTreeMap, LinkedList};
    use std::cmp::{Ordering};
    use std::f64;

    use symbols;
    use num::BigInt;
    use ordered_float::OrderedFloat;
    use std::iter::FromIterator;

    #[test]
    fn test_value_from() {
        assert_eq!(Value::from(42f64), Value::Float(OrderedFloat::from(42f64)));
        assert_eq!(Value::from_bigint("42").unwrap(), Value::BigInteger(BigInt::from(42)));
    }

    #[test]
    fn test_print_edn() {
        assert_eq!("[ 1 2 ( 3.14 ) #{ 4N } { :foo/bar 42 } [ ] :five :six/seven eight nine/ten true false nil #f NaN #f -Infinity #f +Infinity ]",
            Value::Vector(vec![
                Value::Integer(1),
                Value::Integer(2),
                Value::List(LinkedList::from_iter(vec![
                    Value::Float(OrderedFloat(3.14))
                ])),
                Value::Set(BTreeSet::from_iter(vec![
                    Value::from_bigint("4").unwrap()
                ])),
                Value::Map(BTreeMap::from_iter(vec![
                    (Value::from_symbol("foo", "bar"), Value::Integer(42))
                ])),
                Value::Vector(vec![]),
                Value::Keyword(symbols::Keyword::new("five")),
                Value::NamespacedKeyword(symbols::NamespacedKeyword::new("six", "seven")),
                Value::PlainSymbol(symbols::PlainSymbol::new("eight")),
                Value::NamespacedSymbol(symbols::NamespacedSymbol::new("nine", "ten")),
                Value::Boolean(true),
                Value::Boolean(false),
                Value::Nil,
                Value::Float(OrderedFloat(f64::NAN)),
                Value::Float(OrderedFloat(f64::NEG_INFINITY)),
                Value::Float(OrderedFloat(f64::INFINITY)),
            ]
        ).to_string());
    }

    #[test]
    fn test_ord() {
        // TODO: Check we follow the equality rules at the bottom of https://github.com/edn-format/edn
        assert_eq!(Value::Nil.cmp(&Value::Nil), Ordering::Equal);
        assert_eq!(Value::Boolean(false).cmp(&Value::Boolean(true)), Ordering::Greater);
        assert_eq!(Value::Integer(1).cmp(&Value::Integer(2)), Ordering::Greater);
        assert_eq!(Value::from_bigint("1").cmp(&Value::from_bigint("2")), Ordering::Greater);
        assert_eq!(Value::from(1f64).cmp(&Value::from(2f64)), Ordering::Greater);
        assert_eq!(Value::Text("1".to_string()).cmp(&Value::Text("2".to_string())), Ordering::Greater);
        assert_eq!(Value::from_symbol("a", "b").cmp(&Value::from_symbol("c", "d")), Ordering::Greater);
        assert_eq!(Value::from_symbol(None, "a").cmp(&Value::from_symbol(None, "b")), Ordering::Greater);
        assert_eq!(Value::from_keyword(":a", ":b").cmp(&Value::from_keyword(":c", ":d")), Ordering::Greater);
        assert_eq!(Value::from_keyword(None, ":a").cmp(&Value::from_keyword(None, ":b")), Ordering::Greater);
        assert_eq!(Value::Vector(vec![]).cmp(&Value::Vector(vec![])), Ordering::Equal);
        assert_eq!(Value::List(LinkedList::new()).cmp(&Value::List(LinkedList::new())), Ordering::Equal);
        assert_eq!(Value::Set(BTreeSet::new()).cmp(&Value::Set(BTreeSet::new())), Ordering::Equal);
        assert_eq!(Value::Map(BTreeMap::new()).cmp(&Value::Map(BTreeMap::new())), Ordering::Equal);
    }
}
