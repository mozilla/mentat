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
    Symbol(String),
    Keyword(String),
    Vector(Vec<Value>),
    List(LinkedList<Value>),
    // We're using BTree{Set, Map} rather than Hash{Set, Map} because the BTree variants
    // implement Hash (unlike the Hash variants which don't in order to preserve O(n) hashing
    // time which is hard given recurrsive data structures)
    // See https://internals.rust-lang.org/t/implementing-hash-for-hashset-hashmap/3817/1
    Set(BTreeSet<Value>),
    Map(BTreeMap<Value, Value>),
}

use self::Value::*;

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
            Symbol(ref ss)  => match *other { Symbol(ref so)  => so.cmp(&ss), _ => ord_order },
            Keyword(ref ks) => match *other { Keyword(ref ko) => ko.cmp(&ks), _ => ord_order },
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
        Symbol(_) => 6,
        Keyword(_) => 7,
        Vector(_) => 8,
        List(_) => 9,
        Set(_) => 10,
        Map(_) => 12,
    }
}

pub struct Pair(Value, Value);
