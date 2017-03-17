// Copyright 2016 Mozilla
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

#![allow(dead_code)]

extern crate combine;
#[macro_use]
extern crate error_chain;

extern crate edn;
extern crate mentat_tx;

#[macro_use]
extern crate mentat_parser_utils;

use combine::{eof, many, parser, satisfy_map, token, Parser, ParseResult, Stream};
use combine::combinator::{Expected, FnParser};
use edn::symbols::{
    NamespacedKeyword,
    PlainSymbol,
};
use edn::types::Value;
use mentat_tx::entities::{
    Entid,
    EntidOrLookupRefOrTempId,
    Entity,
    LookupRef,
    OpType,
    AtomOrLookupRefOrVector,
};
use mentat_parser_utils::{ResultParser, ValueParseError};

pub mod errors;
pub use errors::*;

pub struct Tx<I>(::std::marker::PhantomData<fn(I) -> I>);

type TxParser<O, I> = Expected<FnParser<I, fn(I) -> ParseResult<O, I>>>;

fn fn_parser<O, I>(f: fn(I) -> ParseResult<O, I>, err: &'static str) -> TxParser<O, I>
    where I: Stream<Item = Value>
{
    parser(f).expected(err)
}

def_value_satisfy_parser_fn!(Tx, integer, i64, Value::as_integer);

fn value_to_namespaced_keyword(val: &Value) -> Option<NamespacedKeyword> {
    val.as_namespaced_keyword().cloned()
}
def_value_satisfy_parser_fn!(Tx, keyword, NamespacedKeyword, value_to_namespaced_keyword);

def_parser_fn!(Tx, entid, Value, Entid, input, {
    Tx::<I>::integer()
        .map(|x| Entid::Entid(x))
        .or(Tx::<I>::keyword().map(|x| Entid::Ident(x)))
        .parse_lazy(input)
        .into()
});

fn value_to_lookup_ref(val: &Value) -> Option<LookupRef> {
    val.as_list().and_then(|list| {
        let vs: Vec<Value> = list.into_iter().cloned().collect();
        let mut p = (token(Value::PlainSymbol(PlainSymbol::new("lookup-ref"))),
                     Tx::<&[Value]>::entid(),
                     Tx::<&[Value]>::atom(),
                     eof())
            .map(|(_, a, v, _)| LookupRef { a: a, v: v });
        let r: ParseResult<LookupRef, _> = p.parse_lazy(&vs[..]).into();
        r.ok().map(|x| x.0)
    })
}
def_value_satisfy_parser_fn!(Tx, lookup_ref, LookupRef, value_to_lookup_ref);

def_parser_fn!(Tx, entid_or_lookup_ref_or_temp_id, Value, EntidOrLookupRefOrTempId, input, {
    Tx::<I>::entid().map(|x| EntidOrLookupRefOrTempId::Entid(x))
        .or(Tx::<I>::lookup_ref().map(|x| EntidOrLookupRefOrTempId::LookupRef(x)))
        .or(Tx::<I>::temp_id().map(|x| EntidOrLookupRefOrTempId::TempId(x)))
        .parse_lazy(input)
        .into()
});

def_parser_fn!(Tx, temp_id, Value, String, input, {
    satisfy_map(|x: Value| x.into_text())
        .parse_stream(input)
});

def_parser_fn!(Tx, atom, Value, Value, input, {
    satisfy_map(|x: Value| if x.is_atom() { Some(x) } else { None })
        .parse_stream(input)
});

fn value_to_nested_vector(val: &Value) -> Option<Vec<AtomOrLookupRefOrVector>> {
    val.as_vector().and_then(|vs| {
        let mut p = (many(Tx::<&[Value]>::atom_or_lookup_ref_or_vector()),
                     eof())
            .map(|(vs, _)| vs);
        let r: ParseResult<Vec<AtomOrLookupRefOrVector>, _> =  p.parse_lazy(&vs[..]).into();
        r.map(|x| x.0).ok()
    })
}
def_value_satisfy_parser_fn!(Tx, nested_vector, Vec<AtomOrLookupRefOrVector>, value_to_nested_vector);

def_parser_fn!(Tx, atom_or_lookup_ref_or_vector, Value, AtomOrLookupRefOrVector, input, {
    Tx::<I>::lookup_ref().map(AtomOrLookupRefOrVector::LookupRef)
        .or(Tx::<I>::nested_vector().map(AtomOrLookupRefOrVector::Vector))
        .or(Tx::<I>::atom().map(AtomOrLookupRefOrVector::Atom))
        .parse_lazy(input)
        .into()
});

fn value_to_add_or_retract(val: &Value) -> Option<Entity> {
    val.as_vector().and_then(|vs| {
        let add = token(Value::NamespacedKeyword(NamespacedKeyword::new("db", "add")))
            .map(|_| OpType::Add);
        let retract = token(Value::NamespacedKeyword(NamespacedKeyword::new("db", "retract")))
            .map(|_| OpType::Retract);
        let mut p = (add.or(retract),
                     Tx::<&[Value]>::entid_or_lookup_ref_or_temp_id(),
                     Tx::<&[Value]>::entid(),
                     Tx::<&[Value]>::atom_or_lookup_ref_or_vector(),
                     eof())
            .map(|(op, e, a, v, _)| {
                Entity::AddOrRetract {
                    op: op,
                    e: e,
                    a: a,
                    v: v,
                }
            });
        let r: ParseResult<Entity, _> =  p.parse_lazy(&vs[..]).into();
        r.map(|x| x.0).ok()
    })
}
def_value_satisfy_parser_fn!(Tx, add_or_retract, Entity, value_to_add_or_retract);

def_parser_fn!(Tx, entity, Value, Entity, input, {
    let mut p = Tx::<I>::add_or_retract();
    p.parse_stream(input)
});

fn value_to_entities(val: &Value) -> Option<Vec<Entity>> {
    val.as_vector().and_then(|vs| {
        let mut p = (many(Tx::<&[Value]>::entity()), eof())
            .map(|(es, _)| es);
        let r: ParseResult<Vec<Entity>, _> = p.parse_lazy(&vs[..]).into();
        r.ok().map(|x| x.0)
    })
}

def_value_satisfy_parser_fn!(Tx, entities, Vec<Entity>, value_to_entities);

impl<'a> Tx<&'a [edn::Value]> {
    pub fn parse(input: &'a [edn::Value]) -> std::result::Result<Vec<Entity>, errors::Error> {
        (Tx::<_>::entities(), eof())
            .map(|(es, _)| es)
            .parse(input)
            .map(|x| x.0)
            .map_err::<ValueParseError, _>(|e| e.translate_position(input).into())
            .map_err(|e| Error::from_kind(ErrorKind::ParseError(e)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::LinkedList;
    use combine::Parser;
    use edn::symbols::NamespacedKeyword;
    use edn::types::Value;
    use mentat_tx::entities::{
        Entid,
        EntidOrLookupRefOrTempId,
        Entity,
        LookupRef,
        OpType,
        AtomOrLookupRefOrVector,
    };

    fn kw(namespace: &str, name: &str) -> Value {
        Value::NamespacedKeyword(NamespacedKeyword::new(namespace, name))
    }

    #[test]
    fn test_add() {
        let input = [Value::Vector(vec![kw("db", "add"),
                                        kw("test", "entid"),
                                        kw("test", "a"),
                                        Value::Text("v".into())])];
        let mut parser = Tx::entity();
        let result = parser.parse(&input[..]);
        assert_eq!(result,
                   Ok((Entity::AddOrRetract {
                       op: OpType::Add,
                       e: EntidOrLookupRefOrTempId::Entid(Entid::Ident(NamespacedKeyword::new("test",
                                                                                              "entid"))),
                       a: Entid::Ident(NamespacedKeyword::new("test", "a")),
                       v: AtomOrLookupRefOrVector::Atom(Value::Text("v".into())),
                   },
                       &[][..])));
    }

    #[test]
    fn test_retract() {
        let input = [Value::Vector(vec![kw("db", "retract"),
                                        Value::Integer(101),
                                        kw("test", "a"),
                                        Value::Text("v".into())])];
        let mut parser = Tx::entity();
        let result = parser.parse(&input[..]);
        assert_eq!(result,
                   Ok((Entity::AddOrRetract {
                       op: OpType::Retract,
                       e: EntidOrLookupRefOrTempId::Entid(Entid::Entid(101)),
                       a: Entid::Ident(NamespacedKeyword::new("test", "a")),
                       v: AtomOrLookupRefOrVector::Atom(Value::Text("v".into())),
                   },
                       &[][..])));
    }

    #[test]
    fn test_lookup_ref() {
        let mut list = LinkedList::new();
        list.push_back(Value::PlainSymbol(PlainSymbol::new("lookup-ref")));
        list.push_back(kw("test", "a1"));
        list.push_back(Value::Text("v1".into()));

        let input = [Value::Vector(vec![kw("db", "add"),
                                        Value::List(list),
                                        kw("test", "a"),
                                        Value::Text("v".into())])];
        let mut parser = Tx::entity();
        let result = parser.parse(&input[..]);
        assert_eq!(result,
                   Ok((Entity::AddOrRetract {
                       op: OpType::Add,
                       e: EntidOrLookupRefOrTempId::LookupRef(LookupRef {
                           a: Entid::Ident(NamespacedKeyword::new("test", "a1")),
                           v: Value::Text("v1".into()),
                       }),
                       a: Entid::Ident(NamespacedKeyword::new("test", "a")),
                       v: AtomOrLookupRefOrVector::Atom(Value::Text("v".into())),
                   },
                       &[][..])));
    }

    #[test]
    fn test_nested_vector() {
        let mut list = LinkedList::new();
        list.push_back(Value::PlainSymbol(PlainSymbol::new("lookup-ref")));
        list.push_back(kw("test", "a1"));
        list.push_back(Value::Text("v1".into()));

        let input = [Value::Vector(vec![kw("db", "add"),
                                        Value::List(list),
                                        kw("test", "a"),
                                        Value::Vector(vec![Value::Text("v1".into()), Value::Text("v2".into())])])];
        let mut parser = Tx::entity();
        let result = parser.parse(&input[..]);
        assert_eq!(result,
                   Ok((Entity::AddOrRetract {
                       op: OpType::Add,
                       e: EntidOrLookupRefOrTempId::LookupRef(LookupRef {
                           a: Entid::Ident(NamespacedKeyword::new("test", "a1")),
                           v: Value::Text("v1".into()),
                       }),
                       a: Entid::Ident(NamespacedKeyword::new("test", "a")),
                       v: AtomOrLookupRefOrVector::Vector(vec![AtomOrLookupRefOrVector::Atom(Value::Text("v1".into())),
                                                               AtomOrLookupRefOrVector::Atom(Value::Text("v2".into()))]),
                   },
                       &[][..])));
    }
}
