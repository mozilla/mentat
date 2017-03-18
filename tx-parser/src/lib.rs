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

use std::iter::once;

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
    AtomOrLookupRefOrVectorOrMapNotation,
    Entid,
    EntidOrLookupRefOrTempId,
    Entity,
    LookupRef,
    MapNotation,
    OpType,
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

fn value_to_nested_vector(val: &Value) -> Option<Vec<AtomOrLookupRefOrVectorOrMapNotation>> {
    val.as_vector().and_then(|vs| {
        let mut p = (many(Tx::<&[Value]>::atom_or_lookup_ref_or_vector()),
                     eof())
            .map(|(vs, _)| vs);
        let r: ParseResult<Vec<AtomOrLookupRefOrVectorOrMapNotation>, _> =  p.parse_lazy(&vs[..]).into();
        r.map(|x| x.0).ok()
    })
}
def_value_satisfy_parser_fn!(Tx, nested_vector, Vec<AtomOrLookupRefOrVectorOrMapNotation>, value_to_nested_vector);

def_parser_fn!(Tx, atom_or_lookup_ref_or_vector, Value, AtomOrLookupRefOrVectorOrMapNotation, input, {
    Tx::<I>::lookup_ref().map(AtomOrLookupRefOrVectorOrMapNotation::LookupRef)
        .or(Tx::<I>::nested_vector().map(AtomOrLookupRefOrVectorOrMapNotation::Vector))
        .or(Tx::<I>::map_notation().map(AtomOrLookupRefOrVectorOrMapNotation::MapNotation))
        .or(Tx::<I>::atom().map(AtomOrLookupRefOrVectorOrMapNotation::Atom))
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

fn value_to_map_notation(val: &Value) -> Option<MapNotation> {
    val.as_map().cloned().and_then(|map| {
        // Parsing pairs is tricky; parsing sequences is easy.
        let avseq: Vec<Value> = map.into_iter().flat_map(|(a, v)| once(a).chain(once(v))).collect();

        let av = (Tx::<&[Value]>::entid(),
                  Tx::<&[Value]>::atom_or_lookup_ref_or_vector())
            .map(|(a, v)| -> (Entid, AtomOrLookupRefOrVectorOrMapNotation) { (a, v) });
        let mut p = (many(av),
                     eof())
            .map(|(avs, _): (Vec<(Entid, AtomOrLookupRefOrVectorOrMapNotation)>, _)| -> MapNotation {
                avs.into_iter().collect()
            });
        let r: ParseResult<MapNotation, _> =  p.parse_lazy(&avseq[..]).into();
        r.map(|x| x.0).ok()
    })
}
def_value_satisfy_parser_fn!(Tx, map_notation, MapNotation, value_to_map_notation);

def_parser_fn!(Tx, entity, Value, Entity, input, {
    let mut p = Tx::<I>::add_or_retract()
        .or(Tx::<I>::map_notation().map(Entity::MapNotation));
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

impl<'a> Tx<&'a [edn::Value]> {
    pub fn parse_entid_or_lookup_ref_or_temp_id(input: &'a [edn::Value]) -> std::result::Result<EntidOrLookupRefOrTempId, errors::Error> {
        (Tx::<_>::entid_or_lookup_ref_or_temp_id(), eof())
            .map(|(es, _)| es)
            .parse(input)
            .map(|x| x.0)
            .map_err::<ValueParseError, _>(|e| e.translate_position(input).into())
            .map_err(|e| Error::from_kind(ErrorKind::ParseError(e)))
    }
}

/// Remove any :db/id value from the given map notation, converting the returned value into
/// something suitable for the entity position rather than something suitable for a value position.
///
/// This is here simply to not expose some of the internal APIs of the tx-parser.
pub fn remove_db_id(map: &mut MapNotation) -> std::result::Result<Option<EntidOrLookupRefOrTempId>, errors::Error> {
    // TODO: extract lazy defined constant.
    let db_id_key = Entid::Ident(edn::NamespacedKeyword::new("db", "id"));

    let db_id: Option<EntidOrLookupRefOrTempId> = if let Some(id) = map.remove(&db_id_key) {
        match id {
            AtomOrLookupRefOrVectorOrMapNotation::Atom(v) => {
                let db_id = Tx::<_>::parse_entid_or_lookup_ref_or_temp_id(&[v][..])
                    .chain_err(|| Error::from(ErrorKind::DbIdError))?;
                Some(db_id)
            },
            AtomOrLookupRefOrVectorOrMapNotation::LookupRef(_) |
            AtomOrLookupRefOrVectorOrMapNotation::Vector(_) |
            AtomOrLookupRefOrVectorOrMapNotation::MapNotation(_) => {
                bail!(ErrorKind::DbIdError)
            },
        }
    } else {
        None
    };

    Ok(db_id)
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
        AtomOrLookupRefOrVectorOrMapNotation,
    };
    use std::collections::BTreeMap;

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
                       v: AtomOrLookupRefOrVectorOrMapNotation::Atom(Value::Text("v".into())),
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
                       v: AtomOrLookupRefOrVectorOrMapNotation::Atom(Value::Text("v".into())),
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
                       v: AtomOrLookupRefOrVectorOrMapNotation::Atom(Value::Text("v".into())),
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
                       v: AtomOrLookupRefOrVectorOrMapNotation::Vector(vec![AtomOrLookupRefOrVectorOrMapNotation::Atom(Value::Text("v1".into())),
                                                                            AtomOrLookupRefOrVectorOrMapNotation::Atom(Value::Text("v2".into()))]),
                   },
                       &[][..])));
    }

    #[test]
    fn test_map_notation() {
        let mut expected: MapNotation = BTreeMap::default();
        expected.insert(Entid::Ident(NamespacedKeyword::new("db", "id")), AtomOrLookupRefOrVectorOrMapNotation::Atom(Value::Text("t".to_string())));
        expected.insert(Entid::Ident(NamespacedKeyword::new("db", "ident")), AtomOrLookupRefOrVectorOrMapNotation::Atom(kw("test", "attribute")));

        let mut map: BTreeMap<Value, Value> = BTreeMap::default();
        map.insert(kw("db", "id"), Value::Text("t".to_string()));
        map.insert(kw("db", "ident"), kw("test", "attribute"));
        let input = [Value::Map(map.clone())];
        let mut parser = Tx::entity();
        let result = parser.parse(&input[..]);
        assert_eq!(result,
                   Ok((Entity::MapNotation(expected),
                       &[][..])));
    }
}
