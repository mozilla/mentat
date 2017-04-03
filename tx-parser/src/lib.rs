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

use combine::{
    eof,
    many,
    parser,
    satisfy_map,
    Parser,
    ParseResult,
};
use edn::symbols::{
    NamespacedKeyword,
    PlainSymbol,
};
use mentat_tx::entities::{
    AtomOrLookupRefOrVectorOrMapNotation,
    Entid,
    EntidOrLookupRefOrTempId,
    Entity,
    LookupRef,
    MapNotation,
    OpType,
    TempId,
};
use mentat_parser_utils::{ResultParser};
use mentat_parser_utils::value_and_span::{
    Item,
    OfExactlyParsing,
    integer,
    list,
    map,
    namespaced_keyword,
    value,
    vector,
};

pub mod errors;
pub use errors::*;

pub struct Tx;

def_parser!(Tx, entid, Entid, {
    integer()
        .map(|x| Entid::Entid(x))
        .or(namespaced_keyword().map(|x| Entid::Ident(x)))
});

def_parser!(Tx, lookup_ref, LookupRef, {
    list().of_exactly(
        value(edn::Value::PlainSymbol(PlainSymbol::new("lookup-ref")))
            .with((Tx::entid(),
                   Tx::atom()))
            .map(|(a, v)| LookupRef { a: a, v: v.without_spans() }))
});

def_parser!(Tx, entid_or_lookup_ref_or_temp_id, EntidOrLookupRefOrTempId, {
    Tx::entid().map(EntidOrLookupRefOrTempId::Entid)
        .or(Tx::lookup_ref().map(EntidOrLookupRefOrTempId::LookupRef))
        .or(Tx::temp_id().map(EntidOrLookupRefOrTempId::TempId))
});

def_parser!(Tx, temp_id, TempId, {
    satisfy_map(|x: edn::ValueAndSpan| x.into_text().map(TempId::External))
});

def_parser!(Tx, atom, edn::ValueAndSpan, {
    satisfy_map(|x: edn::ValueAndSpan| x.into_atom().map(|v| v))
});

def_parser!(Tx, nested_vector, Vec<AtomOrLookupRefOrVectorOrMapNotation>, {
    vector().of_exactly(many(Tx::atom_or_lookup_ref_or_vector()))
});

def_parser!(Tx, atom_or_lookup_ref_or_vector, AtomOrLookupRefOrVectorOrMapNotation, {
    Tx::lookup_ref().map(AtomOrLookupRefOrVectorOrMapNotation::LookupRef)
        .or(Tx::nested_vector().map(AtomOrLookupRefOrVectorOrMapNotation::Vector))
        .or(Tx::map_notation().map(AtomOrLookupRefOrVectorOrMapNotation::MapNotation))
        .or(Tx::atom().map(AtomOrLookupRefOrVectorOrMapNotation::Atom))
});

def_parser!(Tx, add_or_retract, Entity, {
    let add = value(edn::Value::NamespacedKeyword(NamespacedKeyword::new("db", "add")))
        .map(|_| OpType::Add);
    let retract = value(edn::Value::NamespacedKeyword(NamespacedKeyword::new("db", "retract")))
        .map(|_| OpType::Retract);
    let p = (add.or(retract),
             Tx::entid_or_lookup_ref_or_temp_id(),
             Tx::entid(),
             Tx::atom_or_lookup_ref_or_vector())
        .map(|(op, e, a, v)| {
            Entity::AddOrRetract {
                op: op,
                e: e,
                a: a,
                v: v,
            }
        });

    vector().of_exactly(p)
});

def_parser!(Tx, map_notation, MapNotation, {
    map()
        .of_exactly(many((Tx::entid(), Tx::atom_or_lookup_ref_or_vector())))
        .map(|avs: Vec<(Entid, AtomOrLookupRefOrVectorOrMapNotation)>| -> MapNotation {
            avs.into_iter().collect()
        })
});

def_parser!(Tx, entity, Entity, {
    Tx::add_or_retract()
        .or(Tx::map_notation().map(Entity::MapNotation))
});

def_parser!(Tx, entities, Vec<Entity>, {
    vector().of_exactly(many(Tx::entity()))
});

impl Tx {
    pub fn parse(input: edn::ValueAndSpan) -> std::result::Result<Vec<Entity>, errors::Error> {
        Tx::entities()
            .skip(eof())
            .parse(input.into_atom_stream())
            .map(|x| x.0)
            .map_err(|e| Error::from_kind(ErrorKind::ParseError(e)))
    }

    fn parse_entid_or_lookup_ref_or_temp_id(input: edn::ValueAndSpan) -> std::result::Result<EntidOrLookupRefOrTempId, errors::Error> {
        Tx::entid_or_lookup_ref_or_temp_id()
            .skip(eof())
            .parse(input.into_atom_stream())
            .map(|x| x.0)
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
                let db_id = Tx::parse_entid_or_lookup_ref_or_temp_id(v)
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

    use std::collections::BTreeMap;

    use combine::Parser;
    use edn::symbols::NamespacedKeyword;
    use edn::{
        Span,
        SpannedValue,
        Value,
        ValueAndSpan,
    };
    use mentat_tx::entities::{
        Entid,
        EntidOrLookupRefOrTempId,
        Entity,
        OpType,
        AtomOrLookupRefOrVectorOrMapNotation,
    };

    fn kw(namespace: &str, name: &str) -> Value {
        Value::NamespacedKeyword(NamespacedKeyword::new(namespace, name))
    }

    #[test]
    fn test_add() {
        let input = Value::Vector(vec![kw("db", "add"),
                                       kw("test", "entid"),
                                       kw("test", "a"),
                                       Value::Text("v".into())]);
        let mut parser = Tx::entity();
        let result = parser.parse(input.with_spans().into_atom_stream()).map(|x| x.0);
        assert_eq!(result,
                   Ok(Entity::AddOrRetract {
                       op: OpType::Add,
                       e: EntidOrLookupRefOrTempId::Entid(Entid::Ident(NamespacedKeyword::new("test",
                                                                                              "entid"))),
                       a: Entid::Ident(NamespacedKeyword::new("test", "a")),
                       v: AtomOrLookupRefOrVectorOrMapNotation::Atom(ValueAndSpan::new(SpannedValue::Text("v".into()), Span(29, 32))),
                   }));
    }

    #[test]
    fn test_retract() {
        let input = Value::Vector(vec![kw("db", "retract"),
                                       Value::Integer(101),
                                       kw("test", "a"),
                                       Value::Text("v".into())]);
        let mut parser = Tx::entity();
        let result = parser.parse(input.with_spans().into_atom_stream()).map(|x| x.0);
        assert_eq!(result,
                   Ok(Entity::AddOrRetract {
                       op: OpType::Retract,
                       e: EntidOrLookupRefOrTempId::Entid(Entid::Entid(101)),
                       a: Entid::Ident(NamespacedKeyword::new("test", "a")),
                       v: AtomOrLookupRefOrVectorOrMapNotation::Atom(ValueAndSpan::new(SpannedValue::Text("v".into()), Span(25, 28))),
                   }));
    }

    #[test]
    fn test_lookup_ref() {
        let input = Value::Vector(vec![kw("db", "add"),
                                       Value::List(vec![Value::PlainSymbol(PlainSymbol::new("lookup-ref")),
                                                        kw("test", "a1"),
                                                        Value::Text("v1".into())].into_iter().collect()),
                                       kw("test", "a"),
                                       Value::Text("v".into())]);
        let mut parser = Tx::entity();
        let result = parser.parse(input.with_spans().into_atom_stream()).map(|x| x.0);
        assert_eq!(result,
                   Ok(Entity::AddOrRetract {
                       op: OpType::Add,
                       e: EntidOrLookupRefOrTempId::LookupRef(LookupRef {
                           a: Entid::Ident(NamespacedKeyword::new("test", "a1")),
                           v: Value::Text("v1".into()),
                       }),
                       a: Entid::Ident(NamespacedKeyword::new("test", "a")),
                       v: AtomOrLookupRefOrVectorOrMapNotation::Atom(ValueAndSpan::new(SpannedValue::Text("v".into()), Span(44, 47))),
                   }));
    }

    #[test]
    fn test_nested_vector() {
        let input = Value::Vector(vec![kw("db", "add"),
                                       Value::List(vec![Value::PlainSymbol(PlainSymbol::new("lookup-ref")),
                                                        kw("test", "a1"),
                                                        Value::Text("v1".into())].into_iter().collect()),
                                       kw("test", "a"),
                                       Value::Vector(vec![Value::Text("v1".into()), Value::Text("v2".into())])]);
        let mut parser = Tx::entity();
        let result = parser.parse(input.with_spans().into_atom_stream()).map(|x| x.0);
        assert_eq!(result,
                   Ok(Entity::AddOrRetract {
                       op: OpType::Add,
                       e: EntidOrLookupRefOrTempId::LookupRef(LookupRef {
                           a: Entid::Ident(NamespacedKeyword::new("test", "a1")),
                           v: Value::Text("v1".into()),
                       }),
                       a: Entid::Ident(NamespacedKeyword::new("test", "a")),
                       v: AtomOrLookupRefOrVectorOrMapNotation::Vector(vec![AtomOrLookupRefOrVectorOrMapNotation::Atom(ValueAndSpan::new(SpannedValue::Text("v1".into()), Span(45, 49))),
                                                                            AtomOrLookupRefOrVectorOrMapNotation::Atom(ValueAndSpan::new(SpannedValue::Text("v2".into()), Span(50, 54)))]),
                   }));
    }

    #[test]
    fn test_map_notation() {
        let mut expected: MapNotation = BTreeMap::default();
        expected.insert(Entid::Ident(NamespacedKeyword::new("db", "id")), AtomOrLookupRefOrVectorOrMapNotation::Atom(ValueAndSpan::new(SpannedValue::Text("t".to_string()), Span(8, 11))));
        expected.insert(Entid::Ident(NamespacedKeyword::new("db", "ident")), AtomOrLookupRefOrVectorOrMapNotation::Atom(ValueAndSpan::new(SpannedValue::NamespacedKeyword(NamespacedKeyword::new("test", "attribute")), Span(22, 37))));

        let mut map: BTreeMap<Value, Value> = BTreeMap::default();
        map.insert(kw("db", "id"), Value::Text("t".to_string()));
        map.insert(kw("db", "ident"), kw("test", "attribute"));
        let input = Value::Map(map.clone());

        let mut parser = Tx::entity();
        let result = parser.parse(input.with_spans().into_atom_stream()).map(|x| x.0);
        assert_eq!(result,
                   Ok(Entity::MapNotation(expected)));
    }
}
