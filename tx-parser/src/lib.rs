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
    choice,
    eof,
    many,
    parser,
    satisfy,
    satisfy_map,
    try,
    Parser,
    ParseResult,
};
use mentat_tx::entities::{
    AtomOrLookupRefOrVectorOrMapNotation,
    Entid,
    EntidOrLookupRef,
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
    backward_keyword,
    forward_keyword,
    integer,
    list,
    map,
    namespaced_keyword,
    vector,
};

pub mod errors;
pub use errors::*;

pub struct Tx<'a>(std::marker::PhantomData<&'a ()>);

// Accepts entid, :attribute/forward, and :attribute/_backward.
def_parser!(Tx, entid, Entid, {
    integer()
        .map(|x| Entid::Entid(x))
        .or(namespaced_keyword().map(|x| Entid::Ident(x.clone())))
});

// Accepts entid and :attribute/forward.
def_parser!(Tx, forward_entid, Entid, {
    integer()
        .map(|x| Entid::Entid(x))
        .or(forward_keyword().map(|x| Entid::Ident(x.clone())))
});

// Accepts only :attribute/_backward.
def_parser!(Tx, backward_entid, Entid, {
    backward_keyword().map(|x| Entid::Ident(x.to_reversed()))
});

def_matches_plain_symbol!(Tx, literal_lookup_ref, "lookup-ref");

def_parser!(Tx, lookup_ref, LookupRef, {
    list().of_exactly(
        Tx::literal_lookup_ref()
            .with((Tx::entid(),
                   Tx::atom()))
            .map(|(a, v)| LookupRef { a: a, v: v.clone().without_spans() }))
});

def_parser!(Tx, entid_or_lookup_ref, EntidOrLookupRef, {
    Tx::entid().map(EntidOrLookupRef::Entid)
        .or(Tx::lookup_ref().map(EntidOrLookupRef::LookupRef))
});

def_parser!(Tx, entid_or_lookup_ref_or_temp_id, EntidOrLookupRefOrTempId, {
    Tx::db_tx().map(EntidOrLookupRefOrTempId::TempId)
        .or(Tx::entid().map(EntidOrLookupRefOrTempId::Entid))
        .or(Tx::lookup_ref().map(EntidOrLookupRefOrTempId::LookupRef))
        .or(Tx::temp_id().map(EntidOrLookupRefOrTempId::TempId))
});

def_matches_namespaced_keyword!(Tx, literal_db_tx, "db", "tx");

def_parser!(Tx, db_tx, TempId, {
    Tx::literal_db_tx().map(|_| TempId::Tx)
});

def_parser!(Tx, temp_id, TempId, {
    satisfy_map(|x: &'a edn::ValueAndSpan| x.as_text().cloned().map(TempId::External))
});

def_parser!(Tx, atom, &'a edn::ValueAndSpan, {
    satisfy_map(|x: &'a edn::ValueAndSpan| x.as_atom())
});

def_parser!(Tx, nested_vector, Vec<AtomOrLookupRefOrVectorOrMapNotation>, {
    vector().of_exactly(many(Tx::atom_or_lookup_ref_or_vector()))
});

def_parser!(Tx, atom_or_lookup_ref_or_vector, AtomOrLookupRefOrVectorOrMapNotation, {
    choice::<[&mut Parser<Input = _, Output = AtomOrLookupRefOrVectorOrMapNotation>; 4], _>
        ([&mut try(Tx::lookup_ref().map(AtomOrLookupRefOrVectorOrMapNotation::LookupRef)),
          &mut Tx::nested_vector().map(AtomOrLookupRefOrVectorOrMapNotation::Vector),
          &mut Tx::map_notation().map(AtomOrLookupRefOrVectorOrMapNotation::MapNotation),
          &mut Tx::atom().map(|x| x.clone()).map(AtomOrLookupRefOrVectorOrMapNotation::Atom)
        ])
});

def_matches_namespaced_keyword!(Tx, literal_db_add, "db", "add");

def_matches_namespaced_keyword!(Tx, literal_db_retract, "db", "retract");

def_parser!(Tx, add_or_retract, Entity, {
    try(vector().of_exactly(
        (Tx::literal_db_add().map(|_| OpType::Add).or(Tx::literal_db_retract().map(|_| OpType::Retract)),
          try((Tx::entid_or_lookup_ref_or_temp_id(),
               Tx::forward_entid(),
               Tx::atom_or_lookup_ref_or_vector()))
          .or(try((Tx::atom_or_lookup_ref_or_vector(),
                   Tx::backward_entid(),
                   Tx::entid_or_lookup_ref_or_temp_id()))
              .map(|(v, a, e)| (e, a, v))))
            .map(|(op, (e, a, v))| {
                Entity::AddOrRetract {
                    op: op,
                    e: e,
                    a: a,
                    v: v,
                }
            })))
});

def_matches_namespaced_keyword!(Tx, literal_db_retract_entity, "db", "retractEntity");

def_parser!(Tx, retract_entity, Entity, {
    try(vector().of_exactly(
        (Tx::literal_db_retract_entity(), Tx::entid_or_lookup_ref()).map(|(_, e)| Entity::RetractEntity(e))))
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
        .or(Tx::retract_entity())
        .or(Tx::map_notation().map(Entity::MapNotation))
});

def_parser!(Tx, entities, Vec<Entity>, {
    vector().of_exactly(many(Tx::entity()))
});

impl<'a> Tx<'a> {
    pub fn parse(input: &'a edn::ValueAndSpan) -> std::result::Result<Vec<Entity>, errors::Error> {
        Tx::entities()
            .skip(eof())
            .parse(input.atom_stream())
            .map(|x| x.0)
            .map_err(|e| Error::from_kind(ErrorKind::ParseError(e.into())))
    }

    fn parse_entid_or_lookup_ref_or_temp_id(input: edn::ValueAndSpan) -> std::result::Result<EntidOrLookupRefOrTempId, errors::Error> {
        Tx::entid_or_lookup_ref_or_temp_id()
            .skip(eof())
            .parse(input.atom_stream())
            .map(|x| x.0)
            .map_err(|e| Error::from_kind(ErrorKind::ParseError(e.into())))
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
    use edn::{
        NamespacedKeyword,
        PlainSymbol,
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

        let input = input.with_spans();
        let stream = input.atom_stream();
        let result = Tx::entity().parse(stream).map(|x| x.0);

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

        let input = input.with_spans();
        let stream = input.atom_stream();
        let result = Tx::entity().parse(stream).map(|x| x.0);

        assert_eq!(result,
                   Ok(Entity::AddOrRetract {
                       op: OpType::Retract,
                       e: EntidOrLookupRefOrTempId::Entid(Entid::Entid(101)),
                       a: Entid::Ident(NamespacedKeyword::new("test", "a")),
                       v: AtomOrLookupRefOrVectorOrMapNotation::Atom(ValueAndSpan::new(SpannedValue::Text("v".into()), Span(25, 28))),
                   }));
    }


    #[test]
    fn test_retract_entity() {
        let input = Value::Vector(vec![kw("db", "retractEntity"),
                                       Value::Integer(101)]);

        let input = input.with_spans();
        let stream = input.atom_stream();
        let result = Tx::entity().parse(stream).map(|x| x.0);

        assert_eq!(result,
                   Ok(Entity::RetractEntity(EntidOrLookupRef::Entid(Entid::Entid(101)))));
    }

    #[test]
    fn test_retract_entity_kw() {
        let input = Value::Vector(vec![kw("db", "retractEntity"),
                                       kw("known", "ident")]);

        let input = input.with_spans();
        let stream = input.atom_stream();
        let result = Tx::entity().parse(stream).map(|x| x.0);

        assert_eq!(result,
                   Ok(Entity::RetractEntity(EntidOrLookupRef::Entid(Entid::Ident(NamespacedKeyword::new("known", "ident"))))));
    }

    #[test]
    fn test_retract_entity_lookup_ref() {
        let input = Value::Vector(vec![kw("db", "retractEntity"),
                                       Value::List(vec![Value::PlainSymbol(PlainSymbol::new("lookup-ref")),
                                                        kw("test", "a1"),
                                                        Value::Text("v1".into())].into_iter().collect()),
                                       ]);

        let input = input.with_spans();
        let stream = input.atom_stream();
        let result = Tx::entity().parse(stream).map(|x| x.0);

        assert_eq!(result,
                   Ok(Entity::RetractEntity(EntidOrLookupRef::LookupRef(LookupRef {
                           a: Entid::Ident(NamespacedKeyword::new("test", "a1")),
                           v: Value::Text("v1".into()),
                       }))));
    }

    #[test]
    fn test_lookup_ref() {
        let input = Value::Vector(vec![kw("db", "add"),
                                       Value::List(vec![Value::PlainSymbol(PlainSymbol::new("lookup-ref")),
                                                        kw("test", "a1"),
                                                        Value::Text("v1".into())].into_iter().collect()),
                                       kw("test", "a"),
                                       Value::Text("v".into())]);

        let input = input.with_spans();
        let stream = input.atom_stream();
        let result = Tx::entity().parse(stream).map(|x| x.0);

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

        let input = input.with_spans();
        let stream = input.atom_stream();
        let result = Tx::entity().parse(stream).map(|x| x.0);

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

        let input = input.with_spans();
        let stream = input.atom_stream();
        let result = Tx::entity().parse(stream).map(|x| x.0);

        assert_eq!(result,
                   Ok(Entity::MapNotation(expected)));
    }
}
