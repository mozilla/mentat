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

extern crate edn;
extern crate combine;
extern crate mentat_tx;

#[macro_use]
extern crate mentat_parser_utils;

use combine::{any, eof, many, parser, satisfy_map, token, Parser, ParseResult, Stream};
use combine::combinator::{Expected, FnParser};
use edn::symbols::NamespacedKeyword;
use edn::types::Value;
use mentat_tx::entities::{Entid, EntidOrLookupRefOrTempId, Entity, LookupRef, OpType};
use mentat_parser_utils::ResultParser;

pub struct Tx<I>(::std::marker::PhantomData<fn(I) -> I>);

type TxParser<O, I> = Expected<FnParser<I, fn(I) -> ParseResult<O, I>>>;

fn fn_parser<O, I>(f: fn(I) -> ParseResult<O, I>, err: &'static str) -> TxParser<O, I>
    where I: Stream<Item = Value>
{
    parser(f).expected(err)
}

def_value_satisfy_parser_fn!(Tx, integer, i64, Value::as_integer);

fn value_to_namespaced_keyword(val: &Value) -> Option<NamespacedKeyword> {
    val.as_namespaced_keyword().map(|x| x.clone())
}
def_value_satisfy_parser_fn!(Tx, keyword, NamespacedKeyword, value_to_namespaced_keyword);

def_parser_fn!(Tx, entid, Value, Entid, input, {
    Tx::<I>::integer()
        .map(|x| Entid::Entid(x))
        .or(Tx::<I>::keyword().map(|x| Entid::Ident(x)))
        .parse_lazy(input)
        .into()
});

def_parser_fn!(Tx, lookup_ref, Value, LookupRef, input, {
    satisfy_map(|x: Value| if let Value::Vector(y) = x {
            let mut p = (Tx::<&[Value]>::entid(), any(), eof())
                .map(|(a, v, _)| LookupRef { a: a, v: v });
            let r = p.parse_lazy(&y[..]).into();
            match r {
                Ok((r, _)) => Some(r),
                _ => None,
            }
        } else {
            None
        })
        .parse_stream(input)
});

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

// TODO: abstract the "match Vector, parse internal stream" pattern to remove this boilerplate.
def_parser_fn!(Tx, add, Value, Entity, input, {
    satisfy_map(|x: Value| -> Option<Entity> {
            if let Value::Vector(y) = x {
                let mut p = (token(Value::NamespacedKeyword(NamespacedKeyword::new("db", "add"))),
                             Tx::<&[Value]>::entid_or_lookup_ref_or_temp_id(),
                             Tx::<&[Value]>::entid(),
                             // TODO: handle lookup-ref.
                             any(),
                             eof())
                    .map(|(_, e, a, v, _)| {
                        Entity::AddOrRetract {
                            op: OpType::Add,
                            e: e,
                            a: a,
                            v: v,
                        }
                    });
                // TODO: use ok() with a type annotation rather than explicit match.
                match p.parse_lazy(&y[..]).into() {
                    Ok((r, _)) => Some(r),
                    _ => None,
                }
            } else {
                None
            }
        })
        .parse_stream(input)
});

def_parser_fn!(Tx, retract, Value, Entity, input, {
    satisfy_map(|x: Value| -> Option<Entity> {
            if let Value::Vector(y) = x {
                let mut p = (token(Value::NamespacedKeyword(NamespacedKeyword::new("db", "retract"))),
                             Tx::<&[Value]>::entid_or_lookup_ref_or_temp_id(),
                             Tx::<&[Value]>::entid(),
                             // TODO: handle lookup-ref.
                             any(),
                             eof())
                    .map(|(_, e, a, v, _)| {
                        Entity::AddOrRetract {
                            op: OpType::Retract,
                            e: e,
                            a: a,
                            v: v,
                        }
                    });
                // TODO: use ok() with a type annotation rather than explicit match.
                match p.parse_lazy(&y[..]).into() {
                    Ok((r, _)) => Some(r),
                    _ => None,
                }
            } else {
                None
            }
        })
        .parse_stream(input)
});

def_parser_fn!(Tx, entity, Value, Entity, input, {
    let mut p = Tx::<I>::add()
        .or(Tx::<I>::retract());
    p.parse_stream(input)
});

def_parser_fn!(Tx, entities, Value, Vec<Entity>, input, {
    satisfy_map(|x: Value| -> Option<Vec<Entity>> {
            if let Value::Vector(y) = x {
                let mut p = (many(Tx::<&[Value]>::entity()), eof()).map(|(es, _)| es);
                // TODO: use ok() with a type annotation rather than explicit match.
                match p.parse_lazy(&y[..]).into() {
                    Ok((r, _)) => Some(r),
                    _ => None,
                }
            } else {
                None
            }
        })
        .parse_stream(input)
});

impl<I> Tx<I>
    where I: Stream<Item = Value>
{
    pub fn parse(input: I) -> Result<Vec<Entity>, combine::ParseError<I>> {
        (Tx::<I>::entities(), eof())
            .map(|(es, _)| es)
            .parse(input)
            .map(|x| x.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use combine::Parser;
    use edn::symbols::NamespacedKeyword;
    use edn::types::Value;
    use mentat_tx::entities::{Entid, EntidOrLookupRefOrTempId, Entity, LookupRef, OpType};

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
                       v: Value::Text("v".into()),
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
                       v: Value::Text("v".into()),
                   },
                       &[][..])));
    }

    #[test]
    fn test_lookup_ref() {
        let input = [Value::Vector(vec![kw("db", "add"),
                                        Value::Vector(vec![kw("test", "a1"),
                                                           Value::Text("v1".into())]),
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
                       v: Value::Text("v".into()),
                   },
                       &[][..])));
    }
}
