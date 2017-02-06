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
extern crate mentat_parser_utils;

#[macro_use]
extern crate mentat_query_parser;

use combine::{any, eof, many, optional, parser, satisfy_map, token, Parser, ParseResult, Stream};
use combine::combinator::{Expected, FnParser};
use edn::symbols::NamespacedKeyword;
use edn::types::Value;
use mentat_tx::entities::{Entid, EntidOrLookupRef, Entity, LookupRef, ValueOrLookupRef};
use mentat_parser_utils::ResultParser;

pub struct Tx<I>(::std::marker::PhantomData<fn(I) -> I>);

type TxParser<O, I> = Expected<FnParser<I, fn(I) -> ParseResult<O, I>>>;

fn fn_parser<O, I>(f: fn(I) -> ParseResult<O, I>, err: &'static str) -> TxParser<O, I>
    where I: Stream<Item = Value>
{
    parser(f).expected(err)
}

def_parser_fn!(Tx, integer, Value, i64, input, {
    return satisfy_map(|x: Value| if let Value::Integer(y) = x {
            Some(y)
        } else {
            None
        })
        .parse_stream(input);
});

impl<I> Tx<I>
    where I: Stream<Item = Value>
{
    /*
    fn integer() -> TxParser<i64, I> {
        fn_parser(Tx::<I>::integer_, "integer")
    }

    fn integer_(input: I) -> ParseResult<i64, I> {
        return satisfy_map(|x: Value| if let Value::Integer(y) = x {
                Some(y)
            } else {
                None
            })
            .parse_stream(input);
    }
    */

    fn keyword() -> TxParser<NamespacedKeyword, I> {
        fn_parser(Tx::<I>::keyword_, "keyword")
    }

    fn keyword_(input: I) -> ParseResult<NamespacedKeyword, I> {
        return satisfy_map(|x: Value| if let Value::NamespacedKeyword(y) = x {
                Some(y)
            } else {
                None
            })
            .parse_stream(input);
    }

    fn entid() -> TxParser<Entid, I> {
        fn_parser(Tx::<I>::entid_, "entid")
    }

    fn entid_(input: I) -> ParseResult<Entid, I> {
        let p = Tx::<I>::integer()
            .map(|x| Entid::Entid(x))
            .or(Tx::<I>::keyword().map(|x| Entid::Ident(x)))
            .parse_lazy(input)
            .into();
        return p;
    }

    fn lookup_ref() -> TxParser<LookupRef, I> {
        fn_parser(Tx::<I>::lookup_ref_, "lookup-ref")
    }

    fn lookup_ref_(input: I) -> ParseResult<LookupRef, I> {
        return satisfy_map(|x: Value| if let Value::Vector(y) = x {
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
            .parse_stream(input);
    }

    fn entid_or_lookup_ref() -> TxParser<EntidOrLookupRef, I> {
        fn_parser(Tx::<I>::entid_or_lookup_ref_, "entid|lookup-ref")
    }

    fn entid_or_lookup_ref_(input: I) -> ParseResult<EntidOrLookupRef, I> {
        let p = Tx::<I>::entid()
            .map(|x| EntidOrLookupRef::Entid(x))
            .or(Tx::<I>::lookup_ref().map(|x| EntidOrLookupRef::LookupRef(x)))
            .parse_lazy(input)
            .into();
        return p;
    }

    // TODO: abstract the "match Vector, parse internal stream" pattern to remove this boilerplate.
    fn add_(input: I) -> ParseResult<Entity, I> {
        return satisfy_map(|x: Value| -> Option<Entity> {
                if let Value::Vector(y) = x {
                    let mut p = (token(Value::NamespacedKeyword(NamespacedKeyword::new("db",
                                                                                       "add"))),
                                 Tx::<&[Value]>::entid_or_lookup_ref(),
                                 Tx::<&[Value]>::entid(),
                                 // TODO: handle lookup-ref.
                                 any(),
                                 // TODO: entid or special keyword :db/tx?
                                 optional(Tx::<&[Value]>::entid()),
                                 eof())
                        .map(|(_, e, a, v, tx, _)| {
                            Entity::Add {
                                e: e,
                                a: a,
                                v: ValueOrLookupRef::Value(v),
                                tx: tx,
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
            .parse_stream(input);
    }

    fn add() -> TxParser<Entity, I> {
        fn_parser(Tx::<I>::add_, "[:db/add e a v tx?]")
    }

    fn retract_(input: I) -> ParseResult<Entity, I> {
        return satisfy_map(|x: Value| -> Option<Entity> {
                if let Value::Vector(y) = x {
                    let mut p = (token(Value::NamespacedKeyword(NamespacedKeyword::new("db",
                                                                                       "retract"))),
                                 Tx::<&[Value]>::entid_or_lookup_ref(),
                                 Tx::<&[Value]>::entid(),
                                 // TODO: handle lookup-ref.
                                 any(),
                                 eof())
                        .map(|(_, e, a, v, _)| {
                            Entity::Retract {
                                e: e,
                                a: a,
                                v: ValueOrLookupRef::Value(v),
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
            .parse_stream(input);
    }

    fn retract() -> TxParser<Entity, I> {
        fn_parser(Tx::<I>::retract_, "[:db/retract e a v]")
    }

    fn retract_attribute_(input: I) -> ParseResult<Entity, I> {
        return satisfy_map(|x: Value| -> Option<Entity> {
                if let Value::Vector(y) = x {
                    let mut p = (token(Value::NamespacedKeyword(NamespacedKeyword::new("db", "retractAttribute"))),
                                 Tx::<&[Value]>::entid_or_lookup_ref(),
                                 Tx::<&[Value]>::entid(),
                                 eof())
                        .map(|(_, e, a, _)| Entity::RetractAttribute { e: e, a: a });
                    // TODO: use ok() with a type annotation rather than explicit match.
                    match p.parse_lazy(&y[..]).into() {
                        Ok((r, _)) => Some(r),
                        _ => None,
                    }
                } else {
                    None
                }
            })
            .parse_stream(input);
    }

    fn retract_attribute() -> TxParser<Entity, I> {
        fn_parser(Tx::<I>::retract_attribute_, "[:db/retractAttribute e a]")
    }

    fn retract_entity_(input: I) -> ParseResult<Entity, I> {
        return satisfy_map(|x: Value| -> Option<Entity> {
                if let Value::Vector(y) = x {
                    let mut p =
                        (token(Value::NamespacedKeyword(NamespacedKeyword::new("db",
                                                                               "retractEntity"))),
                         Tx::<&[Value]>::entid_or_lookup_ref(),
                         eof())
                            .map(|(_, e, _)| Entity::RetractEntity { e: e });
                    // TODO: use ok() with a type annotation rather than explicit match.
                    match p.parse_lazy(&y[..]).into() {
                        Ok((r, _)) => Some(r),
                        _ => None,
                    }
                } else {
                    None
                }
            })
            .parse_stream(input);
    }

    fn retract_entity() -> TxParser<Entity, I> {
        fn_parser(Tx::<I>::retract_entity_, "[:db/retractEntity e]")
    }

    fn entity_(input: I) -> ParseResult<Entity, I> {
        let mut p = Tx::<I>::add()
            .or(Tx::<I>::retract())
            .or(Tx::<I>::retract_attribute())
            .or(Tx::<I>::retract_entity());
        p.parse_stream(input)
    }

    fn entity() -> TxParser<Entity, I> {
        fn_parser(Tx::<I>::entity_,
                  "[:db/add|:db/retract|:db/retractAttribute|:db/retractEntity ...]")
    }

    fn entities_(input: I) -> ParseResult<Vec<Entity>, I> {
        return satisfy_map(|x: Value| -> Option<Vec<Entity>> {
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
            .parse_stream(input);
    }

    fn entities() -> TxParser<Vec<Entity>, I> {
        fn_parser(Tx::<I>::entities_,
                  "[[:db/add|:db/retract|:db/retractAttribute|:db/retractEntity ...]*]")
    }

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
                   Ok((Entity::Add {
                       e: EntidOrLookupRef::Entid(Entid::Ident(NamespacedKeyword::new("test",
                                                                                      "entid"))),
                       a: Entid::Ident(NamespacedKeyword::new("test", "a")),
                       v: ValueOrLookupRef::Value(Value::Text("v".into())),
                       tx: None,
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
                   Ok((Entity::Retract {
                       e: EntidOrLookupRef::Entid(Entid::Entid(101)),
                       a: Entid::Ident(NamespacedKeyword::new("test", "a")),
                       v: ValueOrLookupRef::Value(Value::Text("v".into())),
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
                   Ok((Entity::Add {
                       e: EntidOrLookupRef::LookupRef(LookupRef {
                           a: Entid::Ident(NamespacedKeyword::new("test", "a1")),
                           v: Value::Text("v1".into()),
                       }),
                       a: Entid::Ident(NamespacedKeyword::new("test", "a")),
                       v: ValueOrLookupRef::Value(Value::Text("v".into())),
                       tx: None,
                   },
                       &[][..])));
    }
}
