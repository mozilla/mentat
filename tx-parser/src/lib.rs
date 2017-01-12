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

use combine::{any, eof, many, optional, parser, satisfy_map, token, Parser, ParseResult, Stream};
use combine::combinator::{Expected, FnParser};
use edn::types::Value;
use mentat_tx::entities::*;

// TODO: implement combine::Positioner on Value.  We can't do this
// right now because Value is defined in edn and the trait is defined
// in combine.

// #[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
// pub struct ValuePosition {
//     pub position: usize,
// }

// impl fmt::Display for ValuePosition {
//     fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
//         write!(f, "value position: {}", self.position)
//     }
// }

// impl combine::primitives::Positioner for Value {
//     type Position = ValuePosition;
//     fn start() -> ValuePosition {
//         ValuePosition { position: 1 }
//     }
//     fn update(&self, position: &mut ValuePosition) {
//         position.position += 1;
//     }
// }

pub struct Tx<I>(::std::marker::PhantomData<fn(I) -> I>);

type TxParser<O, I> = Expected<FnParser<I, fn(I) -> ParseResult<O, I>>>;

fn fn_parser<O, I>(f: fn(I) -> ParseResult<O, I>, err: &'static str) -> TxParser<O, I>
    where I: Stream<Item = Value>
{
    parser(f).expected(err)
}

impl<I> Tx<I>
    where I: Stream<Item = Value>
{
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

    fn keyword() -> TxParser<String, I> {
        fn_parser(Tx::<I>::keyword_, "keyword")
    }

    fn keyword_(input: I) -> ParseResult<String, I> {
        return satisfy_map(|x: Value| if let Value::Keyword(y) = x {
                Some(y)
            } else {
                None
            })
            .parse_stream(input);
    }

    fn entid() -> TxParser<EntId, I> {
        fn_parser(Tx::<I>::entid_, "entid")
    }

    fn entid_(input: I) -> ParseResult<EntId, I> {
        let p = Tx::<I>::integer()
            .map(|x| EntId::EntId(x))
            .or(Tx::<I>::keyword().map(|x| EntId::Ident(x)))
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

    fn entid_or_lookup_ref() -> TxParser<EntIdOrLookupRef, I> {
        fn_parser(Tx::<I>::entid_or_lookup_ref_, "entid|lookup-ref")
    }

    fn entid_or_lookup_ref_(input: I) -> ParseResult<EntIdOrLookupRef, I> {
        let p = Tx::<I>::entid()
            .map(|x| EntIdOrLookupRef::EntId(x))
            .or(Tx::<I>::lookup_ref().map(|x| EntIdOrLookupRef::LookupRef(x)))
            .parse_lazy(input)
            .into();
        return p;
    }

    // TODO: abstract the "match Vector, parse internal stream" pattern to remove this boilerplate.
    fn add_(input: I) -> ParseResult<Entity, I> {
        return satisfy_map(|x: Value| -> Option<Entity> {
                if let Value::Vector(y) = x {
                    let mut p = (token(Value::Keyword("db/add".into())),
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
                    let mut p = (token(Value::Keyword("db/retract".into())),
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
                    let mut p = (token(Value::Keyword("db/retractAttribute".into())),
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
                    let mut p = (token(Value::Keyword("db/retractEntity".into())),
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

    pub fn entity() -> TxParser<Entity, I> {
        fn_parser(Tx::<I>::entity_,
                  "[:db/add|:db/retract|:db/retractAttribute|:db/retractEntity ...]")
    }

    fn entities_(input: I) -> ParseResult<Vec<Entity>, I> {
        return satisfy_map(|x: Value| -> Option<Vec<Entity>> {
                if let Value::Vector(y) = x {
                    let mut p = (many(Tx::<&[Value]>::entity()), eof())
                        .map(|(es, _)| es);
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

    pub fn entities() -> TxParser<Vec<Entity>, I> {
        fn_parser(Tx::<I>::entities_,
                  "[[:db/add|:db/retract|:db/retractAttribute|:db/retractEntity ...]*]")
    }
}
