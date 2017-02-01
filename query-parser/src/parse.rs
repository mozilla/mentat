// Copyright 2016 Mozilla
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

extern crate combine;
extern crate edn;
extern crate mentat_query;

use self::combine::{eof, many1, parser, satisfy_map, Parser, ParseResult, Stream};
use self::combine::combinator::{Expected, FnParser, choice, try};
use self::edn::Value::PlainSymbol;
use self::mentat_query::{Element, FromValue, FindSpec, Variable};

use super::error::{FindParseError, FindParseResult};

pub struct FindSp<I>(::std::marker::PhantomData<fn(I) -> I>);

type FindSpParser<O, I> = Expected<FnParser<I, fn(I) -> ParseResult<O, I>>>;

fn fn_parser<O, I>(f: fn(I) -> ParseResult<O, I>, err: &'static str) -> FindSpParser<O, I>
    where I: Stream<Item = edn::Value>
{
    parser(f).expected(err)
}

/// `satisfy_unwrap!` makes it a little easier to implement a `satisfy_map`
/// body that matches a particular `Value` enum case, otherwise returning `None`.
macro_rules! satisfy_unwrap {
    ( $cas: path, $var: ident, $body: block ) => {
        satisfy_map(|x: edn::Value| if let $cas($var) = x $body else { None })
    }
}

impl<I> FindSp<I>
    where I: Stream<Item = edn::Value>
{
    fn variable() -> FindSpParser<Variable, I> {
        fn_parser(FindSp::<I>::variable_, "variable")
    }

    fn variable_(input: I) -> ParseResult<Variable, I> {
        satisfy_map(|x: edn::Value| Variable::from_value(&x)).parse_stream(input)
    }

    fn period() -> FindSpParser<(), I> {
        fn_parser(FindSp::<I>::period_, "period")
    }

    fn period_(input: I) -> ParseResult<(), I> {
        satisfy_map(|x: edn::Value| {
                if let PlainSymbol(ref s) = x {
                    if s.0.as_str() == "." {
                        return Some(());
                    }
                }
                return None;
            })
            .parse_stream(input)
    }

    fn ellipsis() -> FindSpParser<(), I> {
        fn_parser(FindSp::<I>::ellipsis_, "ellipsis")
    }

    fn ellipsis_(input: I) -> ParseResult<(), I> {
        satisfy_map(|x: edn::Value| {
                if let PlainSymbol(ref s) = x {
                    if s.0.as_str() == "..." {
                        return Some(());
                    }
                }
                return None;
            })
            .parse_stream(input)
    }

    fn find_scalar() -> FindSpParser<FindSpec, I> {
        fn_parser(FindSp::<I>::find_scalar_, "find_scalar")
    }

    fn find_scalar_(input: I) -> ParseResult<FindSpec, I> {
        (FindSp::variable(), FindSp::period(), eof())
            .map(|(var, _, _)| FindSpec::FindScalar(Element::Variable(var)))
            .parse_stream(input)
    }

    fn find_coll() -> FindSpParser<FindSpec, I> {
        fn_parser(FindSp::<I>::find_coll_, "find_coll")
    }

    fn find_coll_(input: I) -> ParseResult<FindSpec, I> {
        satisfy_unwrap!(edn::Value::Vector, y, {
                let mut p = (FindSp::variable(), FindSp::ellipsis(), eof())
                    .map(|(var, _, _)| FindSpec::FindColl(Element::Variable(var)));
                let r: ParseResult<FindSpec, _> = p.parse_lazy(&y[..]).into();
                FindSp::to_parsed_value(r)
            })
            .parse_stream(input)
    }

    fn elements() -> FindSpParser<Vec<Element>, I> {
        fn_parser(FindSp::<I>::elements_, "elements")
    }

    fn elements_(input: I) -> ParseResult<Vec<Element>, I> {
        (many1::<Vec<Variable>, _>(FindSp::variable()), eof())
            .map(|(vars, _)| {
                vars.into_iter()
                    .map(Element::Variable)
                    .collect()
            })
            .parse_stream(input)
    }

    fn find_rel() -> FindSpParser<FindSpec, I> {
        fn_parser(FindSp::<I>::find_rel_, "find_rel")
    }

    fn find_rel_(input: I) -> ParseResult<FindSpec, I> {
        FindSp::elements().map(FindSpec::FindRel).parse_stream(input)
    }

    fn find_tuple() -> FindSpParser<FindSpec, I> {
        fn_parser(FindSp::<I>::find_tuple_, "find_tuple")
    }

    fn find_tuple_(input: I) -> ParseResult<FindSpec, I> {
        satisfy_unwrap!(edn::Value::Vector, y, {
                let r: ParseResult<FindSpec, _> =
                    FindSp::elements().map(FindSpec::FindTuple).parse_lazy(&y[..]).into();
                FindSp::to_parsed_value(r)
            })
            .parse_stream(input)
    }

    fn find() -> FindSpParser<FindSpec, I> {
        fn_parser(FindSp::<I>::find_, "find")
    }

    fn find_(input: I) -> ParseResult<FindSpec, I> {
        // Any one of the four specs might apply, so we combine them with `choice`.
        // Our parsers consume input, so we need to wrap them in `try` so that they
        // operate independently.
        choice::<[&mut Parser<Input = I, Output = FindSpec>; 4],
                 _>([&mut try(FindSp::find_scalar()),
                     &mut try(FindSp::find_coll()),
                     &mut try(FindSp::find_tuple()),
                     &mut try(FindSp::find_rel())])
            .parse_stream(input)
    }

    fn to_parsed_value<T>(r: ParseResult<T, I>) -> Option<T> {
        r.ok().map(|x| x.0)
    }
}

// Parse a sequence of values into one of four find specs.
//
// `:find` must be an array of plain var symbols (?foo), pull expressions, and aggregates.
// For now we only support variables and the annotations necessary to declare which
// flavor of :find we want:
//
//
//     `?x ?y ?z  `     = FindRel
//     `[?x ...]  `     = FindColl
//     `?x .      `     = FindScalar
//     `[?x ?y ?z]`     = FindTuple
//
pub fn find_seq_to_find_spec(find: &[edn::Value]) -> FindParseResult {
    FindSp::find()
        .parse(find)
        .map(|x| x.0)
        .map_err(|_| FindParseError::Err)
}

#[cfg(test)]
mod test {
    extern crate combine;
    extern crate edn;
    extern crate mentat_query;

    use self::combine::Parser;
    use self::mentat_query::{Element, FindSpec, Variable};

    use super::*;

    #[test]
    fn test_find_sp_variable() {
        let sym = edn::PlainSymbol::new("?x");
        let input = [edn::Value::PlainSymbol(sym.clone())];
        assert_parses_to!(FindSp::variable, input, Variable(sym));
    }

    #[test]
    fn test_find_scalar() {
        let sym = edn::PlainSymbol::new("?x");
        let period = edn::PlainSymbol::new(".");
        let input = [edn::Value::PlainSymbol(sym.clone()), edn::Value::PlainSymbol(period.clone())];
        assert_parses_to!(FindSp::find_scalar,
                          input,
                          FindSpec::FindScalar(Element::Variable(Variable(sym))));
    }

    #[test]
    fn test_find_coll() {
        let sym = edn::PlainSymbol::new("?x");
        let period = edn::PlainSymbol::new("...");
        let input = [edn::Value::Vector(vec![edn::Value::PlainSymbol(sym.clone()),
                                             edn::Value::PlainSymbol(period.clone())])];
        assert_parses_to!(FindSp::find_coll,
                          input,
                          FindSpec::FindColl(Element::Variable(Variable(sym))));
    }

    #[test]
    fn test_find_rel() {
        let vx = edn::PlainSymbol::new("?x");
        let vy = edn::PlainSymbol::new("?y");
        let input = [edn::Value::PlainSymbol(vx.clone()), edn::Value::PlainSymbol(vy.clone())];
        assert_parses_to!(FindSp::find_rel,
                          input,
                          FindSpec::FindRel(vec![Element::Variable(Variable(vx)),
                                                 Element::Variable(Variable(vy))]));
    }

    #[test]
    fn test_find_tuple() {
        let vx = edn::PlainSymbol::new("?x");
        let vy = edn::PlainSymbol::new("?y");
        let input = [edn::Value::Vector(vec![edn::Value::PlainSymbol(vx.clone()),
                                             edn::Value::PlainSymbol(vy.clone())])];
        assert_parses_to!(FindSp::find_tuple,
                          input,
                          FindSpec::FindTuple(vec![Element::Variable(Variable(vx)),
                                                   Element::Variable(Variable(vy))]));
    }

    #[test]
    fn test_find_processing() {
        let vx = edn::PlainSymbol::new("?x");
        let vy = edn::PlainSymbol::new("?y");
        let ellipsis = edn::PlainSymbol::new("...");
        let period = edn::PlainSymbol::new(".");

        let scalar = [edn::Value::PlainSymbol(vx.clone()), edn::Value::PlainSymbol(period.clone())];
        let tuple = [edn::Value::Vector(vec![edn::Value::PlainSymbol(vx.clone()),
                                             edn::Value::PlainSymbol(vy.clone())])];
        let coll = [edn::Value::Vector(vec![edn::Value::PlainSymbol(vx.clone()),
                                            edn::Value::PlainSymbol(ellipsis.clone())])];
        let rel = [edn::Value::PlainSymbol(vx.clone()), edn::Value::PlainSymbol(vy.clone())];

        assert_eq!(Ok(FindSpec::FindScalar(Element::Variable(Variable(vx.clone())))),
                   find_seq_to_find_spec(&scalar));
        assert_eq!(Ok(FindSpec::FindTuple(vec![Element::Variable(Variable(vx.clone())),
                                               Element::Variable(Variable(vy.clone()))])),
                   find_seq_to_find_spec(&tuple));
        assert_eq!(Ok(FindSpec::FindColl(Element::Variable(Variable(vx.clone())))),
                   find_seq_to_find_spec(&coll));
        assert_eq!(Ok(FindSpec::FindRel(vec![Element::Variable(Variable(vx.clone())),
                                             Element::Variable(Variable(vy.clone()))])),
                   find_seq_to_find_spec(&rel));
    }
}
