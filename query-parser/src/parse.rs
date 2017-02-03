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
extern crate mentat_parser_utils;
extern crate mentat_query;

use self::mentat_parser_utils::ResultParser;
use self::combine::{eof, many1, optional, parser, satisfy_map, Parser, ParseResult, Stream};
use self::combine::combinator::{choice, try};
use self::mentat_query::{
    Element,
    FindQuery,
    FindSpec,
    FromValue,
    Pattern,
    PatternNonValuePlace,
    PatternValuePlace,
    SrcVar,
    Variable,
    WhereClause,
};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NotAVariableError(pub edn::Value);

#[allow(dead_code)]
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum FindParseError {
    Err,
}

#[allow(dead_code)]
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum WhereParseError {
    Err,
}

#[derive(Clone,Debug,Eq,PartialEq)]
pub enum QueryParseError {
    InvalidInput(edn::Value),
    EdnParseError(edn::parse::ParseError),
    MissingField(edn::Keyword),
    FindParseError(FindParseError),
    WhereParseError(WhereParseError),
    WithParseError(NotAVariableError),
}

impl From<edn::parse::ParseError> for QueryParseError {
    fn from(err: edn::parse::ParseError) -> QueryParseError {
        QueryParseError::EdnParseError(err)
    }
}

impl From<WhereParseError> for QueryParseError {
    fn from(err: WhereParseError) -> QueryParseError {
        QueryParseError::WhereParseError(err)
    }
}

impl From<NotAVariableError> for QueryParseError {
    fn from(err: NotAVariableError) -> QueryParseError {
        QueryParseError::WithParseError(err)
    }
}

pub type WhereParseResult = Result<Vec<WhereClause>, WhereParseError>;
pub type FindParseResult = Result<FindSpec, FindParseError>;
pub type QueryParseResult = Result<FindQuery, QueryParseError>;


pub struct Query<I>(::std::marker::PhantomData<fn(I) -> I>);

impl<I> Query<I>
    where I: Stream<Item = edn::Value>
{
    fn to_parsed_value<T>(r: ParseResult<T, I>) -> Option<T> {
        r.ok().map(|x| x.0)
    }
}

def_value_satisfy_parser_fn!(Query, variable, Variable, Variable::from_value);
def_value_satisfy_parser_fn!(Query, source_var, SrcVar, SrcVar::from_value);

pub struct Where<I>(::std::marker::PhantomData<fn(I) -> I>);

def_value_satisfy_parser_fn!(Where, pattern_value_place, PatternValuePlace, PatternValuePlace::from_value);
def_value_satisfy_parser_fn!(Where, pattern_non_value_place, PatternNonValuePlace, PatternNonValuePlace::from_value);

def_value_parser_fn!(Where, pattern, Pattern, input, {
    satisfy_map(|x: edn::Value| {
        if let edn::Value::Vector(y) = x {
            // While *technically* Datomic allows you to have a query like:
            // [:find … :where [[?x]]]
            // We don't -- we require at list e, a.
            let mut p =
                (optional(Query::source_var()),                 // src
                 Where::pattern_non_value_place(),              // e
                 Where::pattern_non_value_place(),              // a
                 optional(Where::pattern_value_place()),        // v
                 optional(Where::pattern_non_value_place()),    // tx
                 eof())
                .map(|(src, e, a, v, tx, _)| {
                    let v = v.unwrap_or(PatternValuePlace::Placeholder);
                    let tx = tx.unwrap_or(PatternNonValuePlace::Placeholder);

                    // Pattern::new takes care of reversal of reversed
                    // attributes: [?x :foo/_bar ?y] turns into
                    // [?y :foo/bar ?x].
                    Pattern::new(src, e, a, v, tx)
                });

            // This is a bit messy: the inner conversion to a Pattern can
            // fail if the input is something like
            //
            // ```edn
            // [?x :foo/_reversed 23.4]
            // ```
            //
            // because
            //
            // ```edn
            // [23.4 :foo/reversed ?x]
            // ```
            //
            // is nonsense. That leaves us with nested optionals; we unwrap them here.
            let r: ParseResult<Option<Pattern>, _> = p.parse_lazy(&y[..]).into();
            let v: Option<Option<Pattern>> = r.ok().map(|x| x.0);
            v.unwrap_or(None)
        } else {
            None
        }
    }).parse_stream(input)
});

def_value_parser_fn!(Where, clauses, Vec<WhereClause>, input, {
    // Right now we only support patterns. See #239 for more.
    (many1::<Vec<Pattern>, _>(Where::pattern()), eof())
        .map(|(patterns, _)| {
            patterns.into_iter().map(WhereClause::Pattern).collect()
        })
    .parse_stream(input)
});

pub struct Find<I>(::std::marker::PhantomData<fn(I) -> I>);

def_value_parser_fn!(Find, period, (), input, {
    matches_plain_symbol!(".", input)
});

def_value_parser_fn!(Find, ellipsis, (), input, {
    matches_plain_symbol!("...", input)
});

def_value_parser_fn!(Find, find_scalar, FindSpec, input, {
    (Query::variable(), Find::period(), eof())
        .map(|(var, _, _)| FindSpec::FindScalar(Element::Variable(var)))
        .parse_stream(input)
});

def_value_parser_fn!(Find, find_coll, FindSpec, input, {
    satisfy_unwrap!(edn::Value::Vector, y, {
            let mut p = (Query::variable(), Find::ellipsis(), eof())
                .map(|(var, _, _)| FindSpec::FindColl(Element::Variable(var)));
            let r: ParseResult<FindSpec, _> = p.parse_lazy(&y[..]).into();
            Query::to_parsed_value(r)
        })
        .parse_stream(input)
});

def_value_parser_fn!(Find, elements, Vec<Element>, input, {
    (many1::<Vec<Variable>, _>(Query::variable()), eof())
        .map(|(vars, _)| {
            vars.into_iter()
                .map(Element::Variable)
                .collect()
        })
        .parse_stream(input)
});

def_value_parser_fn!(Find, find_rel, FindSpec, input, {
    Find::elements().map(FindSpec::FindRel).parse_stream(input)
});

def_value_parser_fn!(Find, find_tuple, FindSpec, input, {
    satisfy_unwrap!(edn::Value::Vector, y, {
        let r: ParseResult<FindSpec, _> =
            Find::elements().map(FindSpec::FindTuple).parse_lazy(&y[..]).into();
        Query::to_parsed_value(r)
    })
    .parse_stream(input)
});

def_value_parser_fn!(Find, find, FindSpec, input, {
    // Any one of the four specs might apply, so we combine them with `choice`.
    // Our parsers consume input, so we need to wrap them in `try` so that they
    // operate independently.
    choice::<[&mut Parser<Input = I, Output = FindSpec>; 4], _>
        ([&mut try(Find::find_scalar()),
          &mut try(Find::find_coll()),
          &mut try(Find::find_tuple()),
          &mut try(Find::find_rel())])
        .parse_stream(input)
});

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
#[allow(dead_code)]
pub fn find_seq_to_find_spec(find: &[edn::Value]) -> FindParseResult {
    Find::find()
        .parse(find)
        .map(|x| x.0)
        .map_err(|_| FindParseError::Err)
}

#[allow(dead_code)]
pub fn clause_seq_to_patterns(clauses: &[edn::Value]) -> WhereParseResult {
    Where::clauses()
        .parse(clauses)
        .map(|x| x.0)
        .map_err(|_| WhereParseError::Err)
}

#[cfg(test)]
mod test {
    extern crate combine;
    extern crate edn;
    extern crate mentat_query;
    extern crate ordered_float;

    use self::combine::Parser;
    use self::ordered_float::OrderedFloat;
    use self::mentat_query::{
        Element,
        FindSpec,
        NonIntegerConstant,
        Pattern,
        PatternNonValuePlace,
        PatternValuePlace,
        SrcVar,
        Variable,
    };

    use super::*;

    #[test]
    fn test_pattern_mixed() {
        let e = edn::PlainSymbol::new("_");
        let a = edn::NamespacedKeyword::new("foo", "bar");
        let v = OrderedFloat(99.9);
        let tx = edn::PlainSymbol::new("?tx");
        let input = [edn::Value::Vector(
            vec!(edn::Value::PlainSymbol(e.clone()),
                 edn::Value::NamespacedKeyword(a.clone()),
                 edn::Value::Float(v.clone()),
                 edn::Value::PlainSymbol(tx.clone())))];
        assert_parses_to!(Where::pattern, input, Pattern {
            source: None,
            entity: PatternNonValuePlace::Placeholder,
            attribute: PatternNonValuePlace::Ident(a),
            value: PatternValuePlace::Constant(NonIntegerConstant::Float(v)),
            tx: PatternNonValuePlace::Variable(Variable(tx)),
        });
    }

    #[test]
    fn test_pattern_vars() {
        let s = edn::PlainSymbol::new("$x");
        let e = edn::PlainSymbol::new("?e");
        let a = edn::PlainSymbol::new("?a");
        let v = edn::PlainSymbol::new("?v");
        let tx = edn::PlainSymbol::new("?tx");
        let input = [edn::Value::Vector(
            vec!(edn::Value::PlainSymbol(s.clone()),
                 edn::Value::PlainSymbol(e.clone()),
                 edn::Value::PlainSymbol(a.clone()),
                 edn::Value::PlainSymbol(v.clone()),
                 edn::Value::PlainSymbol(tx.clone())))];
        assert_parses_to!(Where::pattern, input, Pattern {
            source: Some(SrcVar::NamedSrc("x".to_string())),
            entity: PatternNonValuePlace::Variable(Variable(e)),
            attribute: PatternNonValuePlace::Variable(Variable(a)),
            value: PatternValuePlace::Variable(Variable(v)),
            tx: PatternNonValuePlace::Variable(Variable(tx)),
        });
    }

    #[test]
    fn test_pattern_reversed_invalid() {
        let e = edn::PlainSymbol::new("_");
        let a = edn::NamespacedKeyword::new("foo", "_bar");
        let v = OrderedFloat(99.9);
        let tx = edn::PlainSymbol::new("?tx");
        let input = [edn::Value::Vector(
            vec!(edn::Value::PlainSymbol(e.clone()),
                 edn::Value::NamespacedKeyword(a.clone()),
                 edn::Value::Float(v.clone()),
                 edn::Value::PlainSymbol(tx.clone())))];

        let mut par = Where::pattern();
        let result = par.parse(&input[..]);
        assert!(matches!(result, Err(_)), "Expected a parse error.");
    }

    #[test]
    fn test_pattern_reversed() {
        let e = edn::PlainSymbol::new("_");
        let a = edn::NamespacedKeyword::new("foo", "_bar");
        let v = edn::PlainSymbol::new("?v");
        let tx = edn::PlainSymbol::new("?tx");
        let input = [edn::Value::Vector(
            vec!(edn::Value::PlainSymbol(e.clone()),
                 edn::Value::NamespacedKeyword(a.clone()),
                 edn::Value::PlainSymbol(v.clone()),
                 edn::Value::PlainSymbol(tx.clone())))];

        // Note that the attribute is no longer reversed, and the entity and value have
        // switched places.
        assert_parses_to!(Where::pattern, input, Pattern {
            source: None,
            entity: PatternNonValuePlace::Variable(Variable(v)),
            attribute: PatternNonValuePlace::Ident(edn::NamespacedKeyword::new("foo", "bar")),
            value: PatternValuePlace::Placeholder,
            tx: PatternNonValuePlace::Variable(Variable(tx)),
        });
    }

    #[test]
    fn test_find_sp_variable() {
        let sym = edn::PlainSymbol::new("?x");
        let input = [edn::Value::PlainSymbol(sym.clone())];
        assert_parses_to!(Query::variable, input, Variable(sym));
    }

    #[test]
    fn test_find_scalar() {
        let sym = edn::PlainSymbol::new("?x");
        let period = edn::PlainSymbol::new(".");
        let input = [edn::Value::PlainSymbol(sym.clone()), edn::Value::PlainSymbol(period.clone())];
        assert_parses_to!(Find::find_scalar,
                          input,
                          FindSpec::FindScalar(Element::Variable(Variable(sym))));
    }

    #[test]
    fn test_find_coll() {
        let sym = edn::PlainSymbol::new("?x");
        let period = edn::PlainSymbol::new("...");
        let input = [edn::Value::Vector(vec![edn::Value::PlainSymbol(sym.clone()),
                                             edn::Value::PlainSymbol(period.clone())])];
        assert_parses_to!(Find::find_coll,
                          input,
                          FindSpec::FindColl(Element::Variable(Variable(sym))));
    }

    #[test]
    fn test_find_rel() {
        let vx = edn::PlainSymbol::new("?x");
        let vy = edn::PlainSymbol::new("?y");
        let input = [edn::Value::PlainSymbol(vx.clone()), edn::Value::PlainSymbol(vy.clone())];
        assert_parses_to!(Find::find_rel,
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
        assert_parses_to!(Find::find_tuple,
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
