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

use std; // To refer to std::result::Result.

use self::combine::{eof, many, many1, optional, parser, satisfy, satisfy_map, Parser, ParseResult, Stream};
use self::combine::combinator::{choice, or, try};

use self::mentat_parser_utils::{
    ResultParser,
    ValueParseError,
};

use self::mentat_parser_utils::value_and_span::Stream as ValueStream;
use self::mentat_parser_utils::value_and_span::{
    Item,
    OfExactlyParsing,
    keyword_map,
    list,
    map,
    seq,
    vector,
};

use self::mentat_query::{
    Element,
    FindQuery,
    FindSpec,
    FnArg,
    FromValue,
    OrJoin,
    OrWhereClause,
    NotJoin,
    WhereNotClause,
    Pattern,
    PatternNonValuePlace,
    PatternValuePlace,
    Predicate,
    PredicateFn,
    SrcVar,
    UnifyVars,
    Variable,
    WhereClause,
};

error_chain! {
    types {
        Error, ErrorKind, ResultExt, Result;
    }

    foreign_links {
        EdnParseError(edn::parse::ParseError);
    }

    errors {
        NotAVariableError(value: edn::ValueAndSpan) {
            description("not a variable")
            display("not a variable: '{}'", value)
        }

        FindParseError(e: combine::ParseError<ValueStream>) {
            description(":find parse error")
            display(":find parse error")
        }

        WhereParseError(e: combine::ParseError<ValueStream>) {
            description(":where parse error")
            display(":where parse error")
        }

        // Not yet used.
        WithParseError {
            description(":with parse error")
            display(":with parse error")
        }

        InvalidInputError(value: edn::Value) {
            description("invalid input")
            display("invalid input: '{}'", value)
        }

        MissingFieldError(value: edn::Keyword) {
            description("missing field")
            display("missing field: '{}'", value)
        }
    }
}

pub struct Query;

def_parser!(Query, variable, Variable, {
    satisfy_map(Variable::from_value)
});

def_parser!(Query, source_var, SrcVar, {
    satisfy_map(SrcVar::from_value)
});

// TODO: interning.
def_parser!(Query, predicate_fn, PredicateFn, {
    satisfy_map(PredicateFn::from_value)
});

def_parser!(Query, fn_arg, FnArg, {
    satisfy_map(FnArg::from_value)
});

def_parser!(Query, arguments, Vec<FnArg>, {
    (many::<Vec<FnArg>, _>(Query::fn_arg()))
});

pub struct Where;

def_parser!(Where, pattern_value_place, PatternValuePlace,  {
    satisfy_map(PatternValuePlace::from_value)
});

def_parser!(Where, pattern_non_value_place, PatternNonValuePlace, {
    satisfy_map(PatternNonValuePlace::from_value)
});

def_matches_plain_symbol!(Where, and, "and");

def_matches_plain_symbol!(Where, or, "or");

def_matches_plain_symbol!(Where, or_join, "or-join");

def_matches_plain_symbol!(Where, or_join, "not");

def_matches_plain_symbol!(Where, or_join, "not-join");

def_parser!(Where, rule_vars, Vec<Variable>, {
    seq()
        .of_exactly(many1(Query::variable()))
});

def_parser!(Where, or_pattern_clause, OrWhereClause, {
    Where::clause().map(|clause| OrWhereClause::Clause(clause))
});

def_parser!(Where, or_and_clause, OrWhereClause, {
    seq()
        .of_exactly(Where::and()
            .with(many1(Where::clause()))
            .map(OrWhereClause::And))
});

def_parser!(Where, or_where_clause, OrWhereClause, {
    choice([Where::or_pattern_clause(), Where::or_and_clause()])
});

def_parser!(Where, or_clause, WhereClause, {
    seq()
        .of_exactly(Where::or()
            .with(many1(Where::or_where_clause()))
            .map(|clauses| {
                WhereClause::OrJoin(
                    OrJoin {
                        unify_vars: UnifyVars::Implicit,
                        clauses: clauses,
                    })
            }))
});

def_parser!(Where, or_join_clause, WhereClause, {
    seq()
        .of_exactly(Where::or_join()
            .with(Where::rule_vars())
            .and(many1(Where::or_where_clause()))
            .map(|(vars, clauses)| {
                WhereClause::OrJoin(
                    OrJoin {
                        unify_vars: UnifyVars::Explicit(vars),
                        clauses: clauses,
                    })
            }))
});

def_value_parser_fn!(Where, not_pattern_clause, WhereNotClause, input, {
    Where::clause().map(|clause| WhereNotClause::Clause(clause)).parse_stream(input)
});

def_value_parser_fn!(Where, where_not_clause, WhereNotClause, input, {
    choice([Where::not_pattern_clause()]).parse_stream(input)
});

def_value_parser_fn!(Where, not_clause, WhereClause, input, {
    satisfy_map(|x: edn::Value| {
        seq(x).and_then(|items| {
            let mut p = Where::not()
                        .with(many1(Where::where_not_clause()))
                        .skip(eof())
                        .map(|clauses| {
                            WhereClause::NotJoin(
                               NotJoin {
                                   unify_vars: UnifyVars::Implicit,
                                   clauses: clauses,
                               })
                        });
            let r: ParseResult<WhereClause, _> = p.parse_lazy(&items[..]).into();
            Query::to_parsed_value(r)
        })
    }).parse_stream(input)
});

def_value_parser_fn!(Where, not_join_clause, WhereClause, input, {
    satisfy_map(|x: edn::Value| {
        seq(x).and_then(|items| {
            let mut p = Where::not_join()
                        .with(Where::rule_vars())
                        .and(many1(Where::where_not_clause()))
                        .skip(eof())
                        .map(|(vars, clauses)| {
                            WhereClause::NotJoin(
                               NotJoin {
                                   unify_vars: UnifyVars::Explicit(vars),
                                   clauses: clauses,
                               })
                        });
            let r: ParseResult<WhereClause, _> = p.parse_lazy(&items[..]).into();
            Query::to_parsed_value(r)
        })
    }).parse_stream(input)
});

/// A vector containing just a parenthesized filter expression.
def_parser!(Where, pred, WhereClause, {
    // Accept either a nested list or a nested vector here:
    // `[(foo ?x ?y)]` or `[[foo ?x ?y]]`
    vector()
        .of_exactly(seq()
            .of_exactly((Query::predicate_fn(), Query::arguments())
                .map(|(f, args)| {
                    WhereClause::Pred(
                        Predicate {
                            operator: f.0,
                            args: args,
                        })
                })))
});

def_parser!(Where, pattern, WhereClause, {
    vector()
        .of_exactly(
            // While *technically* Datomic allows you to have a query like:
            // [:find … :where [[?x]]]
            // We don't -- we require at least e, a.
            (optional(Query::source_var()),              // src
             Where::pattern_non_value_place(),           // e
             Where::pattern_non_value_place(),           // a
             optional(Where::pattern_value_place()),     // v
             optional(Where::pattern_non_value_place())) // tx
                .and_then(|(src, e, a, v, tx)| {
                    let v = v.unwrap_or(PatternValuePlace::Placeholder);
                    let tx = tx.unwrap_or(PatternNonValuePlace::Placeholder);

                    // Pattern::new takes care of reversal of reversed
                    // attributes: [?x :foo/_bar ?y] turns into
                    // [?y :foo/bar ?x].
                    //
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
                    // is nonsense. That leaves us with a nested optional, which we unwrap here.
                    Pattern::new(src, e, a, v, tx)
                        .map(WhereClause::Pattern)
                        .ok_or(combine::primitives::Error::Expected("pattern".into()))
                }))
});

def_parser!(Where, clause, WhereClause, {
    choice([try(Where::pattern()),
            // It's either
            //   (or-join [vars] clauses…)
            // or
            //   (or clauses…)
            // We don't yet handle source vars.
            try(Where::or_join_clause()),
            try(Where::or_clause()),
            try(Where::not_join_clause()),
            try(Where::not_clause()),

            try(Where::pred()),
    ])
});

def_parser!(Where, clauses, Vec<WhereClause>, {
    // Right now we only support patterns and predicates. See #239 for more.
    (many1::<Vec<WhereClause>, _>(Where::clause()))
});

pub struct Find;

def_matches_plain_symbol!(Find, period, ".");

def_matches_plain_symbol!(Find, ellipsis, "...");

def_parser!(Find, find_scalar, FindSpec, {
    Query::variable()
        .skip(Find::period())
        .skip(eof())
        .map(|var| FindSpec::FindScalar(Element::Variable(var)))
});

def_parser!(Find, find_coll, FindSpec, {
    vector()
        .of_exactly(Query::variable()
            .skip(Find::ellipsis()))
            .map(|var| FindSpec::FindColl(Element::Variable(var)))
});

def_parser!(Find, elements, Vec<Element>, {
    many1::<Vec<Element>, _>(Query::variable().map(Element::Variable))
});

def_parser!(Find, find_rel, FindSpec, {
    Find::elements().map(FindSpec::FindRel)
});

def_parser!(Find, find_tuple, FindSpec, {
    vector()
        .of_exactly(Find::elements().map(FindSpec::FindTuple))
});

/// Parse a stream of values into one of four find specs.
///
/// `:find` must be an array of plain var symbols (?foo), pull expressions, and aggregates.  For now
/// we only support variables and the annotations necessary to declare which flavor of :find we
/// want:
///
///
///     `?x ?y ?z  `     = FindRel
///     `[?x ...]  `     = FindColl
///     `?x .      `     = FindScalar
///     `[?x ?y ?z]`     = FindTuple
def_parser!(Find, spec, FindSpec, {
    // Any one of the four specs might apply, so we combine them with `choice`.  Our parsers consume
    // input, so we need to wrap them in `try` so that they operate independently.
    choice::<[&mut Parser<Input = _, Output = FindSpec>; 4], _>
        ([&mut try(Find::find_scalar()),
          &mut try(Find::find_coll()),
          &mut try(Find::find_tuple()),
          &mut try(Find::find_rel())])
});

def_matches_keyword!(Find, literal_find, "find");

def_matches_keyword!(Find, literal_with, "with");

def_matches_keyword!(Find, literal_where, "where");

/// Express something close to a builder pattern for a `FindQuery`.
enum FindQueryPart {
    FindSpec(FindSpec),
    With(Vec<Variable>),
    WhereClauses(Vec<WhereClause>),
}

/// This is awkward, but will do for now.  We use `keyword_map()` to optionally accept vector find
/// queries, then we use `FindQueryPart` to collect parts that have heterogeneous types; and then we
/// construct a `FindQuery` from them.
def_parser!(Find, query, FindQuery, {
    let p_find_spec = Find::literal_find()
        .with(vector().of_exactly(Find::spec().map(FindQueryPart::FindSpec)));

    let p_with_vars = Find::literal_with()
        .with(vector().of_exactly(many(Query::variable()).map(FindQueryPart::With)));

    let p_where_clauses = Find::literal_where()
        .with(vector().of_exactly(Where::clauses().map(FindQueryPart::WhereClauses))).expected(":where clauses");

    (or(map(), keyword_map()))
        .of_exactly(many(choice::<[&mut Parser<Input = ValueStream, Output = FindQueryPart>; 3], _>([
            &mut try(p_find_spec),
            &mut try(p_with_vars),
            &mut try(p_where_clauses),
        ])))
        .and_then(|parts: Vec<FindQueryPart>| -> std::result::Result<FindQuery, combine::primitives::Error<edn::ValueAndSpan, edn::ValueAndSpan>>  {
            let mut find_spec = None;
            let mut with_vars = None;
            let mut where_clauses = None;

            for part in parts {
                match part {
                    FindQueryPart::FindSpec(x) => find_spec = Some(x),
                    FindQueryPart::With(x) => with_vars = Some(x),
                    FindQueryPart::WhereClauses(x) => where_clauses = Some(x),
                }
            }

            Ok(FindQuery {
                find_spec: find_spec.clone().ok_or(combine::primitives::Error::Unexpected("expected :find".into()))?,
                default_source: SrcVar::DefaultSrc,
                with: with_vars.unwrap_or(vec![]),
                in_vars: vec![],       // TODO
                in_sources: vec![],    // TODO
                where_clauses: where_clauses.ok_or(combine::primitives::Error::Unexpected("expected :where".into()))?,
            })
        })
});

pub fn parse_find_string(string: &str) -> Result<FindQuery> {
    let expr = edn::parse::value(string)?;
    Find::query()
        .parse(expr.into_atom_stream())
        .map(|x| x.0)
        .map_err(|e| Error::from_kind(ErrorKind::FindParseError(e)))
}

#[cfg(test)]
mod test {
    extern crate combine;
    extern crate edn;
    extern crate mentat_query;

    use std::rc::Rc;

    use self::combine::Parser;
    use self::edn::OrderedFloat;
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

    fn variable(x: edn::PlainSymbol) -> Variable {
        Variable(Rc::new(x))
    }

    fn ident_kw(kw: edn::NamespacedKeyword) -> PatternNonValuePlace {
        PatternNonValuePlace::Ident(Rc::new(kw))
    }

    fn ident(ns: &str, name: &str) -> PatternNonValuePlace {
        ident_kw(edn::NamespacedKeyword::new(ns, name))
    }

    #[test]
    fn test_pattern_mixed() {
        let e = edn::PlainSymbol::new("_");
        let a = edn::NamespacedKeyword::new("foo", "bar");
        let v = OrderedFloat(99.9);
        let tx = edn::PlainSymbol::new("?tx");
        let input = edn::Value::Vector(vec!(edn::Value::PlainSymbol(e.clone()),
                                            edn::Value::NamespacedKeyword(a.clone()),
                                            edn::Value::Float(v.clone()),
                                            edn::Value::PlainSymbol(tx.clone())));
        assert_parses_to!(Where::pattern, input, WhereClause::Pattern(Pattern {
            source: None,
            entity: PatternNonValuePlace::Placeholder,
            attribute: ident_kw(a),
            value: PatternValuePlace::Constant(NonIntegerConstant::Float(v)),
            tx: PatternNonValuePlace::Variable(variable(tx)),
        }));
    }

    #[test]
    fn test_pattern_vars() {
        let s = edn::PlainSymbol::new("$x");
        let e = edn::PlainSymbol::new("?e");
        let a = edn::PlainSymbol::new("?a");
        let v = edn::PlainSymbol::new("?v");
        let tx = edn::PlainSymbol::new("?tx");
        let input = edn::Value::Vector(vec!(edn::Value::PlainSymbol(s.clone()),
                 edn::Value::PlainSymbol(e.clone()),
                 edn::Value::PlainSymbol(a.clone()),
                 edn::Value::PlainSymbol(v.clone()),
                 edn::Value::PlainSymbol(tx.clone())));
        assert_parses_to!(Where::pattern, input, WhereClause::Pattern(Pattern {
            source: Some(SrcVar::NamedSrc("x".to_string())),
            entity: PatternNonValuePlace::Variable(variable(e)),
            attribute: PatternNonValuePlace::Variable(variable(a)),
            value: PatternValuePlace::Variable(variable(v)),
            tx: PatternNonValuePlace::Variable(variable(tx)),
        }));
    }

    #[test]
    fn test_pattern_reversed_invalid() {
        let e = edn::PlainSymbol::new("_");
        let a = edn::NamespacedKeyword::new("foo", "_bar");
        let v = OrderedFloat(99.9);
        let tx = edn::PlainSymbol::new("?tx");
        let input = edn::Value::Vector(vec!(edn::Value::PlainSymbol(e.clone()),
                 edn::Value::NamespacedKeyword(a.clone()),
                 edn::Value::Float(v.clone()),
                 edn::Value::PlainSymbol(tx.clone())));

        let mut par = Where::pattern();
        let result = par.parse(input.with_spans().into_atom_stream());
        assert!(matches!(result, Err(_)), "Expected a parse error.");
    }

    #[test]
    fn test_pattern_reversed() {
        let e = edn::PlainSymbol::new("_");
        let a = edn::NamespacedKeyword::new("foo", "_bar");
        let v = edn::PlainSymbol::new("?v");
        let tx = edn::PlainSymbol::new("?tx");
        let input = edn::Value::Vector(vec!(edn::Value::PlainSymbol(e.clone()),
                 edn::Value::NamespacedKeyword(a.clone()),
                 edn::Value::PlainSymbol(v.clone()),
                 edn::Value::PlainSymbol(tx.clone())));

        // Note that the attribute is no longer reversed, and the entity and value have
        // switched places.
        assert_parses_to!(Where::pattern, input, WhereClause::Pattern(Pattern {
            source: None,
            entity: PatternNonValuePlace::Variable(variable(v)),
            attribute: ident("foo", "bar"),
            value: PatternValuePlace::Placeholder,
            tx: PatternNonValuePlace::Variable(variable(tx)),
        }));
    }

    #[test]
    fn test_rule_vars() {
        let e = edn::PlainSymbol::new("?e");
        let input = edn::Value::Vector(vec![edn::Value::PlainSymbol(e.clone())]);
        assert_parses_to!(Where::rule_vars, input,
                          vec![variable(e.clone())]);
    }

    #[test]
    fn test_or() {
        let oj = edn::PlainSymbol::new("or");
        let e = edn::PlainSymbol::new("?e");
        let a = edn::PlainSymbol::new("?a");
        let v = edn::PlainSymbol::new("?v");
        let input = edn::Value::List(
            vec![edn::Value::PlainSymbol(oj),
                 edn::Value::Vector(vec![edn::Value::PlainSymbol(e.clone()),
                                         edn::Value::PlainSymbol(a.clone()),
                                         edn::Value::PlainSymbol(v.clone())])].into_iter().collect());
        assert_parses_to!(Where::or_clause, input,
                          WhereClause::OrJoin(
                              OrJoin {
                                  unify_vars: UnifyVars::Implicit,
                                  clauses: vec![OrWhereClause::Clause(
                                      WhereClause::Pattern(Pattern {
                                          source: None,
                                          entity: PatternNonValuePlace::Variable(variable(e)),
                                          attribute: PatternNonValuePlace::Variable(variable(a)),
                                          value: PatternValuePlace::Variable(variable(v)),
                                          tx: PatternNonValuePlace::Placeholder,
                                      }))],
                              }));
    }

    #[test]
    fn test_or_join() {
        let oj = edn::PlainSymbol::new("or-join");
        let e = edn::PlainSymbol::new("?e");
        let a = edn::PlainSymbol::new("?a");
        let v = edn::PlainSymbol::new("?v");
        let input = edn::Value::List(
            vec![edn::Value::PlainSymbol(oj),
                 edn::Value::Vector(vec![edn::Value::PlainSymbol(e.clone())]),
                 edn::Value::Vector(vec![edn::Value::PlainSymbol(e.clone()),
                                         edn::Value::PlainSymbol(a.clone()),
                                         edn::Value::PlainSymbol(v.clone())])].into_iter().collect());
        assert_parses_to!(Where::or_join_clause, input,
                          WhereClause::OrJoin(
                              OrJoin {
                                  unify_vars: UnifyVars::Explicit(vec![variable(e.clone())]),
                                  clauses: vec![OrWhereClause::Clause(
                                      WhereClause::Pattern(Pattern {
                                          source: None,
                                          entity: PatternNonValuePlace::Variable(variable(e)),
                                          attribute: PatternNonValuePlace::Variable(variable(a)),
                                          value: PatternValuePlace::Variable(variable(v)),
                                          tx: PatternNonValuePlace::Placeholder,
                                      }))],
                              }));
    }

    #[test]
    fn test_not() {
        let oj = edn::PlainSymbol::new("not");
        let e = edn::PlainSymbol::new("?e");
        let a = edn::PlainSymbol::new("?a");
        let v = edn::PlainSymbol::new("?v");
        let input = [edn::Value::List(
            vec![edn::Value::PlainSymbol(oj),
                 edn::Value::Vector(vec![edn::Value::PlainSymbol(e.clone()),
                                         edn::Value::PlainSymbol(a.clone()),
                                         edn::Value::PlainSymbol(v.clone())])].into_iter().collect())];
        assert_parses_to!(Where::not_clause, input,
                          WhereClause::NotJoin(
                              NotJoin {
                                  unify_vars: UnifyVars::Implicit,
                                  clauses: vec![WhereNotClause::Clause(
                                      WhereClause::Pattern(Pattern {
                                          source: None,
                                          entity: PatternNonValuePlace::Variable(variable(e)),
                                          attribute: PatternNonValuePlace::Variable(variable(a)),
                                          value: PatternValuePlace::Variable(variable(v)),
                                          tx: PatternNonValuePlace::Placeholder,
                                      }))],
                              }));
    }

    #[test]
    fn test_not_join() {
        let oj = edn::PlainSymbol::new("not-join");
        let e = edn::PlainSymbol::new("?e");
        let a = edn::PlainSymbol::new("?a");
        let v = edn::PlainSymbol::new("?v");
        let input = [edn::Value::List(
            vec![edn::Value::PlainSymbol(oj),
                 edn::Value::Vector(vec![edn::Value::PlainSymbol(e.clone())]),
                 edn::Value::Vector(vec![edn::Value::PlainSymbol(e.clone()),
                                         edn::Value::PlainSymbol(a.clone()),
                                         edn::Value::PlainSymbol(v.clone())])].into_iter().collect())];
        assert_parses_to!(Where::not_join_clause, input,
                          WhereClause::NotJoin(
                              NotJoin {
                                  unify_vars: UnifyVars::Explicit(vec![variable(e.clone())]),
                                  clauses: vec![WhereNotClause::Clause(
                                      WhereClause::Pattern(Pattern {
                                          source: None,
                                          entity: PatternNonValuePlace::Variable(variable(e)),
                                          attribute: PatternNonValuePlace::Variable(variable(a)),
                                          value: PatternValuePlace::Variable(variable(v)),
                                          tx: PatternNonValuePlace::Placeholder,
                                      }))],
                              }));
    }

    #[test]
    fn test_find_sp_variable() {
        let sym = edn::PlainSymbol::new("?x");
        let input = edn::Value::Vector(vec![edn::Value::PlainSymbol(sym.clone())]);
        assert_parses_to!(|| vector().of_exactly(Query::variable()), input, variable(sym));
    }

    #[test]
    fn test_find_scalar() {
        let sym = edn::PlainSymbol::new("?x");
        let period = edn::PlainSymbol::new(".");
        let input = edn::Value::Vector(vec![edn::Value::PlainSymbol(sym.clone()), edn::Value::PlainSymbol(period.clone())]);
        assert_parses_to!(|| vector().of_exactly(Find::find_scalar()),
                          input,
                          FindSpec::FindScalar(Element::Variable(variable(sym))));
    }

    #[test]
    fn test_find_coll() {
        let sym = edn::PlainSymbol::new("?x");
        let period = edn::PlainSymbol::new("...");
        let input = edn::Value::Vector(vec![edn::Value::PlainSymbol(sym.clone()),
                                            edn::Value::PlainSymbol(period.clone())]);
        assert_parses_to!(Find::find_coll,
                          input,
                          FindSpec::FindColl(Element::Variable(variable(sym))));
    }

    #[test]
    fn test_find_rel() {
        let vx = edn::PlainSymbol::new("?x");
        let vy = edn::PlainSymbol::new("?y");
        let input = edn::Value::Vector(vec![edn::Value::PlainSymbol(vx.clone()), edn::Value::PlainSymbol(vy.clone())]);
        assert_parses_to!(|| vector().of_exactly(Find::find_rel()),
                          input,
                          FindSpec::FindRel(vec![Element::Variable(variable(vx)),
                                                 Element::Variable(variable(vy))]));
    }

    #[test]
    fn test_find_tuple() {
        let vx = edn::PlainSymbol::new("?x");
        let vy = edn::PlainSymbol::new("?y");
        let input = edn::Value::Vector(vec![edn::Value::PlainSymbol(vx.clone()),
                                             edn::Value::PlainSymbol(vy.clone())]);
        assert_parses_to!(Find::find_tuple,
                          input,
                          FindSpec::FindTuple(vec![Element::Variable(variable(vx)),
                                                   Element::Variable(variable(vy))]));
    }
}
