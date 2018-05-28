// Copyright 2016 Mozilla
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

#[macro_use]
extern crate maplit;

extern crate edn;
extern crate mentat_query;
extern crate mentat_query_parser;

use edn::{
    Keyword,
    PlainSymbol,
};

use mentat_query::{
    Direction,
    Element,
    FindSpec,
    FnArg,
    Limit,
    NonIntegerConstant,
    Order,
    OrJoin,
    OrWhereClause,
    Pattern,
    PatternNonValuePlace,
    PatternValuePlace,
    Predicate,
    UnifyVars,
    Variable,
    WhereClause,
};

use mentat_query_parser::parse_find_string;

///! N.B., parsing a query can be done without reference to a DB.
///! Processing the parsed query into something we can work with
///! for planning involves interrogating the schema and idents in
///! the store.
///! See <https://github.com/mozilla/mentat/wiki/Querying> for more.
#[test]
fn can_parse_predicates() {
    let s = "[:find [?x ...] :where [?x _ ?y] [(< ?y 10)]]";
    let p = parse_find_string(s).unwrap();

    assert_eq!(p.find_spec,
               FindSpec::FindColl(Element::Variable(Variable::from_valid_name("?x"))));
    assert_eq!(p.where_clauses,
               vec![
                   WhereClause::Pattern(Pattern {
                       source: None,
                       entity: PatternNonValuePlace::Variable(Variable::from_valid_name("?x")),
                       attribute: PatternNonValuePlace::Placeholder,
                       value: PatternValuePlace::Variable(Variable::from_valid_name("?y")),
                       tx: PatternNonValuePlace::Placeholder,
                   }),
                   WhereClause::Pred(Predicate { operator: PlainSymbol::plain("<"), args: vec![
                       FnArg::Variable(Variable::from_valid_name("?y")), FnArg::EntidOrInteger(10),
                   ]}),
               ]);
}

#[test]
fn can_parse_simple_or() {
    let s = "[:find ?x . :where (or [?x _ 10] [?x _ 15])]";
    let p = parse_find_string(s).unwrap();

    assert_eq!(p.find_spec,
               FindSpec::FindScalar(Element::Variable(Variable::from_valid_name("?x"))));
    assert_eq!(p.where_clauses,
               vec![
                   WhereClause::OrJoin(OrJoin::new(
                       UnifyVars::Implicit,
                       vec![
                           OrWhereClause::Clause(
                               WhereClause::Pattern(Pattern {
                                   source: None,
                                   entity: PatternNonValuePlace::Variable(Variable::from_valid_name("?x")),
                                   attribute: PatternNonValuePlace::Placeholder,
                                   value: PatternValuePlace::EntidOrInteger(10),
                                   tx: PatternNonValuePlace::Placeholder,
                               })),
                           OrWhereClause::Clause(
                               WhereClause::Pattern(Pattern {
                                   source: None,
                                   entity: PatternNonValuePlace::Variable(Variable::from_valid_name("?x")),
                                   attribute: PatternNonValuePlace::Placeholder,
                                   value: PatternValuePlace::EntidOrInteger(15),
                                   tx: PatternNonValuePlace::Placeholder,
                               })),
                       ],
                   )),
               ]);
}

#[test]
fn can_parse_unit_or_join() {
    let s = "[:find ?x . :where (or-join [?x] [?x _ 15])]";
    let p = parse_find_string(s).expect("to be able to parse find");

    assert_eq!(p.find_spec,
               FindSpec::FindScalar(Element::Variable(Variable::from_valid_name("?x"))));
    assert_eq!(p.where_clauses,
               vec![
                   WhereClause::OrJoin(OrJoin::new(
                       UnifyVars::Explicit(btreeset!{Variable::from_valid_name("?x")}),
                       vec![
                           OrWhereClause::Clause(
                               WhereClause::Pattern(Pattern {
                                   source: None,
                                   entity: PatternNonValuePlace::Variable(Variable::from_valid_name("?x")),
                                   attribute: PatternNonValuePlace::Placeholder,
                                   value: PatternValuePlace::EntidOrInteger(15),
                                   tx: PatternNonValuePlace::Placeholder,
                               })),
                       ],
                   )),
               ]);
}

#[test]
fn can_parse_simple_or_join() {
    let s = "[:find ?x . :where (or-join [?x] [?x _ 10] [?x _ -15])]";
    let p = parse_find_string(s).unwrap();

    assert_eq!(p.find_spec,
               FindSpec::FindScalar(Element::Variable(Variable::from_valid_name("?x"))));
    assert_eq!(p.where_clauses,
               vec![
                   WhereClause::OrJoin(OrJoin::new(
                       UnifyVars::Explicit(btreeset!{Variable::from_valid_name("?x")}),
                       vec![
                           OrWhereClause::Clause(
                               WhereClause::Pattern(Pattern {
                                   source: None,
                                   entity: PatternNonValuePlace::Variable(Variable::from_valid_name("?x")),
                                   attribute: PatternNonValuePlace::Placeholder,
                                   value: PatternValuePlace::EntidOrInteger(10),
                                   tx: PatternNonValuePlace::Placeholder,
                               })),
                           OrWhereClause::Clause(
                               WhereClause::Pattern(Pattern {
                                   source: None,
                                   entity: PatternNonValuePlace::Variable(Variable::from_valid_name("?x")),
                                   attribute: PatternNonValuePlace::Placeholder,
                                   value: PatternValuePlace::EntidOrInteger(-15),
                                   tx: PatternNonValuePlace::Placeholder,
                               })),
                       ],
                   )),
               ]);
}

#[cfg(test)]
fn ident(ns: &str, name: &str) -> PatternNonValuePlace {
    Keyword::namespaced(ns, name).into()
}

#[test]
fn can_parse_simple_or_and_join() {
    let s = "[:find ?x . :where (or [?x _ 10] (and (or [?x :foo/bar ?y] [?x :foo/baz ?y]) [(< ?y 1)]))]";
    let p = parse_find_string(s).unwrap();

    assert_eq!(p.find_spec,
               FindSpec::FindScalar(Element::Variable(Variable::from_valid_name("?x"))));
    assert_eq!(p.where_clauses,
               vec![
                   WhereClause::OrJoin(OrJoin::new(
                       UnifyVars::Implicit,
                       vec![
                           OrWhereClause::Clause(
                               WhereClause::Pattern(Pattern {
                                   source: None,
                                   entity: PatternNonValuePlace::Variable(Variable::from_valid_name("?x")),
                                   attribute: PatternNonValuePlace::Placeholder,
                                   value: PatternValuePlace::EntidOrInteger(10),
                                   tx: PatternNonValuePlace::Placeholder,
                               })),
                           OrWhereClause::And(
                               vec![
                                   WhereClause::OrJoin(OrJoin::new(
                                       UnifyVars::Implicit,
                                       vec![
                                           OrWhereClause::Clause(WhereClause::Pattern(Pattern {
                                               source: None,
                                               entity: PatternNonValuePlace::Variable(Variable::from_valid_name("?x")),
                                               attribute: ident("foo", "bar"),
                                               value: PatternValuePlace::Variable(Variable::from_valid_name("?y")),
                                               tx: PatternNonValuePlace::Placeholder,
                                           })),
                                           OrWhereClause::Clause(WhereClause::Pattern(Pattern {
                                               source: None,
                                               entity: PatternNonValuePlace::Variable(Variable::from_valid_name("?x")),
                                               attribute: ident("foo", "baz"),
                                               value: PatternValuePlace::Variable(Variable::from_valid_name("?y")),
                                               tx: PatternNonValuePlace::Placeholder,
                                           })),
                                       ],
                                   )),

                                   WhereClause::Pred(Predicate { operator: PlainSymbol::plain("<"), args: vec![
                                       FnArg::Variable(Variable::from_valid_name("?y")), FnArg::EntidOrInteger(1),
                                   ]}),
                               ],
                           )
                       ],
                   )),
               ]);
}

#[test]
fn can_parse_order_by() {
    let invalid = "[:find ?x :where [?x :foo/baz ?y] :order]";
    assert!(parse_find_string(invalid).is_err());

    // Defaults to ascending.
    let default = "[:find ?x :where [?x :foo/baz ?y] :order ?y]";
    assert_eq!(parse_find_string(default).unwrap().order,
               Some(vec![Order(Direction::Ascending, Variable::from_valid_name("?y"))]));

    let ascending = "[:find ?x :where [?x :foo/baz ?y] :order (asc ?y)]";
    assert_eq!(parse_find_string(ascending).unwrap().order,
               Some(vec![Order(Direction::Ascending, Variable::from_valid_name("?y"))]));

    let descending = "[:find ?x :where [?x :foo/baz ?y] :order (desc ?y)]";
    assert_eq!(parse_find_string(descending).unwrap().order,
               Some(vec![Order(Direction::Descending, Variable::from_valid_name("?y"))]));

    let mixed = "[:find ?x :where [?x :foo/baz ?y] :order (desc ?y) (asc ?x)]";
    assert_eq!(parse_find_string(mixed).unwrap().order,
               Some(vec![Order(Direction::Descending, Variable::from_valid_name("?y")),
                         Order(Direction::Ascending, Variable::from_valid_name("?x"))]));
}

#[test]
fn can_parse_limit() {
    let invalid = "[:find ?x :where [?x :foo/baz ?y] :limit]";
    assert!(parse_find_string(invalid).is_err());

    let zero_invalid = "[:find ?x :where [?x :foo/baz ?y] :limit 00]";
    assert!(parse_find_string(zero_invalid).is_err());

    let none = "[:find ?x :where [?x :foo/baz ?y]]";
    assert_eq!(parse_find_string(none).unwrap().limit,
               Limit::None);

    let one = "[:find ?x :where [?x :foo/baz ?y] :limit 1]";
    assert_eq!(parse_find_string(one).unwrap().limit,
               Limit::Fixed(1));

    let onethousand = "[:find ?x :where [?x :foo/baz ?y] :limit 1000]";
    assert_eq!(parse_find_string(onethousand).unwrap().limit,
               Limit::Fixed(1000));

    let variable_with_in = "[:find ?x :in ?limit :where [?x :foo/baz ?y] :limit ?limit]";
    assert_eq!(parse_find_string(variable_with_in).unwrap().limit,
               Limit::Variable(Variable::from_valid_name("?limit")));

    let variable_with_in_used = "[:find ?x :in ?limit :where [?x :foo/baz ?limit] :limit ?limit]";
    assert_eq!(parse_find_string(variable_with_in_used).unwrap().limit,
               Limit::Variable(Variable::from_valid_name("?limit")));

    let variable_without_in = "[:find ?x :where [?x :foo/baz ?y] :limit ?limit]";
    assert!(parse_find_string(variable_without_in).is_err());
}

#[test]
fn can_parse_uuid() {
    let expected = edn::Uuid::parse_str("4cb3f828-752d-497a-90c9-b1fd516d5644").expect("valid uuid");
    let s = "[:find ?x :where [?x :foo/baz #uuid \"4cb3f828-752d-497a-90c9-b1fd516d5644\"]]";
    assert_eq!(parse_find_string(s).expect("parsed").where_clauses.pop().expect("a where clause"),
               WhereClause::Pattern(
                   Pattern::new(None,
                                PatternNonValuePlace::Variable(Variable::from_valid_name("?x")),
                                Keyword::namespaced("foo", "baz").into(),
                                PatternValuePlace::Constant(NonIntegerConstant::Uuid(expected)),
                                PatternNonValuePlace::Placeholder)
                       .expect("valid pattern")));
}
