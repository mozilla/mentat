// Copyright 2016 Mozilla
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

extern crate mentat_query_parser;
extern crate mentat_query;
extern crate edn;

use edn::{
    NamespacedKeyword,
    PlainSymbol,
};

use mentat_query::{
    Element,
    FindSpec,
    FnArg,
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
                   WhereClause::Pred(Predicate { operator: PlainSymbol::new("<"), args: vec![
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
                   WhereClause::OrJoin(OrJoin {
                       unify_vars: UnifyVars::Implicit,
                       clauses: vec![
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
                   }),
               ]);
}

#[test]
fn can_parse_unit_or_join() {
    let s = "[:find ?x . :where (or-join [?x] [?x _ 15])]";
    let p = parse_find_string(s).unwrap();

    assert_eq!(p.find_spec,
               FindSpec::FindScalar(Element::Variable(Variable::from_valid_name("?x"))));
    assert_eq!(p.where_clauses,
               vec![
                   WhereClause::OrJoin(OrJoin {
                       unify_vars: UnifyVars::Explicit(vec![Variable::from_valid_name("?x")]),
                       clauses: vec![
                           OrWhereClause::Clause(
                               WhereClause::Pattern(Pattern {
                                   source: None,
                                   entity: PatternNonValuePlace::Variable(Variable::from_valid_name("?x")),
                                   attribute: PatternNonValuePlace::Placeholder,
                                   value: PatternValuePlace::EntidOrInteger(15),
                                   tx: PatternNonValuePlace::Placeholder,
                               })),
                       ],
                   }),
               ]);
}

#[test]
fn can_parse_simple_or_join() {
    let s = "[:find ?x . :where (or-join [?x] [?x _ 10] [?x _ 15])]";
    let p = parse_find_string(s).unwrap();

    assert_eq!(p.find_spec,
               FindSpec::FindScalar(Element::Variable(Variable::from_valid_name("?x"))));
    assert_eq!(p.where_clauses,
               vec![
                   WhereClause::OrJoin(OrJoin {
                       unify_vars: UnifyVars::Explicit(vec![Variable::from_valid_name("?x")]),
                       clauses: vec![
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
                   }),
               ]);
}

#[test]
fn can_parse_simple_or_and_join() {
    let s = "[:find ?x . :where (or [?x _ 10] (and (or [?x :foo/bar ?y] [?x :foo/baz ?y]) [(< ?y 1)]))]";
    let p = parse_find_string(s).unwrap();

    assert_eq!(p.find_spec,
               FindSpec::FindScalar(Element::Variable(Variable::from_valid_name("?x"))));
    assert_eq!(p.where_clauses,
               vec![
                   WhereClause::OrJoin(OrJoin {
                       unify_vars: UnifyVars::Implicit,
                       clauses: vec![
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
                                   WhereClause::OrJoin(OrJoin {
                                       unify_vars: UnifyVars::Implicit,
                                       clauses: vec![
                                           OrWhereClause::Clause(WhereClause::Pattern(Pattern {
                                               source: None,
                                               entity: PatternNonValuePlace::Variable(Variable::from_valid_name("?x")),
                                               attribute: PatternNonValuePlace::Ident(NamespacedKeyword::new("foo", "bar")),
                                               value: PatternValuePlace::Variable(Variable::from_valid_name("?y")),
                                               tx: PatternNonValuePlace::Placeholder,
                                           })),
                                           OrWhereClause::Clause(WhereClause::Pattern(Pattern {
                                               source: None,
                                               entity: PatternNonValuePlace::Variable(Variable::from_valid_name("?x")),
                                               attribute: PatternNonValuePlace::Ident(NamespacedKeyword::new("foo", "baz")),
                                               value: PatternValuePlace::Variable(Variable::from_valid_name("?y")),
                                               tx: PatternNonValuePlace::Placeholder,
                                           })),
                                       ],
                                   }),

                                   WhereClause::Pred(Predicate { operator: PlainSymbol::new("<"), args: vec![
                                       FnArg::Variable(Variable::from_valid_name("?y")), FnArg::EntidOrInteger(1),
                                   ]}),
                               ],
                           )
                       ],
                   }),
               ]);
}
