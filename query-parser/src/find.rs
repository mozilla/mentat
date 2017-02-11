// Copyright 2016 Mozilla
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

/// ! This module defines the interface and implementation for parsing an EDN
/// ! input into a structured Datalog query.
/// !
/// ! The query types are defined in the `query` crate, because they
/// ! are shared between the parser (EDN -> query), the translator
/// ! (query -> SQL), and the executor (query, SQL -> running code).
/// !
/// ! The query input can be in two forms: a 'flat' human-oriented
/// ! sequence:
/// !
/// ! ```clojure
/// ! [:find ?y :in $ ?x :where [?x :foaf/knows ?y]]
/// ! ```
/// !
/// ! or a more programmatically generable map:
/// !
/// ! ```clojure
/// ! {:find [?y]
/// !  :in [$]
/// !  :where [[?x :foaf/knows ?y]]}
/// ! ```
/// !
/// ! We parse by expanding the array format into four parts, treating them as the four
/// ! parts of the map.

extern crate edn;
extern crate mentat_query;

use std::collections::BTreeMap;

use self::mentat_query::{
    FindQuery,
    FromValue,
    SrcVar,
    Variable,
};

use super::parse::{
    NotAVariableError,
    QueryParseError,
    QueryParseResult,
    clause_seq_to_patterns,
};

use super::util::vec_to_keyword_map;

/// If the provided slice of EDN values are all variables as
/// defined by `value_to_variable`, return a `Vec` of `Variable`s.
/// Otherwise, return the unrecognized Value in a `NotAVariableError`.
fn values_to_variables(vals: &[edn::Value]) -> Result<Vec<Variable>, NotAVariableError> {
    let mut out: Vec<Variable> = Vec::with_capacity(vals.len());
    for v in vals {
        if let Some(var) = Variable::from_value(v) {
            out.push(var);
            continue;
        }
        return Err(NotAVariableError(v.clone()));
    }
    return Ok(out);
}

#[allow(unused_variables)]
fn parse_find_parts(find: &[edn::Value],
                    ins: Option<&[edn::Value]>,
                    with: Option<&[edn::Value]>,
                    wheres: &[edn::Value])
                    -> QueryParseResult {
    // :find must be an array of plain var symbols (?foo), pull expressions, and aggregates.
    // For now we only support variables and the annotations necessary to declare which
    // flavor of :find we want:
    //     ?x ?y ?z       = FindRel
    //     [?x ...]       = FindColl
    //     ?x .           = FindScalar
    //     [?x ?y ?z]     = FindTuple
    //
    // :in must be an array of sources ($), rules (%), and vars (?). For now we only support the
    // default source. :in can be omitted, in which case the default is equivalent to `:in $`.
    // TODO: process `ins`.
    let source = SrcVar::DefaultSrc;

    // :with is an array of variables. This is simple, so we don't use a parser.
    let with_vars = if let Some(vals) = with {
        values_to_variables(vals)?
    } else {
        vec![]
    };

    // :wheres is a whole datastructure.
    let where_clauses = clause_seq_to_patterns(wheres)?;

    super::parse::find_seq_to_find_spec(find)
        .map(|spec| {
            FindQuery {
                find_spec: spec,
                default_source: source,
                with: with_vars,
                in_vars: vec!(),       // TODO
                in_sources: vec!(),    // TODO
                where_clauses: where_clauses,
            }
        })
        .map_err(QueryParseError::FindParseError)
}

fn parse_find_map(map: BTreeMap<edn::Keyword, Vec<edn::Value>>) -> QueryParseResult {
    // Eagerly awaiting `const fn`.
    let kw_find = edn::Keyword::new("find");
    let kw_in = edn::Keyword::new("in");
    let kw_with = edn::Keyword::new("with");
    let kw_where = edn::Keyword::new("where");

    // Oh, if only we had `guard`.
    if let Some(find) = map.get(&kw_find) {
        if let Some(wheres) = map.get(&kw_where) {
            return parse_find_parts(find,
                                    map.get(&kw_in).map(|x| x.as_slice()),
                                    map.get(&kw_with).map(|x| x.as_slice()),
                                    wheres);
        } else {
            return Err(QueryParseError::MissingField(kw_where));
        }
    } else {
        return Err(QueryParseError::MissingField(kw_find));
    }
}

fn parse_find_edn_map(map: BTreeMap<edn::Value, edn::Value>) -> QueryParseResult {
    // Every key must be a Keyword. Every value must be a Vec.
    let mut m = BTreeMap::new();

    if map.is_empty() {
        return parse_find_map(m);
    }

    for (k, v) in map {
        if let edn::Value::Keyword(kw) = k {
            if let edn::Value::Vector(vec) = v {
                m.insert(kw, vec);
                continue;
            } else {
                return Err(QueryParseError::InvalidInput(v));
            }
        } else {
            return Err(QueryParseError::InvalidInput(k));
        }
    }

    parse_find_map(m)
}

impl From<edn::parse::ParseError> for QueryParseError {
    fn from(err: edn::parse::ParseError) -> QueryParseError {
        QueryParseError::EdnParseError(err)
    }
}

pub fn parse_find_string(string: &str) -> QueryParseResult {
    let expr = edn::parse::value(string)?;
    parse_find(expr)
}

pub fn parse_find(expr: edn::Value) -> QueryParseResult {
    // No `match` because scoping and use of `expr` in error handling is nuts.
    if let edn::Value::Map(m) = expr {
        return parse_find_edn_map(m);
    }
    if let edn::Value::Vector(ref v) = expr {
        if let Some(m) = vec_to_keyword_map(v) {
            return parse_find_map(m);
        }
    }
    return Err(QueryParseError::InvalidInput(expr));
}

#[cfg(test)]
mod test_parse {
    extern crate edn;

    use self::edn::{NamespacedKeyword, PlainSymbol};
    use self::edn::types::{to_keyword, to_symbol};
    use super::mentat_query::{
        Element,
        FindSpec,
        Pattern,
        PatternNonValuePlace,
        PatternValuePlace,
        SrcVar,
        Variable,
        WhereClause,
    };
    use super::*;

    // TODO: when #224 lands, fix to_keyword to be variadic.
    #[test]
    fn test_parse_find() {
        let truncated_input = edn::Value::Vector(vec![to_keyword(None, "find")]);
        assert!(parse_find(truncated_input).is_err());

        let input = edn::Value::Vector(vec![
                                       to_keyword(None, "find"),
                                       to_symbol(None, "?x"),
                                       to_symbol(None, "?y"),
                                       to_keyword(None, "where"),
                                       edn::Value::Vector(vec![
                                                          to_symbol(None, "?x"),
                                                          to_keyword("foo", "bar"),
                                                          to_symbol(None, "?y"),
                                       ]),
        ]);

        let parsed = parse_find(input).unwrap();
        if let FindSpec::FindRel(elems) = parsed.find_spec {
            assert_eq!(2, elems.len());
            assert_eq!(vec![
                       Element::Variable(Variable(edn::PlainSymbol::new("?x"))),
                       Element::Variable(Variable(edn::PlainSymbol::new("?y"))),
            ], elems);
        } else {
            panic!("Expected FindRel.");
        }

        assert_eq!(SrcVar::DefaultSrc, parsed.default_source);
        assert_eq!(parsed.where_clauses,
                   vec![
                   WhereClause::Pattern(Pattern {
                       source: None,
                       entity: PatternNonValuePlace::Variable(Variable(PlainSymbol::new("?x"))),
                       attribute: PatternNonValuePlace::Ident(NamespacedKeyword::new("foo", "bar")),
                       value: PatternValuePlace::Variable(Variable(PlainSymbol::new("?y"))),
                       tx: PatternNonValuePlace::Placeholder,
                   })]);

    }
}
