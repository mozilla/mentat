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

use self::mentat_query::{FindQuery, SrcVar};

use super::error::{QueryParseError, QueryParseResult};
use super::util::{values_to_variables, vec_to_keyword_map};

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
    let with_vars = with.map(values_to_variables);
    // :wheres is a whole datastructure.

    super::parse::find_seq_to_find_spec(find)
        .map(|spec| {
            FindQuery {
                find_spec: spec,
                default_source: source,
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
