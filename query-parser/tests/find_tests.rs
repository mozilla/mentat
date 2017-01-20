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

use mentat_query::FindSpec::*;
use mentat_query::Element;
use mentat_query::Variable;
use edn::PlainSymbol;

///! N.B., parsing a query can be done without reference to a DB.
///! Processing the parsed query into something we can work with
///! for planning involves interrogating the schema and idents in
///! the store.
///! See <https://github.com/mozilla/mentat/wiki/Querying> for more.

#[test]
fn can_parse_trivial_find() {
    let find = FindScalar(Element::Variable(Variable(PlainSymbol("?foo".to_string()))));

    if let FindScalar(Element::Variable(Variable(PlainSymbol(name)))) = find {
        assert_eq!("?foo", name);
    } else {
        panic!()
    }
}
