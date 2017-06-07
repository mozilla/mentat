// Copyright 2016 Mozilla
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

extern crate edn;
extern crate combine;
extern crate mentat_tx;
extern crate mentat_tx_parser;

use edn::parse;
use edn::symbols::NamespacedKeyword;
use mentat_tx::entities::{
    AtomOrLookupRefOrVectorOrMapNotation,
    Entid,
    EntidOrLookupRefOrTempId,
    Entity,
    OpType,
    TempId,
};
use mentat_tx_parser::Tx;

#[test]
fn test_float_and_uuid() {
    let expected_uuid = edn::Uuid::parse_str("267bab92-ee39-4ca2-b7f0-1163a85af1fb").expect("valid uuid");
    let input = r#"
[[:db/add 101 :test/a #uuid "267bab92-ee39-4ca2-b7f0-1163a85af1fb"]
 [:db/add 102 :test/b #f NaN]]
 "#;
    let edn = parse::value(input).expect("to parse test input");

    let result = Tx::parse(&edn);
    assert_eq!(result.unwrap(),
               vec![
                   Entity::AddOrRetract {
                       op: OpType::Add,
                       e: EntidOrLookupRefOrTempId::Entid(Entid::Entid(101)),
                       a: Entid::Ident(NamespacedKeyword::new("test", "a")),
                       v: AtomOrLookupRefOrVectorOrMapNotation::Atom(edn::ValueAndSpan::new(edn::SpannedValue::Uuid(expected_uuid), edn::Span(23, 67))),
                   },
                   Entity::AddOrRetract {
                       op: OpType::Add,
                       e: EntidOrLookupRefOrTempId::Entid(Entid::Entid(102)),
                       a: Entid::Ident(NamespacedKeyword::new("test", "b")),
                       v: AtomOrLookupRefOrVectorOrMapNotation::Atom(edn::ValueAndSpan::new(edn::SpannedValue::Float(edn::OrderedFloat(std::f64::NAN)), edn::Span(91, 97))),
                   },
               ]);
}

#[test]
fn test_entities() {
    let input = r#"
[[:db/add 101 :test/a "v"]
 [:db/add "tempid" :test/a "v"]
 [:db/retract 102 :test/b "w"]]"#;

    let edn = parse::value(input).expect("to parse test input");

    let result = Tx::parse(&edn);
    assert_eq!(result.unwrap(),
               vec![
                   Entity::AddOrRetract {
                       op: OpType::Add,
                       e: EntidOrLookupRefOrTempId::Entid(Entid::Entid(101)),
                       a: Entid::Ident(NamespacedKeyword::new("test", "a")),
                       v: AtomOrLookupRefOrVectorOrMapNotation::Atom(edn::ValueAndSpan::new(edn::SpannedValue::Text("v".into()), edn::Span(23, 26))),
                   },
                   Entity::AddOrRetract {
                       op: OpType::Add,
                       e: EntidOrLookupRefOrTempId::TempId(TempId::External("tempid".into())),
                       a: Entid::Ident(NamespacedKeyword::new("test", "a")),
                       v: AtomOrLookupRefOrVectorOrMapNotation::Atom(edn::ValueAndSpan::new(edn::SpannedValue::Text("v".into()), edn::Span(55, 58))),
                   },
                   Entity::AddOrRetract {
                       op: OpType::Retract,
                       e: EntidOrLookupRefOrTempId::Entid(Entid::Entid(102)),
                       a: Entid::Ident(NamespacedKeyword::new("test", "b")),
                       v: AtomOrLookupRefOrVectorOrMapNotation::Atom(edn::ValueAndSpan::new(edn::SpannedValue::Text("w".into()), edn::Span(86, 89))),
                   },
               ]);
}

#[test]
fn test_reverse_notation_illegal_nested_values() {
    // Verify that we refuse to parse direct reverse notation with nested value maps or vectors.

    let input = "[[:db/add 100 :test/_dangling {:test/many 13}]]";
    let edn = parse::value(input).expect("to parse test input");
    let result = Tx::parse(&edn);
    // TODO: it would be much better to assert details about the error (here and below), but right
    // now the error message isn't clear that the given value isn't valid for the backward attribute
    // :test/_dangling.
    assert!(result.is_err());

    let input = "[[:db/add 100 :test/_dangling [:test/many 13]]]";
    let edn = parse::value(input).expect("to parse test input");
    let result = Tx::parse(&edn);
    assert!(result.is_err());
}

// TODO: test error handling in select cases.
