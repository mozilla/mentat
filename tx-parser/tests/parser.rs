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

use combine::Parser;
use edn::types::Value;
use mentat_tx::entities::*;
use mentat_tx_parser::Tx;

#[test]
fn test_add() {
    let input = [Value::Vector(vec![Value::Keyword("db/add".into()),
                                    Value::Keyword("ident".into()),
                                    Value::Keyword("a".into()),
                                    Value::Text("v".into())])];
    let mut parser = Tx::entity();
    let result = parser.parse(&input[..]);
    assert_eq!(result,
               Ok((Entity::Add {
                   e: EntIdOrLookupRef::EntId(EntId::Ident("ident".into())),
                   a: EntId::Ident("a".into()),
                   v: ValueOrLookupRef::Value(Value::Text("v".into())),
                   tx: None,
               },
                   &[][..])));
}

#[test]
fn test_retract() {
    let input = [Value::Vector(vec![Value::Keyword("db/retract".into()),
                                    Value::Integer(101),
                                    Value::Keyword("a".into()),
                                    Value::Text("v".into())])];
    let mut parser = Tx::entity();
    let result = parser.parse(&input[..]);
    assert_eq!(result,
               Ok((Entity::Retract {
                   e: EntIdOrLookupRef::EntId(EntId::EntId(101)),
                   a: EntId::Ident("a".into()),
                   v: ValueOrLookupRef::Value(Value::Text("v".into())),
               },
                   &[][..])));
}

#[test]
fn test_entities() {
    let input = [Value::Vector(vec![
        Value::Vector(vec![Value::Keyword("db/add".into()),
                           Value::Integer(101),
                           Value::Keyword("a".into()),
                           Value::Text("v".into())]),
        Value::Vector(vec![Value::Keyword("db/retract".into()),
                           Value::Integer(102),
                           Value::Keyword("b".into()),
                           Value::Text("w".into())])])];

    let mut parser = Tx::entities();
    let result = parser.parse(&input[..]);
    assert_eq!(result,
               Ok((vec![
                   Entity::Add {
                       e: EntIdOrLookupRef::EntId(EntId::EntId(101)),
                       a: EntId::Ident("a".into()),
                       v: ValueOrLookupRef::Value(Value::Text("v".into())),
                       tx: None,
                   },
                   Entity::Retract {
                       e: EntIdOrLookupRef::EntId(EntId::EntId(102)),
                       a: EntId::Ident("b".into()),
                       v: ValueOrLookupRef::Value(Value::Text("w".into())),
                   },
                   ],
                   &[][..])));
}

#[test]
fn test_lookup_ref() {
    let input = [Value::Vector(vec![Value::Keyword("db/add".into()),
                                    Value::Vector(vec![Value::Keyword("a1".into()),
                                                       Value::Text("v1".into())]),
                                    Value::Keyword("a".into()),
                                    Value::Text("v".into())])];
    let mut parser = Tx::entity();
    let result = parser.parse(&input[..]);
    assert_eq!(result,
               Ok((Entity::Add {
                   e: EntIdOrLookupRef::LookupRef(LookupRef {
                       a: EntId::Ident("a1".into()),
                       v: Value::Text("v1".into()),
                   }),
                   a: EntId::Ident("a".into()),
                   v: ValueOrLookupRef::Value(Value::Text("v".into())),
                   tx: None,
               },
                   &[][..])));
}

// TODO: test error handling in select cases.  It's tricky to do
// this without combine::Positioner; see the TODO at the top of
// the file.

