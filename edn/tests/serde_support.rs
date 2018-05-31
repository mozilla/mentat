// Copyright 2018 Mozilla
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.


#![cfg(feature = "serde_support")]

extern crate serde_test;
extern crate serde_json;

extern crate edn;
use edn::symbols::Keyword;
use edn::{parse, Value};
use serde_test::{assert_tokens, Token};

#[test]
fn test_serialize_keyword() {
    let kw = Keyword::namespaced("foo", "bar");
    assert_tokens(&kw, &[
        Token::NewtypeStruct { name: "Keyword" },
        Token::Struct { name: "NamespaceableName", len: 2 },
        Token::Str("namespace"),
        Token::Some,
        Token::BorrowedStr("foo"),
        Token::Str("name"),
        Token::BorrowedStr("bar"),
        Token::StructEnd,
    ]);
}


#[test]
fn test_deserialize_keyword() {
    let json = r#"{"name": "foo", "namespace": "bar"}"#;
    let kw = serde_json::from_str::<Keyword>(json).unwrap();
    assert_eq!(kw.name(), "foo");
    assert_eq!(kw.namespace(), Some("bar"));

    let bad_ns_json = r#"{"name": "foo", "namespace": ""}"#;
    let not_kw = serde_json::from_str::<Keyword>(bad_ns_json);
    assert!(not_kw.is_err());

    let bad_ns_json = r#"{"name": "", "namespace": "bar"}"#;
    let not_kw = serde_json::from_str::<Keyword>(bad_ns_json);
    assert!(not_kw.is_err());
}


#[test]
fn test_serialize_value() {
    let test = "[
        :find ?id ?reason ?ts
        :in $
        :where
            [?id :session/startReason ?reason ?tx]
            [?tx :db/txInstant ?ts]
            (not-join [?id] [?id :session/endReason _])
    ]";

    let expected = r#"
{
  "Vector": [
    {
      "Keyword": {
        "namespace": null,
        "name": "find"
      }
    },
    {
      "PlainSymbol": "?id"
    },
    {
      "PlainSymbol": "?reason"
    },
    {
      "PlainSymbol": "?ts"
    },
    {
      "Keyword": {
        "namespace": null,
        "name": "in"
      }
    },
    {
      "PlainSymbol": "$"
    },
    {
      "Keyword": {
        "namespace": null,
        "name": "where"
      }
    },
    {
      "Vector": [
        {
          "PlainSymbol": "?id"
        },
        {
          "Keyword": {
            "namespace": "session",
            "name": "startReason"
          }
        },
        {
          "PlainSymbol": "?reason"
        },
        {
          "PlainSymbol": "?tx"
        }
      ]
    },
    {
      "Vector": [
        {
          "PlainSymbol": "?tx"
        },
        {
          "Keyword": {
            "namespace": "db",
            "name": "txInstant"
          }
        },
        {
          "PlainSymbol": "?ts"
        }
      ]
    },
    {
      "List": [
        {
          "PlainSymbol": "not-join"
        },
        {
          "Vector": [
            {
              "PlainSymbol": "?id"
            }
          ]
        },
        {
          "Vector": [
            {
              "PlainSymbol": "?id"
            },
            {
              "Keyword": {
                "namespace": "session",
                "name": "endReason"
              }
            },
            {
              "PlainSymbol": "_"
            }
          ]
        }
      ]
    }
  ]
}"#;

    let edn: Value = parse::value(test).unwrap().into();
    let parsed: Value = serde_json::from_str(expected).unwrap();

    assert_eq!(edn, parsed);
}
