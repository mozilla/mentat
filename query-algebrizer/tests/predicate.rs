// Copyright 2016 Mozilla
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

extern crate mentat_core;
extern crate mentat_query;
extern crate mentat_query_algebrizer;
extern crate mentat_query_parser;

use mentat_core::{
    Attribute,
    Entid,
    Schema,
    ValueType,
    ValueTypeSet,
};

use mentat_query_parser::{
    parse_find_string,
};

use mentat_query::{
    NamespacedKeyword,
    PlainSymbol,
    Variable,
};

use mentat_query_algebrizer::{
    ConjoiningClauses,
    EmptyBecause,
    Error,
    ErrorKind,
    algebrize,
};

// These are helpers that tests use to build Schema instances.
#[cfg(test)]
fn associate_ident(schema: &mut Schema, i: NamespacedKeyword, e: Entid) {
    schema.entid_map.insert(e, i.clone());
    schema.ident_map.insert(i.clone(), e);
}

#[cfg(test)]
fn add_attribute(schema: &mut Schema, e: Entid, a: Attribute) {
    schema.attribute_map.insert(e, a);
}

fn prepopulated_schema() -> Schema {
    let mut schema = Schema::default();
    associate_ident(&mut schema, NamespacedKeyword::new("foo", "date"), 65);
    associate_ident(&mut schema, NamespacedKeyword::new("foo", "double"), 66);
    add_attribute(&mut schema, 65, Attribute {
        value_type: ValueType::Instant,
        multival: false,
        ..Default::default()
    });
    add_attribute(&mut schema, 66, Attribute {
        value_type: ValueType::Double,
        multival: false,
        ..Default::default()
    });
    schema
}

fn bails(schema: &Schema, input: &str) -> Error {
    let parsed = parse_find_string(input).expect("query input to have parsed");
    algebrize(schema.into(), parsed).expect_err("algebrize to have failed")
}

fn alg(schema: &Schema, input: &str) -> ConjoiningClauses {
    let parsed = parse_find_string(input).expect("query input to have parsed");
    algebrize(schema.into(), parsed).expect("algebrizing to have succeeded").cc
}

#[test]
fn test_instant_predicates_require_instants() {
    let schema = prepopulated_schema();

    // You can't use a string for an inequality: this is a straight-up error.
    let query = r#"[:find ?e
                    :where
                    [?e :foo/date ?t]
                    [(> ?t "2017-06-16T00:56:41.257Z")]]"#;
    match bails(&schema, query).0 {
        ErrorKind::InvalidArgument(op, why, idx) => {
            assert_eq!(op, PlainSymbol::new(">"));
            assert_eq!(why, "numeric or instant");
            assert_eq!(idx, 1);
        },
        _ => panic!("Expected InvalidArgument."),
    }

    let query = r#"[:find ?e
                    :where
                    [?e :foo/date ?t]
                    [(> "2017-06-16T00:56:41.257Z", ?t)]]"#;
    match bails(&schema, query).0 {
        ErrorKind::InvalidArgument(op, why, idx) => {
            assert_eq!(op, PlainSymbol::new(">"));
            assert_eq!(why, "numeric or instant");
            assert_eq!(idx, 0);                      // We get this right.
        },
        _ => panic!("Expected InvalidArgument."),
    }

    // You can try using a number, which is valid input to a numeric predicate.
    // In this store and query, though, that means we expect `?t` to be both
    // an instant and a number, so the query is known-empty.
    let query = r#"[:find ?e
                    :where
                    [?e :foo/date ?t]
                    [(> ?t 1234512345)]]"#;
    let cc = alg(&schema, query);
    assert!(cc.is_known_empty());
    assert_eq!(cc.empty_because.unwrap(),
               EmptyBecause::TypeMismatch {
                   var: Variable::from_valid_name("?t"),
                   existing: ValueTypeSet::of_one(ValueType::Instant),
                   desired: ValueTypeSet::of_numeric_types(),
    });

    // You can compare doubles to longs.
    let query = r#"[:find ?e
                    :where
                    [?e :foo/double ?t]
                    [(< ?t 1234512345)]]"#;
    let cc = alg(&schema, query);
    assert!(!cc.is_known_empty());
    assert_eq!(cc.known_type(&Variable::from_valid_name("?t")).expect("?t is known"),
               ValueType::Double);
}
