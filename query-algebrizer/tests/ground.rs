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
    TypedValue,
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
    BindingError,
    ConjoiningClauses,
    ComputedTable,
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
    schema.schema_map.insert(e, a);
}

fn prepopulated_schema() -> Schema {
    let mut schema = Schema::default();
    associate_ident(&mut schema, NamespacedKeyword::new("foo", "name"), 65);
    associate_ident(&mut schema, NamespacedKeyword::new("foo", "knows"), 66);
    associate_ident(&mut schema, NamespacedKeyword::new("foo", "parent"), 67);
    associate_ident(&mut schema, NamespacedKeyword::new("foo", "age"), 68);
    associate_ident(&mut schema, NamespacedKeyword::new("foo", "height"), 69);
    add_attribute(&mut schema, 65, Attribute {
        value_type: ValueType::String,
        multival: false,
        ..Default::default()
    });
    add_attribute(&mut schema, 66, Attribute {
        value_type: ValueType::Ref,
        multival: true,
        ..Default::default()
    });
    add_attribute(&mut schema, 67, Attribute {
        value_type: ValueType::String,
        multival: true,
        ..Default::default()
    });
    add_attribute(&mut schema, 68, Attribute {
        value_type: ValueType::Long,
        multival: false,
        ..Default::default()
    });
    add_attribute(&mut schema, 69, Attribute {
        value_type: ValueType::Long,
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
fn test_ground_doesnt_bail_for_type_conflicts() {
    // We know `?x` to be a ref, but we're attempting to ground it to a Double.
    // The query can return no results.
    let q = r#"[:find ?x :where [?x :foo/knows ?p] [(ground 9.95) ?x]]"#;
    let schema = prepopulated_schema();
    let cc = alg(&schema, &q);
    assert!(cc.empty_because.is_some());
}

#[test]
fn test_ground_tuple_fails_impossible() {
    let q = r#"[:find ?x :where [?x :foo/knows ?p] [(ground [5 9.95]) [?x ?p]]]"#;
    let schema = prepopulated_schema();
    let cc = alg(&schema, &q);
    assert!(cc.empty_because.is_some());
}

#[test]
fn test_ground_scalar_fails_impossible() {
    let q = r#"[:find ?x :where [?x :foo/knows ?p] [(ground true) ?p]]"#;
    let schema = prepopulated_schema();
    let cc = alg(&schema, &q);
    assert!(cc.empty_because.is_some());
}

#[test]
fn test_ground_coll_skips_impossible() {
    // We know `?x` to be a ref, but we're attempting to ground it to a Double.
    // The query can return no results.
    let q = r#"[:find ?x :where [?x :foo/knows ?p] [(ground [5 9.95 11]) [?x ...]]]"#;
    let schema = prepopulated_schema();
    let cc = alg(&schema, &q);
    assert!(cc.empty_because.is_none());
    assert_eq!(cc.computed_tables[0], ComputedTable::NamedValues {
        names: vec![Variable::from_valid_name("?x")],
        values: vec![TypedValue::Ref(5), TypedValue::Ref(11)],
    });
}

#[test]
fn test_ground_coll_fails_if_all_impossible() {
    let q = r#"[:find ?x :where [?x :foo/knows ?p] [(ground [5.1 5.2]) [?p ...]]]"#;
    let schema = prepopulated_schema();
    let cc = alg(&schema, &q);
    assert!(cc.empty_because.is_some());
}

#[test]
fn test_ground_rel_skips_impossible() {
    let q = r#"[:find ?x :where [?x :foo/knows ?p] [(ground [[8 "foo"] [5 7] [9.95 9] [11 12]]) [[?x ?p]]]]"#;
    let schema = prepopulated_schema();
    let cc = alg(&schema, &q);
    assert!(cc.empty_because.is_none());
    assert_eq!(cc.computed_tables[0], ComputedTable::NamedValues {
        names: vec![Variable::from_valid_name("?x"), Variable::from_valid_name("?p")],
        values: vec![TypedValue::Ref(5), TypedValue::Ref(7), TypedValue::Ref(11), TypedValue::Ref(12)],
    });
}

#[test]
fn test_ground_rel_fails_if_all_impossible() {
    let q = r#"[:find ?x :where [?x :foo/knows ?p] [(ground [[11 5.1] [12 5.2]]) [[?x ?p]]]]"#;
    let schema = prepopulated_schema();
    let cc = alg(&schema, &q);
    assert!(cc.empty_because.is_some());
}

#[test]
fn test_ground_tuple_rejects_all_placeholders() {
    let q = r#"[:find ?x :where [?x :foo/knows ?p] [(ground [8 "foo" 3]) [_ _ _]]]"#;
    let schema = prepopulated_schema();
    bails(&schema, &q);
}

#[test]
fn test_ground_rel_rejects_all_placeholders() {
    let q = r#"[:find ?x :where [?x :foo/knows ?p] [(ground [[8 "foo"]]) [[_ _]]]]"#;
    let schema = prepopulated_schema();
    bails(&schema, &q);
}

#[test]
fn test_ground_tuple_placeholders() {
    let q = r#"[:find ?x :where [?x :foo/knows ?p] [(ground [8 "foo" 3]) [?x _ ?p]]]"#;
    let schema = prepopulated_schema();
    let cc = alg(&schema, &q);
    assert!(cc.empty_because.is_none());
    assert_eq!(cc.bound_value(&Variable::from_valid_name("?x")), Some(TypedValue::Ref(8)));
    assert_eq!(cc.bound_value(&Variable::from_valid_name("?p")), Some(TypedValue::Ref(3)));
}

#[test]
fn test_ground_rel_placeholders() {
    let q = r#"[:find ?x :where [?x :foo/knows ?p] [(ground [[8 "foo" 3] [5 false 7] [5 9.95 9]]) [[?x _ ?p]]]]"#;
    let schema = prepopulated_schema();
    let cc = alg(&schema, &q);
    assert!(cc.empty_because.is_none());
    assert_eq!(cc.computed_tables[0], ComputedTable::NamedValues {
        names: vec![Variable::from_valid_name("?x"), Variable::from_valid_name("?p")],
        values: vec![
            TypedValue::Ref(8),
            TypedValue::Ref(3),
            TypedValue::Ref(5),
            TypedValue::Ref(7),
            TypedValue::Ref(5),
            TypedValue::Ref(9),
        ],
    });
}

// Nothing to do with ground, but while we're here…
#[test]
fn test_multiple_reference_type_failure() {
    let q = r#"[:find ?x :where [?x :foo/age ?y] [?x :foo/knows ?y]]"#;
    let schema = prepopulated_schema();
    let cc = alg(&schema, &q);
    assert!(cc.empty_because.is_some());
}

#[test]
fn test_ground_tuple_infers_types() {
    let q = r#"[:find ?x :where [?x :foo/age ?v] [(ground [8 10]) [?x ?v]]]"#;
    let schema = prepopulated_schema();
    let cc = alg(&schema, &q);
    assert!(cc.empty_because.is_none());
    assert_eq!(cc.bound_value(&Variable::from_valid_name("?x")), Some(TypedValue::Ref(8)));
    assert_eq!(cc.bound_value(&Variable::from_valid_name("?v")), Some(TypedValue::Long(10)));
}

#[test]
fn test_ground_rel_infers_types() {
    let q = r#"[:find ?x :where [?x :foo/age ?v] [(ground [[8 10]]) [[?x ?v]]]]"#;
    let schema = prepopulated_schema();
    let cc = alg(&schema, &q);
    assert!(cc.empty_because.is_none());
    assert_eq!(cc.computed_tables[0], ComputedTable::NamedValues {
        names: vec![Variable::from_valid_name("?x"), Variable::from_valid_name("?v")],
        values: vec![TypedValue::Ref(8), TypedValue::Long(10)],
    });
}

#[test]
fn test_ground_coll_heterogeneous_types() {
    let q = r#"[:find ?x :where [?x _ ?v] [(ground [false 8.5]) [?v ...]]]"#;
    let schema = prepopulated_schema();
    let e = bails(&schema, &q);
    match e {
        Error(ErrorKind::InvalidGroundConstant, _) => {
        },
        _ => {
            panic!();
        },
    }
}

#[test]
fn test_ground_rel_heterogeneous_types() {
    let q = r#"[:find ?x :where [?x _ ?v] [(ground [[false] [5]]) [[?v]]]]"#;
    let schema = prepopulated_schema();
    let e = bails(&schema, &q);
    match e {
        Error(ErrorKind::InvalidGroundConstant, _) => {
        },
        _ => {
            panic!();
        },
    }
}

#[test]
fn test_ground_tuple_duplicate_vars() {
    let q = r#"[:find ?x :where [?x :foo/age ?v] [(ground [8 10]) [?x ?x]]]"#;
    let schema = prepopulated_schema();
    let e = bails(&schema, &q);
    match e {
        Error(ErrorKind::InvalidBinding(v, e), _) => {
            assert_eq!(v, PlainSymbol::new("ground"));
            assert_eq!(e, BindingError::RepeatedBoundVariable);
        },
        _ => {
            panic!();
        },
    }
}

#[test]
fn test_ground_rel_duplicate_vars() {
    let q = r#"[:find ?x :where [?x :foo/age ?v] [(ground [[8 10]]) [[?x ?x]]]]"#;
    let schema = prepopulated_schema();
    let e = bails(&schema, &q);
    match e {
        Error(ErrorKind::InvalidBinding(v, e), _) => {
            assert_eq!(v, PlainSymbol::new("ground"));
            assert_eq!(e, BindingError::RepeatedBoundVariable);
        },
        _ => {
            panic!();
        },
    }
}

#[test]
fn test_ground_nonexistent_variable_invalid() {
    let q = r#"[:find ?x ?e :where [?e _ ?x] (not [(ground 17) ?v])]"#;
    let schema = prepopulated_schema();
    let e = bails(&schema, &q);
    match e {
        Error(ErrorKind::UnboundVariable(PlainSymbol(v)), _) => {
            assert_eq!(v, "?v".to_string());
        },
        _ => {
            panic!();
        },
    }
}
