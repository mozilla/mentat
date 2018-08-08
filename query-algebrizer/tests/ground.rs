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
extern crate mentat_core;
extern crate core_traits;
extern crate mentat_query_algebrizer;

mod utils;

use std::collections::BTreeMap;

use core_traits::{
    ValueType,
    TypedValue,
};

use mentat_core::{
    Attribute,
    Schema,
};

use edn::query::{
    Keyword,
    PlainSymbol,
    Variable,
};

use mentat_query_algebrizer::{
    AlgebrizerError,
    BindingError,
    ComputedTable,
    Known,
    QueryInputs,
};

use utils::{
    add_attribute,
    alg,
    associate_ident,
    bails,
    bails_with_inputs,
};

fn prepopulated_schema() -> Schema {
    let mut schema = Schema::default();
    associate_ident(&mut schema, Keyword::namespaced("foo", "name"), 65);
    associate_ident(&mut schema, Keyword::namespaced("foo", "knows"), 66);
    associate_ident(&mut schema, Keyword::namespaced("foo", "parent"), 67);
    associate_ident(&mut schema, Keyword::namespaced("foo", "age"), 68);
    associate_ident(&mut schema, Keyword::namespaced("foo", "height"), 69);
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

#[test]
fn test_ground_doesnt_bail_for_type_conflicts() {
    // We know `?x` to be a ref, but we're attempting to ground it to a Double.
    // The query can return no results.
    let q = r#"[:find ?x :where [?x :foo/knows ?p] [(ground 9.95) ?x]]"#;
    let schema = prepopulated_schema();
    let known = Known::for_schema(&schema);
    let cc = alg(known, &q);
    assert!(cc.empty_because.is_some());
}

#[test]
fn test_ground_tuple_fails_impossible() {
    let q = r#"[:find ?x :where [?x :foo/knows ?p] [(ground [5 9.95]) [?x ?p]]]"#;
    let schema = prepopulated_schema();
    let known = Known::for_schema(&schema);
    let cc = alg(known, &q);
    assert!(cc.empty_because.is_some());
}

#[test]
fn test_ground_scalar_fails_impossible() {
    let q = r#"[:find ?x :where [?x :foo/knows ?p] [(ground true) ?p]]"#;
    let schema = prepopulated_schema();
    let known = Known::for_schema(&schema);
    let cc = alg(known, &q);
    assert!(cc.empty_because.is_some());
}

#[test]
fn test_ground_coll_skips_impossible() {
    // We know `?x` to be a ref, but we're attempting to ground it to a Double.
    // The query can return no results.
    let q = r#"[:find ?x :where [?x :foo/knows ?p] [(ground [5 9.95 11]) [?x ...]]]"#;
    let schema = prepopulated_schema();
    let known = Known::for_schema(&schema);
    let cc = alg(known, &q);
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
    let known = Known::for_schema(&schema);
    let cc = alg(known, &q);
    assert!(cc.empty_because.is_some());
}

#[test]
fn test_ground_rel_skips_impossible() {
    let q = r#"[:find ?x :where [?x :foo/knows ?p] [(ground [[8 "foo"] [5 7] [9.95 9] [11 12]]) [[?x ?p]]]]"#;
    let schema = prepopulated_schema();
    let known = Known::for_schema(&schema);
    let cc = alg(known, &q);
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
    let known = Known::for_schema(&schema);
    let cc = alg(known, &q);
    assert!(cc.empty_because.is_some());
}

#[test]
fn test_ground_tuple_rejects_all_placeholders() {
    let q = r#"[:find ?x :where [?x :foo/knows ?p] [(ground [8 "foo" 3]) [_ _ _]]]"#;
    let schema = prepopulated_schema();
    let known = Known::for_schema(&schema);
    bails(known, &q);
}

#[test]
fn test_ground_rel_rejects_all_placeholders() {
    let q = r#"[:find ?x :where [?x :foo/knows ?p] [(ground [[8 "foo"]]) [[_ _]]]]"#;
    let schema = prepopulated_schema();
    let known = Known::for_schema(&schema);
    bails(known, &q);
}

#[test]
fn test_ground_tuple_placeholders() {
    let q = r#"[:find ?x :where [?x :foo/knows ?p] [(ground [8 "foo" 3]) [?x _ ?p]]]"#;
    let schema = prepopulated_schema();
    let known = Known::for_schema(&schema);
    let cc = alg(known, &q);
    assert!(cc.empty_because.is_none());
    assert_eq!(cc.bound_value(&Variable::from_valid_name("?x")), Some(TypedValue::Ref(8)));
    assert_eq!(cc.bound_value(&Variable::from_valid_name("?p")), Some(TypedValue::Ref(3)));
}

#[test]
fn test_ground_rel_placeholders() {
    let q = r#"[:find ?x :where [?x :foo/knows ?p] [(ground [[8 "foo" 3] [5 false 7] [5 9.95 9]]) [[?x _ ?p]]]]"#;
    let schema = prepopulated_schema();
    let known = Known::for_schema(&schema);
    let cc = alg(known, &q);
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
    let known = Known::for_schema(&schema);
    let cc = alg(known, &q);
    assert!(cc.empty_because.is_some());
}

#[test]
fn test_ground_tuple_infers_types() {
    let q = r#"[:find ?x :where [?x :foo/age ?v] [(ground [8 10]) [?x ?v]]]"#;
    let schema = prepopulated_schema();
    let known = Known::for_schema(&schema);
    let cc = alg(known, &q);
    assert!(cc.empty_because.is_none());
    assert_eq!(cc.bound_value(&Variable::from_valid_name("?x")), Some(TypedValue::Ref(8)));
    assert_eq!(cc.bound_value(&Variable::from_valid_name("?v")), Some(TypedValue::Long(10)));
}

// We determine the types of variables in the query in an early first pass, and thus we can
// safely use idents to name entities, including attributes.
#[test]
fn test_ground_coll_infers_attribute_types() {
    let q = r#"[:find ?x
                :where [(ground [:foo/age :foo/height]) [?a ...]]
                       [?x ?a ?v]]"#;
    let schema = prepopulated_schema();
    let known = Known::for_schema(&schema);
    let cc = alg(known, &q);
    assert!(cc.empty_because.is_none());
}

#[test]
fn test_ground_rel_infers_types() {
    let q = r#"[:find ?x :where [?x :foo/age ?v] [(ground [[8 10]]) [[?x ?v]]]]"#;
    let schema = prepopulated_schema();
    let known = Known::for_schema(&schema);
    let cc = alg(known, &q);
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
    let known = Known::for_schema(&schema);
    assert_eq!(bails(known, &q),
               AlgebrizerError::InvalidGroundConstant);
}

#[test]
fn test_ground_rel_heterogeneous_types() {
    let q = r#"[:find ?x :where [?x _ ?v] [(ground [[false] [5]]) [[?v]]]]"#;
    let schema = prepopulated_schema();
    let known = Known::for_schema(&schema);
    assert_eq!(bails(known, &q),
               AlgebrizerError::InvalidGroundConstant);
}

#[test]
fn test_ground_tuple_duplicate_vars() {
    let q = r#"[:find ?x :where [?x :foo/age ?v] [(ground [8 10]) [?x ?x]]]"#;
    let schema = prepopulated_schema();
    let known = Known::for_schema(&schema);
    assert_eq!(bails(known, &q),
               AlgebrizerError::InvalidBinding(PlainSymbol::plain("ground"), BindingError::RepeatedBoundVariable));
}

#[test]
fn test_ground_rel_duplicate_vars() {
    let q = r#"[:find ?x :where [?x :foo/age ?v] [(ground [[8 10]]) [[?x ?x]]]]"#;
    let schema = prepopulated_schema();
    let known = Known::for_schema(&schema);
    assert_eq!(bails(known, &q),
               AlgebrizerError::InvalidBinding(PlainSymbol::plain("ground"), BindingError::RepeatedBoundVariable));
}

#[test]
fn test_ground_nonexistent_variable_invalid() {
    let q = r#"[:find ?x ?e :where [?e _ ?x] (not [(ground 17) ?v])]"#;
    let schema = prepopulated_schema();
    let known = Known::for_schema(&schema);
    assert_eq!(bails(known, &q),
               AlgebrizerError::UnboundVariable(PlainSymbol::plain("?v")));
}

#[test]
fn test_unbound_input_variable_invalid() {
    let schema = prepopulated_schema();
    let known = Known::for_schema(&schema);
    let q = r#"[:find ?y ?age :in ?x :where [(ground [?x]) [?y ...]] [?y :foo/age ?age]]"#;

    // This fails even if we know the type: we don't support grounding bindings
    // that aren't known at algebrizing time.
    let mut types = BTreeMap::default();
    types.insert(Variable::from_valid_name("?x"), ValueType::Ref);

    let i = QueryInputs::new(types, BTreeMap::default()).expect("valid QueryInputs");

    assert_eq!(bails_with_inputs(known, &q, i),
               AlgebrizerError::UnboundVariable(PlainSymbol::plain("?x")));
}
