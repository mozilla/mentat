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
extern crate mentat_query_projector;
extern crate query_projector_traits;

use core_traits::{
    Entid,
    ValueType,
};

use mentat_core::{
    Attribute,
    Schema,
};

use edn::query::{
    Keyword,
};

use mentat_query_algebrizer::{
    Known,
    algebrize,
    parse_find_string,
};

use mentat_query_projector::{
    query_projection,
};

// These are helpers that tests use to build Schema instances.
fn associate_ident(schema: &mut Schema, i: Keyword, e: Entid) {
    schema.entid_map.insert(e, i.clone());
    schema.ident_map.insert(i.clone(), e);
}

fn add_attribute(schema: &mut Schema, e: Entid, a: Attribute) {
    schema.attribute_map.insert(e, a);
}

fn prepopulated_schema() -> Schema {
    let mut schema = Schema::default();
    associate_ident(&mut schema, Keyword::namespaced("foo", "name"), 65);
    associate_ident(&mut schema, Keyword::namespaced("foo", "age"), 68);
    associate_ident(&mut schema, Keyword::namespaced("foo", "height"), 69);
    add_attribute(&mut schema, 65, Attribute {
        value_type: ValueType::String,
        multival: false,
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
fn test_aggregate_unsuitable_type() {
    let schema = prepopulated_schema();

    let query = r#"[:find (avg ?e)
                    :where
                    [?e :foo/age ?a]]"#;

    // While the query itself algebrizes and parses…
    let parsed = parse_find_string(query).expect("query input to have parsed");
    let algebrized = algebrize(Known::for_schema(&schema), parsed).expect("query algebrizes");

    // … when we look at the projection list, we cannot reconcile the types.
    assert!(query_projection(&schema, &algebrized).is_err());
}

#[test]
fn test_the_without_max_or_min() {
    let schema = prepopulated_schema();

    let query = r#"[:find (the ?e) ?a
                    :where
                    [?e :foo/age ?a]]"#;

    // While the query itself algebrizes and parses…
    let parsed = parse_find_string(query).expect("query input to have parsed");
    let algebrized = algebrize(Known::for_schema(&schema), parsed).expect("query algebrizes");

    // … when we look at the projection list, we cannot reconcile the types.
    let projection = query_projection(&schema, &algebrized);
    assert!(projection.is_err());
    use query_projector_traits::errors::{
        ProjectorError,
    };
    match projection.err().expect("expected failure") {
        ProjectorError::InvalidProjection(s) => {
                assert_eq!(s.as_str(), "Warning: used `the` without `min` or `max`.");
            },
        _ => panic!(),
    }
}
