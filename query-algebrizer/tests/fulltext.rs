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
extern crate core_traits;
extern crate mentat_query;
extern crate mentat_query_algebrizer;

mod utils;

use core_traits::{
    ValueType,
};

use mentat_core::{
    Attribute,
    Schema,
};

use mentat_query::{
    Keyword,
};

use utils::{
    add_attribute,
    alg,
    associate_ident,
};

use mentat_query_algebrizer::Known;

fn prepopulated_schema() -> Schema {
    let mut schema = Schema::default();
    associate_ident(&mut schema, Keyword::namespaced("foo", "name"), 65);
    associate_ident(&mut schema, Keyword::namespaced("foo", "description"), 66);
    associate_ident(&mut schema, Keyword::namespaced("foo", "parent"), 67);
    associate_ident(&mut schema, Keyword::namespaced("foo", "age"), 68);
    associate_ident(&mut schema, Keyword::namespaced("foo", "height"), 69);
    add_attribute(&mut schema, 65, Attribute {
        value_type: ValueType::String,
        multival: false,
        ..Default::default()
    });
    add_attribute(&mut schema, 66, Attribute {
        value_type: ValueType::String,
        index: true,
        fulltext: true,
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
fn test_apply_fulltext() {
    let schema = prepopulated_schema();
    let known = Known::for_schema(&schema);

    // If you use a non-FTS attribute, we will short-circuit.
    let query = r#"[:find ?val
                    :where [(fulltext $ :foo/name "hello") [[?entity ?val _ _]]]]"#;
    assert!(alg(known, query).is_known_empty());

    // If you get a type mismatch, we will short-circuit.
    let query = r#"[:find ?val
                    :where [(fulltext $ :foo/description "hello") [[?entity ?val ?tx ?score]]]
                    [?score :foo/bar _]]"#;
    assert!(alg(known, query).is_known_empty());
}
