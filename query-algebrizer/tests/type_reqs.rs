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

mod utils;

use utils::{
    alg,
    SchemaBuilder,
    bails,
};

use mentat_core::{
    Schema,
    ValueType,
};

fn prepopulated_schema() -> Schema {
    SchemaBuilder::new()
        .define_simple_attr("test", "boolean", ValueType::Boolean, false)
        .define_simple_attr("test", "long", ValueType::Long, false)
        .define_simple_attr("test", "double", ValueType::Double, false)
        .define_simple_attr("test", "string", ValueType::String, false)
        .define_simple_attr("test", "keyword", ValueType::Keyword, false)
        .define_simple_attr("test", "uuid", ValueType::Uuid, false)
        .define_simple_attr("test", "instant", ValueType::Instant, false)
        .define_simple_attr("test", "ref", ValueType::Ref, false)
        .schema
}

#[test]
fn test_empty_known() {
    let type_names = [
        "boolean",
        "long",
        "double",
        "string",
        "keyword",
        "uuid",
        "instant",
        "ref",
    ];
    let schema = prepopulated_schema();
    for known_type in type_names.iter() {
        for required in type_names.iter() {
            let q = format!("[:find ?e :where [?e :test/{} ?v] [({} ?v)]]",
                            known_type, required);
            println!("Query: {}", q);
            let cc = alg(&schema, &q);
            // It should only be empty if the known type and our requirement differ.
            assert_eq!(cc.empty_because.is_some(), known_type != required,
                       "known_type = {}; required = {}", known_type, required);
        }
    }
}

#[test]
fn test_multiple() {
    let schema = prepopulated_schema();
    let q = "[:find ?e :where [?e _ ?v] [(long ?v)] [(double ?v)]]";
    let cc = alg(&schema, &q);
    assert!(cc.empty_because.is_some());
}

#[test]
fn test_unbound() {
    let schema = prepopulated_schema();
    bails(&schema, "[:find ?e :where [(string ?e)]]");
}
