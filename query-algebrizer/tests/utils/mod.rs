// Copyright 2018 Mozilla
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

// This is required to prevent warnings about unused functions in this file just
// because it's unused in a single file (tests that don't use every function in
// this module will get warnings otherwise).
#![allow(dead_code)]

use mentat_core::{
    Attribute,
    Entid,
    Schema,
    ValueType,
};

use mentat_query_parser::{
    parse_find_string,
};

use mentat_query::{
    NamespacedKeyword,
};

use mentat_query_algebrizer::{
    ConjoiningClauses,
    Error,
    Known,
    QueryInputs,
    algebrize,
    algebrize_with_inputs,
};

// Common utility functions used in multiple test files.

// These are helpers that tests use to build Schema instances.
pub fn associate_ident(schema: &mut Schema, i: NamespacedKeyword, e: Entid) {
    schema.entid_map.insert(e, i.clone());
    schema.ident_map.insert(i.clone(), e);
}

pub fn add_attribute(schema: &mut Schema, e: Entid, a: Attribute) {
    schema.attribute_map.insert(e, a);
}

pub struct SchemaBuilder {
    pub schema: Schema,
    pub counter: Entid,
}

impl SchemaBuilder {
    pub fn new() -> SchemaBuilder {
        SchemaBuilder {
            schema: Schema::default(),
            counter: 65
        }
    }

    pub fn define_attr(mut self, kw: NamespacedKeyword, attr: Attribute) -> Self {
        associate_ident(&mut self.schema, kw, self.counter);
        add_attribute(&mut self.schema, self.counter, attr);
        self.counter += 1;
        self
    }

    pub fn define_simple_attr<T>(self,
                                 keyword_ns: T,
                                 keyword_name: T,
                                 value_type: ValueType,
                                 multival: bool) -> Self
        where T: Into<String>
    {
        self.define_attr(NamespacedKeyword::new(keyword_ns, keyword_name), Attribute {
            value_type,
            multival,
            ..Default::default()
        })
    }
}

pub fn bails(known: Known, input: &str) -> Error {
    let parsed = parse_find_string(input).expect("query input to have parsed");
    algebrize(known, parsed).expect_err("algebrize to have failed")
}

pub fn bails_with_inputs(known: Known, input: &str, inputs: QueryInputs) -> Error {
    let parsed = parse_find_string(input).expect("query input to have parsed");
    algebrize_with_inputs(known, parsed, 0, inputs).expect_err("algebrize to have failed")
}

pub fn alg(known: Known, input: &str) -> ConjoiningClauses {
    let parsed = parse_find_string(input).expect("query input to have parsed");
    algebrize(known, parsed).expect("algebrizing to have succeeded").cc
}
