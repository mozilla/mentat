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
extern crate mentat_query_translator;
extern crate mentat_sql;

use mentat_query::NamespacedKeyword;

use mentat_core::{
    Attribute,
    Entid,
    Schema,
    ValueType,
};

use mentat_query_parser::parse_find_string;
use mentat_query_algebrizer::algebrize;
use mentat_query_translator::{
    cc_to_exists,
};

use mentat_sql::SQLQuery;

fn associate_ident(schema: &mut Schema, i: NamespacedKeyword, e: Entid) {
    schema.entid_map.insert(e, i.clone());
    schema.ident_map.insert(i.clone(), e);
}

fn add_attribute(schema: &mut Schema, e: Entid, a: Attribute) {
    schema.schema_map.insert(e, a);
}

#[test]
#[should_panic(expected = "parse failed")]
fn test_coll() {
    let mut schema = Schema::default();
    associate_ident(&mut schema, NamespacedKeyword::new("foo", "bar"), 99);
    add_attribute(&mut schema, 99, Attribute {
        value_type: ValueType::String,
        ..Default::default()
    });

    let input = r#"[:find [?x ...] :where [?x :foo/bar "yyy"]]"#;
    let parsed = parse_find_string(input).expect("parse failed");
    let algebrized = algebrize(&schema, parsed);
    let select = query_to_select(algebrized);
    let SQLQuery { sql, args } = select.to_sql_query().unwrap();
    assert_eq!(sql, "SELECT `datoms00`.e AS `?x` FROM `datoms` AS `datoms00` WHERE `datoms00`.a = 99 AND `datoms00`.v = $v0");
    assert_eq!(args, vec![("$v0".to_string(), "yyy".to_string())]);
}

#[test]
fn test_rel() {
    let mut schema = Schema::default();
    associate_ident(&mut schema, NamespacedKeyword::new("foo", "bar"), 99);
    add_attribute(&mut schema, 99, Attribute {
        value_type: ValueType::String,
        ..Default::default()
    });

    let input = r#"[:find ?x :where [?x :foo/bar "yyy"]]"#;
    let parsed = parse_find_string(input).expect("parse failed");
    let algebrized = algebrize(&schema, parsed);
    let select = cc_to_exists(algebrized.cc);
    let SQLQuery { sql, args } = select.to_sql_query().unwrap();
    assert_eq!(sql, "SELECT 1 FROM `datoms` AS `datoms00` WHERE `datoms00`.a = 99 AND `datoms00`.v = $v0");
    assert_eq!(args, vec![("$v0".to_string(), "yyy".to_string())]);
}
