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

use std::rc::Rc;

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
    query_to_select,
};

use mentat_sql::SQLQuery;

fn associate_ident(schema: &mut Schema, i: NamespacedKeyword, e: Entid) {
    schema.entid_map.insert(e, i.clone());
    schema.ident_map.insert(i.clone(), e);
}

fn add_attribute(schema: &mut Schema, e: Entid, a: Attribute) {
    schema.schema_map.insert(e, a);
}

fn translate<T: Into<Option<u64>>>(schema: &Schema, input: &'static str, limit: T) -> SQLQuery {
    let parsed = parse_find_string(input).expect("parse failed");
    let mut algebrized = algebrize(schema, parsed).expect("algebrize failed");
    algebrized.apply_limit(limit.into());
    let select = query_to_select(algebrized);
    select.query.to_sql_query().unwrap()
}

fn prepopulated_typed_schema(foo_type: ValueType) -> Schema {
    let mut schema = Schema::default();
    associate_ident(&mut schema, NamespacedKeyword::new("foo", "bar"), 99);
    add_attribute(&mut schema, 99, Attribute {
        value_type: foo_type,
        ..Default::default()
    });
    schema
}

fn prepopulated_schema() -> Schema {
    prepopulated_typed_schema(ValueType::String)
}

fn make_arg(name: &'static str, value: &'static str) -> (String, Rc<String>) {
    (name.to_string(), Rc::new(value.to_string()))
}

#[test]
fn test_scalar() {
    let schema = prepopulated_schema();

    let input = r#"[:find ?x . :where [?x :foo/bar "yyy"]]"#;
    let SQLQuery { sql, args } = translate(&schema, input, None);
    assert_eq!(sql, "SELECT `datoms00`.e AS `?x` FROM `datoms` AS `datoms00` WHERE `datoms00`.a = 99 AND `datoms00`.v = $v0 LIMIT 1");
    assert_eq!(args, vec![make_arg("$v0", "yyy")]);
}

#[test]
fn test_tuple() {
    let schema = prepopulated_schema();

    let input = r#"[:find [?x] :where [?x :foo/bar "yyy"]]"#;
    let SQLQuery { sql, args } = translate(&schema, input, None);
    assert_eq!(sql, "SELECT `datoms00`.e AS `?x` FROM `datoms` AS `datoms00` WHERE `datoms00`.a = 99 AND `datoms00`.v = $v0 LIMIT 1");
    assert_eq!(args, vec![make_arg("$v0", "yyy")]);
}

#[test]
fn test_coll() {
    let schema = prepopulated_schema();

    let input = r#"[:find [?x ...] :where [?x :foo/bar "yyy"]]"#;
    let SQLQuery { sql, args } = translate(&schema, input, None);
    assert_eq!(sql, "SELECT DISTINCT `datoms00`.e AS `?x` FROM `datoms` AS `datoms00` WHERE `datoms00`.a = 99 AND `datoms00`.v = $v0");
    assert_eq!(args, vec![make_arg("$v0", "yyy")]);
}

#[test]
fn test_rel() {
    let schema = prepopulated_schema();

    let input = r#"[:find ?x :where [?x :foo/bar "yyy"]]"#;
    let SQLQuery { sql, args } = translate(&schema, input, None);
    assert_eq!(sql, "SELECT DISTINCT `datoms00`.e AS `?x` FROM `datoms` AS `datoms00` WHERE `datoms00`.a = 99 AND `datoms00`.v = $v0");
    assert_eq!(args, vec![make_arg("$v0", "yyy")]);
}

#[test]
fn test_limit() {
    let schema = prepopulated_schema();

    let input = r#"[:find ?x :where [?x :foo/bar "yyy"]]"#;
    let SQLQuery { sql, args } = translate(&schema, input, 5);
    assert_eq!(sql, "SELECT DISTINCT `datoms00`.e AS `?x` FROM `datoms` AS `datoms00` WHERE `datoms00`.a = 99 AND `datoms00`.v = $v0 LIMIT 5");
    assert_eq!(args, vec![make_arg("$v0", "yyy")]);
}

#[test]
fn test_unknown_attribute_keyword_value() {
    let schema = Schema::default();

    let input = r#"[:find ?x :where [?x _ :ab/yyy]]"#;
    let SQLQuery { sql, args } = translate(&schema, input, None);

    // Only match keywords, not strings: tag = 13.
    assert_eq!(sql, "SELECT DISTINCT `datoms00`.e AS `?x` FROM `datoms` AS `datoms00` WHERE `datoms00`.v = $v0 AND `datoms00`.value_type_tag = 13");
    assert_eq!(args, vec![make_arg("$v0", ":ab/yyy")]);
}

#[test]
fn test_unknown_attribute_string_value() {
    let schema = Schema::default();

    let input = r#"[:find ?x :where [?x _ "horses"]]"#;
    let SQLQuery { sql, args } = translate(&schema, input, None);

    // We expect all_datoms because we're querying for a string. Magic, that.
    // We don't want keywords etc., so tag = 10.
    assert_eq!(sql, "SELECT DISTINCT `all_datoms00`.e AS `?x` FROM `all_datoms` AS `all_datoms00` WHERE `all_datoms00`.v = $v0 AND `all_datoms00`.value_type_tag = 10");
    assert_eq!(args, vec![make_arg("$v0", "horses")]);
}

#[test]
fn test_unknown_attribute_double_value() {
    let schema = Schema::default();

    let input = r#"[:find ?x :where [?x _ 9.95]]"#;
    let SQLQuery { sql, args } = translate(&schema, input, None);

    // In general, doubles _could_ be 1.0, which might match a boolean or a ref. Set tag = 5 to
    // make sure we only match numbers.
    assert_eq!(sql, "SELECT DISTINCT `datoms00`.e AS `?x` FROM `datoms` AS `datoms00` WHERE `datoms00`.v = 9.95 AND `datoms00`.value_type_tag = 5");
    assert_eq!(args, vec![]);
}

#[test]
fn test_unknown_attribute_integer_value() {
    let schema = Schema::default();

    let negative = r#"[:find ?x :where [?x _ -1]]"#;
    let zero = r#"[:find ?x :where [?x _ 0]]"#;
    let one = r#"[:find ?x :where [?x _ 1]]"#;
    let two = r#"[:find ?x :where [?x _ 2]]"#;

    // Can't match boolean; no need to filter it out.
    let SQLQuery { sql, args } = translate(&schema, negative, None);
    assert_eq!(sql, "SELECT DISTINCT `datoms00`.e AS `?x` FROM `datoms` AS `datoms00` WHERE `datoms00`.v = -1");
    assert_eq!(args, vec![]);

    // Excludes booleans.
    let SQLQuery { sql, args } = translate(&schema, zero, None);
    assert_eq!(sql, "SELECT DISTINCT `datoms00`.e AS `?x` FROM `datoms` AS `datoms00` WHERE (`datoms00`.v = 0 AND `datoms00`.value_type_tag <> 1)");
    assert_eq!(args, vec![]);

    // Excludes booleans.
    let SQLQuery { sql, args } = translate(&schema, one, None);
    assert_eq!(sql, "SELECT DISTINCT `datoms00`.e AS `?x` FROM `datoms` AS `datoms00` WHERE (`datoms00`.v = 1 AND `datoms00`.value_type_tag <> 1)");
    assert_eq!(args, vec![]);

    // Can't match boolean; no need to filter it out.
    let SQLQuery { sql, args } = translate(&schema, two, None);
    assert_eq!(sql, "SELECT DISTINCT `datoms00`.e AS `?x` FROM `datoms` AS `datoms00` WHERE `datoms00`.v = 2");
    assert_eq!(args, vec![]);
}

#[test]
fn test_unknown_ident() {
    let schema = Schema::default();

    let impossible = r#"[:find ?x :where [?x :db/ident :no/exist]]"#;
    let parsed = parse_find_string(impossible).expect("parse failed");
    let algebrized = algebrize(&schema, parsed).expect("algebrize failed");

    // This query cannot return results: the ident doesn't resolve for a ref-typed attribute.
    assert!(algebrized.is_known_empty());

    // If you insist…
    let select = query_to_select(algebrized);
    let sql = select.query.to_sql_query().unwrap().sql;
    assert_eq!("SELECT 1 LIMIT 0", sql);
}

#[test]
fn test_numeric_less_than_unknown_attribute() {
    let schema = Schema::default();

    let input = r#"[:find ?x :where [?x _ ?y] [(< ?y 10)]]"#;
    let SQLQuery { sql, args } = translate(&schema, input, None);

    // Although we infer numericness from numeric predicates, we've already assigned a table to the
    // first pattern, and so this is _still_ `all_datoms`.
    assert_eq!(sql, "SELECT DISTINCT `all_datoms00`.e AS `?x` FROM `all_datoms` AS `all_datoms00` WHERE `all_datoms00`.v < 10");
    assert_eq!(args, vec![]);
}

#[test]
fn test_numeric_gte_known_attribute() {
    let schema = prepopulated_typed_schema(ValueType::Double);
    let input = r#"[:find ?x :where [?x :foo/bar ?y] [(>= ?y 12.9)]]"#;
    let SQLQuery { sql, args } = translate(&schema, input, None);
    assert_eq!(sql, "SELECT DISTINCT `datoms00`.e AS `?x` FROM `datoms` AS `datoms00` WHERE `datoms00`.a = 99 AND `datoms00`.v >= 12.9");
    assert_eq!(args, vec![]);
}

#[test]
fn test_numeric_not_equals_known_attribute() {
    let schema = prepopulated_typed_schema(ValueType::Long);
    let input = r#"[:find ?x . :where [?x :foo/bar ?y] [(!= ?y 12)]]"#;
    let SQLQuery { sql, args } = translate(&schema, input, None);
    assert_eq!(sql, "SELECT `datoms00`.e AS `?x` FROM `datoms` AS `datoms00` WHERE `datoms00`.a = 99 AND `datoms00`.v <> 12 LIMIT 1");
    assert_eq!(args, vec![]);
}

#[test]
fn test_simple_or_join() {
    let mut schema = Schema::default();
    associate_ident(&mut schema, NamespacedKeyword::new("page", "url"), 97);
    associate_ident(&mut schema, NamespacedKeyword::new("page", "title"), 98);
    associate_ident(&mut schema, NamespacedKeyword::new("page", "description"), 99);
    for x in 97..100 {
        add_attribute(&mut schema, x, Attribute {
            value_type: ValueType::String,
            ..Default::default()
        });
    }

    let input = r#"[:find [?url ?description]
                    :where
                    (or-join [?page]
                      [?page :page/url "http://foo.com/"]
                      [?page :page/title "Foo"])
                    [?page :page/url ?url]
                    [?page :page/description ?description]]"#;
    let SQLQuery { sql, args } = translate(&schema, input, None);
    assert_eq!(sql, "SELECT `datoms01`.v AS `?url`, `datoms02`.v AS `?description` FROM `datoms` AS `datoms00`, `datoms` AS `datoms01`, `datoms` AS `datoms02` WHERE ((`datoms00`.a = 97 AND `datoms00`.v = $v0) OR (`datoms00`.a = 98 AND `datoms00`.v = $v1)) AND `datoms01`.a = 97 AND `datoms02`.a = 99 AND `datoms00`.e = `datoms01`.e AND `datoms00`.e = `datoms02`.e LIMIT 1");
    assert_eq!(args, vec![make_arg("$v0", "http://foo.com/"), make_arg("$v1", "Foo")]);
}

#[test]
fn test_complex_or_join() {
    let mut schema = Schema::default();
    associate_ident(&mut schema, NamespacedKeyword::new("page", "save"), 95);
    add_attribute(&mut schema, 95, Attribute {
        value_type: ValueType::Ref,
        ..Default::default()
    });

    associate_ident(&mut schema, NamespacedKeyword::new("save", "title"), 96);
    associate_ident(&mut schema, NamespacedKeyword::new("page", "url"), 97);
    associate_ident(&mut schema, NamespacedKeyword::new("page", "title"), 98);
    associate_ident(&mut schema, NamespacedKeyword::new("page", "description"), 99);
    for x in 96..100 {
        add_attribute(&mut schema, x, Attribute {
            value_type: ValueType::String,
            ..Default::default()
        });
    }

    let input = r#"[:find [?url ?description]
                    :where
                    (or-join [?page]
                      [?page :page/url "http://foo.com/"]
                      [?page :page/title "Foo"]
                      (and
                        [?page :page/save ?save]
                        [?save :save/title "Foo"]))
                    [?page :page/url ?url]
                    [?page :page/description ?description]]"#;
    let SQLQuery { sql, args } = translate(&schema, input, None);
    assert_eq!(sql, "SELECT `datoms04`.v AS `?url`, \
                            `datoms05`.v AS `?description` \
                     FROM (SELECT `datoms00`.e AS `?page` \
                           FROM `datoms` AS `datoms00` \
                           WHERE `datoms00`.a = 97 \
                           AND `datoms00`.v = $v0 \
                           UNION \
                           SELECT `datoms01`.e AS `?page` \
                               FROM `datoms` AS `datoms01` \
                               WHERE `datoms01`.a = 98 \
                               AND `datoms01`.v = $v1 \
                           UNION \
                           SELECT `datoms02`.e AS `?page` \
                               FROM `datoms` AS `datoms02`, \
                                   `datoms` AS `datoms03` \
                               WHERE `datoms02`.a = 95 \
                               AND `datoms03`.a = 96 \
                               AND `datoms03`.v = $v2 \
                               AND `datoms02`.v = `datoms03`.e) AS `c00`, \
                           `datoms` AS `datoms04`, \
                           `datoms` AS `datoms05` \
                    WHERE `datoms04`.a = 97 \
                    AND `datoms05`.a = 99 \
                    AND `c00`.`?page` = `datoms04`.e \
                    AND `c00`.`?page` = `datoms05`.e \
                    LIMIT 1");
    assert_eq!(args, vec![make_arg("$v0", "http://foo.com/"),
                          make_arg("$v1", "Foo"),
                          make_arg("$v2", "Foo")]);
}


#[test]
fn test_complex_or_join_type_projection() {
    let mut schema = Schema::default();
    associate_ident(&mut schema, NamespacedKeyword::new("page", "title"), 98);
    add_attribute(&mut schema, 98, Attribute {
        value_type: ValueType::String,
        ..Default::default()
    });

    let input = r#"[:find [?y]
                    :where
                    (or
                      [6 :page/title ?y]
                      [5 _ ?y])]"#;
    let SQLQuery { sql, args } = translate(&schema, input, None);
    assert_eq!(sql, "SELECT `c00`.`?y` AS `?y`, \
                            `c00`.`?y_value_type_tag` AS `?y_value_type_tag` \
                       FROM (SELECT `datoms00`.v AS `?y`, \
                                    10 AS `?y_value_type_tag` \
                            FROM `datoms` AS `datoms00` \
                            WHERE `datoms00`.e = 6 \
                            AND `datoms00`.a = 98 \
                            UNION \
                            SELECT `all_datoms01`.v AS `?y`, \
                                `all_datoms01`.value_type_tag AS `?y_value_type_tag` \
                            FROM `all_datoms` AS `all_datoms01` \
                            WHERE `all_datoms01`.e = 5) AS `c00` \
                    LIMIT 1");
    assert_eq!(args, vec![]);
}

#[test]
fn test_with_without_aggregate() {
    let schema = prepopulated_schema();

    // Known type.
    let input = r#"[:find ?x :with ?y :where [?x :foo/bar ?y]]"#;
    let SQLQuery { sql, args } = translate(&schema, input, None);
    assert_eq!(sql, "SELECT DISTINCT `datoms00`.e AS `?x`, `datoms00`.v AS `?y` FROM `datoms` AS `datoms00` WHERE `datoms00`.a = 99");
    assert_eq!(args, vec![]);

    // Unknown type.
    let input = r#"[:find ?x :with ?y :where [?x _ ?y]]"#;
    let SQLQuery { sql, args } = translate(&schema, input, None);
    assert_eq!(sql, "SELECT DISTINCT `all_datoms00`.e AS `?x`, `all_datoms00`.v AS `?y`, `all_datoms00`.value_type_tag AS `?y_value_type_tag` FROM `all_datoms` AS `all_datoms00`");
    assert_eq!(args, vec![]);
}