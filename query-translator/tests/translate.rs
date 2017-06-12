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

use mentat_query::{
    NamespacedKeyword,
    Variable,
};

use mentat_core::{
    Attribute,
    Entid,
    Schema,
    TypedValue,
    ValueType,
};

use mentat_query_parser::parse_find_string;
use mentat_query_algebrizer::{
    QueryInputs,
    algebrize,
    algebrize_with_inputs,
};
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

fn translate_with_inputs(schema: &Schema, query: &'static str, inputs: QueryInputs) -> SQLQuery {
    let parsed = parse_find_string(query).expect("parse failed");
    let algebrized = algebrize_with_inputs(schema, parsed, 0, inputs).expect("algebrize failed");
    let select = query_to_select(algebrized);
    select.query.to_sql_query().unwrap()
}

fn translate(schema: &Schema, query: &'static str) -> SQLQuery {
    translate_with_inputs(schema, query, QueryInputs::default())
}

fn prepopulated_typed_schema(foo_type: ValueType) -> Schema {
    let mut schema = Schema::default();
    associate_ident(&mut schema, NamespacedKeyword::new("foo", "bar"), 99);
    add_attribute(&mut schema, 99, Attribute {
        value_type: foo_type,
        ..Default::default()
    });
    associate_ident(&mut schema, NamespacedKeyword::new("foo", "fts"), 100);
    add_attribute(&mut schema, 100, Attribute {
        value_type: ValueType::String,
        index: true,
        fulltext: true,
        ..Default::default()
    });
    schema
}

fn prepopulated_schema() -> Schema {
    prepopulated_typed_schema(ValueType::String)
}

fn make_arg(name: &'static str, value: &'static str) -> (String, Rc<mentat_sql::Value>) {
    (name.to_string(), Rc::new(mentat_sql::Value::Text(value.to_string())))
}

#[test]
fn test_scalar() {
    let schema = prepopulated_schema();

    let query = r#"[:find ?x . :where [?x :foo/bar "yyy"]]"#;
    let SQLQuery { sql, args } = translate(&schema, query);
    assert_eq!(sql, "SELECT `datoms00`.e AS `?x` FROM `datoms` AS `datoms00` WHERE `datoms00`.a = 99 AND `datoms00`.v = $v0 LIMIT 1");
    assert_eq!(args, vec![make_arg("$v0", "yyy")]);
}

#[test]
fn test_tuple() {
    let schema = prepopulated_schema();

    let query = r#"[:find [?x] :where [?x :foo/bar "yyy"]]"#;
    let SQLQuery { sql, args } = translate(&schema, query);
    assert_eq!(sql, "SELECT `datoms00`.e AS `?x` FROM `datoms` AS `datoms00` WHERE `datoms00`.a = 99 AND `datoms00`.v = $v0 LIMIT 1");
    assert_eq!(args, vec![make_arg("$v0", "yyy")]);
}

#[test]
fn test_coll() {
    let schema = prepopulated_schema();

    let query = r#"[:find [?x ...] :where [?x :foo/bar "yyy"]]"#;
    let SQLQuery { sql, args } = translate(&schema, query);
    assert_eq!(sql, "SELECT DISTINCT `datoms00`.e AS `?x` FROM `datoms` AS `datoms00` WHERE `datoms00`.a = 99 AND `datoms00`.v = $v0");
    assert_eq!(args, vec![make_arg("$v0", "yyy")]);
}

#[test]
fn test_rel() {
    let schema = prepopulated_schema();

    let query = r#"[:find ?x :where [?x :foo/bar "yyy"]]"#;
    let SQLQuery { sql, args } = translate(&schema, query);
    assert_eq!(sql, "SELECT DISTINCT `datoms00`.e AS `?x` FROM `datoms` AS `datoms00` WHERE `datoms00`.a = 99 AND `datoms00`.v = $v0");
    assert_eq!(args, vec![make_arg("$v0", "yyy")]);
}

#[test]
fn test_limit() {
    let schema = prepopulated_schema();

    let query = r#"[:find ?x :where [?x :foo/bar "yyy"] :limit 5]"#;
    let SQLQuery { sql, args } = translate(&schema, query);
    assert_eq!(sql, "SELECT DISTINCT `datoms00`.e AS `?x` FROM `datoms` AS `datoms00` WHERE `datoms00`.a = 99 AND `datoms00`.v = $v0 LIMIT 5");
    assert_eq!(args, vec![make_arg("$v0", "yyy")]);
}

#[test]
fn test_unbound_variable_limit() {
    let schema = prepopulated_schema();

    // We don't know the value of the limit var, so we produce an escaped SQL variable to handle
    // later input.
    let query = r#"[:find ?x :in ?limit-is-9-great :where [?x :foo/bar "yyy"] :limit ?limit-is-9-great]"#;
    let SQLQuery { sql, args } = translate_with_inputs(&schema, query, QueryInputs::default());
    assert_eq!(sql, "SELECT DISTINCT `datoms00`.e AS `?x` \
                     FROM `datoms` AS `datoms00` \
                     WHERE `datoms00`.a = 99 AND `datoms00`.v = $v0 \
                     LIMIT $ilimit_is_9_great");
    assert_eq!(args, vec![make_arg("$v0", "yyy")]);
}

#[test]
fn test_bound_variable_limit() {
    let schema = prepopulated_schema();

    // We know the value of `?limit` at algebrizing time, so we substitute directly.
    let query = r#"[:find ?x :in ?limit :where [?x :foo/bar "yyy"] :limit ?limit]"#;
    let inputs = QueryInputs::with_value_sequence(vec![(Variable::from_valid_name("?limit"), TypedValue::Long(92))]);
    let SQLQuery { sql, args } = translate_with_inputs(&schema, query, inputs);
    assert_eq!(sql, "SELECT DISTINCT `datoms00`.e AS `?x` FROM `datoms` AS `datoms00` WHERE `datoms00`.a = 99 AND `datoms00`.v = $v0 LIMIT 92");
    assert_eq!(args, vec![make_arg("$v0", "yyy")]);
}

#[test]
fn test_bound_variable_limit_affects_distinct() {
    let schema = prepopulated_schema();

    // We know the value of `?limit` at algebrizing time, so we substitute directly.
    // As it's `1`, we know we don't need `DISTINCT`!
    let query = r#"[:find ?x :in ?limit :where [?x :foo/bar "yyy"] :limit ?limit]"#;
    let inputs = QueryInputs::with_value_sequence(vec![(Variable::from_valid_name("?limit"), TypedValue::Long(1))]);
    let SQLQuery { sql, args } = translate_with_inputs(&schema, query, inputs);
    assert_eq!(sql, "SELECT `datoms00`.e AS `?x` FROM `datoms` AS `datoms00` WHERE `datoms00`.a = 99 AND `datoms00`.v = $v0 LIMIT 1");
    assert_eq!(args, vec![make_arg("$v0", "yyy")]);
}

#[test]
fn test_bound_variable_limit_affects_types() {
    let schema = prepopulated_schema();

    let query = r#"[:find ?x ?limit :in ?limit :where [?x _ ?limit] :limit ?limit]"#;
    let parsed = parse_find_string(query).expect("parse failed");
    let algebrized = algebrize(&schema, parsed).expect("algebrize failed");

    // The type is known.
    assert_eq!(Some(ValueType::Long),
               algebrized.cc.known_type(&Variable::from_valid_name("?limit")));

    let select = query_to_select(algebrized);
    let SQLQuery { sql, args } = select.query.to_sql_query().unwrap();

    // TODO: this query isn't actually correct -- we don't yet algebrize for variables that are
    // specified in `:in` but not provided at algebrizing time. But it shows what we care about
    // at the moment: we don't project a type column, because we know it's a Long.
    assert_eq!(sql, "SELECT DISTINCT `datoms00`.e AS `?x`, `datoms00`.v AS `?limit` FROM `datoms` AS `datoms00` LIMIT $ilimit");
    assert_eq!(args, vec![]);
}

#[test]
fn test_unknown_attribute_keyword_value() {
    let schema = Schema::default();

    let query = r#"[:find ?x :where [?x _ :ab/yyy]]"#;
    let SQLQuery { sql, args } = translate(&schema, query);

    // Only match keywords, not strings: tag = 13.
    assert_eq!(sql, "SELECT DISTINCT `datoms00`.e AS `?x` FROM `datoms` AS `datoms00` WHERE `datoms00`.v = $v0 AND `datoms00`.value_type_tag = 13");
    assert_eq!(args, vec![make_arg("$v0", ":ab/yyy")]);
}

#[test]
fn test_unknown_attribute_string_value() {
    let schema = Schema::default();

    let query = r#"[:find ?x :where [?x _ "horses"]]"#;
    let SQLQuery { sql, args } = translate(&schema, query);

    // We expect all_datoms because we're querying for a string. Magic, that.
    // We don't want keywords etc., so tag = 10.
    assert_eq!(sql, "SELECT DISTINCT `all_datoms00`.e AS `?x` FROM `all_datoms` AS `all_datoms00` WHERE `all_datoms00`.v = $v0 AND `all_datoms00`.value_type_tag = 10");
    assert_eq!(args, vec![make_arg("$v0", "horses")]);
}

#[test]
fn test_unknown_attribute_double_value() {
    let schema = Schema::default();

    let query = r#"[:find ?x :where [?x _ 9.95]]"#;
    let SQLQuery { sql, args } = translate(&schema, query);

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
    let SQLQuery { sql, args } = translate(&schema, negative);
    assert_eq!(sql, "SELECT DISTINCT `datoms00`.e AS `?x` FROM `datoms` AS `datoms00` WHERE `datoms00`.v = -1");
    assert_eq!(args, vec![]);

    // Excludes booleans.
    let SQLQuery { sql, args } = translate(&schema, zero);
    assert_eq!(sql, "SELECT DISTINCT `datoms00`.e AS `?x` FROM `datoms` AS `datoms00` WHERE (`datoms00`.v = 0 AND `datoms00`.value_type_tag <> 1)");
    assert_eq!(args, vec![]);

    // Excludes booleans.
    let SQLQuery { sql, args } = translate(&schema, one);
    assert_eq!(sql, "SELECT DISTINCT `datoms00`.e AS `?x` FROM `datoms` AS `datoms00` WHERE (`datoms00`.v = 1 AND `datoms00`.value_type_tag <> 1)");
    assert_eq!(args, vec![]);

    // Can't match boolean; no need to filter it out.
    let SQLQuery { sql, args } = translate(&schema, two);
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

    let query = r#"[:find ?x :where [?x _ ?y] [(< ?y 10)]]"#;
    let SQLQuery { sql, args } = translate(&schema, query);

    // Although we infer numericness from numeric predicates, we've already assigned a table to the
    // first pattern, and so this is _still_ `all_datoms`.
    assert_eq!(sql, "SELECT DISTINCT `all_datoms00`.e AS `?x` FROM `all_datoms` AS `all_datoms00` WHERE `all_datoms00`.v < 10");
    assert_eq!(args, vec![]);
}

#[test]
fn test_numeric_gte_known_attribute() {
    let schema = prepopulated_typed_schema(ValueType::Double);
    let query = r#"[:find ?x :where [?x :foo/bar ?y] [(>= ?y 12.9)]]"#;
    let SQLQuery { sql, args } = translate(&schema, query);
    assert_eq!(sql, "SELECT DISTINCT `datoms00`.e AS `?x` FROM `datoms` AS `datoms00` WHERE `datoms00`.a = 99 AND `datoms00`.v >= 12.9");
    assert_eq!(args, vec![]);
}

#[test]
fn test_numeric_not_equals_known_attribute() {
    let schema = prepopulated_typed_schema(ValueType::Long);
    let query = r#"[:find ?x . :where [?x :foo/bar ?y] [(!= ?y 12)]]"#;
    let SQLQuery { sql, args } = translate(&schema, query);
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

    let query = r#"[:find [?url ?description]
                    :where
                    (or-join [?page]
                      [?page :page/url "http://foo.com/"]
                      [?page :page/title "Foo"])
                    [?page :page/url ?url]
                    [?page :page/description ?description]]"#;
    let SQLQuery { sql, args } = translate(&schema, query);
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

    let query = r#"[:find [?url ?description]
                    :where
                    (or-join [?page]
                      [?page :page/url "http://foo.com/"]
                      [?page :page/title "Foo"]
                      (and
                        [?page :page/save ?save]
                        [?save :save/title "Foo"]))
                    [?page :page/url ?url]
                    [?page :page/description ?description]]"#;
    let SQLQuery { sql, args } = translate(&schema, query);
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
                               AND `datoms03`.v = $v1 \
                               AND `datoms02`.v = `datoms03`.e) AS `c00`, \
                           `datoms` AS `datoms04`, \
                           `datoms` AS `datoms05` \
                    WHERE `datoms04`.a = 97 \
                    AND `datoms05`.a = 99 \
                    AND `c00`.`?page` = `datoms04`.e \
                    AND `c00`.`?page` = `datoms05`.e \
                    LIMIT 1");
    assert_eq!(args, vec![make_arg("$v0", "http://foo.com/"),
                          make_arg("$v1", "Foo")]);
}

#[test]
fn test_complex_or_join_type_projection() {
    let mut schema = Schema::default();
    associate_ident(&mut schema, NamespacedKeyword::new("page", "title"), 98);
    add_attribute(&mut schema, 98, Attribute {
        value_type: ValueType::String,
        ..Default::default()
    });

    let query = r#"[:find [?y]
                    :where
                    (or
                      [6 :page/title ?y]
                      [5 _ ?y])]"#;
    let SQLQuery { sql, args } = translate(&schema, query);
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
fn test_not() {
    let mut schema = Schema::default();
    associate_ident(&mut schema, NamespacedKeyword::new("page", "url"), 97);
    associate_ident(&mut schema, NamespacedKeyword::new("page", "title"), 98);
    associate_ident(&mut schema, NamespacedKeyword::new("page", "bookmarked"), 99);
    for x in 97..99 {
        add_attribute(&mut schema, x, Attribute {
            value_type: ValueType::String,
            ..Default::default()
        });
    }
    add_attribute(&mut schema, 99, Attribute {
        value_type: ValueType::Boolean,
        ..Default::default()
    });

    let query = r#"[:find ?title
                    :where [?page :page/title ?title]
                           (not [?page :page/url "http://foo.com/"]
                                [?page :page/bookmarked true])]"#;
    let SQLQuery { sql, args } = translate(&schema, query);
    assert_eq!(sql, "SELECT DISTINCT `datoms00`.v AS `?title` FROM `datoms` AS `datoms00` WHERE `datoms00`.a = 98 AND NOT EXISTS (SELECT 1 FROM `datoms` AS `datoms01`, `datoms` AS `datoms02` WHERE `datoms01`.a = 97 AND `datoms01`.v = $v0 AND `datoms02`.a = 99 AND `datoms02`.v = 1 AND `datoms00`.e = `datoms01`.e AND `datoms00`.e = `datoms02`.e)");
    assert_eq!(args, vec![make_arg("$v0", "http://foo.com/")]);
}

#[test]
fn test_not_join() {
    let mut schema = Schema::default();
    associate_ident(&mut schema, NamespacedKeyword::new("page", "url"), 97);
    associate_ident(&mut schema, NamespacedKeyword::new("bookmarks", "page"), 98);
    associate_ident(&mut schema, NamespacedKeyword::new("bookmarks", "date_created"), 99);
    add_attribute(&mut schema, 97, Attribute {
        value_type: ValueType::String,
        ..Default::default()
    });
    add_attribute(&mut schema, 98, Attribute {
        value_type: ValueType::Ref,
        ..Default::default()
    });
    add_attribute(&mut schema, 99, Attribute {
        value_type: ValueType::String,
        ..Default::default()
    });

    let query = r#"[:find ?url
                        :where [?url :page/url]
                               (not-join [?url]
                                   [?page :bookmarks/page ?url]
                                   [?page :bookmarks/date_created "4/4/2017"])]"#;
    let SQLQuery { sql, args } = translate(&schema, query);
    assert_eq!(sql, "SELECT DISTINCT `datoms00`.e AS `?url` FROM `datoms` AS `datoms00` WHERE `datoms00`.a = 97 AND NOT EXISTS (SELECT 1 FROM `datoms` AS `datoms01`, `datoms` AS `datoms02` WHERE `datoms01`.a = 98 AND `datoms02`.a = 99 AND `datoms02`.v = $v0 AND `datoms01`.e = `datoms02`.e AND `datoms00`.e = `datoms01`.v)");
    assert_eq!(args, vec![make_arg("$v0", "4/4/2017")]);
}

#[test]
fn test_with_without_aggregate() {
    let schema = prepopulated_schema();

    // Known type.
    let query = r#"[:find ?x :with ?y :where [?x :foo/bar ?y]]"#;
    let SQLQuery { sql, args } = translate(&schema, query);
    assert_eq!(sql, "SELECT DISTINCT `datoms00`.e AS `?x`, `datoms00`.v AS `?y` FROM `datoms` AS `datoms00` WHERE `datoms00`.a = 99");
    assert_eq!(args, vec![]);

    // Unknown type.
    let query = r#"[:find ?x :with ?y :where [?x _ ?y]]"#;
    let SQLQuery { sql, args } = translate(&schema, query);
    assert_eq!(sql, "SELECT DISTINCT `all_datoms00`.e AS `?x`, `all_datoms00`.v AS `?y`, `all_datoms00`.value_type_tag AS `?y_value_type_tag` FROM `all_datoms` AS `all_datoms00`");
    assert_eq!(args, vec![]);
}

#[test]
fn test_order_by() {
    let schema = prepopulated_schema();

    // Known type.
    let query = r#"[:find ?x :where [?x :foo/bar ?y] :order (desc ?y)]"#;
    let SQLQuery { sql, args } = translate(&schema, query);
    assert_eq!(sql, "SELECT DISTINCT `datoms00`.e AS `?x`, `datoms00`.v AS `?y` \
                     FROM `datoms` AS `datoms00` \
                     WHERE `datoms00`.a = 99 \
                     ORDER BY `?y` DESC");
    assert_eq!(args, vec![]);

    // Unknown type.
    let query = r#"[:find ?x :with ?y :where [?x _ ?y] :order ?y ?x]"#;
    let SQLQuery { sql, args } = translate(&schema, query);
    assert_eq!(sql, "SELECT DISTINCT `all_datoms00`.e AS `?x`, `all_datoms00`.v AS `?y`, \
                                     `all_datoms00`.value_type_tag AS `?y_value_type_tag` \
                     FROM `all_datoms` AS `all_datoms00` \
                     ORDER BY `?y_value_type_tag` ASC, `?y` ASC, `?x` ASC");
    assert_eq!(args, vec![]);
}

#[test]
fn test_complex_nested_or_join_type_projection() {
    let mut schema = Schema::default();
    associate_ident(&mut schema, NamespacedKeyword::new("page", "title"), 98);
    add_attribute(&mut schema, 98, Attribute {
        value_type: ValueType::String,
        ..Default::default()
    });

    let input = r#"[:find [?y]
                    :where
                    (or
                      (or
                        [_ :page/title ?y])
                      (or
                        [_ :page/title ?y]))]"#;

    let SQLQuery { sql, args } = translate(&schema, input);
    assert_eq!(sql, "SELECT `c00`.`?y` AS `?y` \
                     FROM (SELECT `datoms00`.v AS `?y` \
                           FROM `datoms` AS `datoms00` \
                           WHERE `datoms00`.a = 98 \
                           UNION \
                           SELECT `datoms01`.v AS `?y` \
                           FROM `datoms` AS `datoms01` \
                           WHERE `datoms01`.a = 98) \
                           AS `c00` \
                     LIMIT 1");
    assert_eq!(args, vec![]);
}

#[test]
fn test_ground_scalar() {
    let schema = prepopulated_schema();

    // Verify that we accept inline constants.
    let query = r#"[:find ?x . :where [(ground "yyy") ?x]]"#;
    let SQLQuery { sql, args } = translate(&schema, query);
    assert_eq!(sql, "SELECT $v0 AS `?x` LIMIT 1");
    assert_eq!(args, vec![make_arg("$v0", "yyy")]);

    // Verify that we accept bound input constants.
    let query = r#"[:find ?x . :in ?v :where [(ground ?v) ?x]]"#;
    let inputs = QueryInputs::with_value_sequence(vec![(Variable::from_valid_name("?v"), TypedValue::String(Rc::new("aaa".into())))]);
    let SQLQuery { sql, args } = translate_with_inputs(&schema, query, inputs);
    assert_eq!(sql, "SELECT $v0 AS `?x` LIMIT 1");
    assert_eq!(args, vec![make_arg("$v0", "aaa"),]);
}

#[test]
fn test_ground_tuple() {
    let schema = prepopulated_schema();

    // Verify that we accept inline constants.
    let query = r#"[:find ?x ?y :where [(ground [1 "yyy"]) [?x ?y]]]"#;
    let SQLQuery { sql, args } = translate(&schema, query);
    assert_eq!(sql, "SELECT DISTINCT 1 AS `?x`, $v0 AS `?y`");
    assert_eq!(args, vec![make_arg("$v0", "yyy")]);

    // Verify that we accept bound input constants.
    let query = r#"[:find [?x ?y] :in ?u ?v :where [(ground [?u ?v]) [?x ?y]]]"#;
    let inputs = QueryInputs::with_value_sequence(vec![(Variable::from_valid_name("?u"), TypedValue::Long(2)),
                                                       (Variable::from_valid_name("?v"), TypedValue::String(Rc::new("aaa".into()))),]);
    let SQLQuery { sql, args } = translate_with_inputs(&schema, query, inputs);
    // TODO: treat 2 as an input variable that could be bound late, rather than eagerly binding it.
    assert_eq!(sql, "SELECT 2 AS `?x`, $v0 AS `?y` LIMIT 1");
    assert_eq!(args, vec![make_arg("$v0", "aaa"),]);
}

#[test]
fn test_ground_coll() {
    let schema = prepopulated_schema();

    // Verify that we accept inline constants.
    let query = r#"[:find ?x :where [(ground ["xxx" "yyy"]) [?x ...]]]"#;
    let SQLQuery { sql, args } = translate(&schema, query);
    assert_eq!(sql, "SELECT DISTINCT `c00`.`?x` AS `?x` FROM \
                         (SELECT 0 AS `?x` WHERE 0 UNION ALL VALUES ($v0), ($v1)) AS `c00`");
    assert_eq!(args, vec![make_arg("$v0", "xxx"),
                          make_arg("$v1", "yyy")]);

    // Verify that we accept bound input constants.
    let query = r#"[:find ?x :in ?u ?v :where [(ground [?u ?v]) [?x ...]]]"#;
    let inputs = QueryInputs::with_value_sequence(vec![(Variable::from_valid_name("?u"), TypedValue::Long(2)),
                                                       (Variable::from_valid_name("?v"), TypedValue::Long(3)),]);
    let SQLQuery { sql, args } = translate_with_inputs(&schema, query, inputs);
    // TODO: treat 2 and 3 as input variables that could be bound late, rather than eagerly binding.
    assert_eq!(sql, "SELECT DISTINCT `c00`.`?x` AS `?x` FROM \
                         (SELECT 0 AS `?x` WHERE 0 UNION ALL VALUES (2), (3)) AS `c00`");
    assert_eq!(args, vec![]);
}

#[test]
fn test_ground_rel() {
    let schema = prepopulated_schema();

    // Verify that we accept inline constants.
    let query = r#"[:find ?x ?y :where [(ground [[1 "xxx"] [2 "yyy"]]) [[?x ?y]]]]"#;
    let SQLQuery { sql, args } = translate(&schema, query);
    assert_eq!(sql, "SELECT DISTINCT `c00`.`?x` AS `?x`, `c00`.`?y` AS `?y` FROM \
                         (SELECT 0 AS `?x`, 0 AS `?y` WHERE 0 UNION ALL VALUES (1, $v0), (2, $v1)) AS `c00`");
    assert_eq!(args, vec![make_arg("$v0", "xxx"),
                          make_arg("$v1", "yyy")]);

    // Verify that we accept bound input constants.
    let query = r#"[:find ?x ?y :in ?u ?v :where [(ground [[?u 1] [?v 2]]) [[?x ?y]]]]"#;
    let inputs = QueryInputs::with_value_sequence(vec![(Variable::from_valid_name("?u"), TypedValue::Long(3)),
                                                       (Variable::from_valid_name("?v"), TypedValue::Long(4)),]);
    let SQLQuery { sql, args } = translate_with_inputs(&schema, query, inputs);
    // TODO: treat 3 and 4 as input variables that could be bound late, rather than eagerly binding.
    assert_eq!(sql, "SELECT DISTINCT `c00`.`?x` AS `?x`, `c00`.`?y` AS `?y` FROM \
                         (SELECT 0 AS `?x`, 0 AS `?y` WHERE 0 UNION ALL VALUES (3, 1), (4, 2)) AS `c00`");
    assert_eq!(args, vec![]);
}

#[test]
fn test_compound_with_ground() {
    let schema = prepopulated_schema();

    // Verify that we can use the resulting CCs as children in compound CCs.
    let query = r#"[:find ?x :where (or [(ground "yyy") ?x]
                                        [(ground "zzz") ?x])]"#;
    let SQLQuery { sql, args } = translate(&schema, query);

    // This is confusing because the computed tables (like `c00`) are numbered sequentially in each
    // arm of the `or` rather than numbered globally.  But SQLite scopes the names correctly, so it
    // works.  In the future, we might number the computed tables globally to make this more clear.
    assert_eq!(sql, "SELECT DISTINCT `c00`.`?x` AS `?x` FROM (\
                         SELECT $v0 AS `?x` UNION \
                         SELECT $v1 AS `?x`) AS `c00`");
    assert_eq!(args, vec![make_arg("$v0", "yyy"),
                          make_arg("$v1", "zzz"),]);

    // Verify that we can use ground to constrain the bindings produced by earlier clauses.
    let query = r#"[:find ?x . :where [_ :foo/bar ?x] [(ground "yyy") ?x]]"#;
    let SQLQuery { sql, args } = translate(&schema, query);
    assert_eq!(sql, "SELECT $v0 AS `?x` FROM `datoms` AS `datoms00` \
                     WHERE `datoms00`.a = 99 AND `datoms00`.v = $v0 LIMIT 1");

    assert_eq!(args, vec![make_arg("$v0", "yyy")]);

    // Verify that we can further constrain the bindings produced by our clause.
    let query = r#"[:find ?x . :where [(ground "yyy") ?x] [_ :foo/bar ?x]]"#;
    let SQLQuery { sql, args } = translate(&schema, query);
    assert_eq!(sql, "SELECT $v0 AS `?x` FROM `datoms` AS `datoms00` \
                     WHERE `datoms00`.a = 99 AND `datoms00`.v = $v0 LIMIT 1");

    assert_eq!(args, vec![make_arg("$v0", "yyy")]);
}

#[test]
fn test_unbound_attribute_with_ground_entity() {
    let query = r#"[:find ?x ?v :where [?x _ ?v] (not [(ground 17) ?x])]"#;
    let schema = prepopulated_schema();
    let SQLQuery { sql, .. } = translate(&schema, query);
    assert_eq!(sql, "SELECT DISTINCT `all_datoms00`.e AS `?x`, \
                                     `all_datoms00`.v AS `?v`, \
                                     `all_datoms00`.value_type_tag AS `?v_value_type_tag` \
                     FROM `all_datoms` AS `all_datoms00` \
                     WHERE NOT EXISTS (SELECT 1 WHERE `all_datoms00`.e = 17)");
}

#[test]
fn test_unbound_attribute_with_ground() {
    let query = r#"[:find ?x ?v :where [?x _ ?v] (not [(ground 17) ?v])]"#;
    let schema = prepopulated_schema();
    let SQLQuery { sql, .. } = translate(&schema, query);
    assert_eq!(sql, "SELECT DISTINCT `all_datoms00`.e AS `?x`, \
                                     `all_datoms00`.v AS `?v`, \
                                     `all_datoms00`.value_type_tag AS `?v_value_type_tag` \
                     FROM `all_datoms` AS `all_datoms00` \
                     WHERE NOT EXISTS (SELECT 1 WHERE `all_datoms00`.v = 17 AND \
                                                      `all_datoms00`.value_type_tag = 5)");
}


#[test]
fn test_not_with_ground() {
    let mut schema = prepopulated_schema();
    associate_ident(&mut schema, NamespacedKeyword::new("db", "valueType"), 7);
    associate_ident(&mut schema, NamespacedKeyword::new("db.type", "ref"), 23);
    associate_ident(&mut schema, NamespacedKeyword::new("db.type", "bool"), 28);
    associate_ident(&mut schema, NamespacedKeyword::new("db.type", "instant"), 29);
    add_attribute(&mut schema, 7, Attribute {
        value_type: ValueType::Ref,
        multival: false,
        ..Default::default()
    });

    // Scalar.
    // TODO: this kind of simple `not` should be implemented without the subquery. #476.
    let query = r#"[:find ?x :where [?x :db/valueType ?v] (not [(ground :db.type/instant) ?v])]"#;
    let SQLQuery { sql, .. } = translate(&schema, query);
    assert_eq!(sql,
               "SELECT DISTINCT `datoms00`.e AS `?x` FROM `datoms` AS `datoms00` WHERE `datoms00`.a = 7 AND NOT \
                EXISTS (SELECT 1 WHERE `datoms00`.v = 29)");

    // Coll.
    // TODO: we can generate better SQL for this, too. #476.
    let query = r#"[:find ?x :where [?x :db/valueType ?v] (not [(ground [:db.type/bool :db.type/instant]) [?v ...]])]"#;
    let SQLQuery { sql, .. } = translate(&schema, query);
    assert_eq!(sql,
               "SELECT DISTINCT `datoms00`.e AS `?x` FROM `datoms` AS `datoms00` \
                WHERE `datoms00`.a = 7 AND NOT EXISTS \
                (SELECT 1 FROM (SELECT 0 AS `?v` WHERE 0 UNION ALL VALUES (28), (29)) AS `c00` \
                 WHERE `datoms00`.v = `c00`.`?v`)");
}

#[test]
fn test_fulltext() {
    let schema = prepopulated_typed_schema(ValueType::Double);

    let query = r#"[:find ?entity ?value ?tx ?score :where [(fulltext $ :foo/fts "needle") [[?entity ?value ?tx ?score]]]]"#;
    let SQLQuery { sql, args } = translate(&schema, query);
    assert_eq!(sql, "SELECT DISTINCT `datoms01`.e AS `?entity`, `fulltext_values00`.text AS `?value`, `datoms01`.tx AS `?tx`, `c00`.`?score` AS `?score` FROM `fulltext_values` AS `fulltext_values00`, `datoms` AS `datoms01`, (SELECT 0 AS `?score` WHERE 0 UNION ALL VALUES (0)) AS `c00` WHERE `datoms01`.a = 100 AND `datoms01`.v = `fulltext_values00`.rowid AND `fulltext_values00`.text MATCH $v0");
    assert_eq!(args, vec![make_arg("$v0", "needle"),]);

    let query = r#"[:find ?entity ?value ?tx :where [(fulltext $ :foo/fts "needle") [[?entity ?value ?tx ?score]]]]"#;
    let SQLQuery { sql, args } = translate(&schema, query);
    // Observe that the computed table isn't dropped, even though `?score` isn't bound in the final conjoining clause.
    assert_eq!(sql, "SELECT DISTINCT `datoms01`.e AS `?entity`, `fulltext_values00`.text AS `?value`, `datoms01`.tx AS `?tx` FROM `fulltext_values` AS `fulltext_values00`, `datoms` AS `datoms01`, (SELECT 0 AS `?score` WHERE 0 UNION ALL VALUES (0)) AS `c00` WHERE `datoms01`.a = 100 AND `datoms01`.v = `fulltext_values00`.rowid AND `fulltext_values00`.text MATCH $v0");
    assert_eq!(args, vec![make_arg("$v0", "needle"),]);

    let query = r#"[:find ?entity ?value ?tx :where [(fulltext $ :foo/fts "needle") [[?entity ?value ?tx _]]]]"#;
    let SQLQuery { sql, args } = translate(&schema, query);
    // Observe that the computed table isn't included at all when `?score` isn't bound.
    assert_eq!(sql, "SELECT DISTINCT `datoms01`.e AS `?entity`, `fulltext_values00`.text AS `?value`, `datoms01`.tx AS `?tx` FROM `fulltext_values` AS `fulltext_values00`, `datoms` AS `datoms01` WHERE `datoms01`.a = 100 AND `datoms01`.v = `fulltext_values00`.rowid AND `fulltext_values00`.text MATCH $v0");
    assert_eq!(args, vec![make_arg("$v0", "needle"),]);

    let query = r#"[:find ?entity ?value ?tx :where [(fulltext $ :foo/fts "needle") [[?entity ?value ?tx ?score]]] [?entity :foo/bar ?score]]"#;
    let SQLQuery { sql, args } = translate(&schema, query);
    assert_eq!(sql, "SELECT DISTINCT `datoms01`.e AS `?entity`, `fulltext_values00`.text AS `?value`, `datoms01`.tx AS `?tx` FROM `fulltext_values` AS `fulltext_values00`, `datoms` AS `datoms01`, (SELECT 0 AS `?score` WHERE 0 UNION ALL VALUES (0)) AS `c00`, `datoms` AS `datoms02` WHERE `datoms01`.a = 100 AND `datoms01`.v = `fulltext_values00`.rowid AND `fulltext_values00`.text MATCH $v0 AND `datoms02`.a = 99 AND `datoms01`.e = `datoms02`.e AND `c00`.`?score` = `datoms02`.v");
    assert_eq!(args, vec![make_arg("$v0", "needle"),]);

    let query = r#"[:find ?entity ?value ?tx :where [?entity :foo/bar ?score] [(fulltext $ :foo/fts "needle") [[?entity ?value ?tx ?score]]]]"#;
    let SQLQuery { sql, args } = translate(&schema, query);
    assert_eq!(sql, "SELECT DISTINCT `datoms00`.e AS `?entity`, `fulltext_values01`.text AS `?value`, `datoms02`.tx AS `?tx` FROM `datoms` AS `datoms00`, `fulltext_values` AS `fulltext_values01`, `datoms` AS `datoms02`, (SELECT 0 AS `?score` WHERE 0 UNION ALL VALUES (0)) AS `c00` WHERE `datoms00`.a = 99 AND `datoms02`.a = 100 AND `datoms02`.v = `fulltext_values01`.rowid AND `fulltext_values01`.text MATCH $v0 AND `datoms00`.e = `datoms02`.e AND `datoms00`.v = `c00`.`?score`");
    assert_eq!(args, vec![make_arg("$v0", "needle"),]);
}
