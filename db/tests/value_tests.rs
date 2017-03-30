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
extern crate mentat_db;
extern crate ordered_float;
extern crate rusqlite;

use std::rc::Rc;

use ordered_float::OrderedFloat;

use edn::symbols;

use mentat_core::{TypedValue, ValueType};
use mentat_db::db::TypedSQLValue;

fn typed_keyword(ns: &'static str, name: &'static str) -> TypedValue {
    TypedValue::Keyword(Rc::new(symbols::NamespacedKeyword::new(ns, name)))
}

fn typed_string(s: &'static str) -> TypedValue {
    TypedValue::String(Rc::new(s.to_string()))
}


// It's not possible to test to_sql_value_pair since rusqlite::ToSqlOutput doesn't implement
// PartialEq.
#[test]
fn test_from_sql_value_pair() {
    assert_eq!(TypedValue::from_sql_value_pair(rusqlite::types::Value::Integer(1234), 0).unwrap(), TypedValue::Ref(1234));

    assert_eq!(TypedValue::from_sql_value_pair(rusqlite::types::Value::Integer(0), 1).unwrap(), TypedValue::Boolean(false));
    assert_eq!(TypedValue::from_sql_value_pair(rusqlite::types::Value::Integer(1), 1).unwrap(), TypedValue::Boolean(true));

    assert_eq!(TypedValue::from_sql_value_pair(rusqlite::types::Value::Integer(0), 5).unwrap(), TypedValue::Long(0));
    assert_eq!(TypedValue::from_sql_value_pair(rusqlite::types::Value::Integer(1234), 5).unwrap(), TypedValue::Long(1234));

    assert_eq!(TypedValue::from_sql_value_pair(rusqlite::types::Value::Real(0.0), 5).unwrap(), TypedValue::Double(OrderedFloat(0.0)));
    assert_eq!(TypedValue::from_sql_value_pair(rusqlite::types::Value::Real(0.5), 5).unwrap(), TypedValue::Double(OrderedFloat(0.5)));

    assert_eq!(TypedValue::from_sql_value_pair(rusqlite::types::Value::Text(":db/keyword".into()), 10).unwrap(), typed_string(":db/keyword"));
    assert_eq!(TypedValue::from_sql_value_pair(rusqlite::types::Value::Text(":db/keyword".into()), 13).unwrap(), typed_keyword("db", "keyword"));
}

#[test]
fn test_to_edn_value_pair() {
    assert_eq!(TypedValue::Ref(1234).to_edn_value_pair(), (edn::Value::Integer(1234), ValueType::Ref));

    assert_eq!(TypedValue::Boolean(false).to_edn_value_pair(), (edn::Value::Boolean(false), ValueType::Boolean));
    assert_eq!(TypedValue::Boolean(true).to_edn_value_pair(), (edn::Value::Boolean(true), ValueType::Boolean));

    assert_eq!(TypedValue::Long(0).to_edn_value_pair(), (edn::Value::Integer(0), ValueType::Long));
    assert_eq!(TypedValue::Long(1234).to_edn_value_pair(), (edn::Value::Integer(1234), ValueType::Long));

    assert_eq!(TypedValue::Double(OrderedFloat(0.0)).to_edn_value_pair(), (edn::Value::Float(OrderedFloat(0.0)), ValueType::Double));
    assert_eq!(TypedValue::Double(OrderedFloat(0.5)).to_edn_value_pair(), (edn::Value::Float(OrderedFloat(0.5)), ValueType::Double));

    assert_eq!(typed_string(":db/keyword").to_edn_value_pair(), (edn::Value::Text(":db/keyword".into()), ValueType::String));
    assert_eq!(typed_keyword("db", "keyword").to_edn_value_pair(), (edn::Value::NamespacedKeyword(symbols::NamespacedKeyword::new("db", "keyword")), ValueType::Keyword));
}
