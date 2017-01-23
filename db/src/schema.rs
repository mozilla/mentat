// Copyright 2016 Mozilla
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

#![allow(dead_code)]

use edn::types::Value;
use errors::*;
use types::{Attribute, Entid, EntidMap, IdentMap, Schema, SchemaMap, ValueType};
use values;

/// Return `Ok(())` if `schema_map` defines a valid Mentat schema.
fn validate_schema_map(entid_map: &EntidMap, schema_map: &SchemaMap) -> Result<()> {
    for (entid, attribute) in schema_map {
        let ident = entid_map.get(entid).ok_or(ErrorKind::BadSchemaAssertion(format!("Could not get ident for entid: {}", entid)))?;

        if attribute.unique_identity && !attribute.unique_value {
            bail!(ErrorKind::BadSchemaAssertion(format!(":db/unique :db/unique_identity without :db/unique :db/unique_value for entid: {}", ident)))
        }
        if attribute.fulltext && attribute.value_type != ValueType::String {
            bail!(ErrorKind::BadSchemaAssertion(format!(":db/fulltext true without :db/valueType :db.type/string for entid: {}", ident)))
        }
        if attribute.component && attribute.value_type != ValueType::Ref {
            bail!(ErrorKind::BadSchemaAssertion(format!(":db/isComponent true without :db/valueType :db.type/ref for entid: {}", ident)))
        }
        // TODO: consider warning if we have :db/index true for :db/valueType :db.type/string,
        // since this may be inefficient.  More generally, we should try to drive complex
        // :db/valueType (string, uri, json in the future) users to opt-in to some hash-indexing
        // scheme, as discussed in https://github.com/mozilla/mentat/issues/69.
    }
    Ok(())
}

impl Schema {
    pub fn get_ident(&self, x: &Entid) -> Option<&String> {
        self.entid_map.get(x)
    }

    pub fn get_entid(&self, x: &String) -> Option<&Entid> {
        self.ident_map.get(x)
    }

    pub fn attribute_for_entid(&self, x: &Entid) -> Option<&Attribute> {
        self.schema_map.get(x)
    }

    /// Create a valid `Schema` from the constituent maps.
    pub fn from(ident_map: IdentMap, schema_map: SchemaMap) -> Result<Schema> {
        let entid_map: EntidMap = ident_map.iter().map(|(k, v)| (v.clone(), k.clone())).collect();

        validate_schema_map(&entid_map, &schema_map)?;

        Ok(Schema {
            ident_map: ident_map,
            entid_map: entid_map,
            schema_map: schema_map,
        })
    }

    /// Turn Value([[IDENT ATTR VALUE] ...]) into a Mentat `Schema`.
    pub fn from_ident_map_and_assertions(ident_map: IdentMap, assertions: &Value) -> Result<Schema> {
        // Convert Value([[IDENT ATTR VALUE] ...]) to vec![(IDENT.to_string(), ATTR.to_string(), VALUE), ...].
        let triples: Vec<(String, String, &Value)> = match *assertions {
            Value::Vector(ref datoms) => {
                datoms.into_iter().map(|datom| {
                    match datom {
                        &Value::Vector(ref values) => {
                            let mut i = values.iter();
                            match (i.next(), i.next(), i.next(), i.next(), i.next()) {
                                (Some(add), Some(&Value::NamespacedKeyword(ref ident)), Some(&Value::NamespacedKeyword(ref attr)), Some(value), None) if *add == *values::DB_ADD =>
                                    Ok((ident.to_string(), attr.to_string(), value)),
                                _ => Err(ErrorKind::BadSchemaAssertion(format!("Expected [[:db/add IDENT ATTR VALUE] ...], got: {:?}", datom)))
                            }
                        },
                        _ => Err(ErrorKind::BadSchemaAssertion(format!("Expected [[...] ...], got: {:?}", datom)))
                    }
                }).collect()
            },
            _ => Err(ErrorKind::BadSchemaAssertion(format!("Expected [...], got: {:?}", assertions)))
        }?;

        let mut schema_map = SchemaMap::new();
        for (ident, attr, value) in triples {
            let entid: &i64 = ident_map.get(&ident).ok_or(ErrorKind::BadSchemaAssertion(format!("Could not get ")))?;
            let attributes = schema_map.entry(*entid).or_insert(Attribute::default());

            // Yes, this is pretty bonkers.  Suggestions appreciated.
            match attr.as_str() {
                ":db/valueType" => {
                    if *value == *values::DB_TYPE_REF {
                        attributes.value_type = ValueType::Ref;
                    } else if *value == *values::DB_TYPE_BOOLEAN {
                        attributes.value_type = ValueType::Boolean;
                    } else if *value == *values::DB_TYPE_INSTANT {
                        attributes.value_type = ValueType::Instant;
                    } else if *value == *values::DB_TYPE_LONG {
                        attributes.value_type = ValueType::Long;
                    } else if *value == *values::DB_TYPE_STRING {
                        attributes.value_type = ValueType::String;
                    } else if *value == *values::DB_TYPE_UUID {
                        attributes.value_type = ValueType::UUID;
                    } else if *value == *values::DB_TYPE_URI {
                        attributes.value_type = ValueType::URI;
                    } else if *value == *values::DB_TYPE_KEYWORD {
                        attributes.value_type = ValueType::Keyword;
                    } else {
                        bail!(ErrorKind::BadSchemaAssertion(format!("Expected [... :db/valueType :db.type/*] but got [... :db/valueType {:?}]", value)))
                    }
                },
                ":db/cardinality" => {
                    if *value == *values::DB_CARDINALITY_MANY {
                        attributes.multival = true;
                    } else if *value == *values::DB_CARDINALITY_ONE {
                        attributes.multival = false;
                    } else {
                        bail!(ErrorKind::BadSchemaAssertion(format!("Expected [... :db/cardinality :db.cardinality/many|:db.cardinality/one] but got [... :db/cardinality {:?}]", value)))
                    }
                },
                ":db/unique" => {
                    if *value == *values::DB_UNIQUE_VALUE {
                        attributes.unique_value = true;
                    } else if *value == *values::DB_UNIQUE_IDENTITY {
                        attributes.unique_value = true;
                        attributes.unique_identity = true;
                    } else {
                        bail!(ErrorKind::BadSchemaAssertion(format!("Expected [... :db/unique :db.unique/value|:db.unique/identity] but got [... :db/unique {:?}]", value)))
                    }
                },
                ":db/index" => {
                    if *value == Value::Boolean(true) {
                        attributes.index = true;
                    } else if *value == Value::Boolean(false) {
                        attributes.index = false;
                    } else {
                        bail!(ErrorKind::BadSchemaAssertion(format!("Expected [... :db/index true|false] but got [... :db/index {:?}]", value)))
                    }
                },
                ":db/fulltext" => {
                    if *value == Value::Boolean(true) {
                        attributes.index = true;
                        attributes.fulltext = true;
                    } else if *value == Value::Boolean(false) {
                        attributes.fulltext = false;
                    } else {
                        bail!(ErrorKind::BadSchemaAssertion(format!("Expected [... :db/fulltext true|false] but got [... :db/fulltext {:?}]", value)))
                    }
                },
                ":db/isComponent" => {
                    if *value == Value::Boolean(true) {
                        attributes.component = true;
                    } else if *value == Value::Boolean(false) {
                        attributes.component = false;
                    } else {
                        bail!(ErrorKind::BadSchemaAssertion(format!("Expected [... :db/isComponent true|false] but got [... :db/isComponent {:?}]", value)))
                    }
                },
                ":db/doc" => {
                    // Nothing for now.
                },
                ":db/ident" => {
                    // Nothing for now.
                },
                ":db.install/attribute" => {
                    // Nothing for now.
                },
                _ => {
                    bail!(ErrorKind::BadSchemaAssertion(format!("Do not recognize attribute '{}' for ident '{}'", attr, ident)))
                }
            }
        };

        Schema::from(ident_map.clone(), schema_map)
    }
}
