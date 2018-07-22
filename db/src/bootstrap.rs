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

use edn;
use errors::{
    DbErrorKind,
    Result,
};
use edn::types::Value;
use edn::symbols;
use entids;
use db::TypedSQLValue;
use edn::entities::Entity;
use mentat_core::{
    IdentMap,
    Schema,
    TypedValue,
    values,
};
use schema::SchemaBuilding;
use types::{Partition, PartitionMap};

/// The first transaction ID applied to the knowledge base.
///
/// This is the start of the :db.part/tx partition.
pub const TX0: i64 = 0x10000000;

/// This is the start of the :db.part/user partition.
pub const USER0: i64 = 0x10000;

// Corresponds to the version of the :db.schema/core vocabulary.
pub const CORE_SCHEMA_VERSION: u32 = 1;

lazy_static! {
    static ref V1_IDENTS: [(symbols::Keyword, i64); 40] = {
            [(ns_keyword!("db", "ident"),             entids::DB_IDENT),
             (ns_keyword!("db.part", "db"),           entids::DB_PART_DB),
             (ns_keyword!("db", "txInstant"),         entids::DB_TX_INSTANT),
             (ns_keyword!("db.install", "partition"), entids::DB_INSTALL_PARTITION),
             (ns_keyword!("db.install", "valueType"), entids::DB_INSTALL_VALUE_TYPE),
             (ns_keyword!("db.install", "attribute"), entids::DB_INSTALL_ATTRIBUTE),
             (ns_keyword!("db", "valueType"),         entids::DB_VALUE_TYPE),
             (ns_keyword!("db", "cardinality"),       entids::DB_CARDINALITY),
             (ns_keyword!("db", "unique"),            entids::DB_UNIQUE),
             (ns_keyword!("db", "isComponent"),       entids::DB_IS_COMPONENT),
             (ns_keyword!("db", "index"),             entids::DB_INDEX),
             (ns_keyword!("db", "fulltext"),          entids::DB_FULLTEXT),
             (ns_keyword!("db", "noHistory"),         entids::DB_NO_HISTORY),
             (ns_keyword!("db", "add"),               entids::DB_ADD),
             (ns_keyword!("db", "retract"),           entids::DB_RETRACT),
             (ns_keyword!("db.part", "user"),         entids::DB_PART_USER),
             (ns_keyword!("db.part", "tx"),           entids::DB_PART_TX),
             (ns_keyword!("db", "excise"),            entids::DB_EXCISE),
             (ns_keyword!("db.excise", "attrs"),      entids::DB_EXCISE_ATTRS),
             (ns_keyword!("db.excise", "beforeT"),    entids::DB_EXCISE_BEFORE_T),
             (ns_keyword!("db.excise", "before"),     entids::DB_EXCISE_BEFORE),
             (ns_keyword!("db.alter", "attribute"),   entids::DB_ALTER_ATTRIBUTE),
             (ns_keyword!("db.type", "ref"),          entids::DB_TYPE_REF),
             (ns_keyword!("db.type", "keyword"),      entids::DB_TYPE_KEYWORD),
             (ns_keyword!("db.type", "long"),         entids::DB_TYPE_LONG),
             (ns_keyword!("db.type", "double"),       entids::DB_TYPE_DOUBLE),
             (ns_keyword!("db.type", "string"),       entids::DB_TYPE_STRING),
             (ns_keyword!("db.type", "uuid"),         entids::DB_TYPE_UUID),
             (ns_keyword!("db.type", "uri"),          entids::DB_TYPE_URI),
             (ns_keyword!("db.type", "boolean"),      entids::DB_TYPE_BOOLEAN),
             (ns_keyword!("db.type", "instant"),      entids::DB_TYPE_INSTANT),
             (ns_keyword!("db.type", "bytes"),        entids::DB_TYPE_BYTES),
             (ns_keyword!("db.cardinality", "one"),   entids::DB_CARDINALITY_ONE),
             (ns_keyword!("db.cardinality", "many"),  entids::DB_CARDINALITY_MANY),
             (ns_keyword!("db.unique", "value"),      entids::DB_UNIQUE_VALUE),
             (ns_keyword!("db.unique", "identity"),   entids::DB_UNIQUE_IDENTITY),
             (ns_keyword!("db", "doc"),               entids::DB_DOC),
             (ns_keyword!("db.schema", "version"),    entids::DB_SCHEMA_VERSION),
             (ns_keyword!("db.schema", "attribute"),  entids::DB_SCHEMA_ATTRIBUTE),
             (ns_keyword!("db.schema", "core"),       entids::DB_SCHEMA_CORE),
        ]
    };

    pub static ref V1_PARTS: [(symbols::Keyword, i64, i64, i64, bool); 3] = {
            [(ns_keyword!("db.part", "db"), 0, USER0 - 1, (1 + V1_IDENTS.len()) as i64, false),
             (ns_keyword!("db.part", "user"), USER0, TX0 - 1, USER0, true),
             (ns_keyword!("db.part", "tx"), TX0, i64::max_value(), TX0, false),
        ]
    };

    static ref V1_CORE_SCHEMA: [(symbols::Keyword); 20] = {
            [(ns_keyword!("db", "ident")),
             (ns_keyword!("db.install", "partition")),
             (ns_keyword!("db.install", "valueType")),
             (ns_keyword!("db.install", "attribute")),
             (ns_keyword!("db", "excise")),
             (ns_keyword!("db.excise", "attrs")),
             (ns_keyword!("db.excise", "beforeT")),
             (ns_keyword!("db.excise", "before")),
             (ns_keyword!("db", "txInstant")),
             (ns_keyword!("db", "valueType")),
             (ns_keyword!("db", "cardinality")),
             (ns_keyword!("db", "doc")),
             (ns_keyword!("db", "unique")),
             (ns_keyword!("db", "isComponent")),
             (ns_keyword!("db", "index")),
             (ns_keyword!("db", "fulltext")),
             (ns_keyword!("db", "noHistory")),
             (ns_keyword!("db.alter", "attribute")),
             (ns_keyword!("db.schema", "version")),
             (ns_keyword!("db.schema", "attribute")),
        ]
    };

    static ref V1_SYMBOLIC_SCHEMA: Value = {
        let s = r#"
{:db/ident             {:db/valueType   :db.type/keyword
                        :db/cardinality :db.cardinality/one
                        :db/index       true
                        :db/unique      :db.unique/identity}
 :db.install/partition {:db/valueType   :db.type/ref
                        :db/cardinality :db.cardinality/many}
 :db.install/valueType {:db/valueType   :db.type/ref
                        :db/cardinality :db.cardinality/many}
 :db.install/attribute {:db/valueType   :db.type/ref
                        :db/cardinality :db.cardinality/many}
 :db/excise            {:db/valueType   :db.type/ref
                        :db/cardinality :db.cardinality/many}
 :db.excise/attrs      {:db/valueType   :db.type/ref
                        :db/cardinality :db.cardinality/many}
 :db.excise/beforeT    {:db/valueType   :db.type/ref
                        :db/cardinality :db.cardinality/one}
 :db.excise/before     {:db/valueType   :db.type/instant
                        :db/cardinality :db.cardinality/one}
 ;; TODO: support user-specified functions in the future.
 ;; :db.install/function {:db/valueType :db.type/ref
 ;;                       :db/cardinality :db.cardinality/many}
 :db/txInstant         {:db/valueType   :db.type/instant
                        :db/cardinality :db.cardinality/one
                        :db/index       true}
 :db/valueType         {:db/valueType   :db.type/ref
                        :db/cardinality :db.cardinality/one}
 :db/cardinality       {:db/valueType   :db.type/ref
                        :db/cardinality :db.cardinality/one}
 :db/doc               {:db/valueType   :db.type/string
                        :db/cardinality :db.cardinality/one}
 :db/unique            {:db/valueType   :db.type/ref
                        :db/cardinality :db.cardinality/one}
 :db/isComponent       {:db/valueType   :db.type/boolean
                        :db/cardinality :db.cardinality/one}
 :db/index             {:db/valueType   :db.type/boolean
                        :db/cardinality :db.cardinality/one}
 :db/fulltext          {:db/valueType   :db.type/boolean
                        :db/cardinality :db.cardinality/one}
 :db/noHistory         {:db/valueType   :db.type/boolean
                        :db/cardinality :db.cardinality/one}
 :db.alter/attribute   {:db/valueType   :db.type/ref
                        :db/cardinality :db.cardinality/many}
 :db.schema/version    {:db/valueType   :db.type/long
                        :db/cardinality :db.cardinality/one}

 ;; unique-value because an attribute can only belong to a single
 ;; schema fragment.
 :db.schema/attribute  {:db/valueType   :db.type/ref
                        :db/index       true
                        :db/unique      :db.unique/value
                        :db/cardinality :db.cardinality/many}}"#;
        edn::parse::value(s)
            .map(|v| v.without_spans())
            .map_err(|_| DbErrorKind::BadBootstrapDefinition("Unable to parse V1_SYMBOLIC_SCHEMA".into()))
            .unwrap()
    };
}

/// Convert (ident, entid) pairs into [:db/add IDENT :db/ident IDENT] `Value` instances.
fn idents_to_assertions(idents: &[(symbols::Keyword, i64)]) -> Vec<Value> {
    idents
        .into_iter()
        .map(|&(ref ident, _)| {
            let value = Value::Keyword(ident.clone());
            Value::Vector(vec![values::DB_ADD.clone(), value.clone(), values::DB_IDENT.clone(), value.clone()])
        })
        .collect()
}

/// Convert an ident list into [:db/add :db.schema/core :db.schema/attribute IDENT] `Value` instances.
fn schema_attrs_to_assertions(version: u32, idents: &[symbols::Keyword]) -> Vec<Value> {
    let schema_core = Value::Keyword(ns_keyword!("db.schema", "core"));
    let schema_attr = Value::Keyword(ns_keyword!("db.schema", "attribute"));
    let schema_version = Value::Keyword(ns_keyword!("db.schema", "version"));
    idents
        .into_iter()
        .map(|ident| {
            let value = Value::Keyword(ident.clone());
            Value::Vector(vec![values::DB_ADD.clone(),
                               schema_core.clone(),
                               schema_attr.clone(),
                               value])
        })
        .chain(::std::iter::once(Value::Vector(vec![values::DB_ADD.clone(),
                                             schema_core.clone(),
                                             schema_version,
                                             Value::Integer(version as i64)])))
        .collect()
}

/// Convert {:ident {:key :value ...} ...} to
/// vec![(symbols::Keyword(:ident), symbols::Keyword(:key), TypedValue(:value)), ...].
///
/// Such triples are closer to what the transactor will produce when processing attribute
/// assertions.
fn symbolic_schema_to_triples(ident_map: &IdentMap, symbolic_schema: &Value) -> Result<Vec<(symbols::Keyword, symbols::Keyword, TypedValue)>> {
    // Failure here is a coding error, not a runtime error.
    let mut triples: Vec<(symbols::Keyword, symbols::Keyword, TypedValue)> = vec![];
    // TODO: Consider `flat_map` and `map` rather than loop.
    match *symbolic_schema {
        Value::Map(ref m) => {
            for (ident, mp) in m {
                let ident = match ident {
                    &Value::Keyword(ref ident) => ident,
                    _ => bail!(DbErrorKind::BadBootstrapDefinition(format!("Expected namespaced keyword for ident but got '{:?}'", ident))),
                };
                match *mp {
                    Value::Map(ref mpp) => {
                        for (attr, value) in mpp {
                            let attr = match attr {
                                &Value::Keyword(ref attr) => attr,
                                _ => bail!(DbErrorKind::BadBootstrapDefinition(format!("Expected namespaced keyword for attr but got '{:?}'", attr))),
                        };

                            // We have symbolic idents but the transactor handles entids.  Ad-hoc
                            // convert right here.  This is a fundamental limitation on the
                            // bootstrap symbolic schema format; we can't represent "real" keywords
                            // at this time.
                            //
                            // TODO: remove this limitation, perhaps by including a type tag in the
                            // bootstrap symbolic schema, or by representing the initial bootstrap
                            // schema directly as Rust data.
                            let typed_value = match TypedValue::from_edn_value(value) {
                                Some(TypedValue::Keyword(ref k)) => {
                                    ident_map.get(k)
                                        .map(|entid| TypedValue::Ref(*entid))
                                        .ok_or(DbErrorKind::UnrecognizedIdent(k.to_string()))?
                                },
                                Some(v) => v,
                                _ => bail!(DbErrorKind::BadBootstrapDefinition(format!("Expected Mentat typed value for value but got '{:?}'", value)))
                            };

                            triples.push((ident.clone(), attr.clone(), typed_value));
                        }
                    },
                    _ => bail!(DbErrorKind::BadBootstrapDefinition("Expected {:db/ident {:db/attr value ...} ...}".into()))
                }
            }
        },
        _ => bail!(DbErrorKind::BadBootstrapDefinition("Expected {...}".into()))
    }
    Ok(triples)
}

/// Convert {IDENT {:key :value ...} ...} to [[:db/add IDENT :key :value] ...].
fn symbolic_schema_to_assertions(symbolic_schema: &Value) -> Result<Vec<Value>> {
    // Failure here is a coding error, not a runtime error.
    let mut assertions: Vec<Value> = vec![];
    match *symbolic_schema {
        Value::Map(ref m) => {
            for (ident, mp) in m {
                match *mp {
                    Value::Map(ref mpp) => {
                        for (attr, value) in mpp {
                            assertions.push(Value::Vector(vec![values::DB_ADD.clone(),
                                                               ident.clone(),
                                                               attr.clone(),
                                                               value.clone()]));
                        }
                    },
                    _ => bail!(DbErrorKind::BadBootstrapDefinition("Expected {:db/ident {:db/attr value ...} ...}".into()))
                }
            }
        },
        _ => bail!(DbErrorKind::BadBootstrapDefinition("Expected {...}".into()))
    }
    Ok(assertions)
}

pub(crate) fn bootstrap_partition_map() -> PartitionMap {
    V1_PARTS.iter()
            .map(|&(ref part, start, end, index, allow_excision)| (part.to_string(), Partition::new(start, end, index, allow_excision)))
            .collect()
}

pub(crate) fn bootstrap_ident_map() -> IdentMap {
    V1_IDENTS.iter()
             .map(|&(ref ident, entid)| (ident.clone(), entid))
             .collect()
}

pub(crate) fn bootstrap_schema() -> Schema {
    let ident_map = bootstrap_ident_map();
    let bootstrap_triples = symbolic_schema_to_triples(&ident_map, &V1_SYMBOLIC_SCHEMA).expect("symbolic schema");
    Schema::from_ident_map_and_triples(ident_map, bootstrap_triples).unwrap()
}

pub(crate) fn bootstrap_entities() -> Vec<Entity<edn::ValueAndSpan>> {
    let bootstrap_assertions: Value = Value::Vector([
        symbolic_schema_to_assertions(&V1_SYMBOLIC_SCHEMA).expect("symbolic schema"),
        idents_to_assertions(&V1_IDENTS[..]),
        schema_attrs_to_assertions(CORE_SCHEMA_VERSION, V1_CORE_SCHEMA.as_ref()),
    ].concat());

    // Failure here is a coding error (since the inputs are fixed), not a runtime error.
    // TODO: represent these bootstrap data errors rather than just panicing.
    let bootstrap_entities: Vec<Entity<edn::ValueAndSpan>> = edn::parse::entities(&bootstrap_assertions.to_string()).expect("bootstrap assertions");
    return bootstrap_entities;
}
