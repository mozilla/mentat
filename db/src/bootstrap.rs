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

use {to_namespaced_keyword};
use edn;
use edn::types::Value;
use errors::*;
use mentat_tx::entities::Entity;
use mentat_tx_parser;
use types::{IdentMap, Partition, PartitionMap, Schema};
use values;

lazy_static! {
    static ref V1_IDENTS: [(&'static str, i64); 35] = {
        [(":db/ident",             1),
         (":db.part/db",           2),
         (":db/txInstant",         3),
         (":db.install/partition", 4),
         (":db.install/valueType", 5),
         (":db.install/attribute", 6),
         (":db/valueType",         7),
         (":db/cardinality",       8),
         (":db/unique",            9),
         (":db/isComponent",       10),
         (":db/index",             11),
         (":db/fulltext",          12),
         (":db/noHistory",         13),
         (":db/add",               14),
         (":db/retract",           15),
         (":db.part/user",         16),
         (":db.part/tx",           17),
         (":db/excise",            18),
         (":db.excise/attrs",      19),
         (":db.excise/beforeT",    20),
         (":db.excise/before",     21),
         (":db.alter/attribute",   22),
         (":db.type/ref",          23),
         (":db.type/keyword",      24),
         (":db.type/long",         25),
         (":db.type/double",       26),
         (":db.type/string",       27),
         (":db.type/boolean",      28),
         (":db.type/instant",      29),
         (":db.type/bytes",        30),
         (":db.cardinality/one",   31),
         (":db.cardinality/many",  32),
         (":db.unique/value",      33),
         (":db.unique/identity",   34),
         (":db/doc",               35),
        ]
    };

    static ref V2_IDENTS: [(&'static str, i64); 2] = {
        [(":db.schema/version",    36),
         (":db.schema/attribute",  37),
        ]
    };

    static ref V1_PARTS: [(&'static str, i64, i64); 3] = {
        [(":db.part/db", 0, (1 + V1_IDENTS.len() + V2_IDENTS.len()) as i64),
         (":db.part/user", 0x10000, 0x10000),
         (":db.part/tx", 0x10000000, 0x10000000),
         ]
    };

    static ref V2_PARTS: [(&'static str, i64, i64); 0] = {
        []
    };

    static ref V1_SYMBOLIC_SCHEMA: Value = {
        let s = r#"
{:db/ident             {:db/valueType   :db.type/keyword
                       :db/cardinality :db.cardinality/one
                       :db/unique      :db.unique/identity}
 :db.install/partition {:db/valueType   :db.type/ref
                        :db/cardinality :db.cardinality/many}
 :db.install/valueType {:db/valueType   :db.type/ref
                        :db/cardinality :db.cardinality/many}
 :db.install/attribute {:db/valueType   :db.type/ref
                        :db/cardinality :db.cardinality/many}
 ;; TODO: support user-specified functions in the future.
 ;; :db.install/function {:db/valueType :db.type/ref
 ;;                       :db/cardinality :db.cardinality/many}
 :db/txInstant         {:db/valueType   :db.type/long
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
                        :db/cardinality :db.cardinality/one}}"#;
        edn::parse::value(s)
            .map_err(|_| ErrorKind::BadBootstrapDefinition("Unable to parse V1_SYMBOLIC_SCHEMA".into()))
            .unwrap()
    };

    static ref V2_SYMBOLIC_SCHEMA: Value = {
        let s = r#"
{:db.alter/attribute   {:db/valueType   :db.type/ref
                        :db/cardinality :db.cardinality/many}
 :db.schema/version    {:db/valueType   :db.type/long
                        :db/cardinality :db.cardinality/one}

 ;; unique-value because an attribute can only belong to a single
 ;; schema fragment.
 :db.schema/attribute  {:db/valueType   :db.type/ref
                        :db/unique      :db.unique/value
                        :db/cardinality :db.cardinality/many}}"#;
        edn::parse::value(s)
            .map_err(|_| ErrorKind::BadBootstrapDefinition("Unable to parse V2_SYMBOLIC_SCHEMA".into()))
            .unwrap()
    };
}

/// Convert (ident, entid) pairs into [:db/add IDENT :db/ident IDENT] `Value` instances.
fn idents_to_assertions(idents: &[(&str, i64)]) -> Vec<Value> {
    idents
        .into_iter()
        .map(|&(ident, _)| {
            let value = Value::NamespacedKeyword(to_namespaced_keyword(&ident).unwrap());
            Value::Vector(vec![values::DB_ADD.clone(), value.clone(), values::DB_IDENT.clone(), value.clone()])
        })
        .collect()
}

/// Convert {IDENT {:key :value ...} ...} to [[:db/add IDENT :key :value] ...].
/// In addition, add [:db.add :db.part/db :db.install/attribute IDENT] installation assertions.
fn symbolic_schema_to_assertions(symbolic_schema: &Value) -> Result<Vec<Value>> {
    // Failure here is a coding error, not a runtime error.
    let mut assertions: Vec<Value> = vec![];
    match *symbolic_schema {
        Value::Map(ref m) => {
            for (ident, mp) in m {
                assertions.push(Value::Vector(vec![values::DB_ADD.clone(),
                                                   values::DB_PART_DB.clone(),
                                                   values::DB_INSTALL_ATTRIBUTE.clone(),
                                                   ident.clone()]));
                match *mp {
                    Value::Map(ref mpp) => {
                        for (attr, value) in mpp {
                            assertions.push(Value::Vector(vec![values::DB_ADD.clone(),
                                                               ident.clone(),
                                                               attr.clone(),
                                                               value.clone()]));
                        }
                    },
                    _ => bail!(ErrorKind::BadBootstrapDefinition("Expected {:db/ident {:db/attr value ...} ...}".into()))
                }
            }
        },
        _ => bail!(ErrorKind::BadBootstrapDefinition("Expected {...}".into()))
    }
    Ok(assertions)
}

pub fn bootstrap_partition_map() -> PartitionMap {
    V1_PARTS[..].into_iter()
        .chain(V2_PARTS[..].into_iter())
        .map(|&(part, start, index)| (part.to_string(), Partition::new(start, index)))
        .collect()
}

pub fn bootstrap_ident_map() -> IdentMap {
    V1_IDENTS[..].into_iter()
        .chain(V2_IDENTS[..].into_iter())
        .map(|&(ident, entid)| (ident.to_string(), entid))
        .collect()
}

pub fn bootstrap_schema() -> Schema {
    let bootstrap_assertions: Value = Value::Vector([
        symbolic_schema_to_assertions(&V1_SYMBOLIC_SCHEMA).unwrap(),
        symbolic_schema_to_assertions(&V2_SYMBOLIC_SCHEMA).unwrap()].concat());
    Schema::from_ident_map_and_assertions(bootstrap_ident_map(), &bootstrap_assertions)
        .unwrap()
}

pub fn bootstrap_entities() -> Vec<Entity> {
    let bootstrap_assertions: Value = Value::Vector([
        symbolic_schema_to_assertions(&V1_SYMBOLIC_SCHEMA).unwrap(),
        symbolic_schema_to_assertions(&V2_SYMBOLIC_SCHEMA).unwrap(),
        idents_to_assertions(&V1_IDENTS[..]),
        idents_to_assertions(&V2_IDENTS[..]),
    ].concat());

    // Failure here is a coding error (since the inputs are fixed), not a runtime error.
    // TODO: represent these bootstrap data errors rather than just panicing.
    let bootstrap_entities: Vec<Entity> = mentat_tx_parser::Tx::parse(&[bootstrap_assertions][..]).unwrap();
    return bootstrap_entities;
}
