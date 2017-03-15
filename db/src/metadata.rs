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

//! Most transactions can mutate the Mentat metadata by transacting assertions:
//!
//! - they can add (and, eventually, retract and alter) recognized idents using the `:db/ident`
//!   attribute;
//!
//! - they can add (and, eventually, retract and alter) schema attributes using
//!   `:db.install/attribute` and various `:db/*` attributes;
//!
//! - eventually, they will be able to add (and possibly retract) entid partitions using the
//!   `:db.install/partition` attribute.
//!
//! This module recognizes, validates, applies, and reports on these mutations.

use std::collections::{BTreeMap, BTreeSet};
use std::collections::btree_map::Entry;

use itertools::Itertools; // For join().

use add_retract_alter_set::{
    AddRetractAlterSet,
};
use edn::symbols;
use entids;
use errors::{
    ErrorKind,
    Result,
};
use mentat_core::{
    Entid,
    Schema,
    SchemaMap,
    TypedValue,
    ValueType,
};
use schema::{
    AttributeBuilder,
};

/// An alteration to an attribute.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialOrd, PartialEq)]
pub enum AttributeAlteration {
    /// From http://blog.datomic.com/2014/01/schema-alteration.html:
    /// - rename attributes
    /// - rename your own programmatic identities (uses of :db/ident)
    /// - add or remove indexes
    Index,
    /// - add or remove uniqueness constraints
    UniqueValue,
    UniqueIdentity,
    /// - change attribute cardinality
    Cardinality,
    /// - change whether history is retained for an attribute
    NoHistory,
    /// - change whether an attribute is treated as a component
    IsComponent,
}

/// An alteration to an ident.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialOrd, PartialEq)]
pub enum IdentAlteration {
    Ident(symbols::NamespacedKeyword),
}

/// Summarizes changes to metadata such as a a `Schema` and (in the future) a `PartitionMap`.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialOrd, PartialEq)]
pub struct MetadataReport {
    // Entids that were not present in the original `SchemaMap` that was mutated.
    pub attributes_installed: BTreeSet<Entid>,

    // Entids that were present in the original `SchemaMap` that was mutated, together with a
    // representation of the mutations that were applied.
    pub attributes_altered: BTreeMap<Entid, Vec<AttributeAlteration>>,

    // Idents that were installed into the `SchemaMap`.
    pub idents_altered: BTreeMap<Entid, IdentAlteration>,
}

/// Update a `SchemaMap` in place from the given `[e a typed_value]` triples.
///
/// This is suitable for producing a `SchemaMap` from the `schema` materialized view, which does not
/// contain install and alter markers.
///
/// Returns a report summarizing the mutations that were applied.
pub fn update_schema_map_from_entid_triples<U>(schema_map: &mut SchemaMap, assertions: U) -> Result<MetadataReport>
    where U: IntoIterator<Item=(Entid, Entid, TypedValue)> {

    // Group mutations by impacted entid.
    let mut builders: BTreeMap<Entid, AttributeBuilder> = BTreeMap::new();

    for (entid, attr, ref value) in assertions.into_iter() {
        let builder = builders.entry(entid).or_insert(AttributeBuilder::default());

        // TODO: improve error messages throughout.
        match attr {
            entids::DB_DOC => {
                match *value {
                    TypedValue::String(_) => {},
                    _ => bail!(ErrorKind::BadSchemaAssertion(format!("Expected [... :db/doc \"string value\"] but got [... :db/doc {:?}] for entid '{}' and attribute '{}'", value, entid, attr)))
                }
            },

            entids::DB_VALUE_TYPE => {
                match *value {
                    TypedValue::Ref(entids::DB_TYPE_REF) => { builder.value_type(ValueType::Ref); },
                    TypedValue::Ref(entids::DB_TYPE_BOOLEAN) => { builder.value_type(ValueType::Boolean); },
                    TypedValue::Ref(entids::DB_TYPE_DOUBLE) => { builder.value_type(ValueType::Double); },
                    TypedValue::Ref(entids::DB_TYPE_LONG) => { builder.value_type(ValueType::Long); },
                    TypedValue::Ref(entids::DB_TYPE_STRING) => { builder.value_type(ValueType::String); },
                    TypedValue::Ref(entids::DB_TYPE_KEYWORD) => { builder.value_type(ValueType::Keyword); },
                    _ => bail!(ErrorKind::BadSchemaAssertion(format!("Expected [... :db/valueType :db.type/*] but got [... :db/valueType {:?}] for entid '{}' and attribute '{}'", value, entid, attr)))
                }
            },

            entids::DB_CARDINALITY => {
                match *value {
                    TypedValue::Ref(entids::DB_CARDINALITY_MANY) => { builder.multival(true); },
                    TypedValue::Ref(entids::DB_CARDINALITY_ONE) => { builder.multival(false); },
                    _ => bail!(ErrorKind::BadSchemaAssertion(format!("Expected [... :db/cardinality :db.cardinality/many|:db.cardinality/one] but got [... :db/cardinality {:?}]", value)))
                }
            },

            entids::DB_UNIQUE => {
                match *value {
                    // TODO: accept nil in some form.
                    // TypedValue::Nil => {
                    //     builder.unique_value(false);
                    //     builder.unique_identity(false);
                    // },
                    TypedValue::Ref(entids::DB_UNIQUE_VALUE) => { builder.unique_value(true); },
                    TypedValue::Ref(entids::DB_UNIQUE_IDENTITY) => { builder.unique_identity(true); },
                    _ => bail!(ErrorKind::BadSchemaAssertion(format!("Expected [... :db/unique :db.unique/value|:db.unique/identity] but got [... :db/unique {:?}]", value)))
                }
            },

            entids::DB_INDEX => {
                match *value {
                    TypedValue::Boolean(x) => { builder.index(x); },
                    _ => bail!(ErrorKind::BadSchemaAssertion(format!("Expected [... :db/index true|false] but got [... :db/index {:?}]", value)))
                }
            },

            entids::DB_FULLTEXT => {
                match *value {
                    TypedValue::Boolean(x) => { builder.fulltext(x); },
                    _ => bail!(ErrorKind::BadSchemaAssertion(format!("Expected [... :db/fulltext true|false] but got [... :db/fulltext {:?}]", value)))
                }
            },

            entids::DB_IS_COMPONENT => {
                match *value {
                    TypedValue::Boolean(x) => { builder.component(x); },
                    _ => bail!(ErrorKind::BadSchemaAssertion(format!("Expected [... :db/isComponent true|false] but got [... :db/isComponent {:?}]", value)))
                }
            },

            _ => {
                bail!(ErrorKind::BadSchemaAssertion(format!("Do not recognize attribute '{}' for entid '{}'", attr, entid)))
            }
        }
    };

    let mut attributes_installed: BTreeSet<Entid> = BTreeSet::default();
    let mut attributes_altered: BTreeMap<Entid, Vec<AttributeAlteration>> = BTreeMap::default();

    for (entid, builder) in builders.into_iter() {
        match schema_map.entry(entid) {
            Entry::Vacant(entry) => {
                if !builder.is_valid_install_attribute() {
                    bail!(ErrorKind::BadSchemaAssertion(format!("Schema attribute for :db.install/attribute with entid '{}' does not set :db/valueType", entid)));
                }
                entry.insert(builder.build());
                attributes_installed.insert(entid);
            },
            Entry::Occupied(mut entry) => {
                if !builder.is_valid_alter_attribute() {
                    bail!(ErrorKind::BadSchemaAssertion(format!("Schema attribute for :db.alter/attribute with entid '{}' must not set :db/valueType", entid)));
                }
                let mutations = builder.mutate(entry.get_mut());
                attributes_altered.insert(entid, mutations);
            },
        }
    }

    Ok(MetadataReport {
        attributes_installed: attributes_installed,
        attributes_altered: attributes_altered,
        idents_altered: BTreeMap::default(),
    })
}

/// Update a `Schema` in place from the given `[e a typed_value added]` quadruples.
///
/// This layer enforces that metadata assertions, as distinct from attribute assertions, and
/// generally of the form
/// - [entid :db/ident ...]
/// - [:db.part/db :db.install/attribute ...]
/// - [:db.part/db :db.alter/attribute ...]
/// are present and correct.
///
/// This is suitable for mutating a `Schema` from an applied transaction.
///
/// Returns a report summarizing the mutations that were applied.
pub fn update_schema_from_entid_quadruples<U>(schema: &mut Schema, assertions: U) -> Result<MetadataReport>
    where U: IntoIterator<Item=(Entid, Entid, TypedValue, bool)> {

    // Group attribute assertions into asserted, retracted, and updated.  We assume all our
    // attribute assertions are :db/cardinality :db.cardinality/one (so they'll only be added or
    // retracted at most once), which means all attribute alterations are simple changes from an old
    // value to a new value.
    //
    // We also collect metadata assertions (:db.install/attribute, :db.alter/attribute, :db/ident)
    // separately; these are naturally :db.cardinality :db/cardinality many.
    let mut attribute_set: AddRetractAlterSet<(Entid, Entid), TypedValue> = AddRetractAlterSet::default();
    let mut ident_set: AddRetractAlterSet<Entid, symbols::NamespacedKeyword> = AddRetractAlterSet::default();

    let mut flagged_install: BTreeSet<Entid> = BTreeSet::default();
    let mut flagged_alter: BTreeSet<Entid> = BTreeSet::default();

    for (e, a, typed_value, added) in assertions.into_iter() {
        // Here we handle metadata assertions.
        match a {
            entids::DB_IDENT => {
                if let TypedValue::Keyword(ref keyword) = typed_value {
                    ident_set.witness(e, keyword.clone(), added);
                    continue
                } else {
                    // Something is terribly wrong: the schema ensures we have a keyword.
                    unreachable!();
                }
            }

            entids::DB_INSTALL_ATTRIBUTE => {
                if !added {
                    bail!(ErrorKind::NotYetImplemented(format!("Handling attribute retractions with attribute {} not yet implemented", a)));
                }

                if e != entids::DB_PART_DB {
                    bail!(ErrorKind::BadSchemaAssertion(format!("Expected [:db.part/db :db.install/attribute ...] but got [{:?} :db.install/attribute ...]", e)))
                }

                match typed_value {
                    TypedValue::Ref(e) => {
                        flagged_install.insert(e);
                        continue
                    },
                    _ => bail!(ErrorKind::BadSchemaAssertion(format!("Expected [:db.part/db :db.install/attribute entid] but got [:db.part/db :db.install/attribute {:?}]", typed_value)))
                }
            },

            entids::DB_ALTER_ATTRIBUTE => {
                if !added {
                    bail!(ErrorKind::NotYetImplemented(format!("Handling attribute retractions with attribute {} not yet implemented", a)));
                }

                if e != entids::DB_PART_DB {
                    bail!(ErrorKind::BadSchemaAssertion(format!("Expected [:db.part/db :db.alter/attribute ...] but got [{:?} :db.alter/attribute ...]", e)))
                }

                match typed_value {
                    TypedValue::Ref(e) => {
                        flagged_alter.insert(e);
                        continue
                    },
                    _ => bail!(ErrorKind::BadSchemaAssertion(format!("Expected [:db.part/db :db.alter/attribute entid] but got [:db.part/db :db.alter/attribute {:?}]", typed_value)))
                }
            },

            _ => {},
        }

        attribute_set.witness((e, a), typed_value, added);
    }

    // Datomic does not allow to retract attributes or idents.  For now, Mentat follows suit.
    if !attribute_set.retracted.is_empty() {
        bail!(ErrorKind::NotYetImplemented(format!("Retracting metadata attribute assertions not yet implemented: retracted [e a] pairs [{}]",
                                                   attribute_set.retracted.keys().map(|&(e, a)| format!("[{} {}]", e, a)).join(", "))));
    }

    if !ident_set.retracted.is_empty() {
        bail!(ErrorKind::NotYetImplemented(format!("Retracting metadata idents assertions not yet implemented: retracted [e :db/ident] pairs [{}]",
                                                   ident_set.retracted.keys().map(|&e| format!("[{} :db/ident]", e)).join(", "))));
    }

    // Collect triples.
    let asserted_triples = attribute_set.asserted.into_iter().map(|((e, a), typed_value)| (e, a, typed_value));
    let altered_triples = attribute_set.altered.into_iter().map(|((e, a), (_old_value, new_value))| (e, a, new_value));

    let report = update_schema_map_from_entid_triples(&mut schema.schema_map, asserted_triples.chain(altered_triples))?;

    // Verify that new schema attributes were flagged with :db.install/attribute, that mutated
    // schema attributes were flagged with :db.alter/attribute, and that no orphaned entids were
    // flagged but not provided.
    for &entid in &report.attributes_installed {
        if !flagged_install.remove(&entid) {
            bail!(ErrorKind::BadSchemaAssertion(format!("Schema attributes given for new attribute with entid {}, but :db.install/attribute not included in transaction", entid)));
        }
        if flagged_alter.contains(&entid) {
            bail!(ErrorKind::BadSchemaAssertion(format!("Schema attributes given for new attribute with entid {}, but :db.alter/attribute included in transaction", entid)));
        }
        // Datomic requires a :db/ident for every schema attribute.  Mentat follows suit.
        if !ident_set.asserted.contains_key(&entid) {
            bail!(ErrorKind::BadSchemaAssertion(format!("Schema attributes given for new attribute with entid {}, but :db/ident not included in transaction", entid)));
        }
    }

    for &entid in report.attributes_altered.keys() {
        if !flagged_alter.remove(&entid) {
            bail!(ErrorKind::BadSchemaAssertion(format!("Schema attributes given for existing attribute with entid {}, but :db.alter/attribute not included in transaction", entid)));
        }
        if flagged_install.contains(&entid) {
            bail!(ErrorKind::BadSchemaAssertion(format!("Schema attributes given for existing attribute with entid {}, but :db.install/attribute included in transaction", entid)));
        }
    }

    // Verify that we have no orphaned entids: we've seen schema attributes for anything installed
    // or altered.
    if !flagged_install.is_empty() {
        bail!(ErrorKind::BadSchemaAssertion(format!("Schema attributes not given for :db.install/attribute entids [{}]", flagged_install.iter().join(", "))));
    }

    if !flagged_alter.is_empty() {
        bail!(ErrorKind::BadSchemaAssertion(format!("Schema attributes not given for :db.alter/attribute entids [{}]", flagged_alter.iter().join(", "))));
    }

    let mut idents_altered: BTreeMap<Entid, IdentAlteration> = BTreeMap::new();

    // Asserted or altered :db/idents update the relevant entids.
    for (entid, ident) in ident_set.asserted {
        schema.entid_map.insert(entid, ident.clone());
        schema.ident_map.insert(ident.clone(), entid);
        idents_altered.insert(entid, IdentAlteration::Ident(ident.clone()));
    }

    for (entid, (_old_ident, new_ident)) in ident_set.altered {
        schema.entid_map.insert(entid, new_ident.clone());
        schema.ident_map.insert(new_ident.clone(), entid);
        idents_altered.insert(entid, IdentAlteration::Ident(new_ident.clone()));
    }

    Ok(MetadataReport {
        idents_altered: idents_altered,
        .. report
    })
}
