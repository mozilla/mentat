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
//! - they can add (and, eventually, retract and alter) schema attributes using various `:db/*`
//!   attributes;
//!
//! - eventually, they will be able to add (and possibly retract) entid partitions using a Mentat
//!   equivalent (perhaps :db/partition or :db.partition/start) to Datomic's `:db.install/partition`
//!   attribute.
//!
//! This module recognizes, validates, applies, and reports on these mutations.

use failure::ResultExt;

use std::collections::{BTreeMap, BTreeSet};
use std::collections::btree_map::Entry;

use add_retract_alter_set::{
    AddRetractAlterSet,
};
use edn::symbols;
use entids;
use errors::{
    DbErrorKind,
    Result,
};

use core_traits::{
    Entid,
};

use mentat_core::{
    attribute,
    Schema,
    AttributeMap,
    TypedValue,
    ValueType,
};

use schema::{
    AttributeBuilder,
    AttributeValidation,
};

use types::{
    EAV,
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
    Unique,
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
    Ident(symbols::Keyword),
}

/// Summarizes changes to metadata such as a a `Schema` and (in the future) a `PartitionMap`.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialOrd, PartialEq)]
pub struct MetadataReport {
    // Entids that were not present in the original `AttributeMap` that was mutated.
    pub attributes_installed: BTreeSet<Entid>,

    // Entids that were present in the original `AttributeMap` that was mutated, together with a
    // representation of the mutations that were applied.
    pub attributes_altered: BTreeMap<Entid, Vec<AttributeAlteration>>,

    // Idents that were installed into the `AttributeMap`.
    pub idents_altered: BTreeMap<Entid, IdentAlteration>,
}

impl MetadataReport {
    pub fn attributes_did_change(&self) -> bool {
        !(self.attributes_installed.is_empty() &&
          self.attributes_altered.is_empty())
    }
}

/// Update an 'AttributeMap' in place given two sets of ident and attribute retractions, which
/// together contain enough information to reason about a "schema retraction".
///
/// Schema may only be retracted if all of its necessary attributes are being retracted:
/// - :db/ident, :db/valueType, :db/cardinality.
///
/// Note that this is currently incomplete/flawed:
/// - we're allowing optional attributes to not be retracted and dangle afterwards
///
/// Returns a set of attribute retractions which do not involve schema-defining attributes.
fn update_attribute_map_from_schema_retractions(attribute_map: &mut AttributeMap, retractions: Vec<EAV>, ident_retractions: &BTreeMap<Entid, symbols::Keyword>) -> Result<Vec<EAV>> {
    // Process retractions of schema attributes first. It's allowed to retract a schema attribute
    // if all of the schema-defining schema attributes are being retracted.
    // A defining set of attributes is :db/ident, :db/valueType, :db/cardinality.
    let mut filtered_retractions = vec![];
    let mut suspect_retractions = vec![];

    // Filter out sets of schema altering retractions.
    let mut eas = BTreeMap::new();
    for (e, a, v) in retractions.into_iter() {
        if entids::is_a_schema_attribute(a) {
            eas.entry(e).or_insert(vec![]).push(a);
            suspect_retractions.push((e, a, v));
        } else {
            filtered_retractions.push((e, a, v));
        }
    }

    // TODO (see https://github.com/mozilla/mentat/issues/796).
    // Retraction of idents is allowed, but if an ident names a schema attribute, then we should enforce
    // retraction of all of the associated schema attributes.
    // Unfortunately, our current in-memory schema representation (namely, how we define an Attribute) is not currently
    // rich enough: it lacks distinction between presence and absence, and instead assumes default values.

    // Currently, in order to do this enforcement correctly, we'd need to inspect 'datoms'.

    // Here is an incorrect way to enforce this. It's incorrect because it prevents us from retracting non-"schema naming" idents.
    // for retracted_e in ident_retractions.keys() {
    //     if !eas.contains_key(retracted_e) {
    //         bail!(DbErrorKind::BadSchemaAssertion(format!("Retracting :db/ident of a schema without retracting its defining attributes is not permitted.")));
    //     }
    // }

    for (e, a, v) in suspect_retractions.into_iter() {
        let attributes = eas.get(&e).unwrap();

        // Found a set of retractions which negate a schema.
        if attributes.contains(&entids::DB_CARDINALITY) && attributes.contains(&entids::DB_VALUE_TYPE) {
            // Ensure that corresponding :db/ident is also being retracted at the same time.
            if ident_retractions.contains_key(&e) {
                // Remove attributes corresponding to retracted attribute.
                attribute_map.remove(&e);
            } else {
                bail!(DbErrorKind::BadSchemaAssertion(format!("Retracting defining attributes of a schema without retracting its :db/ident is not permitted.")));
            }
        } else {
            filtered_retractions.push((e, a, v));
        }
    }

    Ok(filtered_retractions)
}

/// Update a `AttributeMap` in place from the given `[e a typed_value]` triples.
///
/// This is suitable for producing a `AttributeMap` from the `schema` materialized view, which does not
/// contain install and alter markers.
///
/// Returns a report summarizing the mutations that were applied.
pub fn update_attribute_map_from_entid_triples(attribute_map: &mut AttributeMap, assertions: Vec<EAV>, retractions: Vec<EAV>) -> Result<MetadataReport> {
    fn attribute_builder_to_modify(attribute_id: Entid, existing: &AttributeMap) -> AttributeBuilder {
        existing.get(&attribute_id)
                .map(AttributeBuilder::to_modify_attribute)
                .unwrap_or_else(AttributeBuilder::default)
    }

    // Group mutations by impacted entid.
    let mut builders: BTreeMap<Entid, AttributeBuilder> = BTreeMap::new();

    // For retractions, we start with an attribute builder that's pre-populated with the existing
    // attribute values. That allows us to check existing values and unset them.
    for (entid, attr, ref value) in retractions {
        let builder = builders.entry(entid).or_insert_with(|| attribute_builder_to_modify(entid, attribute_map));
        match attr {
            // You can only retract :db/unique, :db/isComponent; all others must be altered instead
            // of retracted, or are not allowed to change.
            entids::DB_IS_COMPONENT => {
                match value {
                    &TypedValue::Boolean(v) if builder.component == Some(v) => {
                        builder.component(false);
                    },
                    v => {
                        bail!(DbErrorKind::BadSchemaAssertion(format!("Attempted to retract :db/isComponent with the wrong value {:?}.", v)));
                    },
                }
            },

            entids::DB_UNIQUE => {
                match *value {
                    TypedValue::Ref(u) => {
                        match u {
                            entids::DB_UNIQUE_VALUE if builder.unique == Some(Some(attribute::Unique::Value)) => {
                                builder.non_unique();
                            },
                            entids::DB_UNIQUE_IDENTITY if builder.unique == Some(Some(attribute::Unique::Identity)) => {
                                builder.non_unique();
                            },
                            v => {
                                bail!(DbErrorKind::BadSchemaAssertion(format!("Attempted to retract :db/unique with the wrong value {}.", v)));
                            },
                        }
                    },
                    _ => bail!(DbErrorKind::BadSchemaAssertion(format!("Expected [:db/retract _ :db/unique :db.unique/_] but got [:db/retract {} :db/unique {:?}]", entid, value)))
                }
            },

            entids::DB_VALUE_TYPE |
            entids::DB_CARDINALITY |
            entids::DB_INDEX |
            entids::DB_FULLTEXT |
            entids::DB_NO_HISTORY => {
                bail!(DbErrorKind::BadSchemaAssertion(format!("Retracting attribute {} for entity {} not permitted.", attr, entid)));
            },

            _ => {
                bail!(DbErrorKind::BadSchemaAssertion(format!("Do not recognize attribute {} for entid {}", attr, entid)))
            }
        }
    }

    for (entid, attr, ref value) in assertions.into_iter() {
        // For assertions, we can start with an empty attribute builder.
        let builder = builders.entry(entid).or_insert_with(Default::default);

        // TODO: improve error messages throughout.
        match attr {
            entids::DB_VALUE_TYPE => {
                match *value {
                    TypedValue::Ref(entids::DB_TYPE_BOOLEAN) => { builder.value_type(ValueType::Boolean); },
                    TypedValue::Ref(entids::DB_TYPE_DOUBLE)  => { builder.value_type(ValueType::Double); },
                    TypedValue::Ref(entids::DB_TYPE_INSTANT) => { builder.value_type(ValueType::Instant); },
                    TypedValue::Ref(entids::DB_TYPE_KEYWORD) => { builder.value_type(ValueType::Keyword); },
                    TypedValue::Ref(entids::DB_TYPE_LONG)    => { builder.value_type(ValueType::Long); },
                    TypedValue::Ref(entids::DB_TYPE_REF)     => { builder.value_type(ValueType::Ref); },
                    TypedValue::Ref(entids::DB_TYPE_STRING)  => { builder.value_type(ValueType::String); },
                    TypedValue::Ref(entids::DB_TYPE_UUID)    => { builder.value_type(ValueType::Uuid); },
                    _ => bail!(DbErrorKind::BadSchemaAssertion(format!("Expected [... :db/valueType :db.type/*] but got [... :db/valueType {:?}] for entid {} and attribute {}", value, entid, attr)))
                }
            },

            entids::DB_CARDINALITY => {
                match *value {
                    TypedValue::Ref(entids::DB_CARDINALITY_MANY) => { builder.multival(true); },
                    TypedValue::Ref(entids::DB_CARDINALITY_ONE) => { builder.multival(false); },
                    _ => bail!(DbErrorKind::BadSchemaAssertion(format!("Expected [... :db/cardinality :db.cardinality/many|:db.cardinality/one] but got [... :db/cardinality {:?}]", value)))
                }
            },

            entids::DB_UNIQUE => {
                match *value {
                    TypedValue::Ref(entids::DB_UNIQUE_VALUE) => { builder.unique(attribute::Unique::Value); },
                    TypedValue::Ref(entids::DB_UNIQUE_IDENTITY) => { builder.unique(attribute::Unique::Identity); },
                    _ => bail!(DbErrorKind::BadSchemaAssertion(format!("Expected [... :db/unique :db.unique/value|:db.unique/identity] but got [... :db/unique {:?}]", value)))
                }
            },

            entids::DB_INDEX => {
                match *value {
                    TypedValue::Boolean(x) => { builder.index(x); },
                    _ => bail!(DbErrorKind::BadSchemaAssertion(format!("Expected [... :db/index true|false] but got [... :db/index {:?}]", value)))
                }
            },

            entids::DB_FULLTEXT => {
                match *value {
                    TypedValue::Boolean(x) => { builder.fulltext(x); },
                    _ => bail!(DbErrorKind::BadSchemaAssertion(format!("Expected [... :db/fulltext true|false] but got [... :db/fulltext {:?}]", value)))
                }
            },

            entids::DB_IS_COMPONENT => {
                match *value {
                    TypedValue::Boolean(x) => { builder.component(x); },
                    _ => bail!(DbErrorKind::BadSchemaAssertion(format!("Expected [... :db/isComponent true|false] but got [... :db/isComponent {:?}]", value)))
                }
            },

            entids::DB_NO_HISTORY => {
                match *value {
                    TypedValue::Boolean(x) => { builder.no_history(x); },
                    _ => bail!(DbErrorKind::BadSchemaAssertion(format!("Expected [... :db/noHistory true|false] but got [... :db/noHistory {:?}]", value)))
                }
            },

            _ => {
                bail!(DbErrorKind::BadSchemaAssertion(format!("Do not recognize attribute {} for entid {}", attr, entid)))
            }
        }
    };

    let mut attributes_installed: BTreeSet<Entid> = BTreeSet::default();
    let mut attributes_altered: BTreeMap<Entid, Vec<AttributeAlteration>> = BTreeMap::default();

    for (entid, builder) in builders.into_iter() {
        match attribute_map.entry(entid) {
            Entry::Vacant(entry) => {
                // Validate once…
                builder.validate_install_attribute().context(DbErrorKind::BadSchemaAssertion(format!("Schema alteration for new attribute with entid {} is not valid", entid)))?;

                // … and twice, now we have the Attribute.
                let a = builder.build();
                a.validate(|| entid.to_string())?;
                entry.insert(a);
                attributes_installed.insert(entid);
            },

            Entry::Occupied(mut entry) => {
                builder.validate_alter_attribute().context(DbErrorKind::BadSchemaAssertion(format!("Schema alteration for existing attribute with entid {} is not valid", entid)))?;
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
/// This layer enforces that ident assertions of the form [entid :db/ident ...] (as distinct from
/// attribute assertions) are present and correct.
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
    let mut attribute_set: AddRetractAlterSet<(Entid, Entid), TypedValue> = AddRetractAlterSet::default();
    let mut ident_set: AddRetractAlterSet<Entid, symbols::Keyword> = AddRetractAlterSet::default();

    for (e, a, typed_value, added) in assertions.into_iter() {
        // Here we handle :db/ident assertions.
        if a == entids::DB_IDENT {
            if let TypedValue::Keyword(ref keyword) = typed_value {
                ident_set.witness(e, keyword.as_ref().clone(), added);
                continue
            } else {
                // Something is terribly wrong: the schema ensures we have a keyword.
                unreachable!();
            }
        }

        attribute_set.witness((e, a), typed_value, added);
    }

    // Collect triples.
    let retracted_triples = attribute_set.retracted.into_iter().map(|((e, a), typed_value)| (e, a, typed_value));
    let asserted_triples = attribute_set.asserted.into_iter().map(|((e, a), typed_value)| (e, a, typed_value));
    let altered_triples = attribute_set.altered.into_iter().map(|((e, a), (_old_value, new_value))| (e, a, new_value));

    // First we process retractions which remove schema.
    // This operation consumes our current list of attribute retractions, producing a filtered one.
    let non_schema_retractions = update_attribute_map_from_schema_retractions(&mut schema.attribute_map,
                                                                              retracted_triples.collect(),
                                                                              &ident_set.retracted)?;

    // Now we process all other retractions.
    let report = update_attribute_map_from_entid_triples(&mut schema.attribute_map,
                                                         asserted_triples.chain(altered_triples).collect(),
                                                         non_schema_retractions)?;

    let mut idents_altered: BTreeMap<Entid, IdentAlteration> = BTreeMap::new();

    // Asserted, altered, or retracted :db/idents update the relevant entids.
    for (entid, ident) in ident_set.asserted {
        schema.entid_map.insert(entid, ident.clone());
        schema.ident_map.insert(ident.clone(), entid);
        idents_altered.insert(entid, IdentAlteration::Ident(ident.clone()));
    }

    for (entid, (old_ident, new_ident)) in ident_set.altered {
        schema.entid_map.insert(entid, new_ident.clone()); // Overwrite existing.
        schema.ident_map.remove(&old_ident); // Remove old.
        schema.ident_map.insert(new_ident.clone(), entid); // Insert new.
        idents_altered.insert(entid, IdentAlteration::Ident(new_ident.clone()));
    }

    for (entid, ident) in &ident_set.retracted {
        schema.entid_map.remove(entid);
        schema.ident_map.remove(ident);
        idents_altered.insert(*entid, IdentAlteration::Ident(ident.clone()));
    }

    // Component attributes need to change if either:
    // - a component attribute changed
    // - a schema attribute that was a component was retracted
    // These two checks are a rather heavy-handed way of keeping schema's
    // component_attributes up-to-date: most of the time we'll rebuild it
    // even though it's not necessary (e.g. a schema attribute that's _not_
    // a component was removed, or a non-component related attribute changed).
    if report.attributes_did_change() || ident_set.retracted.len() > 0 {
        schema.update_component_attributes();
    }

    Ok(MetadataReport {
        idents_altered: idents_altered,
        .. report
    })
}
