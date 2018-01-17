// Copyright 2016 Mozilla
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

use std::collections::BTreeMap;

use mentat_core::{
    Attribute,
    Entid,
    HasSchema,
    KnownEntid,
    NamespacedKeyword,
    TypedValue,
    ValueType,
};

pub use mentat_core::attribute;
use mentat_core::attribute::Unique;

use ::{
    CORE_SCHEMA_VERSION,
    IntoResult,
};

use ::conn::{
    InProgress,
    Queryable,
};

use ::errors::{
    ErrorKind,
    Result,
};

use ::entity_builder::{
    BuildTerms,
    TermBuilder,
    Terms,
};

pub use mentat_db::AttributeBuilder;

pub type Version = u32;
pub type Datom = (Entid, Entid, TypedValue);

/// `Attribute` instances not only aren't named, but don't even have entids.
/// We need two kinds of structure here: an abstract definition of a vocabulary in terms of names,
/// and a concrete instance of a vocabulary in a particular store.
/// Note that, because it's possible to 'flesh out' a vocabulary with attributes without bumping
/// its version number, we need to know the attributes that the application cares about -- it's
/// not enough to know the name and version. Indeed, we even care about the details of each attribute,
/// because that's how we'll detect errors.
#[derive(Debug)]
pub struct Definition {
    pub name: NamespacedKeyword,
    pub version: Version,
    pub attributes: Vec<(NamespacedKeyword, Attribute)>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Vocabulary {
    pub entity: Entid,
    pub version: Version,
    attributes: Vec<(Entid, Attribute)>,
}

#[derive(Debug, Default, Clone)]
pub struct Vocabularies(pub BTreeMap<NamespacedKeyword, Vocabulary>);   // N.B., this has a copy of the attributes in Schema!

impl Vocabularies {
    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn get(&self, name: &NamespacedKeyword) -> Option<&Vocabulary> {
        self.0.get(name)
    }
}

lazy_static! {
    static ref DB_SCHEMA_CORE: NamespacedKeyword = {
        NamespacedKeyword::new("db.schema", "core")
    };
    static ref DB_SCHEMA_ATTRIBUTE: NamespacedKeyword = {
        NamespacedKeyword::new("db.schema", "attribute")
    };
    static ref DB_SCHEMA_VERSION: NamespacedKeyword = {
        NamespacedKeyword::new("db.schema", "version")
    };
    static ref DB_IDENT: NamespacedKeyword = {
        NamespacedKeyword::new("db", "ident")
    };
    static ref DB_UNIQUE: NamespacedKeyword = {
        NamespacedKeyword::new("db", "unique")
    };
    static ref DB_UNIQUE_VALUE: NamespacedKeyword = {
        NamespacedKeyword::new("db.unique", "value")
    };
    static ref DB_UNIQUE_IDENTITY: NamespacedKeyword = {
        NamespacedKeyword::new("db.unique", "identity")
    };
    static ref DB_IS_COMPONENT: NamespacedKeyword = {
        NamespacedKeyword::new("db", "isComponent")
    };
    static ref DB_VALUE_TYPE: NamespacedKeyword = {
        NamespacedKeyword::new("db", "valueType")
    };
    static ref DB_INDEX: NamespacedKeyword = {
        NamespacedKeyword::new("db", "index")
    };
    static ref DB_FULLTEXT: NamespacedKeyword = {
        NamespacedKeyword::new("db", "fulltext")
    };
    static ref DB_CARDINALITY: NamespacedKeyword = {
        NamespacedKeyword::new("db", "cardinality")
    };
    static ref DB_CARDINALITY_ONE: NamespacedKeyword = {
        NamespacedKeyword::new("db.cardinality", "one")
    };
    static ref DB_CARDINALITY_MANY: NamespacedKeyword = {
        NamespacedKeyword::new("db.cardinality", "many")
    };

    // Not yet supported.
    // static ref DB_NO_HISTORY: NamespacedKeyword = {
    //     NamespacedKeyword::new("db", "noHistory")
    // };
}

trait HasCoreSchema {
    /// Return the entity ID for a type. On failure, return `MissingCoreVocabulary`.
    fn core_type(&self, t: ValueType) -> Result<KnownEntid>;

    /// Return the entity ID for an ident. On failure, return `MissingCoreVocabulary`.
    fn core_entid(&self, ident: &NamespacedKeyword) -> Result<KnownEntid>;

    /// Return the entity ID for an attribute's keyword. On failure, return
    /// `MissingCoreVocabulary`.
    fn core_attribute(&self, ident: &NamespacedKeyword) -> Result<KnownEntid>;
}

impl<T> HasCoreSchema for T where T: HasSchema {
    fn core_type(&self, t: ValueType) -> Result<KnownEntid> {
        self.entid_for_type(t)
            .ok_or_else(|| ErrorKind::MissingCoreVocabulary(DB_SCHEMA_VERSION.clone()).into())
    }

    fn core_entid(&self, ident: &NamespacedKeyword) -> Result<KnownEntid> {
        self.get_entid(ident)
            .ok_or_else(|| ErrorKind::MissingCoreVocabulary(DB_SCHEMA_VERSION.clone()).into())
    }

    fn core_attribute(&self, ident: &NamespacedKeyword) -> Result<KnownEntid> {
        self.attribute_for_ident(ident)
            .ok_or_else(|| ErrorKind::MissingCoreVocabulary(DB_SCHEMA_VERSION.clone()).into())
            .map(|(_, e)| e)
    }
}

impl Definition {
    fn description_for_attributes<'s, T, R>(&'s self, attributes: &[R], via: &T) -> Result<Terms>
     where T: HasCoreSchema,
           R: ::std::borrow::Borrow<(NamespacedKeyword, Attribute)> {

        // The attributes we'll need to describe this vocabulary.
        let a_version = via.core_attribute(&DB_SCHEMA_VERSION)?;
        let a_ident = via.core_attribute(&DB_IDENT)?;
        let a_attr = via.core_attribute(&DB_SCHEMA_ATTRIBUTE)?;

        let a_cardinality = via.core_attribute(&DB_CARDINALITY)?;
        let a_fulltext = via.core_attribute(&DB_FULLTEXT)?;
        let a_index = via.core_attribute(&DB_INDEX)?;
        let a_is_component = via.core_attribute(&DB_IS_COMPONENT)?;
        let a_value_type = via.core_attribute(&DB_VALUE_TYPE)?;
        let a_unique = via.core_attribute(&DB_UNIQUE)?;

        // Not yet supported.
        // let a_no_history = via.core_attribute(&DB_NO_HISTORY)?;

        let v_cardinality_many = via.core_entid(&DB_CARDINALITY_MANY)?;
        let v_cardinality_one = via.core_entid(&DB_CARDINALITY_ONE)?;
        let v_unique_identity = via.core_entid(&DB_UNIQUE_IDENTITY)?;
        let v_unique_value = via.core_entid(&DB_UNIQUE_VALUE)?;

        // The properties of the vocabulary itself.
        let name: TypedValue = self.name.clone().into();
        let version: TypedValue = TypedValue::Long(self.version as i64);

        // Describe the vocabulary.
        let mut entity = TermBuilder::new().describe_tempid("s");
        entity.add(a_version, version)?;
        entity.add(a_ident, name)?;
        let (mut builder, schema) = entity.finish();

        // Describe each of its attributes.
        // This is a lot like Schema::to_edn_value; at some point we should tidy this up.
        for ref r in attributes.iter() {
            let &(ref name, ref attr) = r.borrow();

            // Note that we allow tempid resolution to find an existing entity, if it
            // exists. We don't yet support upgrades, which will involve producing
            // alteration statements.
            let tempid = builder.named_tempid(name.to_string());
            let name: TypedValue = name.clone().into();
            builder.add(tempid.clone(), a_ident, name)?;
            builder.add(schema.clone(), a_attr, tempid.clone())?;

            let value_type = via.core_type(attr.value_type)?;
            builder.add(tempid.clone(), a_value_type, value_type)?;

            let c = if attr.multival {
                v_cardinality_many
            } else {
                v_cardinality_one
            };
            builder.add(tempid.clone(), a_cardinality, c)?;

            if attr.index {
                builder.add(tempid.clone(), a_index, TypedValue::Boolean(true))?;
            }
            if attr.fulltext {
                builder.add(tempid.clone(), a_fulltext, TypedValue::Boolean(true))?;
            }
            if attr.component {
                builder.add(tempid.clone(), a_is_component, TypedValue::Boolean(true))?;
            }

            if let Some(u) = attr.unique {
                let uu = match u {
                    Unique::Identity => v_unique_identity,
                    Unique::Value => v_unique_value,
                };
                builder.add(tempid.clone(), a_unique, uu)?;
            }
        }

        builder.build()
    }

    /// Return a sequence of terms that describes this vocabulary definition and its attributes.
    fn description<T>(&self, via: &T) -> Result<Terms> where T: HasSchema {
        self.description_for_attributes(self.attributes.as_slice(), via)
    }
}

#[derive(Debug, Eq, PartialEq)]
pub enum VocabularyCheck<'definition> {
    NotPresent,
    Present,
    PresentButNeedsUpdate { older_version: Vocabulary },
    PresentButTooNew { newer_version: Vocabulary },
    PresentButMissingAttributes { attributes: Vec<&'definition (NamespacedKeyword, Attribute)> },
}

#[derive(Debug, Eq, PartialEq)]
pub enum VocabularyOutcome {
    /// The vocabulary was absent and has been installed.
    Installed,

    /// The vocabulary was present with this version, but some attributes were absent.
    /// They have been installed.
    InstalledMissingAttributes,

    /// The vocabulary was present, at the correct version, and all attributes were present.
    Existed,

    /// The vocabulary was present, at an older version, and it has been upgraded. Any
    /// missing attributes were installed.
    Upgraded,
}

pub trait HasVocabularies {
    fn read_vocabularies(&self) -> Result<Vocabularies>;
    fn read_vocabulary_named(&self, name: &NamespacedKeyword) -> Result<Option<Vocabulary>>;
}

pub trait VersionedStore {
    /// Check whether the vocabulary described by the provided metadata is present in the store.
    fn check_vocabulary<'definition>(&self, definition: &'definition Definition) -> Result<VocabularyCheck<'definition>>;

    /// Check whether the provided vocabulary is present in the store. If it isn't, make it so.
    fn ensure_vocabulary(&mut self, definition: &Definition) -> Result<VocabularyOutcome>;

    /// Make sure that our expectations of the core vocabulary -- basic types and attributes -- are met.
    fn verify_core_schema(&self) -> Result<()>;
}

trait VocabularyMechanics {
    fn install_vocabulary(&mut self, definition: &Definition) -> Result<VocabularyOutcome>;
    fn install_attributes_for<'definition>(&mut self, definition: &'definition Definition, attributes: Vec<&'definition (NamespacedKeyword, Attribute)>) -> Result<VocabularyOutcome>;
    fn upgrade_vocabulary(&mut self, definition: &Definition, from_version: Vocabulary) -> Result<VocabularyOutcome>;
}

impl Vocabulary {
    // TODO: don't do linear search!
    fn find<T>(&self, entid: T) -> Option<&Attribute> where T: Into<Entid> {
        let to_find = entid.into();
        self.attributes.iter().find(|&&(e, _)| e == to_find).map(|&(_, ref a)| a)
    }
}

impl<'a, 'c> VersionedStore for InProgress<'a, 'c> {
    fn verify_core_schema(&self) -> Result<()> {
        if let Some(core) = self.read_vocabulary_named(&DB_SCHEMA_CORE)? {
            if core.version != CORE_SCHEMA_VERSION {
                bail!(ErrorKind::UnexpectedCoreSchema(Some(core.version)));
            }

            // TODO: check things other than the version.
        } else {
            // This would be seriously messed up.
            bail!(ErrorKind::UnexpectedCoreSchema(None));
        }
        Ok(())
    }

    fn check_vocabulary<'definition>(&self, definition: &'definition Definition) -> Result<VocabularyCheck<'definition>> {
        if let Some(vocabulary) = self.read_vocabulary_named(&definition.name)? {
            // The name is present.
            // Check the version.
            if vocabulary.version == definition.version {
                // Same version. Check that all of our attributes are present.
                let mut missing: Vec<&'definition (NamespacedKeyword, Attribute)> = vec![];
                for pair in definition.attributes.iter() {
                    if let Some(entid) = self.get_entid(&pair.0) {
                        if let Some(existing) = vocabulary.find(entid) {
                            if *existing == pair.1 {
                                // Same. Phew.
                                continue;
                            } else {
                                // We have two vocabularies with the same name, same version, and
                                // different definitions for an attribute. That's a coding error.
                                // We can't accept this vocabulary.
                                bail!(ErrorKind::ConflictingAttributeDefinitions(
                                          definition.name.to_string(),
                                          definition.version,
                                          pair.0.to_string(),
                                          existing.clone(),
                                          pair.1.clone())
                                );
                            }
                        }
                    }
                    // It's missing. Collect it.
                    missing.push(pair);
                }
                if missing.is_empty() {
                    Ok(VocabularyCheck::Present)
                } else {
                    Ok(VocabularyCheck::PresentButMissingAttributes { attributes: missing })
                }
            } else if vocabulary.version < definition.version {
                // Ours is newer. Upgrade.
                Ok(VocabularyCheck::PresentButNeedsUpdate { older_version: vocabulary })
            } else {
                // The vocabulary in the store is newer. We are outdated.
                Ok(VocabularyCheck::PresentButTooNew { newer_version: vocabulary })
            }
        } else {
            // The vocabulary isn't present in the store. Install it.
            Ok(VocabularyCheck::NotPresent)
        }
    }

    fn ensure_vocabulary(&mut self, definition: &Definition) -> Result<VocabularyOutcome> {
        match self.check_vocabulary(definition)? {
            VocabularyCheck::Present => Ok(VocabularyOutcome::Existed),
            VocabularyCheck::NotPresent => self.install_vocabulary(definition),
            VocabularyCheck::PresentButNeedsUpdate { older_version } => self.upgrade_vocabulary(definition, older_version),
            VocabularyCheck::PresentButMissingAttributes { attributes } => self.install_attributes_for(definition, attributes),
            VocabularyCheck::PresentButTooNew { newer_version } => Err(ErrorKind::ExistingVocabularyTooNew(definition.name.to_string(), newer_version.version, definition.version).into()),
        }
    }
}

impl<'a, 'c> VocabularyMechanics for InProgress<'a, 'c> {
    /// Turn the vocabulary into datoms, transact them, and on success return the outcome.
    fn install_vocabulary(&mut self, definition: &Definition) -> Result<VocabularyOutcome> {
        let (terms, tempids) = definition.description(self)?;
        self.transact_terms(terms, tempids)?;
        Ok(VocabularyOutcome::Installed)
    }

    fn install_attributes_for<'definition>(&mut self, definition: &'definition Definition, attributes: Vec<&'definition (NamespacedKeyword, Attribute)>) -> Result<VocabularyOutcome> {
        let (terms, tempids) = definition.description_for_attributes(&attributes, self)?;
        self.transact_terms(terms, tempids)?;
        Ok(VocabularyOutcome::InstalledMissingAttributes)
    }

    /// Turn the declarative parts of the vocabulary into alterations. Run the 'pre' steps.
    /// Transact the changes. Run the 'post' steps. Return the result and the new `InProgress`!
    fn upgrade_vocabulary(&mut self, _definition: &Definition, _from_version: Vocabulary) -> Result<VocabularyOutcome> {
        unimplemented!();
        // TODO
        // Ok(VocabularyOutcome::Installed)
    }
}

impl<T> HasVocabularies for T where T: HasSchema + Queryable {
    fn read_vocabulary_named(&self, name: &NamespacedKeyword) -> Result<Option<Vocabulary>> {
        if let Some(entid) = self.get_entid(name) {
            match self.lookup_value_for_attribute(entid, &DB_SCHEMA_VERSION)? {
                None => Ok(None),
                Some(TypedValue::Long(version))
                    if version > 0 && (version < u32::max_value() as i64) => {
                        let version = version as u32;
                        let attributes = self.lookup_values_for_attribute(entid, &DB_SCHEMA_ATTRIBUTE)?
                                             .into_iter()
                                             .filter_map(|a| {
                                                 if let TypedValue::Ref(a) = a {
                                                     self.attribute_for_entid(a)
                                                         .cloned()
                                                         .map(|attr| (a, attr))
                                                 } else {
                                                     None
                                                 }
                                             })
                                             .collect();
                        Ok(Some(Vocabulary {
                            entity: entid.into(),
                            version: version,
                            attributes: attributes,
                        }))
                    },
                Some(_) => bail!(ErrorKind::InvalidVocabularyVersion),
            }
        } else {
            Ok(None)
        }
    }

    fn read_vocabularies(&self) -> Result<Vocabularies> {
        // This would be way easier with pull expressions. #110.
        let versions: BTreeMap<Entid, u32> =
            self.q_once(r#"[:find ?vocab ?version
                            :where [?vocab :db.schema/version ?version]]"#, None)
                .into_rel_result()?
                .into_iter()
                .filter_map(|v|
                    match (&v[0], &v[1]) {
                        (&TypedValue::Ref(vocab), &TypedValue::Long(version))
                        if version > 0 && (version < u32::max_value() as i64) => Some((vocab, version as u32)),
                        (_, _) => None,
                    })
                .collect();

        let mut attributes = BTreeMap::<Entid, Vec<(Entid, Attribute)>>::new();
        let pairs =
            self.q_once("[:find ?vocab ?attr :where [?vocab :db.schema/attribute ?attr]]", None)
                .into_rel_result()?
                .into_iter()
                .filter_map(|v| {
                    match (&v[0], &v[1]) {
                        (&TypedValue::Ref(vocab), &TypedValue::Ref(attr)) => Some((vocab, attr)),
                        (_, _) => None,
                    }
                    });

        // TODO: validate that attributes.keys is a subset of versions.keys.
        for (vocab, attr) in pairs {
            if let Some(attribute) = self.attribute_for_entid(attr).cloned() {
                attributes.entry(vocab).or_insert(Vec::new()).push((attr, attribute));
            }
        }

        // TODO: return more errors?

        // We walk versions first in order to support vocabularies with no attributes.
        Ok(Vocabularies(versions.into_iter().filter_map(|(vocab, version)| {
            // Get the name.
            self.get_ident(vocab).cloned()
                .map(|name| {
                    let attrs = attributes.remove(&vocab).unwrap_or(vec![]);
                    (name.clone(), Vocabulary {
                        entity: vocab,
                        version: version,
                        attributes: attrs,
                    })
                })
        }).collect()))
    }
}

#[cfg(test)]
mod tests {
    use ::{
        NamespacedKeyword,
        Conn,
        new_connection,
    };

    use super::HasVocabularies;

    #[test]
    fn test_read_vocabularies() {
        let mut sqlite = new_connection("").expect("could open conn");
        let mut conn = Conn::connect(&mut sqlite).expect("could open store");
        let vocabularies = conn.begin_read(&mut sqlite).expect("in progress")
                               .read_vocabularies().expect("OK");
        assert_eq!(vocabularies.len(), 1);
        let core = vocabularies.get(&NamespacedKeyword::new("db.schema", "core")).expect("exists");
        assert_eq!(core.version, 1);
    }

    #[test]
    fn test_core_schema() {
        let mut c = new_connection("").expect("could open conn");
        let mut conn = Conn::connect(&mut c).expect("could open store");
        let in_progress = conn.begin_transaction(&mut c).expect("in progress");
        let vocab = in_progress.read_vocabularies().expect("vocabulary");
        assert_eq!(1, vocab.len());
        assert_eq!(1, vocab.get(&NamespacedKeyword::new("db.schema", "core")).expect("core vocab").version);
    }
}
