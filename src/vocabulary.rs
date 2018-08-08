// Copyright 2016 Mozilla
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.


//! This module exposes an interface for programmatic management of vocabularies.
//!
//! A vocabulary is defined by a name, a version number, and a collection of attribute definitions.
//!
//! Operations on vocabularies can include migrations between versions. These are defined
//! programmatically as a pair of functions, `pre` and `post`, that are invoked prior to
//! an upgrade.
//!
//! A Mentat store exposes, via the `HasSchema` trait, operations to read
//! vocabularies by name or in bulk.
//!
//! An in-progress transaction (`InProgress`) further exposes a trait,
//! `VersionedStore`, which allows for a vocabulary definition to be
//! checked for existence in the store, and transacted if needed.
//!
//! Typical use is the following:
//!
//! ```
//! #[macro_use(kw)]
//! extern crate mentat;
//!
//! use mentat::{
//!     Store,
//!     ValueType,
//! };
//!
//! use mentat::vocabulary;
//! use mentat::vocabulary::{
//!     Definition,
//!     HasVocabularies,
//!     VersionedStore,
//!     VocabularyOutcome,
//! };
//!
//! fn main() {
//!     let mut store = Store::open("").expect("connected");
//!
//!     {
//!         // Read the list of installed vocabularies.
//!         let reader = store.begin_read().expect("began read");
//!         let vocabularies = reader.read_vocabularies().expect("read");
//!         for (name, vocabulary) in vocabularies.iter() {
//!             println!("Vocab {} is at version {}.", name, vocabulary.version);
//!             for &(ref name, ref attr) in vocabulary.attributes().iter() {
//!                 println!("  >> {} ({})", name, attr.value_type);
//!             }
//!         }
//!     }
//!
//!     {
//!         let mut in_progress = store.begin_transaction().expect("began transaction");
//!
//!         // Make sure the core vocabulary exists.
//!         in_progress.verify_core_schema().expect("verified");
//!
//!         // Make sure our vocabulary is installed, and install if necessary.
//!         in_progress.ensure_vocabulary(&Definition {
//!             name: kw!(:example/links),
//!             version: 1,
//!             attributes: vec![
//!                 (kw!(:link/title),
//!                  vocabulary::AttributeBuilder::helpful()
//!                    .value_type(ValueType::String)
//!                    .multival(false)
//!                    .fulltext(true)
//!                    .build()),
//!             ],
//!             pre: Definition::no_op,
//!             post: Definition::no_op,
//!         }).expect("ensured");
//!
//!         // Now we can do stuff.
//!         in_progress.transact("[{:link/title \"Title\"}]").expect("transacts");
//!         in_progress.commit().expect("commits");
//!     }
//! }
//! ```
//!
//! A similar approach is taken using the
//! [VocabularyProvider](mentat::vocabulary::VocabularyProvider) trait to handle migrations across
//! multiple vocabularies.

use std::collections::BTreeMap;

use core_traits::{
    KnownEntid,
};

use core_traits::attribute::{
    Unique,
};

use ::{
    CORE_SCHEMA_VERSION,
    Attribute,
    Entid,
    HasSchema,
    IntoResult,
    Keyword,
    Binding,
    TypedValue,
    ValueType,
};

use ::conn::{
    InProgress,
    Queryable,
};

use ::errors::{
    MentatError,
    Result,
};

use ::entity_builder::{
    BuildTerms,
    TermBuilder,
    Terms,
};

/// AttributeBuilder is how you build vocabulary definitions to apply to a store.
pub use mentat_db::AttributeBuilder;

pub type Version = u32;
pub type Datom = (Entid, Entid, TypedValue);

/// A definition of an attribute that is independent of a particular store.
///
/// `Attribute` instances not only aren't named, but don't even have entids.
///
/// We need two kinds of structure: an abstract definition of a vocabulary in terms of names,
/// and a concrete instance of a vocabulary in a particular store.
///
/// `Definition` is the former, and `Vocabulary` is the latter.
///
/// Note that, because it's possible to 'flesh out' a vocabulary with attributes without bumping
/// its version number, we need to track the attributes that the application cares about — it's
/// not enough to know the name and version. Indeed, we even care about the details of each attribute,
/// because that's how we'll detect errors.
///
/// `Definition` includes two additional fields: functions to run if this vocabulary is being
/// upgraded. `pre` and `post` are run before and after the definition is transacted against the
/// store. Each is called with the existing `Vocabulary` instance so that they can do version
/// checks or employ more fine-grained logic.
#[derive(Clone)]
pub struct Definition {
    pub name: Keyword,
    pub version: Version,
    pub attributes: Vec<(Keyword, Attribute)>,
    pub pre: fn(&mut InProgress, &Vocabulary) -> Result<()>,
    pub post: fn(&mut InProgress, &Vocabulary) -> Result<()>,
}

/// ```
/// #[macro_use(kw)]
/// extern crate mentat;
///
/// use mentat::{
///     HasSchema,
///     IntoResult,
///     Queryable,
///     Store,
///     TypedValue,
///     ValueType,
/// };
///
/// use mentat::entity_builder::{
///     BuildTerms,
///     TermBuilder,
/// };
///
/// use mentat::vocabulary;
/// use mentat::vocabulary::{
///     AttributeBuilder,
///     Definition,
///     HasVocabularies,
///     VersionedStore,
/// };
///
/// fn main() {
///     let mut store = Store::open("").expect("connected");
///     let mut in_progress = store.begin_transaction().expect("began transaction");
///
///     // Make sure the core vocabulary exists.
///     in_progress.verify_core_schema().expect("verified");
///
///     // Make sure our vocabulary is installed, and install if necessary.
///     in_progress.ensure_vocabulary(&Definition {
///         name: kw!(:example/links),
///         version: 2,
///         attributes: vec![
///             (kw!(:link/title),
///              AttributeBuilder::helpful()
///                .value_type(ValueType::String)
///                .multival(false)
///                .fulltext(true)
///                .build()),
///         ],
///         pre: |ip, from| {
///             // Version one allowed multiple titles; version two
///             // doesn't. Retract any duplicates we find.
///             if from.version < 2 {
///                 let link_title = ip.get_entid(&kw!(:link/title)).unwrap();
///
///                 let results = ip.q_once(r#"
///                     [:find ?e ?t2
///                      :where [?e :link/title ?t1]
///                             [?e :link/title ?t2]
///                             [(unpermute ?t1 ?t2)]]
///                 "#, None).into_rel_result()?;
///
///                 if !results.is_empty() {
///                     let mut builder = TermBuilder::new();
///                     for row in results.into_iter() {
///                         let mut r = row.into_iter();
///                         let e = r.next().and_then(|e| e.into_known_entid()).expect("entity");
///                         let obsolete = r.next().expect("value").into_scalar().expect("typed value");
///                         builder.retract(e, link_title, obsolete)?;
///                     }
///                     ip.transact_builder(builder)?;
///                 }
///             }
///             Ok(())
///         },
///         post: |_ip, from| {
///             println!("We migrated :example/links from version {}", from.version);
///             Ok(())
///         },
///     }).expect("ensured");
///
///     // Now we can do stuff.
///     in_progress.transact("[{:link/title \"Title\"}]").expect("transacts");
///     in_progress.commit().expect("commits");
/// }
/// ```
impl Definition {
    pub fn no_op(_ip: &mut InProgress, _from: &Vocabulary) -> Result<()> {
        Ok(())
    }

    pub fn new<N, A>(name: N, version: Version, attributes: A) -> Definition
    where N: Into<Keyword>,
          A: Into<Vec<(Keyword, Attribute)>> {
        Definition {
            name: name.into(),
            version: version,
            attributes: attributes.into(),
            pre: Definition::no_op,
            post: Definition::no_op,
        }
    }

    /// Called with an in-progress transaction and the previous vocabulary version
    /// if the definition's version is later than that of the vocabulary in the store.
    fn pre(&self, ip: &mut InProgress, from: &Vocabulary) -> Result<()> {
        (self.pre)(ip, from)
    }

    /// Called with an in-progress transaction and the previous vocabulary version
    /// if the definition's version is later than that of the vocabulary in the store.
    fn post(&self, ip: &mut InProgress, from: &Vocabulary) -> Result<()> {
        (self.post)(ip, from)
    }
}

/// A definition of a vocabulary as retrieved from a particular store.
///
/// A `Vocabulary` is just like `Definition`, but concrete: its name and attributes are identified
/// by `Entid`, not `Keyword`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Vocabulary {
    pub entity: Entid,
    pub version: Version,
    attributes: Vec<(Entid, Attribute)>,
}

impl Vocabulary {
    pub fn attributes(&self) -> &Vec<(Entid, Attribute)> {
        &self.attributes
    }
}

/// A collection of named `Vocabulary` instances, as retrieved from the store.
#[derive(Debug, Default, Clone)]
pub struct Vocabularies(pub BTreeMap<Keyword, Vocabulary>);   // N.B., this has a copy of the attributes in Schema!

impl Vocabularies {
    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn get(&self, name: &Keyword) -> Option<&Vocabulary> {
        self.0.get(name)
    }

    pub fn iter(&self) -> ::std::collections::btree_map::Iter<Keyword, Vocabulary> {
        self.0.iter()
    }
}

lazy_static! {
    static ref DB_SCHEMA_CORE: Keyword = {
        kw!(:db.schema/core)
    };
    static ref DB_SCHEMA_ATTRIBUTE: Keyword = {
        kw!(:db.schema/attribute)
    };
    static ref DB_SCHEMA_VERSION: Keyword = {
        kw!(:db.schema/version)
    };
    static ref DB_IDENT: Keyword = {
        kw!(:db/ident)
    };
    static ref DB_UNIQUE: Keyword = {
        kw!(:db/unique)
    };
    static ref DB_UNIQUE_VALUE: Keyword = {
        kw!(:db.unique/value)
    };
    static ref DB_UNIQUE_IDENTITY: Keyword = {
        kw!(:db.unique/identity)
    };
    static ref DB_IS_COMPONENT: Keyword = {
        Keyword::namespaced("db", "isComponent")
    };
    static ref DB_VALUE_TYPE: Keyword = {
        Keyword::namespaced("db", "valueType")
    };
    static ref DB_INDEX: Keyword = {
        kw!(:db/index)
    };
    static ref DB_FULLTEXT: Keyword = {
        kw!(:db/fulltext)
    };
    static ref DB_CARDINALITY: Keyword = {
        kw!(:db/cardinality)
    };
    static ref DB_CARDINALITY_ONE: Keyword = {
        kw!(:db.cardinality/one)
    };
    static ref DB_CARDINALITY_MANY: Keyword = {
        kw!(:db.cardinality/many)
    };

    static ref DB_NO_HISTORY: Keyword = {
        Keyword::namespaced("db", "noHistory")
    };
}

trait HasCoreSchema {
    /// Return the entity ID for a type. On failure, return `MissingCoreVocabulary`.
    fn core_type(&self, t: ValueType) -> Result<KnownEntid>;

    /// Return the entity ID for an ident. On failure, return `MissingCoreVocabulary`.
    fn core_entid(&self, ident: &Keyword) -> Result<KnownEntid>;

    /// Return the entity ID for an attribute's keyword. On failure, return
    /// `MissingCoreVocabulary`.
    fn core_attribute(&self, ident: &Keyword) -> Result<KnownEntid>;
}

impl<T> HasCoreSchema for T where T: HasSchema {
    fn core_type(&self, t: ValueType) -> Result<KnownEntid> {
        self.entid_for_type(t)
            .ok_or_else(|| MentatError::MissingCoreVocabulary(DB_SCHEMA_VERSION.clone()).into())
    }

    fn core_entid(&self, ident: &Keyword) -> Result<KnownEntid> {
        self.get_entid(ident)
            .ok_or_else(|| MentatError::MissingCoreVocabulary(DB_SCHEMA_VERSION.clone()).into())
    }

    fn core_attribute(&self, ident: &Keyword) -> Result<KnownEntid> {
        self.attribute_for_ident(ident)
            .ok_or_else(|| MentatError::MissingCoreVocabulary(DB_SCHEMA_VERSION.clone()).into())
            .map(|(_, e)| e)
    }
}

impl Definition {
    fn description_for_attributes<'s, T, R>(&'s self, attributes: &[R], via: &T, diff: Option<BTreeMap<Keyword, Attribute>>) -> Result<Terms>
     where T: HasCoreSchema,
           R: ::std::borrow::Borrow<(Keyword, Attribute)> {

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

        let a_no_history = via.core_attribute(&DB_NO_HISTORY)?;

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
            let &(ref kw, ref attr) = r.borrow();

            let tempid = builder.named_tempid(kw.to_string());
            let name: TypedValue = kw.clone().into();
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

            // These are all unconditional because we use attribute descriptions to _alter_, not
            // just to _add_, and so absence is distinct from negation!
            builder.add(tempid.clone(), a_index, TypedValue::Boolean(attr.index))?;
            builder.add(tempid.clone(), a_fulltext, TypedValue::Boolean(attr.fulltext))?;
            builder.add(tempid.clone(), a_is_component, TypedValue::Boolean(attr.component))?;
            builder.add(tempid.clone(), a_no_history, TypedValue::Boolean(attr.no_history))?;

            if let Some(u) = attr.unique {
                let uu = match u {
                    Unique::Identity => v_unique_identity,
                    Unique::Value => v_unique_value,
                };
                builder.add(tempid.clone(), a_unique, uu)?;
            } else {
                 let existing_unique =
                    if let Some(ref diff) = diff {
                        diff.get(kw).and_then(|a| a.unique)
                    } else {
                        None
                    };
                 match existing_unique {
                    None => {
                        // Nothing to do.
                    },
                    Some(Unique::Identity) => {
                        builder.retract(tempid.clone(), a_unique, v_unique_identity.clone())?;
                    },
                    Some(Unique::Value) => {
                        builder.retract(tempid.clone(), a_unique, v_unique_value.clone())?;
                    },
                 }
            }
        }

        builder.build()
    }

    /// Return a sequence of terms that describes this vocabulary definition and its attributes.
    fn description_diff<T>(&self, via: &T, from: &Vocabulary) -> Result<Terms> where T: HasSchema {
        let relevant = self.attributes.iter()
                           .filter_map(|&(ref keyword, _)|
                               // Look up the keyword to see if it's currently in use.
                               via.get_entid(keyword)

                               // If so, map it to the existing attribute.
                                  .and_then(|e| from.find(e).cloned())

                               // Collect enough that we can do lookups.
                                  .map(|e| (keyword.clone(), e)))
                           .collect();
        self.description_for_attributes(self.attributes.as_slice(), via, Some(relevant))
    }

    /// Return a sequence of terms that describes this vocabulary definition and its attributes.
    fn description<T>(&self, via: &T) -> Result<Terms> where T: HasSchema {
        self.description_for_attributes(self.attributes.as_slice(), via, None)
    }
}

/// This enum captures the various relationships between a particular vocabulary pair — one
/// `Definition` and one `Vocabulary`, if present.
#[derive(Debug, Eq, PartialEq)]
pub enum VocabularyCheck<'definition> {
    /// The provided definition is not already present in the store.
    NotPresent,

    /// The provided definition is present in the store, and all of its attributes exist.
    Present,

    /// The provided definition is present in the store with an earlier version number.
    PresentButNeedsUpdate { older_version: Vocabulary },

    /// The provided definition is present in the store with a more recent version number.
    PresentButTooNew { newer_version: Vocabulary },

    /// The provided definition is present in the store, but some of its attributes are not.
    PresentButMissingAttributes { attributes: Vec<&'definition (Keyword, Attribute)> },
}

/// This enum captures the outcome of attempting to ensure that a vocabulary definition is present
/// and up-to-date in the store.
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

/// This trait captures the ability to retrieve and describe stored vocabularies.
pub trait HasVocabularies {
    fn read_vocabularies(&self) -> Result<Vocabularies>;
    fn read_vocabulary_named(&self, name: &Keyword) -> Result<Option<Vocabulary>>;
}

/// This trait captures the ability of a store to check and install/upgrade vocabularies.
pub trait VersionedStore: HasVocabularies + HasSchema {
    /// Check whether the vocabulary described by the provided metadata is present in the store.
    fn check_vocabulary<'definition>(&self, definition: &'definition Definition) -> Result<VocabularyCheck<'definition>> {
        if let Some(vocabulary) = self.read_vocabulary_named(&definition.name)? {
            // The name is present.
            // Check the version.
            if vocabulary.version == definition.version {
                // Same version. Check that all of our attributes are present.
                let mut missing: Vec<&'definition (Keyword, Attribute)> = vec![];
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
                                bail!(MentatError::ConflictingAttributeDefinitions(
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

    /// Check whether the provided vocabulary is present in the store. If it isn't, make it so.
    fn ensure_vocabulary(&mut self, definition: &Definition) -> Result<VocabularyOutcome>;

    /// Check whether the provided vocabularies are present in the store at the correct
    /// version and with all defined attributes. If any are not, invoke the `pre`
    /// function on the provided `VocabularySource`, install or upgrade the necessary vocabularies,
    /// then invoke `post`. Returns `Ok` if all of these steps succeed.
    ///
    /// Use this function instead of calling `ensure_vocabulary` if you need to have pre/post
    /// functions invoked when vocabulary changes are necessary.
    fn ensure_vocabularies(&mut self, vocabularies: &mut VocabularySource) -> Result<BTreeMap<Keyword, VocabularyOutcome>>;

    /// Make sure that our expectations of the core vocabulary — basic types and attributes — are met.
    fn verify_core_schema(&self) -> Result<()> {
        if let Some(core) = self.read_vocabulary_named(&DB_SCHEMA_CORE)? {
            if core.version != CORE_SCHEMA_VERSION {
                bail!(MentatError::UnexpectedCoreSchema(CORE_SCHEMA_VERSION, Some(core.version)));
            }

            // TODO: check things other than the version.
        } else {
            // This would be seriously messed up.
            bail!(MentatError::UnexpectedCoreSchema(CORE_SCHEMA_VERSION, None));
        }
        Ok(())
    }
}

/// `VocabularyStatus` is passed to `pre` function when attempting to add or upgrade vocabularies
/// via `ensure_vocabularies`. This is how you can find the status and versions of existing
/// vocabularies — you can retrieve the requested definition and the resulting `VocabularyCheck`
/// by name.
pub trait VocabularyStatus {
    fn get(&self, name: &Keyword) -> Option<(&Definition, &VocabularyCheck)>;
    fn version(&self, name: &Keyword) -> Option<Version>;
}

#[derive(Default)]
struct CheckedVocabularies<'a> {
    items: BTreeMap<Keyword, (&'a Definition, VocabularyCheck<'a>)>,
}

impl<'a> CheckedVocabularies<'a> {
    fn add(&mut self, definition: &'a Definition, check: VocabularyCheck<'a>) {
        self.items.insert(definition.name.clone(), (definition, check));
    }

    fn is_empty(&self) -> bool {
        self.items.is_empty()
    }
}

impl<'a> VocabularyStatus for CheckedVocabularies<'a> {
    fn get(&self, name: &Keyword) -> Option<(&Definition, &VocabularyCheck)> {
        self.items.get(name).map(|&(ref d, ref c)| (*d, c))
    }

    fn version(&self, name: &Keyword) -> Option<Version> {
        self.items.get(name).map(|&(d, _)| d.version)
    }
}

trait VocabularyMechanics {
    fn install_vocabulary(&mut self, definition: &Definition) -> Result<VocabularyOutcome>;
    fn install_attributes_for<'definition>(&mut self, definition: &'definition Definition, attributes: Vec<&'definition (Keyword, Attribute)>) -> Result<VocabularyOutcome>;
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
    fn ensure_vocabulary(&mut self, definition: &Definition) -> Result<VocabularyOutcome> {
        match self.check_vocabulary(definition)? {
            VocabularyCheck::Present => Ok(VocabularyOutcome::Existed),
            VocabularyCheck::NotPresent => self.install_vocabulary(definition),
            VocabularyCheck::PresentButNeedsUpdate { older_version } => self.upgrade_vocabulary(definition, older_version),
            VocabularyCheck::PresentButMissingAttributes { attributes } => self.install_attributes_for(definition, attributes),
            VocabularyCheck::PresentButTooNew { newer_version } => Err(MentatError::ExistingVocabularyTooNew(definition.name.to_string(), newer_version.version, definition.version).into()),
        }
    }

    fn ensure_vocabularies(&mut self, vocabularies: &mut VocabularySource) -> Result<BTreeMap<Keyword, VocabularyOutcome>> {
        let definitions = vocabularies.definitions();

        let mut update  = Vec::new();
        let mut missing = Vec::new();
        let mut out = BTreeMap::new();

        let mut work = CheckedVocabularies::default();

        for definition in definitions.iter() {
            match self.check_vocabulary(definition)? {
                VocabularyCheck::Present => {
                    out.insert(definition.name.clone(), VocabularyOutcome::Existed);
                },
                VocabularyCheck::PresentButTooNew { newer_version } => {
                    bail!(MentatError::ExistingVocabularyTooNew(definition.name.to_string(), newer_version.version, definition.version));
                },

                c @ VocabularyCheck::NotPresent |
                c @ VocabularyCheck::PresentButNeedsUpdate { older_version: _ } |
                c @ VocabularyCheck::PresentButMissingAttributes { attributes: _ } => {
                    work.add(definition, c);
                },
            }
        }

        if work.is_empty() {
            return Ok(out);
        }

        // If any work needs to be done, run pre/post.
        vocabularies.pre(self, &work)?;

        for (name, (definition, check)) in work.items.into_iter() {
            match check {
                VocabularyCheck::NotPresent => {
                    // Install it directly.
                    out.insert(name, self.install_vocabulary(definition)?);
                },
                VocabularyCheck::PresentButNeedsUpdate { older_version } => {
                    // Save this: we'll do it later.
                    update.push((definition, older_version));
                },
                VocabularyCheck::PresentButMissingAttributes { attributes } => {
                    // Save this: we'll do it later.
                    missing.push((definition, attributes));
                },
                VocabularyCheck::Present |
                VocabularyCheck::PresentButTooNew { newer_version: _ } => {
                    unreachable!();
                }
            }
        }

        for (d, v) in update {
            out.insert(d.name.clone(), self.upgrade_vocabulary(d, v)?);
        }
        for (d, a) in missing {
            out.insert(d.name.clone(), self.install_attributes_for(d, a)?);
        }

        vocabularies.post(self)?;
        Ok(out)
    }
}

/// Implement `VocabularySource` to have full programmatic control over how a set of `Definition`s
/// are checked against and transacted into a store.
pub trait VocabularySource {
    /// Called to obtain the list of `Definition`s to install. This will be called before `pre`.
    fn definitions(&mut self) -> Vec<Definition>;

    /// Called before the supplied `Definition`s are transacted. Do not commit the `InProgress`.
    /// If this function returns `Err`, the entire vocabulary operation will fail.
    fn pre(&mut self, _in_progress: &mut InProgress, _checks: &VocabularyStatus) -> Result<()> {
        Ok(())
    }

    /// Called after the supplied `Definition`s are transacted. Do not commit the `InProgress`.
    /// If this function returns `Err`, the entire vocabulary operation will fail.
    fn post(&mut self, _in_progress: &mut InProgress) -> Result<()> {
        Ok(())
    }
}

/// A convenience struct to package simple `pre` and `post` functions with a collection of
/// vocabulary `Definition`s.
pub struct SimpleVocabularySource {
    pub definitions: Vec<Definition>,
    pub pre: Option<fn(&mut InProgress) -> Result<()>>,
    pub post: Option<fn(&mut InProgress) -> Result<()>>,
}

impl SimpleVocabularySource {
    pub fn new(definitions: Vec<Definition>,
               pre: Option<fn(&mut InProgress) -> Result<()>>,
               post: Option<fn(&mut InProgress) -> Result<()>>) -> SimpleVocabularySource {
        SimpleVocabularySource {
            pre: pre,
            post: post,
            definitions: definitions,
        }
    }

    pub fn with_definitions(definitions: Vec<Definition>) -> SimpleVocabularySource {
        Self::new(definitions, None, None)
    }
}

impl VocabularySource for SimpleVocabularySource {
    fn pre(&mut self, in_progress: &mut InProgress, _checks: &VocabularyStatus) -> Result<()> {
        self.pre.map(|pre| (pre)(in_progress)).unwrap_or(Ok(()))
    }

    fn post(&mut self, in_progress: &mut InProgress) -> Result<()> {
        self.post.map(|pre| (pre)(in_progress)).unwrap_or(Ok(()))
    }

    fn definitions(&mut self) -> Vec<Definition> {
        self.definitions.clone()
    }
}

impl<'a, 'c> VocabularyMechanics for InProgress<'a, 'c> {
    /// Turn the vocabulary into datoms, transact them, and on success return the outcome.
    fn install_vocabulary(&mut self, definition: &Definition) -> Result<VocabularyOutcome> {
        let (terms, _tempids) = definition.description(self)?;
        self.transact_entities(terms)?;
        Ok(VocabularyOutcome::Installed)
    }

    fn install_attributes_for<'definition>(&mut self, definition: &'definition Definition, attributes: Vec<&'definition (Keyword, Attribute)>) -> Result<VocabularyOutcome> {
        let (terms, _tempids) = definition.description_for_attributes(&attributes, self, None)?;
        self.transact_entities(terms)?;
        Ok(VocabularyOutcome::InstalledMissingAttributes)
    }

    /// Turn the declarative parts of the vocabulary into alterations. Run the 'pre' steps.
    /// Transact the changes. Run the 'post' steps. Return the result and the new `InProgress`!
    fn upgrade_vocabulary(&mut self, definition: &Definition, from_version: Vocabulary) -> Result<VocabularyOutcome> {
        // It's sufficient for us to generate the datom form of each attribute and transact that.
        // We trust that the vocabulary will implement a 'pre' function that cleans up data for any
        // failable conversion (e.g., cardinality-many to cardinality-one).

        definition.pre(self, &from_version)?;

        // TODO: don't do work for attributes that are unchanged. Here we rely on the transactor
        // to elide duplicate datoms.
        let (terms, _tempids) = definition.description_diff(self, &from_version)?;
        self.transact_entities(terms)?;

        definition.post(self, &from_version)?;
        Ok(VocabularyOutcome::Upgraded)
    }
}

impl<T> HasVocabularies for T where T: HasSchema + Queryable {
    fn read_vocabulary_named(&self, name: &Keyword) -> Result<Option<Vocabulary>> {
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
                Some(_) => bail!(MentatError::InvalidVocabularyVersion),
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
                        (&Binding::Scalar(TypedValue::Ref(vocab)),
                         &Binding::Scalar(TypedValue::Long(version)))
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
                        (&Binding::Scalar(TypedValue::Ref(vocab)),
                         &Binding::Scalar(TypedValue::Ref(attr))) => Some((vocab, attr)),
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
        Store,
    };

    use super::{
        HasVocabularies,
    };

    #[test]
    fn test_read_vocabularies() {
        let mut store = Store::open("").expect("opened");
        let vocabularies = store.begin_read().expect("in progress")
                                .read_vocabularies().expect("OK");
        assert_eq!(vocabularies.len(), 1);
        let core = vocabularies.get(&kw!(:db.schema/core)).expect("exists");
        assert_eq!(core.version, 1);
    }

    #[test]
    fn test_core_schema() {
        let mut store = Store::open("").expect("opened");
        let in_progress = store.begin_transaction().expect("in progress");
        let vocab = in_progress.read_vocabularies().expect("vocabulary");
        assert_eq!(1, vocab.len());
        assert_eq!(1, vocab.get(&kw!(:db.schema/core)).expect("core vocab").version);
    }
}
