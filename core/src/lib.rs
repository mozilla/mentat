// Copyright 2016 Mozilla
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

extern crate chrono;
extern crate enum_set;
extern crate indexmap;
extern crate ordered_float;
extern crate uuid;
extern crate serde;

#[macro_use]
extern crate lazy_static;

#[macro_use]
extern crate serde_derive;

extern crate edn;

pub mod values;
mod cache;

use std::collections::{
    BTreeMap,
};

pub use uuid::Uuid;

pub use chrono::{
    DateTime,
    Timelike,       // For truncation.
};

pub use edn::{
    FromMicros,
    NamespacedKeyword,
    ToMicros,
    Utc,
};

pub use cache::{
    CachedAttributes,
    UpdateableCache,
};

/// Core types defining a Mentat knowledge base.
mod types;
mod value_type_set;
mod sql_types;

pub use types::{
    Binding,
    Cloned,
    Entid,
    FromRc,
    KnownEntid,
    StructuredMap,
    TypedValue,
    ValueType,
    ValueTypeTag,
    ValueRc,
    now,
};

pub use value_type_set::{
    ValueTypeSet,
};

pub use sql_types::{
    SQLTypeAffinity,
    SQLValueType,
    SQLValueTypeSet,
};

/// Bit flags used in `flags0` column in temporary tables created during search,
/// such as the `search_results`, `inexact_searches` and `exact_searches` tables.
/// When moving to a more concrete table, such as `datoms`, they are expanded out
/// via these flags and put into their own column rather than a bit field.
pub enum AttributeBitFlags {
    IndexAVET     = 1 << 0,
    IndexVAET     = 1 << 1,
    IndexFulltext = 1 << 2,
    UniqueValue   = 1 << 3,
}

pub mod attribute {
    use TypedValue;

    #[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialOrd, PartialEq)]
    pub enum Unique {
        Value,
        Identity,
    }

    impl Unique {
        // This is easier than rejigging DB_UNIQUE_VALUE to not be EDN.
        pub fn into_typed_value(self) -> TypedValue {
            match self {
                Unique::Value => TypedValue::typed_ns_keyword("db.unique", "value"),
                Unique::Identity => TypedValue::typed_ns_keyword("db.unique", "identity"),
            }
        }
    }
}

/// A Mentat schema attribute has a value type and several other flags determining how assertions
/// with the attribute are interpreted.
///
/// TODO: consider packing this into a bitfield or similar.
#[derive(Clone,Debug,Eq,Hash,Ord,PartialOrd,PartialEq)]
pub struct Attribute {
    /// The associated value type, i.e., `:db/valueType`?
    pub value_type: ValueType,

    /// `true` if this attribute is multi-valued, i.e., it is `:db/cardinality
    /// :db.cardinality/many`.  `false` if this attribute is single-valued (the default), i.e., it
    /// is `:db/cardinality :db.cardinality/one`.
    pub multival: bool,

    /// `None` if this attribute is neither unique-value nor unique-identity.
    ///
    /// `Some(attribute::Unique::Value)` if this attribute is unique-value, i.e., it is `:db/unique
    /// :db.unique/value`.
    ///
    /// *Unique-value* means that there is at most one assertion with the attribute and a
    /// particular value in the datom store.  Unique-value attributes can be used in lookup-refs.
    ///
    /// `Some(attribute::Unique::Identity)` if this attribute is unique-identity, i.e., it is `:db/unique
    /// :db.unique/identity`.
    ///
    /// Unique-identity attributes always have value type `Ref`.
    ///
    /// *Unique-identity* means that the attribute is *unique-value* and that they can be used in
    /// lookup-refs and will automatically upsert where appropriate.
    pub unique: Option<attribute::Unique>,

    /// `true` if this attribute is automatically indexed, i.e., it is `:db/indexing true`.
    pub index: bool,

    /// `true` if this attribute is automatically fulltext indexed, i.e., it is `:db/fulltext true`.
    ///
    /// Fulltext attributes always have string values.
    pub fulltext: bool,

    /// `true` if this attribute is a component, i.e., it is `:db/isComponent true`.
    ///
    /// Component attributes always have value type `Ref`.
    ///
    /// They are used to compose entities from component sub-entities: they are fetched recursively
    /// by pull expressions, and they are automatically recursively deleted where appropriate.
    pub component: bool,

    /// `true` if this attribute doesn't require history to be kept, i.e., it is `:db/noHistory true`.
    pub no_history: bool,
}

impl Attribute {
    /// Combine several attribute flags into a bitfield used in temporary search tables.
    pub fn flags(&self) -> u8 {
        let mut flags: u8 = 0;

        if self.index {
            flags |= AttributeBitFlags::IndexAVET as u8;
        }
        if self.value_type == ValueType::Ref {
            flags |= AttributeBitFlags::IndexVAET as u8;
        }
        if self.fulltext {
            flags |= AttributeBitFlags::IndexFulltext as u8;
        }
        if self.unique.is_some() {
            flags |= AttributeBitFlags::UniqueValue as u8;
        }
        flags
    }

    pub fn to_edn_value(&self, ident: Option<NamespacedKeyword>) -> edn::Value {
        let mut attribute_map: BTreeMap<edn::Value, edn::Value> = BTreeMap::default();
        if let Some(ident) = ident {
            attribute_map.insert(values::DB_IDENT.clone(), edn::Value::NamespacedKeyword(ident));
        }

        attribute_map.insert(values::DB_VALUE_TYPE.clone(), self.value_type.into_edn_value());

        attribute_map.insert(values::DB_CARDINALITY.clone(), if self.multival { values::DB_CARDINALITY_MANY.clone() } else { values::DB_CARDINALITY_ONE.clone() });

        match self.unique {
            Some(attribute::Unique::Value) => { attribute_map.insert(values::DB_UNIQUE.clone(), values::DB_UNIQUE_VALUE.clone()); },
            Some(attribute::Unique::Identity) => { attribute_map.insert(values::DB_UNIQUE.clone(), values::DB_UNIQUE_IDENTITY.clone()); },
            None => (),
        }

        if self.index {
            attribute_map.insert(values::DB_INDEX.clone(), edn::Value::Boolean(true));
        }

        if self.fulltext {
            attribute_map.insert(values::DB_FULLTEXT.clone(), edn::Value::Boolean(true));
        }

        if self.component {
            attribute_map.insert(values::DB_IS_COMPONENT.clone(), edn::Value::Boolean(true));
        }

        if self.no_history {
            attribute_map.insert(values::DB_NO_HISTORY.clone(), edn::Value::Boolean(true));
        }

        edn::Value::Map(attribute_map)
    }
}

impl Default for Attribute {
    fn default() -> Attribute {
        Attribute {
            // There's no particular reason to favour one value type, so Ref it is.
            value_type: ValueType::Ref,
            fulltext: false,
            index: false,
            multival: false,
            unique: None,
            component: false,
            no_history: false,
        }
    }
}

/// Map `NamespacedKeyword` idents (`:db/ident`) to positive integer entids (`1`).
pub type IdentMap = BTreeMap<NamespacedKeyword, Entid>;

/// Map positive integer entids (`1`) to `NamespacedKeyword` idents (`:db/ident`).
pub type EntidMap = BTreeMap<Entid, NamespacedKeyword>;

/// Map attribute entids to `Attribute` instances.
pub type AttributeMap = BTreeMap<Entid, Attribute>;

/// Represents a Mentat schema.
///
/// Maintains the mapping between string idents and positive integer entids; and exposes the schema
/// flags associated to a given entid (equivalently, ident).
///
/// TODO: consider a single bi-directional map instead of separate ident->entid and entid->ident
/// maps.
#[derive(Clone, Debug, Default, Eq, Hash, Ord, PartialOrd, PartialEq)]
pub struct Schema {
    /// Map entid->ident.
    ///
    /// Invariant: is the inverse map of `ident_map`.
    pub entid_map: EntidMap,

    /// Map ident->entid.
    ///
    /// Invariant: is the inverse map of `entid_map`.
    pub ident_map: IdentMap,

    /// Map entid->attribute flags.
    ///
    /// Invariant: key-set is the same as the key-set of `entid_map` (equivalently, the value-set of
    /// `ident_map`).
    pub attribute_map: AttributeMap,

    /// Maintain a vec of unique attribute IDs for which the corresponding attribute in `attribute_map`
    /// has `.component == true`.
    pub component_attributes: Vec<Entid>,
}

pub trait HasSchema {
    fn entid_for_type(&self, t: ValueType) -> Option<KnownEntid>;

    fn get_ident<T>(&self, x: T) -> Option<&NamespacedKeyword> where T: Into<Entid>;
    fn get_entid(&self, x: &NamespacedKeyword) -> Option<KnownEntid>;
    fn attribute_for_entid<T>(&self, x: T) -> Option<&Attribute> where T: Into<Entid>;

    // Returns the attribute and the entid named by the provided ident.
    fn attribute_for_ident(&self, ident: &NamespacedKeyword) -> Option<(&Attribute, KnownEntid)>;

    /// Return true if the provided entid identifies an attribute in this schema.
    fn is_attribute<T>(&self, x: T) -> bool where T: Into<Entid>;

    /// Return true if the provided ident identifies an attribute in this schema.
    fn identifies_attribute(&self, x: &NamespacedKeyword) -> bool;

    fn component_attributes(&self) -> &[Entid];
}

impl Schema {
    pub fn new(ident_map: IdentMap, entid_map: EntidMap, attribute_map: AttributeMap) -> Schema {
        let mut s = Schema { ident_map, entid_map, attribute_map, component_attributes: Vec::new() };
        s.update_component_attributes();
        s
    }

    /// Returns an symbolic representation of the schema suitable for applying across Mentat stores.
    pub fn to_edn_value(&self) -> edn::Value {
        edn::Value::Vector((&self.attribute_map).iter()
            .map(|(entid, attribute)|
                attribute.to_edn_value(self.get_ident(*entid).cloned()))
            .collect())
    }

    fn get_raw_entid(&self, x: &NamespacedKeyword) -> Option<Entid> {
        self.ident_map.get(x).map(|x| *x)
    }

    pub fn update_component_attributes(&mut self) {
        let mut components: Vec<Entid>;
        components = self.attribute_map
                         .iter()
                         .filter_map(|(k, v)| if v.component { Some(*k) } else { None })
                         .collect();
        components.sort_unstable();
        self.component_attributes = components;
    }
}

impl HasSchema for Schema {
    fn entid_for_type(&self, t: ValueType) -> Option<KnownEntid> {
        // TODO: this can be made more efficient.
        self.get_entid(&t.into_keyword())
    }

    fn get_ident<T>(&self, x: T) -> Option<&NamespacedKeyword> where T: Into<Entid> {
        self.entid_map.get(&x.into())
    }

    fn get_entid(&self, x: &NamespacedKeyword) -> Option<KnownEntid> {
        self.get_raw_entid(x).map(KnownEntid)
    }

    fn attribute_for_entid<T>(&self, x: T) -> Option<&Attribute> where T: Into<Entid> {
        self.attribute_map.get(&x.into())
    }

    fn attribute_for_ident(&self, ident: &NamespacedKeyword) -> Option<(&Attribute, KnownEntid)> {
        self.get_raw_entid(&ident)
            .and_then(|entid| {
                self.attribute_for_entid(entid).map(|a| (a, KnownEntid(entid)))
            })
    }

    /// Return true if the provided entid identifies an attribute in this schema.
    fn is_attribute<T>(&self, x: T) -> bool where T: Into<Entid> {
        self.attribute_map.contains_key(&x.into())
    }

    /// Return true if the provided ident identifies an attribute in this schema.
    fn identifies_attribute(&self, x: &NamespacedKeyword) -> bool {
        self.get_raw_entid(x).map(|e| self.is_attribute(e)).unwrap_or(false)
    }

    fn component_attributes(&self) -> &[Entid] {
        &self.component_attributes
    }
}

pub mod intern_set;
pub mod counter;
pub mod util;

/// A helper macro to sequentially process an iterable sequence,
/// evaluating a block between each pair of items.
///
/// This is used to simply and efficiently produce output like
///
/// ```sql
///   1, 2, 3
/// ```
///
/// or
///
/// ```sql
/// x = 1 AND y = 2
/// ```
///
/// without producing an intermediate string sequence.
#[macro_export]
macro_rules! interpose {
    ( $name: pat, $across: expr, $body: block, $inter: block ) => {
        interpose_iter!($name, $across.iter(), $body, $inter)
    }
}

/// A helper to bind `name` to values in `across`, running `body` for each value,
/// and running `inter` between each value. See `interpose` for examples.
#[macro_export]
macro_rules! interpose_iter {
    ( $name: pat, $across: expr, $body: block, $inter: block ) => {
        let mut seq = $across;
        if let Some($name) = seq.next() {
            $body;
            for $name in seq {
                $inter;
                $body;
            }
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    use std::str::FromStr;

    fn associate_ident(schema: &mut Schema, i: NamespacedKeyword, e: Entid) {
        schema.entid_map.insert(e, i.clone());
        schema.ident_map.insert(i, e);
    }

    fn add_attribute(schema: &mut Schema, e: Entid, a: Attribute) {
        schema.attribute_map.insert(e, a);
    }

    #[test]
    fn test_attribute_flags() {
        let attr1 = Attribute {
            index: true,
            value_type: ValueType::Ref,
            fulltext: false,
            unique: None,
            multival: false,
            component: false,
            no_history: false,
        };

        assert!(attr1.flags() & AttributeBitFlags::IndexAVET as u8 != 0);
        assert!(attr1.flags() & AttributeBitFlags::IndexVAET as u8 != 0);
        assert!(attr1.flags() & AttributeBitFlags::IndexFulltext as u8 == 0);
        assert!(attr1.flags() & AttributeBitFlags::UniqueValue as u8 == 0);

        let attr2 = Attribute {
            index: false,
            value_type: ValueType::Boolean,
            fulltext: true,
            unique: Some(attribute::Unique::Value),
            multival: false,
            component: false,
            no_history: false,
        };

        assert!(attr2.flags() & AttributeBitFlags::IndexAVET as u8 == 0);
        assert!(attr2.flags() & AttributeBitFlags::IndexVAET as u8 == 0);
        assert!(attr2.flags() & AttributeBitFlags::IndexFulltext as u8 != 0);
        assert!(attr2.flags() & AttributeBitFlags::UniqueValue as u8 != 0);

        let attr3 = Attribute {
            index: false,
            value_type: ValueType::Boolean,
            fulltext: true,
            unique: Some(attribute::Unique::Identity),
            multival: false,
            component: false,
            no_history: false,
        };

        assert!(attr3.flags() & AttributeBitFlags::IndexAVET as u8 == 0);
        assert!(attr3.flags() & AttributeBitFlags::IndexVAET as u8 == 0);
        assert!(attr3.flags() & AttributeBitFlags::IndexFulltext as u8 != 0);
        assert!(attr3.flags() & AttributeBitFlags::UniqueValue as u8 != 0);
    }

    #[test]
    fn test_datetime_truncation() {
        let dt: DateTime<Utc> = DateTime::from_str("2018-01-11T00:34:09.273457004Z").expect("parsed");
        let expected: DateTime<Utc> = DateTime::from_str("2018-01-11T00:34:09.273457Z").expect("parsed");

        let tv: TypedValue = dt.into();
        if let TypedValue::Instant(roundtripped) = tv {
            assert_eq!(roundtripped, expected);
        } else {
            panic!();
        }
    }

    #[test]
    fn test_as_edn_value() {
        let mut schema = Schema::default();

        let attr1 = Attribute {
            index: true,
            value_type: ValueType::Ref,
            fulltext: false,
            unique: None,
            multival: false,
            component: false,
            no_history: true,
        };
        associate_ident(&mut schema, NamespacedKeyword::new("foo", "bar"), 97);
        add_attribute(&mut schema, 97, attr1);

        let attr2 = Attribute {
            index: false,
            value_type: ValueType::String,
            fulltext: true,
            unique: Some(attribute::Unique::Value),
            multival: true,
            component: false,
            no_history: false,
        };
        associate_ident(&mut schema, NamespacedKeyword::new("foo", "bas"), 98);
        add_attribute(&mut schema, 98, attr2);

        let attr3 = Attribute {
            index: false,
            value_type: ValueType::Boolean,
            fulltext: false,
            unique: Some(attribute::Unique::Identity),
            multival: false,
            component: true,
            no_history: false,
        };

        associate_ident(&mut schema, NamespacedKeyword::new("foo", "bat"), 99);
        add_attribute(&mut schema, 99, attr3);

        let value = schema.to_edn_value();

        let expected_output = r#"[ {   :db/ident     :foo/bar
    :db/valueType :db.type/ref
    :db/cardinality :db.cardinality/one
    :db/index true
    :db/noHistory true },
{   :db/ident     :foo/bas
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/many
    :db/unique :db.unique/value
    :db/fulltext true },
{   :db/ident     :foo/bat
    :db/valueType :db.type/boolean
    :db/cardinality :db.cardinality/one
    :db/unique :db.unique/identity
    :db/isComponent true }, ]"#;
        let expected_value = edn::parse::value(&expected_output).expect("to be able to parse").without_spans();
        assert_eq!(expected_value, value);

        // let's compare the whole thing again, just to make sure we are not changing anything when we convert to edn.
        let value2 = schema.to_edn_value();
        assert_eq!(expected_value, value2);
    }
}
