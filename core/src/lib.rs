// Copyright 2016 Mozilla
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

extern crate enum_set;

#[macro_use]
extern crate lazy_static;
extern crate ordered_float;

extern crate edn;

pub mod values;

use std::collections::BTreeMap;
use std::fmt;
use std::rc::Rc;

use enum_set::EnumSet;

use self::ordered_float::OrderedFloat;
use self::edn::NamespacedKeyword;

/// Core types defining a Mentat knowledge base.

/// Represents one entid in the entid space.
///
/// Per https://www.sqlite.org/datatype3.html (see also http://stackoverflow.com/a/8499544), SQLite
/// stores signed integers up to 64 bits in size.  Since u32 is not appropriate for our use case, we
/// use i64 rather than manually truncating u64 to u63 and casting to i64 throughout the codebase.
pub type Entid = i64;

/// The attribute of each Mentat assertion has a :db/valueType constraining the value to a
/// particular set.  Mentat recognizes the following :db/valueType values.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialOrd, PartialEq)]
#[repr(u32)]
pub enum ValueType {
    Ref,
    Boolean,
    Instant,
    Long,
    Double,
    String,
    Keyword,
}

impl ValueType {
    pub fn all_enums() -> EnumSet<ValueType> {
        // TODO: lazy_static.
        let mut s = EnumSet::new();
        s.insert(ValueType::Ref);
        s.insert(ValueType::Boolean);
        s.insert(ValueType::Instant);
        s.insert(ValueType::Long);
        s.insert(ValueType::Double);
        s.insert(ValueType::String);
        s.insert(ValueType::Keyword);
        s
    }
}


impl enum_set::CLike for ValueType {
    fn to_u32(&self) -> u32 {
        *self as u32
    }

    unsafe fn from_u32(v: u32) -> ValueType {
        std::mem::transmute(v)
    }
}

impl ValueType {
    pub fn to_edn_value(self) -> edn::Value {
        match self {
            ValueType::Ref => values::DB_TYPE_REF.clone(),
            ValueType::Boolean => values::DB_TYPE_BOOLEAN.clone(),
            ValueType::Instant => values::DB_TYPE_INSTANT.clone(),
            ValueType::Long => values::DB_TYPE_LONG.clone(),
            ValueType::Double => values::DB_TYPE_DOUBLE.clone(),
            ValueType::String => values::DB_TYPE_STRING.clone(),
            ValueType::Keyword => values::DB_TYPE_KEYWORD.clone(),
        }
    }
}

impl fmt::Display for ValueType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", match *self {
            ValueType::Ref =>     ":db.type/ref",
            ValueType::Boolean => ":db.type/boolean",
            ValueType::Instant => ":db.type/instant",
            ValueType::Long =>    ":db.type/long",
            ValueType::Double =>  ":db.type/double",
            ValueType::String =>  ":db.type/string",
            ValueType::Keyword => ":db.type/keyword",
        })
    }
}

/// Represents a Mentat value in a particular value set.
// TODO: expand to include :db.type/{instant,url,uuid}.
// TODO: BigInt?
#[derive(Clone,Debug,Eq,Hash,Ord,PartialOrd,PartialEq)]
pub enum TypedValue {
    Ref(Entid),
    Boolean(bool),
    Long(i64),
    Double(OrderedFloat<f64>),
    // TODO: &str throughout?
    String(Rc<String>),
    Keyword(Rc<NamespacedKeyword>),
}

impl TypedValue {
    /// Returns true if the provided type is `Some` and matches this value's type, or if the
    /// provided type is `None`.
    #[inline]
    pub fn is_congruent_with<T: Into<Option<ValueType>>>(&self, t: T) -> bool {
        t.into().map_or(true, |x| self.matches_type(x))
    }

    #[inline]
    pub fn matches_type(&self, t: ValueType) -> bool {
        self.value_type() == t
    }

    pub fn value_type(&self) -> ValueType {
        match self {
            &TypedValue::Ref(_) => ValueType::Ref,
            &TypedValue::Boolean(_) => ValueType::Boolean,
            &TypedValue::Long(_) => ValueType::Long,
            &TypedValue::Double(_) => ValueType::Double,
            &TypedValue::String(_) => ValueType::String,
            &TypedValue::Keyword(_) => ValueType::Keyword,
        }
    }

    /// Construct a new `TypedValue::Keyword` instance by cloning the provided
    /// values and wrapping them in a new `Rc`. This is expensive, so this might
    /// be best limited to tests.
    pub fn typed_ns_keyword(ns: &str, name: &str) -> TypedValue {
        TypedValue::Keyword(Rc::new(NamespacedKeyword::new(ns, name)))
    }

    /// Construct a new `TypedValue::String` instance by cloning the provided
    /// value and wrapping it in a new `Rc`. This is expensive, so this might
    /// be best limited to tests.
    pub fn typed_string(s: &str) -> TypedValue {
        TypedValue::String(Rc::new(s.to_string()))
    }
}

// Put this here rather than in `db` simply because it's widely needed.
pub trait SQLValueType {
    fn value_type_tag(&self) -> i32;
    fn accommodates_integer(&self, int: i64) -> bool;
}

impl SQLValueType for ValueType {
    fn value_type_tag(&self) -> i32 {
        match *self {
            ValueType::Ref =>      0,
            ValueType::Boolean =>  1,
            ValueType::Instant =>  4,
            // SQLite distinguishes integral from decimal types, allowing long and double to share a tag.
            ValueType::Long =>     5,
            ValueType::Double =>   5,
            ValueType::String =>  10,
            ValueType::Keyword => 13,
        }
    }

    /// Returns true if the provided integer is in the SQLite value space of this type. For
    /// example, `1` is how we encode `true`.
    ///
    /// ```
    /// use mentat_core::{ValueType, SQLValueType};
    /// assert!(ValueType::Boolean.accommodates_integer(1));
    /// assert!(!ValueType::Boolean.accommodates_integer(-1));
    /// assert!(!ValueType::Boolean.accommodates_integer(10));
    /// assert!(!ValueType::String.accommodates_integer(10));
    /// ```
    fn accommodates_integer(&self, int: i64) -> bool {
        use ValueType::*;
        match *self {
            Instant | Long | Double => true,
            Ref                     => int >= 0,
            Boolean                 => (int == 0) || (int == 1),
            ValueType::String       => false,
            Keyword                 => false,
        }
    }
}

#[test]
fn test_typed_value() {
    assert!(TypedValue::Boolean(false).is_congruent_with(None));
    assert!(TypedValue::Boolean(false).is_congruent_with(ValueType::Boolean));
    assert!(!TypedValue::typed_string("foo").is_congruent_with(ValueType::Boolean));
    assert!(TypedValue::typed_string("foo").is_congruent_with(ValueType::String));
    assert!(TypedValue::typed_string("foo").is_congruent_with(None));
}

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
    #[derive(Clone,Debug,Eq,Hash,Ord,PartialOrd,PartialEq)]
    pub enum Unique {
        Value,
        Identity,
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

        attribute_map.insert(values::DB_VALUE_TYPE.clone(), self.value_type.to_edn_value());

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
        }
    }
}

/// Map `NamespacedKeyword` idents (`:db/ident`) to positive integer entids (`1`).
pub type IdentMap = BTreeMap<NamespacedKeyword, Entid>;

/// Map positive integer entids (`1`) to `NamespacedKeyword` idents (`:db/ident`).
pub type EntidMap = BTreeMap<Entid, NamespacedKeyword>;

/// Map attribute entids to `Attribute` instances.
pub type SchemaMap = BTreeMap<Entid, Attribute>;

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
    pub schema_map: SchemaMap,
}

impl Schema {
    pub fn get_ident(&self, x: Entid) -> Option<&NamespacedKeyword> {
        self.entid_map.get(&x)
    }

    pub fn get_entid(&self, x: &NamespacedKeyword) -> Option<Entid> {
        self.ident_map.get(x).map(|x| *x)
    }

    pub fn attribute_for_entid(&self, x: Entid) -> Option<&Attribute> {
        self.schema_map.get(&x)
    }

    pub fn attribute_for_ident(&self, ident: &NamespacedKeyword) -> Option<&Attribute> {
        self.get_entid(&ident)
            .and_then(|x| self.attribute_for_entid(x))
    }

    /// Return true if the provided entid identifies an attribute in this schema.
    pub fn is_attribute(&self, x: Entid) -> bool {
        self.schema_map.contains_key(&x)
    }

    /// Return true if the provided ident identifies an attribute in this schema.
    pub fn identifies_attribute(&self, x: &NamespacedKeyword) -> bool {
        self.get_entid(x).map(|e| self.is_attribute(e)).unwrap_or(false)
    }

    /// Returns an symbolic representation of the schema suitable for applying across Mentat stores.
    pub fn to_edn_value(&self) -> edn::Value {
        edn::Value::Vector((&self.schema_map).iter()
            .map(|(entid, attribute)| 
                attribute.to_edn_value(self.get_ident(*entid).cloned()))
            .collect())
    }
}

#[cfg(test)]
mod test {
    use super::*;

    fn associate_ident(schema: &mut Schema, i: NamespacedKeyword, e: Entid) {
        schema.entid_map.insert(e, i.clone());
        schema.ident_map.insert(i, e);
    }

    fn add_attribute(schema: &mut Schema, e: Entid, a: Attribute) {
        schema.schema_map.insert(e, a);
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
        };

        assert!(attr3.flags() & AttributeBitFlags::IndexAVET as u8 == 0);
        assert!(attr3.flags() & AttributeBitFlags::IndexVAET as u8 == 0);
        assert!(attr3.flags() & AttributeBitFlags::IndexFulltext as u8 != 0);
        assert!(attr3.flags() & AttributeBitFlags::UniqueValue as u8 != 0);
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
        };

        associate_ident(&mut schema, NamespacedKeyword::new("foo", "bat"), 99);
        add_attribute(&mut schema, 99, attr3);

        let value = schema.to_edn_value();

        let expected_output = r#"[ {   :db/ident     :foo/bar
    :db/valueType :db.type/ref
    :db/cardinality :db.cardinality/one
    :db/index true },
{   :db/ident     :foo/bas
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/many
    :db/unique :db.unique/value
    :db/fulltext true },
{   :db/ident     :foo/bat
    :db/valueType :db.type/boolean
    :db/cardinality :db.cardinality/one
    :db/unique :db.unique/identity
    :db/component true }, ]"#;
        let expected_value = edn::parse::value(&expected_output).expect("to be able to parse").without_spans();
        assert_eq!(expected_value, value);

        // let's compare the whole thing again, just to make sure we are not changing anything when we convert to edn.
        let value2 = schema.to_edn_value();
        assert_eq!(expected_value, value2);
    }
}

pub mod intern_set;
pub mod counter;
pub mod util;
