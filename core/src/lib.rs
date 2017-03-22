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
extern crate ordered_float;

use std::collections::BTreeMap;
use self::ordered_float::OrderedFloat;
use self::edn::NamespacedKeyword;
use self::edn::types::Value;

use std::io::{self, Write};

/// Core types defining a Mentat knowledge base.

/// Represents one entid in the entid space.
///
/// Per https://www.sqlite.org/datatype3.html (see also http://stackoverflow.com/a/8499544), SQLite
/// stores signed integers up to 64 bits in size.  Since u32 is not appropriate for our use case, we
/// use i64 rather than manually truncating u64 to u63 and casting to i64 throughout the codebase.
pub type Entid = i64;

/// The attribute of each Mentat assertion has a :db/valueType constraining the value to a
/// particular set.  Mentat recognizes the following :db/valueType values.
#[derive(Clone,Copy,Debug,Eq,Hash,Ord,PartialOrd,PartialEq)]
pub enum ValueType {
    Ref,
    Boolean,
    Instant,
    Long,
    Double,
    String,
    Keyword,
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
    String(String),
    Keyword(NamespacedKeyword),
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
    assert!(!TypedValue::String("foo".to_string()).is_congruent_with(ValueType::Boolean));
    assert!(TypedValue::String("foo".to_string()).is_congruent_with(ValueType::String));
    assert!(TypedValue::String("foo".to_string()).is_congruent_with(None));
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

    pub fn as_edn_value(&self) -> edn::Value {
        let mut s = "[ ".to_string(); 
        for (entid, attribute) in &self.schema_map {
            let ident = self.get_ident(entid.clone()).unwrap();
            s.push_str("{");
            s.push_str(&format!("\t:db/id     :{:?}", entid));
            s.push_str(&format!("\n\t:db/ident     :{}/{}", ident.namespace, ident.name));
            let value_type = format!("{:?}", attribute.value_type).to_lowercase();
            s.push_str(&format!("\n\t:db/valueType :db.type/{}", value_type));
            s.push_str("\n\t:db/cardinality :db.cardinality/");

            if attribute.multival {
                s.push_str("many");
            } else {
                s.push_str("one");
            }

            if attribute.unique == Some(attribute::Unique::Value) {
                s.push_str("\n\t:db/unique :db.unique/identity");
            }

            if attribute.index {
                s.push_str("\n\t:db/index true");
            }
            if attribute.fulltext {
                s.push_str("\n\t:db/fulltext true");
            }
            if attribute.component {
                s.push_str("\n\t:db/isComponent true");
            }
            s.push_str(" },");  
        }
        s.push_str(" ]");
        writeln!(&mut io::stderr(), "{}", s).unwrap();    
        edn::parse::value(&s).unwrap().without_spans()
    }
}

#[cfg(test)]
mod test {
    use super::*;

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
            value_type: ValueType::Boolean,
            fulltext: true,
            unique: Some(attribute::Unique::Value),
            multival: false,
            component: false,
        };
        associate_ident(&mut schema, NamespacedKeyword::new("foo", "bas"), 98);
        add_attribute(&mut schema, 98, attr2);

        let attr3 = Attribute {
            index: false,
            value_type: ValueType::Boolean,
            fulltext: true,
            unique: Some(attribute::Unique::Identity),
            multival: false,
            component: false,
        };

        associate_ident(&mut schema, NamespacedKeyword::new("foo", "bat"), 99);
        add_attribute(&mut schema, 99, attr3);

        let value = schema.as_edn_value();
    }

    fn associate_ident(schema: &mut Schema, i: NamespacedKeyword, e: Entid) {
        schema.entid_map.insert(e, i.clone());
        schema.ident_map.insert(i.clone(), e);
    }

    fn add_attribute(schema: &mut Schema, e: Entid, a: Attribute) {
        schema.schema_map.insert(e, a);
    }

}

pub mod intern_set;
