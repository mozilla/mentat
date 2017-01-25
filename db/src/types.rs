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

use std::collections::{BTreeMap};

use ordered_float::{OrderedFloat};

/// Core types defining a Mentat knowledge base.
///
/// At its core, Mentat maintains a set of assertions of the form entity-attribute-value (EAV).  The
/// assertions conform to a schema whereby the given attribute constrains the associated value/set
/// of associated values.
///
/// ## Assertions
///
/// Mentat assertions are represented as rows in the `datoms` SQLite table, and each Mentat row
/// representing an assertion is with a numeric representation of :db/valueType.
///
/// The tag is used to limit queries, and therefore is placed carefully in the relevant indices to
/// allow searching numeric longs and doubles quickly.  The tag is also used to convert SQLite
/// values to the correct Mentat value type on query egress.
///
/// ## Entities and entids
///
/// A Mentat entity is represented by a *positive* integer.  (This agrees with Datomic.)  We call
/// such a positive integer an *entid*.
///
/// ## Partitions
///
/// Datomic partitions the entid space in order to separate core knowledge base entities required
/// for the healthy function of the system from user-defined entities.  Datomic also partitions in
/// order to ensure that certain index walks of related entities are efficient.  Mentat follows
/// suit, partitioning into the following partitions:
/// * `:db.part/db`, for core knowledge base entities;
/// * `:db.part/user`, for user-defined entities;
/// * `:db.part/tx`, for transaction entities.
/// You almost certainly want to add new entities in the `:db.part/user` partition.
///
/// The entid sequence in a given partition is monotonically increasing, although not necessarily
/// contiguous.  That is, it is possible for a specific entid to have never been present in the
/// system, even though its predecessor and successor are present.

/// Represents one entid in the entid space.
///
/// Per https://www.sqlite.org/datatype3.html (see also http://stackoverflow.com/a/8499544), SQLite
/// stores signed integers up to 64 bits in size.  Since u32 is not appropriate for our use case, we
/// use i64 rather than manually truncating u64 to u63 and casting to i64 throughout the codebase.
pub type Entid = i64;

/// The attribute of each Mentat assertion has a :db/valueType constraining the value to a
/// particular set.  Mentat recognizes the following :db/valueType values.
#[derive(Clone,Debug,Eq,Hash,Ord,PartialOrd,PartialEq)]
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
#[derive(Clone,Debug,Eq,Hash,Ord,PartialOrd,PartialEq)]
pub enum TypedValue {
    Ref(Entid),
    Boolean(bool),
    Long(i64),
    Double(OrderedFloat<f64>),
    // TODO: &str throughout?
    String(String),
    Keyword(String),
}

impl TypedValue {
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

/// Represents one partition of the entid space.
#[derive(Clone,Debug,Eq,Hash,Ord,PartialOrd,PartialEq)]
pub struct Partition {
    /// The first entid in the partition.
    pub start: i64,
    /// The next entid to be allocated in the partition.
    pub index: i64,
}

impl Partition {
    pub fn new(start: i64, next: i64) -> Partition {
        assert!(start <= next, "A partition represents a monotonic increasing sequence of entids.");
        Partition { start: start, index: next }
    }
}

/// Map partition names to `Partition` instances.
pub type PartitionMap = BTreeMap<String, Partition>;

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

    /// `true` if this attribute is unique-value, i.e., it is `:db/unique :db.unique/value`.
    ///
    /// *Unique-value* means that there is at most one assertion with the attribute and a
    /// particular value in the datom store.
    pub unique_value: bool,

    /// `true` if this attribute is unique-identity, i.e., it is `:db/unique :db.unique/identity`.
    ///
    /// Unique-identity attributes always have value type `Ref`.
    ///
    /// *Unique-identity* means that the attribute is *unique-value* and that they can be used in
    /// lookup-refs and will automatically upsert where appropriate.
    pub unique_identity: bool,

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

impl Default for Attribute {
    fn default() -> Attribute {
        Attribute {
            // There's no particular reason to favour one value type, so Ref it is.
            value_type: ValueType::Ref,
            fulltext: false,
            index: false,
            multival: false,
            unique_value: false,
            unique_identity: false,
            component: false,
        }
    }
}

/// Map `String` idents (`:db/ident`) to positive integer entids (`1`).
pub type IdentMap = BTreeMap<String, Entid>;

/// Map positive integer entids (`1`) to `String` idents (`:db/ident`).
pub type EntidMap = BTreeMap<Entid, String>;

/// Map attribute entids to `Attribute` instances.
pub type SchemaMap = BTreeMap<i64, Attribute>;

/// Represents a Mentat schema.
///
/// Maintains the mapping between string idents and positive integer entids; and exposes the schema
/// flags associated to a given entid (equivalently, ident).
///
/// TODO: consider a single bi-directional map instead of separate ident->entid and entid->ident
/// maps.
#[derive(Clone,Debug,Default,Eq,Hash,Ord,PartialOrd,PartialEq)]
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

/// Represents the metadata required to query from, or apply transactions to, a Mentat store.
///
/// See https://github.com/mozilla/mentat/wiki/Thoughts:-modeling-db-conn-in-Rust.
#[derive(Clone,Debug,Default,Eq,Hash,Ord,PartialOrd,PartialEq)]
pub struct DB {
    /// Map partition name->`Partition`.
    ///
    /// TODO: represent partitions as entids.
    pub partition_map: PartitionMap,

    /// The schema of the store.
    pub schema: Schema,
}

impl DB {
    pub fn new(partition_map: PartitionMap, schema: Schema) -> DB {
        DB {
            partition_map: partition_map,
            schema: schema
        }
    }
}
