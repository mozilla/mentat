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

extern crate mentat_core;

pub use self::mentat_core::{
    Entid,
    ValueType,
    TypedValue,
    Attribute,
    AttributeBitFlags,
    Schema,
};

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
