/// Connection metadata required to query from, or apply transactions to, a Mentat store.
///
/// Owned data for the volatile parts (generation and partition map), and `Arc` for the infrequently
/// changing parts (schema) that we want to share across threads.
///
/// See https://github.com/mozilla/mentat/wiki/Thoughts:-modeling-db-conn-in-Rust.

use std::sync::{
    Arc,
};

use mentat_core::{
    Schema,
};

use mentat_db::{
    PartitionMap,
};

use mentat_db::cache::{
    SQLiteAttributeCache,
};

pub struct Metadata {
    pub generation: u64,
    pub partition_map: PartitionMap,
    pub schema: Arc<Schema>,
    pub attribute_cache: SQLiteAttributeCache,
}

impl Metadata {
    // Intentionally not public.
    pub fn new(generation: u64, partition_map: PartitionMap, schema: Arc<Schema>, cache: SQLiteAttributeCache) -> Metadata {
        Metadata {
            generation: generation,
            partition_map: partition_map,
            schema: schema,
            attribute_cache: cache,
        }
    }
}
