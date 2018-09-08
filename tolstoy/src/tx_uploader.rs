// Copyright 2018 Mozilla
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

use std::collections::HashMap;

use uuid::Uuid;

use core_traits::{
    Entid,
};

use mentat_db::{
    PartitionMap,
    V1_PARTS,
};

use public_traits::errors::{
    Result,
};

use tx_processor::{
    TxReceiver,
};

use types::{
    TxPart,
    GlobalTransactionLog,
};

use logger::d;

pub struct UploaderReport {
    pub temp_uuids: HashMap<Entid, Uuid>,
    pub head: Option<Uuid>,
}

pub(crate) struct TxUploader<'c> {
    tx_temp_uuids: HashMap<Entid, Uuid>,
    remote_client: &'c mut GlobalTransactionLog,
    remote_head: &'c Uuid,
    rolling_temp_head: Option<Uuid>,
    local_partitions: PartitionMap,
}

impl<'c> TxUploader<'c> {
    pub fn new(client: &'c mut GlobalTransactionLog, remote_head: &'c Uuid, local_partitions: PartitionMap) -> TxUploader<'c> {
        TxUploader {
            tx_temp_uuids: HashMap::new(),
            remote_client: client,
            remote_head: remote_head,
            rolling_temp_head: None,
            local_partitions: local_partitions,
        }
    }
}

/// Given a set of entids and a partition map, returns a new PartitionMap that would result from
/// expanding the partitions to fit the entids.
fn allocate_partition_map_for_entids<T>(entids: T, local_partitions: &PartitionMap) -> PartitionMap
where T: Iterator<Item=Entid> {
    let mut parts = HashMap::new();
    for name in V1_PARTS.iter().map(|&(ref part, ..)| part.to_string()) {
        // This shouldn't fail: locally-sourced partitions must be present within with V1_PARTS.
        let p = local_partitions.get(&name).unwrap();
        parts.insert(name, (p, p.clone()));
    }

    // For a given partition, set its index to one greater than the largest encountered entid within its partition space.
    for entid in entids {
        for (p, new_p) in parts.values_mut() {
            if p.allows_entid(entid) && entid >= new_p.next_entid() {
                new_p.set_next_entid(entid + 1);
            }
        }
    }

    let mut m = PartitionMap::default();
    for (name, (_, new_p)) in parts {
        m.insert(name, new_p);
    }
    m
}

impl<'c> TxReceiver<UploaderReport> for TxUploader<'c> {
    fn tx<T>(&mut self, tx_id: Entid, datoms: &mut T) -> Result<()>
    where T: Iterator<Item=TxPart> {
        // Yes, we generate a new UUID for a given Tx, even if we might
        // already have one mapped locally. Pre-existing local mapping will
        // be replaced if this sync succeeds entirely.
        // If we're seeing this tx again, it implies that previous attempt
        // to sync didn't update our local head. Something went wrong last time,
        // and it's unwise to try to re-use these remote tx mappings.
        // We just leave garbage txs to be GC'd on the server.
        let tx_uuid = Uuid::new_v4();
        self.tx_temp_uuids.insert(tx_id, tx_uuid);
        let mut tx_chunks = vec![];

        // TODO separate bits of network work should be combined into single 'future'

        let mut datoms: Vec<TxPart> = datoms.collect();

        // TODO this should live within a transaction, once server support is in place.
        // For now, we're uploading the PartitionMap in transaction's first chunk.
        datoms[0].partitions = Some(allocate_partition_map_for_entids(datoms.iter().map(|d| d.e), &self.local_partitions));

        // Upload all chunks.
        for datom in &datoms {
            let datom_uuid = Uuid::new_v4();
            tx_chunks.push(datom_uuid);
            d(&format!("putting chunk: {:?}, {:?}", &datom_uuid, &datom));
            // TODO switch over to CBOR once we're past debugging stuff.
            // See https://github.com/mozilla/mentat/issues/570
            // let cbor_val = serde_cbor::to_value(&datom)?;
            // self.remote_client.put_chunk(&datom_uuid, &serde_cbor::ser::to_vec_sd(&cbor_val)?)?;
            self.remote_client.put_chunk(&datom_uuid, &datom)?;
        }

        // Upload tx.
        // NB: At this point, we may choose to update remote & local heads.
        // Depending on how much we're uploading, and how unreliable our connection
        // is, this might be a good thing to do to ensure we make at least some progress.
        // Comes at a cost of possibly increasing racing against other clients.
        let tx_parent = match self.rolling_temp_head {
            Some(p) => p,
            None => *self.remote_head,
        };
        d(&format!("putting transaction: {:?}, {:?}, {:?}", &tx_uuid, &tx_parent, &tx_chunks));
        self.remote_client.put_transaction(&tx_uuid, &tx_parent, &tx_chunks)?;

        d(&format!("updating rolling head: {:?}", tx_uuid));
        self.rolling_temp_head = Some(tx_uuid.clone());

        Ok(())
    }

    fn done(self) -> UploaderReport {
        UploaderReport {
            temp_uuids: self.tx_temp_uuids,
            head: self.rolling_temp_head,
        }
    }
}

#[cfg(test)]
pub mod tests {
    use super::*;

    use mentat_db::{
        Partition,
        V1_PARTS,
    };

    use schema::{
        PARTITION_USER,
        PARTITION_TX,
        PARTITION_DB,
    };

    fn bootstrap_partition_map() -> PartitionMap {
        V1_PARTS.iter()
            .map(|&(ref part, start, end, index, allow_excision)| (part.to_string(), Partition::new(start, end, index, allow_excision)))
            .collect()
    }

    #[test]
    fn test_allocate_partition_map_for_entids() {
        let bootstrap_map = bootstrap_partition_map();

        // Empty list of entids should not allocate any space in partitions.
        let entids: Vec<Entid> = vec![];
        let no_op_map = allocate_partition_map_for_entids(entids.into_iter(), &bootstrap_map);
        assert_eq!(bootstrap_map, no_op_map);

        // Only user partition.
        let entids = vec![65536];
        let new_map = allocate_partition_map_for_entids(entids.into_iter(), &bootstrap_map);
        assert_eq!(65537, new_map.get(PARTITION_USER).unwrap().next_entid());
        // Other partitions are untouched.
        assert_eq!(41, new_map.get(PARTITION_DB).unwrap().next_entid());
        assert_eq!(268435456, new_map.get(PARTITION_TX).unwrap().next_entid());

        // Only tx partition.
        let entids = vec![268435666];
        let new_map = allocate_partition_map_for_entids(entids.into_iter(), &bootstrap_map);
        assert_eq!(268435667, new_map.get(PARTITION_TX).unwrap().next_entid());
        // Other partitions are untouched.
        assert_eq!(65536, new_map.get(PARTITION_USER).unwrap().next_entid());
        assert_eq!(41, new_map.get(PARTITION_DB).unwrap().next_entid());

        // Only DB partition.
        let entids = vec![41];
        let new_map = allocate_partition_map_for_entids(entids.into_iter(), &bootstrap_map);
        assert_eq!(42, new_map.get(PARTITION_DB).unwrap().next_entid());
        // Other partitions are untouched.
        assert_eq!(65536, new_map.get(PARTITION_USER).unwrap().next_entid());
        assert_eq!(268435456, new_map.get(PARTITION_TX).unwrap().next_entid());

        // User and tx partitions.
        let entids = vec![65537, 268435456];
        let new_map = allocate_partition_map_for_entids(entids.into_iter(), &bootstrap_map);
        assert_eq!(65538, new_map.get(PARTITION_USER).unwrap().next_entid());
        assert_eq!(268435457, new_map.get(PARTITION_TX).unwrap().next_entid());
        // DB partition is untouched.
        assert_eq!(41, new_map.get(PARTITION_DB).unwrap().next_entid());

        // DB, user and tx partitions.
        let entids = vec![41, 65666, 268435457];
        let new_map = allocate_partition_map_for_entids(entids.into_iter(), &bootstrap_map);
        assert_eq!(65667, new_map.get(PARTITION_USER).unwrap().next_entid());
        assert_eq!(268435458, new_map.get(PARTITION_TX).unwrap().next_entid());
        assert_eq!(42, new_map.get(PARTITION_DB).unwrap().next_entid());
    }
}
