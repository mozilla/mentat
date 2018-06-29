// Copyright 2018 Mozilla
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

use uuid::Uuid;

use rusqlite;

use conn::{
    Conn,
    InProgress,
};

use errors::{
    Result,
};

use mentat_core::{
    KnownEntid,
};
use mentat_db::{
    renumber,
    PartitionMap,
};

use entity_builder::{
    BuildTerms,
    TermBuilder,
};

use mentat_tolstoy::{
    Syncer,
    SyncMetadataClient,
    SyncResult,
    Tx,
    TxMapper,
    TolstoyError,
};

pub trait Syncable {
    fn sync(&mut self, server_uri: &String, user_uuid: &String) -> ::std::result::Result<(), ::errors::MentatError>;
}

fn fast_forward_local<'a, 'c>(in_progress: &mut InProgress<'a, 'c>, txs: Vec<Tx>) -> Result<Option<PartitionMap>> {
    let mut last_tx = None;

    // During fast-forwarding, we will insert datoms with known entids
    // which, by definition, fall outside of our user partition.
    // Once we've done with insertion, we need to ensure that user
    // partition's next allocation will not overlap with just-inserted datoms.
    // To allow for "holes" in the user partition (due to data excision),
    // we track the highest incoming entid we saw, and expand our
    // local partition to match.
    // In absence of excision and implementation bugs, this should work
    // just as if we counted number of incoming entids and expanded by
    // that number instead.
    let mut last_encountered_partition_map = None;

    for tx in txs {
        let mut builder = TermBuilder::new();

        last_encountered_partition_map = match tx.parts[0].partitions.clone() {
            Some(parts) => Some(parts),
            None => bail!(TolstoyError::BadServerState("Missing partition map in incoming transaction".to_string()))
        };

        for part in tx.parts {
            if part.added {
                builder.add(KnownEntid(part.e), KnownEntid(part.a), part.v)?;
            } else {
                builder.retract(KnownEntid(part.e), KnownEntid(part.a), part.v)?;
            }
        }

        let report = in_progress.transact_builder(builder)?;

        last_tx = Some((report.tx_id, tx.tx.clone()));
    }

    // We've just transacted a new tx, and generated a new tx entid.  Map it to the corresponding
    // incoming tx uuid, advance our "locally known remote head".
    if let Some((entid, uuid)) = last_tx {
        SyncMetadataClient::set_remote_head(&mut in_progress.transaction, &uuid)?;
        TxMapper::set_tx_uuid(&mut in_progress.transaction, entid, &uuid)?;
    }

    Ok(last_encountered_partition_map)
}

impl Conn {
    pub(crate) fn sync(&mut self,
                       sqlite: &mut rusqlite::Connection,
                       server_uri: &String, user_uuid: &String) -> ::std::result::Result<(), ::errors::MentatError> {
        let uuid = Uuid::parse_str(&user_uuid)?;

        // Take an IMMEDIATE transaction right away.  We have an SQL transaction, and complete
        // control over the `Conn` metadata at this point, just like `transact()`.
        let mut in_progress = self.begin_transaction(sqlite)?;

        let sync_result = Syncer::flow(&mut in_progress.transaction, server_uri, &uuid)?;
        let mut incoming_partition = None;

        match sync_result {
            SyncResult::EmptyServer => (),
            SyncResult::NoChanges => (),
            SyncResult::ServerFastForward => (),
            SyncResult::Merge => bail!(TolstoyError::NotYetImplemented(
                format!("Can't sync against diverged local.")
            )),
            SyncResult::LocalFastForward(txs) => {
                incoming_partition = fast_forward_local(&mut in_progress, txs)?;
                ()
            },
            SyncResult::BadServerState => bail!(TolstoyError::NotYetImplemented(
                format!("Bad server state.")
            )),
            SyncResult::AdoptedRemoteOnFirstSync => (),
            SyncResult::IncompatibleBootstrapSchema => bail!(TolstoyError::NotYetImplemented(
                format!("IncompatibleBootstrapSchema.")
            )),
        }

        match incoming_partition {
            Some(incoming) => {
                let root = SyncMetadataClient::get_partitions(&in_progress.transaction, true)?;
                let current = SyncMetadataClient::get_partitions(&in_progress.transaction, false)?;
                let updated_db = renumber(&in_progress.transaction, &root, &current, &incoming)?;
                in_progress.partition_map = updated_db.partition_map;
                ()
            },
            None => ()
        }

        in_progress.commit()
    }
}
