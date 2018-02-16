// Copyright 2016 Mozilla
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

use uuid::Uuid;

use conn::Store;
use errors::{
    Result,
    ErrorKind,
};

use mentat_core::{
    Entid,
    KnownEntid,
};
use mentat_db as db;

use entity_builder::BuildTerms;

use mentat_tolstoy::{
    Syncer,
    SyncMetadataClient,
    TxMapper,
};
use mentat_tolstoy::syncer::{
    Tx,
    SyncResult,
};
use mentat_tolstoy::metadata::HeadTrackable;

pub trait Syncable {
    fn sync(&mut self, server_uri: &String, user_uuid: &String) -> Result<()>;
    fn fast_forward_local(&mut self, txs: Vec<Tx>) -> Result<()>;
}

fn within_user_partition(entid: Entid) -> bool {
    entid >= db::USER0 && entid < db::TX0
}

impl Syncable for Store {
    fn fast_forward_local(&mut self, txs: Vec<Tx>) -> Result<()> {
        let mut last_tx_entid = None;
        let mut last_tx_uuid = None;

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
        let mut largest_endid_encountered = db::USER0;

        for tx in txs {
            let in_progress = self.begin_transaction()?;
            let mut builder = in_progress.builder();
            for part in tx.parts {
                if part.added {
                    builder.add(KnownEntid(part.e), KnownEntid(part.a), part.v.clone())?;
                } else {
                    builder.retract(KnownEntid(part.e), KnownEntid(part.a), part.v.clone())?;
                }
                // Ignore datoms that fall outside of the user partition:
                if within_user_partition(part.e) && part.e > largest_endid_encountered {
                    largest_endid_encountered = part.e;
                }
            }
            let report = builder.commit()?;
            last_tx_entid = Some(report.tx_id);
            last_tx_uuid = Some(tx.tx.clone());
        }

        // We've just transacted a new tx, and generated a new tx entid.
        // Map it to the corresponding incoming tx uuid, advance our
        // "locally known remote head".
        if let Some(uuid) = last_tx_uuid {
            if let Some(entid) = last_tx_entid {
                {
                let mut db_tx = self.sqlite.transaction()?;
                SyncMetadataClient::set_remote_head(&mut db_tx, &uuid)?;
                TxMapper::set_tx_uuid(&mut db_tx, entid, &uuid)?;
                db_tx.commit()?;
                }

                // only need to advance the user partition, since we're using KnownEntid and partition won't
                // get auto-updated; shouldn't be a problem for tx partition, since we're relying on the builder
                // to create a tx and advance the partition for us.
                self.fast_forward_user_partition(largest_endid_encountered)?;
            }
        }

        Ok(())
    }

    fn sync(&mut self, server_uri: &String, user_uuid: &String) -> Result<()> {
        let uuid = Uuid::parse_str(&user_uuid)?;

        let sync_result;
        {
            let mut db_tx = self.sqlite.transaction()?;
            sync_result = Syncer::flow(&mut db_tx, server_uri, &uuid)?;

            // TODO this should be done _after_ all of the operations below conclude!
            // Commits any changes Syncer made (schema, updated heads, tu mappings during an upload, etc)
            db_tx.commit()?;
        }

        // TODO These operations need to borrow self as mutable; but we already borrow it for db_tx above,
        // and so for now we split up sync into multiple db transactions /o\
        // Fixing this likely involves either implementing flow on InProgress, or changing flow to
        // take an InProgress instead of a raw sql transaction.

        match sync_result {
            SyncResult::EmptyServer => Ok(()),
            SyncResult::NoChanges => Ok(()),
            SyncResult::ServerFastForward => Ok(()),
            SyncResult::Merge => bail!(ErrorKind::NotYetImplemented(
                format!("Can't sync against diverged local.")
            )),
            SyncResult::LocalFastForward(txs) => self.fast_forward_local(txs)
        }
    }
}
