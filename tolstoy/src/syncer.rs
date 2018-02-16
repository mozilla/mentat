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

use rusqlite;
use uuid::Uuid;

use mentat_core::Entid;
use metadata::SyncMetadataClient;
use metadata::HeadTrackable;
use schema::ensure_current_version;

use errors::{
    ErrorKind,
    Result,
};

use remote_client::{
    RemoteClient,
};

use tx_processor::{
    Processor,
    TxReceiver,
    TxPart,
};

use tx_mapper::TxMapper;

// TODO it would be nice to be able to pass
// in a logger into Syncer::flow; would allow for a "debug mode"
// and getting useful logs out of clients.
// See https://github.com/mozilla/mentat/issues/571
// Below is some debug Android-friendly logging:

// use std::os::raw::c_char;
// use std::os::raw::c_int;
// use std::ffi::CString;
// pub const ANDROID_LOG_DEBUG: i32 = 3;
// extern { pub fn __android_log_write(prio: c_int, tag: *const c_char, text: *const c_char) -> c_int; }

pub fn d(message: &str) {
    println!("d: {}", message);
    // let message = CString::new(message).unwrap();
    // let message = message.as_ptr();
    // let tag = CString::new("RustyToodle").unwrap();
    // let tag = tag.as_ptr();
    // unsafe { __android_log_write(ANDROID_LOG_DEBUG, tag, message) };
}

pub struct Syncer {}

// TODO this is sub-optimal, we don't need to walk the table
// to query the last thing in it w/ an index on tx!!
// but it's the hammer at hand!
// See https://github.com/mozilla/mentat/issues/572
struct InquiringTxReceiver {
    pub last_tx: Option<Entid>,
    pub is_done: bool,
}

impl InquiringTxReceiver {
    fn new() -> InquiringTxReceiver {
        InquiringTxReceiver {
            last_tx: None,
            is_done: false,
        }
    }
}

impl TxReceiver for InquiringTxReceiver {
    fn tx<T>(&mut self, tx_id: Entid, _datoms: &mut T) -> Result<()>
    where T: Iterator<Item=TxPart> {
        self.last_tx = Some(tx_id);
        Ok(())
    }

    fn done(&mut self) -> Result<()> {
        self.is_done = true;
        Ok(())
    }
}

struct UploadingTxReceiver<'c> {
    pub tx_temp_uuids: HashMap<Entid, Uuid>,
    pub is_done: bool,
    remote_client: &'c RemoteClient,
    remote_head: &'c Uuid,
    rolling_temp_head: Option<Uuid>,
}

impl<'c> UploadingTxReceiver<'c> {
    fn new(client: &'c RemoteClient, remote_head: &'c Uuid) -> UploadingTxReceiver<'c> {
        UploadingTxReceiver {
            tx_temp_uuids: HashMap::new(),
            remote_client: client,
            remote_head: remote_head,
            rolling_temp_head: None,
            is_done: false
        }
    }
}

impl<'c> TxReceiver for UploadingTxReceiver<'c> {
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

        // Upload all chunks.
        for datom in datoms {
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
        match self.rolling_temp_head {
            Some(parent) => {
                d(&format!("putting transaction: {:?}, {:?}, {:?}", &tx_uuid, &parent, &tx_chunks));
                self.remote_client.put_transaction(&tx_uuid, &parent, &tx_chunks)?;
                
            },
            None => {
                d(&format!("putting transaction: {:?}, {:?}, {:?}", &tx_uuid, &self.remote_head, &tx_chunks));
                self.remote_client.put_transaction(&tx_uuid, self.remote_head, &tx_chunks)?;
            }
        }

        d(&format!("updating rolling head: {:?}", tx_uuid));
        self.rolling_temp_head = Some(tx_uuid.clone());

        Ok(())
    }

    fn done(&mut self) -> Result<()> {
        self.is_done = true;
        Ok(())
    }
}

// For returning out of the downloader as an ordered list.
#[derive(Debug)]
pub struct Tx {
    pub tx: Uuid,
    pub parts: Vec<TxPart>,
}

pub enum SyncResult {
    EmptyServer,
    NoChanges,
    ServerFastForward,
    LocalFastForward(Vec<Tx>),
    Merge,
}

impl Syncer {
    fn fast_forward_server(db_tx: &mut rusqlite::Transaction, from_tx: Option<Entid>, remote_client: &RemoteClient, remote_head: &Uuid) -> Result<()> {
        let mut uploader = UploadingTxReceiver::new(remote_client, remote_head);
        Processor::process(db_tx, from_tx, &mut uploader)?;
        if !uploader.is_done {
            bail!(ErrorKind::TxProcessorUnfinished);
        }
        // Last tx uuid uploaded by the tx receiver.
        // It's going to be our new head.
        if let Some(last_tx_uploaded) = uploader.rolling_temp_head {
            // Upload remote head.
            remote_client.put_head(&last_tx_uploaded)?;

            // On succes:
            // - persist local mappings from the receiver
            // - update our local "remote head".
            TxMapper::set_bulk(db_tx, &uploader.tx_temp_uuids)?;
            SyncMetadataClient::set_remote_head(db_tx, &last_tx_uploaded)?;
        }

        Ok(())
    }

    fn download_theirs(_db_tx: &mut rusqlite::Transaction, remote_client: &RemoteClient, remote_head: &Uuid) -> Result<Vec<Tx>> {
        let new_txs = remote_client.get_transactions(remote_head)?;
        let mut tx_list = Vec::new();

        for tx in new_txs {
            let mut tx_parts = Vec::new();
            let chunks = remote_client.get_chunks(&tx)?;

            // We pass along all of the downloaded parts, including transaction's
            // metadata datom. Transactor is expected to do the right thing, and
            // use txInstant from one of our datoms.
            for chunk in chunks {
                let part = remote_client.get_chunk(&chunk)?;
                tx_parts.push(part);
            }

            tx_list.push(Tx {
                tx: tx,
                parts: tx_parts
            });
        }

        d(&format!("got tx list: {:?}", &tx_list));

        Ok(tx_list)
    }

    pub fn flow(db_tx: &mut rusqlite::Transaction, server_uri: &String, user_uuid: &Uuid) -> Result<SyncResult> {
        d(&format!("sync flowing"));

        ensure_current_version(db_tx)?;
        
        // TODO configure this sync with some auth data
        let remote_client = RemoteClient::new(server_uri.clone(), user_uuid.clone());

        let remote_head = remote_client.get_head()?;
        d(&format!("remote head {:?}", remote_head));

        let locally_known_remote_head = SyncMetadataClient::remote_head(db_tx)?;
        d(&format!("local head {:?}", locally_known_remote_head));

        // Local head: latest transaction that we have in the store,
        // but with one caveat: its tx might will not be mapped if it's
        // never been synced successfully.
        // In other words: if latest tx isn't mapped, then HEAD moved
        // since last sync and server needs to be updated.
        let mut inquiring_tx_receiver = InquiringTxReceiver::new();
        // TODO don't just start from the beginning... but then again, we should do this
        // without walking the table at all, and use the tx index.
        Processor::process(db_tx, None, &mut inquiring_tx_receiver)?;
        if !inquiring_tx_receiver.is_done {
            bail!(ErrorKind::TxProcessorUnfinished);
        }
        let (have_local_changes, local_store_empty) = match inquiring_tx_receiver.last_tx {
            Some(tx) => {
                match TxMapper::get(db_tx, tx)? {
                    Some(_) => (false, false),
                    None => (true, false)
                }
            },
            None => (false, true)
        };

        // Check if the server is empty - populate it.
        if remote_head == Uuid::nil() {
            d(&format!("empty server!"));
            Syncer::fast_forward_server(db_tx, None, &remote_client, &remote_head)?;
            return Ok(SyncResult::EmptyServer);
        
        // Check if the server is the same as us, and if our HEAD moved.
        } else if locally_known_remote_head == remote_head {
            d(&format!("server unchanged since last sync."));
            
            if !have_local_changes {
                d(&format!("local HEAD did not move. Nothing to do!"));
                return Ok(SyncResult::NoChanges);
            }

            d(&format!("local HEAD moved."));
            // TODO it's possible that we've successfully advanced remote head previously,
            // but failed to advance our own local head. If that's the case, and we can recognize it,
            // our sync becomes just bumping our local head. AFAICT below would currently fail.
            if let Some(upload_from_tx) = TxMapper::get_tx_for_uuid(db_tx, &locally_known_remote_head)? {
                d(&format!("Fast-forwarding the server."));
                Syncer::fast_forward_server(db_tx, Some(upload_from_tx), &remote_client, &remote_head)?;
                return Ok(SyncResult::ServerFastForward);
            } else {
                d(&format!("Unable to fast-forward the server; missing local tx mapping"));
                bail!(ErrorKind::TxIncorrectlyMapped(0));
            }
            
        // We diverged from the server. If we're lucky, we can just fast-forward local.
        // Otherwise, a merge (or a rebase) is required.
        } else {
            d(&format!("server changed since last sync."));

            // TODO local store moved forward since we last synced. Need to merge or rebase.
            if !local_store_empty && have_local_changes {
                return Ok(SyncResult::Merge);
            }

            d(&format!("fast-forwarding local store."));
            return Ok(SyncResult::LocalFastForward(
                Syncer::download_theirs(db_tx, &remote_client, &locally_known_remote_head)?
            ));
        }

        // Our caller will commit the tx with our changes when it's done.
    }
}
