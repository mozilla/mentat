// Copyright 2018 Mozilla
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

use std;
use std::str::FromStr;
use std::collections::HashMap;

use futures::{future, Future, Stream};
use hyper;
use hyper::Client;
use hyper::{Method, Request, StatusCode, Error as HyperError};
use hyper::header::{ContentType};
use rusqlite;
use serde_cbor;
use serde_json;
use tokio_core::reactor::Core;
use uuid::Uuid;

use mentat_core::Entid;
use metadata::SyncMetadataClient;
use metadata::HeadTrackable;

use errors::{
    ErrorKind,
    Result,
};

use tx_processor::{
    Processor,
    TxReceiver,
    TxPart,
};

use tx_mapper::TxMapper;

static API_VERSION: &str = "0.1";
static BASE_URL: &str = "https://mentat.dev.lcip.org/mentatsync/";

pub struct Syncer {}

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
    fn tx<T>(&mut self, tx_id: Entid, d: &mut T) -> Result<()>
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
        for datom in d {
            let datom_uuid = Uuid::new_v4();
            tx_chunks.push(datom_uuid);
            self.remote_client.put_chunk(&datom_uuid, serde_cbor::to_vec(&datom)?)?
        }

        // Upload tx.
        // NB: At this point, we may choose to update remote & local heads.
        // Depending on how much we're uploading, and how unreliable our connection
        // is, this might be a good thing to do to ensure we make at least some progress.
        // Comes at a cost of possibly increasing racing against other clients.
        match self.rolling_temp_head {
            Some(parent) => {
                self.remote_client.put_transaction(&tx_uuid, &parent, &tx_chunks)?;
                self.rolling_temp_head = Some(tx_uuid.clone());
            },
            None => self.remote_client.put_transaction(&tx_uuid, self.remote_head, &tx_chunks)?
        }

        Ok(())
    }

    fn done(&mut self) -> Result<()> {
        self.is_done = true;
        Ok(())
    }
}

impl Syncer {
    fn upload_ours(db_tx: &mut rusqlite::Transaction, from_tx: Option<Entid>, remote_client: &RemoteClient, remote_head: &Uuid) -> Result<()> {
        let mut uploader = UploadingTxReceiver::new(remote_client, remote_head);
        Processor::process(db_tx, from_tx, &mut uploader)?;
        if !uploader.is_done {
            bail!(ErrorKind::UploadingProcessorUnfinished);
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

    pub fn flow(sqlite: &mut rusqlite::Connection, username: String) -> Result<()> {
        // Sketch of an upload flow:
        // get remote head
        // compare with local head
        // if the same:
        // - upload any local chunks, transactions
        // - move server remote head
        // - move local remote head
        
        // TODO configure this sync with some auth data
        let remote_client = RemoteClient::new(BASE_URL.into(), username);

        let mut db_tx = sqlite.transaction()?;

        let remote_head = remote_client.get_head()?;
        let locally_known_remote_head = SyncMetadataClient::remote_head(&db_tx)?;

        // TODO it's possible that we've successfully advanced remote head previously,
        // but failed to advance our own local head. If that's the case, and we can recognize it,
        // our sync becomes much cheaper.

        // Check if the server is empty - populate it.
        if remote_head == Uuid::nil() {
            Syncer::upload_ours(&mut db_tx, None, &remote_client, &remote_head)?;
        
        // Check if the server is the same as us - nothing to do.
        } else if locally_known_remote_head == remote_head {
            return Ok(());

        // Server is not the same as us.
        } else {
            // Check if the server can be fast-forwarded.
            if let Some(upload_from_tx) = TxMapper::get_tx_for_uuid(&db_tx, &remote_head)? {
                Syncer::upload_ours(&mut db_tx, Some(upload_from_tx), &remote_client, &remote_head)?;
            
            // Server has new transactions which aren't known locally.
            } else {
                // We need to download and merge them first!
                bail!(ErrorKind::NotYetImplemented(
                    format!("Can't yet sync against changed server. Local head {:?}, remote head {:?}", locally_known_remote_head, remote_head)
                ));
            }
        }

        // if we don't have the tx e mapped in the tu table, then we've never uploaded it
        // but on a first sync, that is what you'd expect
        // so how do we know it's safe to download?
        // if remote head is ahead of us, and we never synced, and we don't know how to merge yet,
        // the only way is to sync to an empty state. e.g. locally we just have _one_ transaction,
        // and that's the bootstrap one.

        // more generally though, this won't matter. we do care about making a distinction between
        // a fast-forward sync and a merge.
        
        // so that's the primitive here:
        // - is it a fast-forward sync?
        // - or is it a merge?

        // fetch remote head. if it's the same as our known remote head, do nothing.
        // - unless it's 0000, then we can upload our state to the server!
        // otherwise:
        // - does our latest tx appear in the remote timeline? meaning that remote timeline
        // -- is derived from our local HEAD.
        // -- then it's a fast-forward.
        // but:
        // - when two different clients come to sync against each other, they will both have
        // -- bootstrap transactions. that is, our "first sync" is always a merge for the second client,
        // -- even if the second client has no user data at all.
        // -- so we need to be able to merge and smush the bootstrap tx at the very least!

        // -> without this, we can sync against ourselves only - pure replication, a.k.a. backup.
        
        // == flow ==
        // initiate download flow
        // fetch txs from the server given our known remote head
        // get chunks for each tx
        // transform downloaded chunks and txs into some local representation (TxPart?)
        // validate that what we downloaded is sane
        // - entids are known (locally or supplied in the downloaded chunks), values are sane, etc.
        // maybe turn these into Terms? Term is a datom with known entid.
        // transact the stuff!
        // once all is transacted, we're ready to upload our state.
        // need to make sure that the server state didn't change while we were doing this!

        // Commit everything, if there's anything to commit!
        // Any new tx->uuid mappings and the new HEAD. We're synced!
        db_tx.commit()?;

        Ok(())
    }
}

#[derive(Serialize)]
struct SerializedHead<'a> {
    head: &'a Uuid
}

#[derive(Serialize)]
struct SerializedTransaction<'a> {
    parent: &'a Uuid,
    chunks: &'a Vec<Uuid>
}

struct RemoteClient {
    base_uri: String,
    user_id: String
}

impl RemoteClient {
    fn new(base_uri: String, user_id: String) -> Self {
        RemoteClient {
            base_uri: base_uri,
            user_id: user_id
        }
    }

    fn bound_base_uri(&self) -> String {
        // TODO escaping
        format!("{}/{}/{}", self.base_uri, API_VERSION, self.user_id)
    }

    fn get_uuid(&self, uri: String) -> Result<Uuid> {
        let mut core = Core::new()?;
        let client = Client::new(&core.handle());

        let uri = uri.parse()?;
        let get = client.get(uri).and_then(|res| {
            res.body().concat2()
        });

        let got = core.run(get)?;
        Ok(Uuid::from_str(std::str::from_utf8(&got)?)?)
    }

    fn put<T>(&self, uri: String, payload: T, expected: StatusCode) -> Result<()>
    where hyper::Body: std::convert::From<T>,
        T: {
        let mut core = Core::new()?;
        let client = Client::new(&core.handle());

        let uri = uri.parse()?;

        let mut req = Request::new(Method::Put, uri);
        req.headers_mut().set(ContentType::json());
        req.set_body(payload);

        let put = client.request(req).and_then(|res| {
            let status_code = res.status();

            if status_code != expected {
                future::err(HyperError::Status)
            } else {
                // body will be empty...
                future::ok(())
            }
        });

        core.run(put)?;
        Ok(())
    }

    fn put_transaction(&self, transaction_uuid: &Uuid, parent_uuid: &Uuid, chunks: &Vec<Uuid>) -> Result<()> {
        // {"parent": uuid, "chunks": [chunk1, chunk2...]}
        let transaction = SerializedTransaction {
            parent: parent_uuid,
            chunks: chunks
        };

        let uri = format!("{}/transactions/{}", self.bound_base_uri(), transaction_uuid);
        let json = serde_json::to_string(&transaction)?;
        self.put(uri, json, StatusCode::Created)
    }

    fn get_head(&self) -> Result<Uuid> {
        let uri = format!("{}/head", self.bound_base_uri());
        self.get_uuid(uri)
    }

    fn put_head(&self, uuid: &Uuid) -> Result<()> {
        // {"head": uuid}
        let head = SerializedHead {
            head: uuid
        };

        let uri = format!("{}/head", self.bound_base_uri());
        
        let json = serde_json::to_string(&head)?;
        self.put(uri, json, StatusCode::NoContent)
    }

    fn put_chunk(&self, chunk_uuid: &Uuid, payload: Vec<u8>) -> Result<()> {
        let uri = format!("{}/chunks/{}", self.bound_base_uri(), chunk_uuid);
        self.put(uri, payload, StatusCode::Created)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_remote_client(uri: &str, user_id: &str) -> RemoteClient {
        RemoteClient::new(uri.into(), user_id.into())
    }

    #[test]
    fn test_remote_client_bound_uri() {
        let remote_client = test_remote_client("https://example.com/api", "test-user");
        assert_eq!("https://example.com/api/0.1/test-user", remote_client.bound_base_uri());
    }
}
