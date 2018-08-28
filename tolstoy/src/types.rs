// Copyright 2018 Mozilla
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

use std::cmp::Ordering;
use uuid::Uuid;

use core_traits::{
    Entid,
    TypedValue,
};

use mentat_db::PartitionMap;

use public_traits::errors::{
    Result,
};

pub struct LocalGlobalTxMapping<'a> {
    pub local: Entid,
    pub remote: &'a Uuid,
}

impl<'a> From<(Entid, &'a Uuid)> for LocalGlobalTxMapping<'a> {
    fn from((local, remote): (Entid, &'a Uuid)) -> LocalGlobalTxMapping {
        LocalGlobalTxMapping {
            local: local,
            remote: remote,
        }
    }
}

impl<'a> LocalGlobalTxMapping<'a> {
    pub fn new(local: Entid, remote: &'a Uuid) -> LocalGlobalTxMapping<'a> {
        LocalGlobalTxMapping {
            local: local,
            remote: remote
        }
    }
}

// TODO unite these around something like `enum TxIdentifier {Global(Uuid), Local(Entid)}`?
#[derive(Debug, Clone)]
pub struct LocalTx {
    pub tx: Entid,
    pub parts: Vec<TxPart>,
}


impl PartialOrd for LocalTx {
    fn partial_cmp(&self, other: &LocalTx) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for LocalTx {
    fn cmp(&self, other: &LocalTx) -> Ordering {
        self.tx.cmp(&other.tx)
    }
}

impl PartialEq for LocalTx {
    fn eq(&self, other: &LocalTx) -> bool {
        self.tx == other.tx
    }
}

impl Eq for LocalTx {}

// For returning out of the downloader as an ordered list.
#[derive(Debug, Clone, PartialEq)]
pub struct Tx {
    pub tx: Uuid,
    pub parts: Vec<TxPart>,
}

#[derive(Debug,Clone,Serialize,Deserialize,PartialEq)]
pub struct TxPart {
    // TODO this is a temporary for development. Only first TxPart in a chunk series should have a non-None 'parts'.
    // 'parts' should actually live in a transaction, but we do this now to avoid changing the server until dust settles.
    pub partitions: Option<PartitionMap>,
    pub e: Entid,
    pub a: Entid,
    pub v: TypedValue,
    pub tx: Entid,
    pub added: bool,
}

pub trait GlobalTransactionLog {
    fn head(&self) -> Result<Uuid>;
    fn transactions_after(&self, tx: &Uuid) -> Result<Vec<Tx>>;
    fn set_head(&mut self, tx: &Uuid) -> Result<()>;
    fn put_transaction(&mut self, tx: &Uuid, parent_tx: &Uuid, chunk_txs: &Vec<Uuid>) -> Result<()>;
    fn put_chunk(&mut self, tx: &Uuid, payload: &TxPart) -> Result<()>;
}
