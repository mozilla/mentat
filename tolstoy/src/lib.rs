// Copyright 2018 Mozilla
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

extern crate failure;

#[macro_use]
extern crate lazy_static;

#[macro_use]
extern crate serde_derive;

extern crate edn;

extern crate hyper;
// TODO https://github.com/mozilla/mentat/issues/569
// extern crate hyper_tls;
extern crate tokio_core;
extern crate futures;
extern crate serde;
extern crate serde_cbor;
extern crate serde_json;

extern crate log;
extern crate mentat_db;

extern crate mentat_core;
extern crate db_traits;
#[macro_use]
extern crate core_traits;
extern crate public_traits;
extern crate rusqlite;
extern crate uuid;

extern crate tolstoy_traits;
extern crate mentat_transaction;

pub mod bootstrap;
pub mod metadata;
pub use metadata::{
    PartitionsTable,
    SyncMetadata,
};
mod datoms;
pub mod debug;
pub mod remote_client;
pub use remote_client::{
    RemoteClient,
};
pub mod schema;
pub mod syncer;
pub use syncer::{
    Syncer,
    SyncReport,
};
mod tx_uploader;
pub mod logger;
pub mod tx_mapper;
pub use tx_mapper::{
    TxMapper,
};
pub mod tx_processor;
pub mod types;
pub use types::{
    Tx,
    TxPart,
    GlobalTransactionLog,
};
