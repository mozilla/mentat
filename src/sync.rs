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

use mentat_transaction::{
    InProgress,
};

use errors::{
    Result,
};

use mentat_tolstoy::{
    Syncer,
    RemoteClient,
    SyncReport,
};

pub trait Syncable {
    fn sync(&mut self, server_uri: &String, user_uuid: &String) -> Result<SyncReport>;
}

impl<'a, 'c> Syncable for InProgress<'a, 'c> {
    fn sync(&mut self, server_uri: &String, user_uuid: &String) -> Result<SyncReport> {
        // Syncer behaves as if it's part of InProgress.
        // This split into a separate crate is segment synchronization functionality
        // in a single crate which can be easily disabled by consumers,
        // and to separate concerns.
        // But for all intents and purposes, Syncer operates over a "mentat transaction",
        // which is exactly what InProgress represents.
        let mut remote_client = RemoteClient::new(
            server_uri.to_string(),
            Uuid::parse_str(&user_uuid)?
        );
        Syncer::sync(self, &mut remote_client)
            .map_err(|e| e.into())
    }
}
