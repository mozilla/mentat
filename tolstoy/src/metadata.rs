// Copyright 2018 Mozilla
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

#![allow(dead_code)]

use rusqlite;
use uuid::Uuid;

use schema;
use tolstoy_traits::errors::{
    TolstoyError,
    Result,
};

pub trait HeadTrackable {
    fn remote_head(tx: &rusqlite::Transaction) -> Result<Uuid>;
    fn set_remote_head(tx: &rusqlite::Transaction, uuid: &Uuid) -> Result<()>;
}

pub struct SyncMetadataClient {}

impl HeadTrackable for SyncMetadataClient {
    fn remote_head(tx: &rusqlite::Transaction) -> Result<Uuid> {
        tx.query_row(
            "SELECT value FROM tolstoy_metadata WHERE key = ?",
            &[&schema::REMOTE_HEAD_KEY], |r| {
                let bytes: Vec<u8> = r.get(0);
                Uuid::from_bytes(bytes.as_slice())
            }
        )?.map_err(|e| e.into())
    }

    fn set_remote_head(tx: &rusqlite::Transaction, uuid: &Uuid) -> Result<()> {
        let uuid_bytes = uuid.as_bytes().to_vec();
        let updated = tx.execute("UPDATE tolstoy_metadata SET value = ? WHERE key = ?",
            &[&uuid_bytes, &schema::REMOTE_HEAD_KEY])?;
        if updated != 1 {
            bail!(TolstoyError::DuplicateMetadata(schema::REMOTE_HEAD_KEY.into()));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_get_remote_head_default() {
        let mut conn = schema::tests::setup_conn();
        let tx = conn.transaction().expect("db tx");
        assert_eq!(Uuid::nil(), SyncMetadataClient::remote_head(&tx).expect("fetch succeeded"));
    }

    #[test]
    fn test_set_and_get_remote_head() {
        let mut conn = schema::tests::setup_conn();
        let uuid = Uuid::new_v4();
        let tx = conn.transaction().expect("db tx");
        SyncMetadataClient::set_remote_head(&tx, &uuid).expect("update succeeded");
        assert_eq!(uuid, SyncMetadataClient::remote_head(&tx).expect("fetch succeeded"));
    }
}
