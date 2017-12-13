// Copyright 2016 Mozilla
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

use edn::Uuid;
use schema;
use rusqlite;
use Result;

trait HeadTrackable {
    fn remote_head(&self) -> Result<Uuid>;
    fn set_remote_head(&mut self, uuid: &Uuid) -> Result<()>;
}

struct SyncMetadataClient {
    conn: rusqlite::Connection
}

impl SyncMetadataClient {
    fn new(conn: rusqlite::Connection) -> Self {
        SyncMetadataClient {
            conn: conn
        }
    }
}

impl HeadTrackable for SyncMetadataClient {
    fn remote_head(&self) -> Result<Uuid> {
        self.conn.query_row(
            "SELECT value FROM tolstoy_metadata WHERE key = ?",
            &[&schema::REMOTE_HEAD_KEY], |r| {
                let bytes: Vec<u8> = r.get(0);
                Uuid::from_bytes(bytes.as_slice())
            }
        )?.map_err(|e| e.into())
    }

    fn set_remote_head(&mut self, uuid: &Uuid) -> Result<()> {
        let tx = self.conn.transaction()?;
        let uuid_bytes = uuid.as_bytes().to_vec();
        tx.execute("UPDATE tolstoy_metadata SET value = ? WHERE key = ?", &[&uuid_bytes, &schema::REMOTE_HEAD_KEY])?;
        tx.commit().map_err(|e| e.into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_get_remote_head_default() {
        let conn = schema::tests::setup_conn();
        let metadata_client: SyncMetadataClient = SyncMetadataClient::new(conn);
        assert_eq!(Uuid::nil(), metadata_client.remote_head().expect("fetch succeeded"));
    }

    #[test]
    fn test_set_and_get_remote_head() {
        let conn = schema::tests::setup_conn();
        let uuid = Uuid::new_v4();
        let mut metadata_client: SyncMetadataClient = SyncMetadataClient::new(conn);
        metadata_client.set_remote_head(&uuid).expect("update succeeded");
        assert_eq!(uuid, metadata_client.remote_head().expect("fetch succeeded"));
    }
}
