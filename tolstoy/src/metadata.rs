// Copyright 2016 Mozilla
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

use TolstoyResult;
use uuid;
use schema;
use rusqlite;
use Result;

trait SyncMetadataClientInterface {
    fn get_remote_head(&self) -> Result<uuid::Uuid>;
    fn set_remote_head(&mut self, uuid: uuid::Uuid) -> TolstoyResult;
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

impl SyncMetadataClientInterface for SyncMetadataClient {
    fn get_remote_head(&self) -> Result<uuid::Uuid> {
        let uuid_query_res = self.conn.query_row(
            "SELECT value FROM tolstoy_metadata WHERE key = ?",
            &[&schema::REMOTE_HEAD_KEY], |r| {
                let raw_uuid: Vec<u8> = r.get(0);
                match uuid::Uuid::from_bytes(raw_uuid.as_slice()) {
                    Ok(uuid) => uuid,
                    Err(e) => panic!("Couldn't parse UUID: {}", e)
                }
            }
        );

        match uuid_query_res {
            Ok(uuid) => Ok(uuid),
            Err(e) => panic!("Could not query for REMOTE_HEAD_KEY: {}", e)
        }
    }

    fn set_remote_head(&mut self, uuid: uuid::Uuid) -> TolstoyResult {
        let tx = self.conn.transaction()?;
        let uuid_bytes = uuid.as_bytes().to_vec();
        tx.execute("UPDATE tolstoy_metadata SET value = ? WHERE key = ?", &[&uuid_bytes, &schema::REMOTE_HEAD_KEY])?;

        match tx.commit() {
            Ok(()) => Ok(()),
            Err(e) => panic!(e)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_get_remote_head_default() {
        let conn = schema::tests::setup_conn();
        let metadata_client: SyncMetadataClient = SyncMetadataClient::new(conn);
        let head_res = metadata_client.get_remote_head();
        match head_res {
            Ok(uuid) => assert_eq!(uuid::Uuid::nil(), uuid),
            Err(e) => panic!("Query error: {}", e)
        }
    }

    #[test]
    fn test_set_and_get_remote_head() {
        let conn = schema::tests::setup_conn();
        let uuid = uuid::Uuid::new_v4();
        let mut metadata_client: SyncMetadataClient = SyncMetadataClient::new(conn);
        match metadata_client.set_remote_head(uuid) {
            Err(e) => panic!("Error setting remote head: {}", e),
            _ => ()
        }

        let head_res = metadata_client.get_remote_head();
        match head_res {
            Ok(read_uuid) => assert_eq!(uuid, read_uuid),
            Err(e) => panic!("Query error: {}", e)
        }
    }
}
