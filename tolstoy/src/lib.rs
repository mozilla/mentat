// Copyright 2016 Mozilla
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

#[macro_use]
extern crate error_chain;

#[macro_use]
extern crate lazy_static;

extern crate hyper;
extern crate tokio_core;
extern crate futures;
extern crate serde;
extern crate serde_json;
extern crate mentat_db;
extern crate rusqlite;
extern crate uuid;

use uuid::Uuid;

pub mod schema;

error_chain! {
    types {
        Error, ErrorKind, ResultExt, Result;
    }

    foreign_links {
        IOError(std::io::Error);
        UriError(hyper::error::UriError);
        HttpError(hyper::Error);
        SqlError(rusqlite::Error);
    }
}

pub type TolstoyResult = Result<()>;

trait SyncMetadataClient {
    fn new(conn: rusqlite::Connection)-> Self;
    fn get_remote_head(&self) -> Result<uuid::Uuid>;
    fn set_remote_head(&mut self, uuid: uuid::Uuid) -> TolstoyResult;
}

struct SyncMetadataClientImpl {
    conn: rusqlite::Connection
}

impl SyncMetadataClient for SyncMetadataClientImpl {
    fn new(conn: rusqlite::Connection) -> Self {
        SyncMetadataClientImpl {
            conn: conn
        }
    }
    fn get_remote_head(&self) -> Result<uuid::Uuid> {
        let uuid_parse_res = self.conn.query_row(
            "SELECT value FROM tolstoy_metadata WHERE key = ?",
            &[&schema::REMOTE_HEAD_KEY], |r| {
                let raw_uuid: Vec<u8> = r.get(0);
                match Uuid::from_bytes(raw_uuid.as_slice()) {
                    Ok(uuid) => uuid,
                    Err(e) => panic!("Couldn't parse UUID: {}", e)
                }
            }
        );

        match uuid_parse_res {
            Ok(res) => Ok(res),
            Err(e) => panic!("Could not parse UUID: {}", e)
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
        let metadata_client: SyncMetadataClientImpl = SyncMetadataClientImpl::new(conn);
        let head_res = metadata_client.get_remote_head();
        match head_res {
            Ok(uuid) => assert_eq!(Uuid::nil(), uuid),
            Err(e) => panic!("Query error: {}", e)
        }
    }

    #[test]
    fn test_set_and_get_remote_head() {
        let conn = schema::tests::setup_conn();
        let uuid = Uuid::new_v4();
        let mut metadata_client: SyncMetadataClientImpl = SyncMetadataClientImpl::new(conn);
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
