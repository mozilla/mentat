// Copyright 2016 Mozilla
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

use rusqlite;
use Result;

pub static REMOTE_HEAD_KEY: &str = r#"remote_head"#;

lazy_static! {
    /// SQL statements to be executed, in order, to create the Tolstoy SQL schema (version 1).
    #[cfg_attr(rustfmt, rustfmt_skip)]
    static ref SCHEMA_STATEMENTS: Vec<&'static str> = { vec![
        r#"CREATE TABLE IF NOT EXISTS tolstoy_tu (tx INTEGER PRIMARY KEY, uuid BLOB NOT NULL UNIQUE) WITHOUT ROWID"#,
        r#"CREATE TABLE IF NOT EXISTS tolstoy_metadata (key BLOB NOT NULL UNIQUE, value BLOB NOT NULL)"#,
        r#"CREATE INDEX IF NOT EXISTS idx_tolstoy_tu_ut ON tolstoy_tu (uuid, tx)"#,
        ]
    };
}

pub fn ensure_current_version(conn: &mut rusqlite::Connection) -> Result<()> {
    let tx = conn.transaction()?;

    for statement in (&SCHEMA_STATEMENTS).iter() {
        tx.execute(statement, &[])?;
    }

    tx.execute("INSERT OR IGNORE INTO tolstoy_metadata (key, value) VALUES (?, zeroblob(16))", &[&REMOTE_HEAD_KEY])?;
    tx.commit().map_err(|e| e.into())
}

#[cfg(test)]
pub mod tests {
    use super::*;
    use edn::Uuid;

    fn setup_conn_bare() -> rusqlite::Connection {
        let conn = rusqlite::Connection::open_in_memory().unwrap();

        conn.execute_batch("
            PRAGMA page_size=32768;
            PRAGMA journal_mode=wal;
            PRAGMA wal_autocheckpoint=32;
            PRAGMA journal_size_limit=3145728;
            PRAGMA foreign_keys=ON;
        ").expect("success");

        conn
    }

    pub fn setup_conn() -> rusqlite::Connection {
        let mut conn = setup_conn_bare();
        ensure_current_version(&mut conn).expect("connection setup");
        conn
    }

    #[test]
    fn test_empty() {
        let mut conn = setup_conn_bare();

        assert!(ensure_current_version(&mut conn).is_ok());
        
        let mut stmt = conn.prepare("SELECT key FROM tolstoy_metadata WHERE value = zeroblob(16)").unwrap();
        let mut keys_iter = stmt.query_map(&[], |r| r.get(0)).expect("query works");

        let first: Result<String> = keys_iter.next().unwrap().map_err(|e| e.into());
        let second: Option<_> = keys_iter.next();
        match (first, second) {
            (Ok(key), None) => {
                assert_eq!(key, REMOTE_HEAD_KEY);
            },
            (_, _) => { panic!("Wrong number of results."); },
        }
    }

    #[test]
    fn test_non_empty() {
        let mut conn = setup_conn_bare();

        assert!(ensure_current_version(&mut conn).is_ok());

        let test_uuid = Uuid::new_v4();
        {
            let tx = conn.transaction().unwrap();
            let uuid_bytes = test_uuid.as_bytes().to_vec();
            match tx.execute("UPDATE tolstoy_metadata SET value = ? WHERE key = ?", &[&uuid_bytes, &REMOTE_HEAD_KEY]) {
                Err(e) => panic!("Error running an update: {}", e),
                _ => ()
            }
            match tx.commit() {
                Err(e) => panic!("Error committing an update: {}", e),
                _ => ()
            }
        }

        assert!(ensure_current_version(&mut conn).is_ok());

        // Check that running ensure_current_version on an initialized conn doesn't change anything.
        let mut stmt = conn.prepare("SELECT value FROM tolstoy_metadata").unwrap();
        let mut values_iter = stmt.query_map(&[], |r| {
            let raw_uuid: Vec<u8> = r.get(0);
            Uuid::from_bytes(raw_uuid.as_slice()).unwrap()
        }).expect("query works");

        let first: Result<Uuid> = values_iter.next().unwrap().map_err(|e| e.into());
        let second: Option<_> = values_iter.next();
        match (first, second) {
            (Ok(uuid), None) => {
                assert_eq!(test_uuid, uuid);
            },
            (_, _) => { panic!("Wrong number of results."); },
        }
    }
}
