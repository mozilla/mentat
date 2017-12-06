use rusqlite;
use TolstoyResult;

static REMOTE_HEAD_KEY: &str = r#"remote_head"#;

lazy_static! {
    /// SQL statements to be executed, in order, to create the Tolstoy SQL schema (version 1).
    #[cfg_attr(rustfmt, rustfmt_skip)]
    static ref V1_STATEMENTS: Vec<&'static str> = { vec![
        r#"CREATE TABLE IF NOT EXISTS tolstoy_tu (tx INTEGER PRIMARY KEY, uuid BLOB NOT NULL UNIQUE) WITHOUT ROWID"#,
        r#"CREATE TABLE IF NOT EXISTS tolstoy_metadata (key BLOB NOT NULL UNIQUE, value BLOB NOT NULL)"#,
        r#"CREATE INDEX IF NOT EXISTS idx_tolstoy_tu_ut ON tolstoy_tu (uuid, tx)"#,
        ]
    };
}

pub fn ensure_current_version(conn: &mut rusqlite::Connection) -> TolstoyResult {
    let tx = conn.transaction()?;

    for statement in (&V1_STATEMENTS).iter() {
        tx.execute(statement, &[])?;
    }

    tx.execute("INSERT OR IGNORE INTO tolstoy_metadata (key, value) VALUES (?, zeroblob(16))", &[&REMOTE_HEAD_KEY])?;

    tx.commit()?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_empty() {
        let mut conn = rusqlite::Connection::open_in_memory().unwrap();

        conn.execute_batch("
            PRAGMA page_size=32768;
            PRAGMA journal_mode=wal;
            PRAGMA wal_autocheckpoint=32;
            PRAGMA journal_size_limit=3145728;
            PRAGMA foreign_keys=ON;
        ").expect("success");

        assert!(ensure_current_version(&mut conn).is_ok());
        {
            let mut stmt = conn.prepare("SELECT key FROM tolstoy_metadata WHERE value = zeroblob(16)").unwrap();
            let mut keys_iter = stmt.query_map(&[], |r| r.get(0)).expect("query works");

            let first: Option<Result<String, _>> = keys_iter.next();
            let second: Option<Result<String, _>> = keys_iter.next();
            match (first, second) {
                (Some(Ok(key)), None) => {
                    assert_eq!(key, REMOTE_HEAD_KEY);
                },
                (_, _) => { panic!("Wrong number of results."); },
            }
        }
        assert!(ensure_current_version(&mut conn).is_ok());

        // Check that running ensure_current_version on an initialized conn doesn't change anything.
        {
            let mut stmt = conn.prepare("SELECT key FROM tolstoy_metadata WHERE value = zeroblob(16)").unwrap();
            let mut keys_iter = stmt.query_map(&[], |r| r.get(0)).expect("query works");

            let first: Option<Result<String, _>> = keys_iter.next();
            let second: Option<Result<String, _>> = keys_iter.next();
            match (first, second) {
                (Some(Ok(key)), None) => {
                    assert_eq!(key, REMOTE_HEAD_KEY);
                },
                (_, _) => { panic!("Wrong number of results."); },
            }
        }
    }
}
