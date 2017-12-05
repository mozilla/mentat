use rusqlite;
use TolstoyResult;

lazy_static! {
    /// SQL statements to be executed, in order, to create the Tolstoy SQL schema (version 1).
    #[cfg_attr(rustfmt, rustfmt_skip)]
    static ref V1_STATEMENTS: Vec<&'static str> = { vec![
        r#"CREATE TABLE IF NOT EXISTS tolstoy_tu (uuid TEXT NOT NULL, tx INTEGER NOT NULL)"#,
        r#"CREATE TABLE IF NOT EXISTS tolstoy_head (uuid TEXT)"#,
        ]
    };
}

// TODO upgrade path - without coupling to user_version, this probably looks like
// versioning names of the tables - e.g. tolstoy_tu_v1 
pub fn ensure_current_version(conn: &mut rusqlite::Connection) -> TolstoyResult {
    let tx = conn.transaction()?;

    for statement in (&V1_STATEMENTS).iter() {
        tx.execute(statement, &[])?;
    }

    tx.commit()?;

    Ok(())
}
