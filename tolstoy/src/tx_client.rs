// Copyright 2016 Mozilla
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

// read all txs from the database
// return a list of structures that represent all we need to know about transactions

// so, what do we need here then?
// we need a "way in"!

// could just query the transactions database directly, read stuff in, and represent
// it as some data structure on the way out

// will need to weed out transactions as we work through the records
// -> and then associate with it the "chunks"

// "transaction" then is a meta-concept, it's a label for a collection of concrete changes

// perhaps mentat has useful primitives, but let's begin by just "doing the work"

use std::collections::HashMap;
use std::collections::hash_map::Entry;

use rusqlite;

use errors::{
    Result
};

use mentat_db::types::{
    Entid
};

use mentat_db::{
    entids,
    TypedSQLValue
};

use mentat_core::{
    DateTime,
    Utc,
    TypedValue
};

use edn::FromMicros;

pub struct TxPart {
    e: Entid,
    a: i64,
    v: TypedValue,
    added: i32
}

pub struct Tx {
    tx: Entid,
    tx_instant: DateTime<Utc>,
    parts: Vec<TxPart>
}

trait TxReader {
    fn txs(&self) -> Result<Vec<Tx>>;
}

struct TxClient {
    conn: rusqlite::Connection
}

// TODO This needs to take a mentat connection, as we're making assumptions about
// what that connection will provide (a transactions table).
impl TxClient {
    fn new(conn: rusqlite::Connection) -> Self {
        TxClient {
            conn: conn
        }
    }
}

impl TxReader for TxClient {
    fn txs(&self) -> Result<Vec<Tx>> {
        let mut txes_by_tx = HashMap::new();

        // Make sure a=txInstant rows are first, so that we process
        // all transactions before we process any transaction parts.
        let mut stmt = self.conn.prepare(
            "SELECT
                e, a, v, tx, added, value_type_tag,
                CASE a WHEN :txInstant THEN 1 ELSE 0 END is_transaction
            FROM transactions ORDER BY is_transaction DESC"
        )?;
        let _ = stmt.query_and_then_named(&[(":txInstant", &entids::DB_TX_INSTANT)], |row| {
            let e = row.get(0);
            let a = row.get(1);
            let v_instant: i64 = row.get(2); // TODO unify this and typed_value below
            let tx = row.get(3);
            let added = row.get(4);
            let value_type_tag = row.get(5);

            let raw_value: rusqlite::types::Value = row.get(2);
            let typed_value = match TypedValue::from_sql_value_pair(raw_value, value_type_tag) {
                Ok(v) => v,
                Err(e) => return Err(e)
            };

            // Row represents a transaction.
            if a == entids::DB_TX_INSTANT {
                txes_by_tx.insert(tx, Tx {
                    tx: tx,
                    // TODO enforce correct type of v and return ErrorKind::BadSQLValuePair
                    // otherwise.
                    tx_instant: DateTime::<Utc>::from_micros(v_instant),
                    parts: Vec::new()
                });
                Ok(())
            // Row represents part of a transaction. Our query statement above guarantees
            // that we've already processed corresponding transaction at this point.
            } else {
                if let Entry::Occupied(mut t) = txes_by_tx.entry(tx) {
                    t.get_mut().parts.push(TxPart {
                        e: e,
                        a: a,
                        v: typed_value,
                        added: added
                    });
                    Ok(())
                } else {
                    // TODO not ok... ErrorKind::UnexpectedError
                    Ok(())
                }
            }
        })?;

        Ok(txes_by_tx.into_iter().map(|(_, tx)| tx).collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
}
