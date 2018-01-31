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

use rusqlite;

use Result;

use mentat_db::types::{
    Entid
};

use mentat_core::{
    DateTime,
    Utc,
    ValueType
};

use mentat_core::SQLValueType;
use edn::FromMicros;

struct TxPart {
    e: Entid,
    a: i32,
    v: Vec<u8>,
    added: i32,
    value_type_tag: i32
}

struct Tx {
    tx: Entid,
    tx_instant: DateTime<Utc>,
    parts: Vec<TxPart>
}

impl Tx {
    fn new(conn: &rusqlite::Connection, tx: i64, tx_instant: i64) -> Result<Tx> {
        let mut stmt = conn.prepare("SELECT e,a,v,added,value_type_tag FROM transactions WHERE tx = :tx")?;
        let mapped_rows = stmt.query_map_named(&[(":tx", &tx)], |row| TxPart {
            e: row.get(0),
            a: row.get(1),
            v: row.get(2),
            added: row.get(3),
            value_type_tag: row.get(4)
        })?;

        let mut parts = Vec::new();
        for part in mapped_rows {
            parts.push(part?);
        }

        Ok(Tx {
            tx: tx,
            tx_instant: DateTime::<Utc>::from_micros(tx_instant),
            parts: parts
        })
    }
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
        let mut stmt = self.conn.prepare("SELECT tx, v FROM transactions GROUP BY tx")?;
        let mapped_rows = stmt.query_map(&[], |row| Tx::new(&self.conn, row.get(0), row.get(1)))?;

        let mut txes = Vec::new();
        for tx in mapped_rows {
            txes.push(match tx? {
                Err(e) => return Err(e),
                Ok(v) => v
            });
        }

        Ok(txes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
}
