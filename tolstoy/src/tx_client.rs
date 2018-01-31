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

pub struct TxPart {
    e: Entid,
    a: i32,
    v: Vec<u8>, // should be TypedValue to allow for variety of types
    added: i32,
    value_type_tag: i32 // with v as TypedValue, shouldn't be necessary
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
        let mut parts_keyed_by_tx = HashMap::new();

        let mut stmt = self.conn.prepare("SELECT e, a, v, tx, added, value_type_tag, CASE a WHEN :txInstant THEN 1 ELSE 0 END is_transaction FROM transactions ORDER BY is_transaction DESC")?;
        let mapped_rows = stmt.query_map_named(&[(":txInstant", entids::DB_TX_INSTANT)], |row| {
            let e = row.get(0);
            let a = row.get(1);
            let v = row.get(2);
            let tx = row.get(3);
            let added = row.get(4);
            let value_type_tag = row.get(5);
            
            // Row represents a transaction.
            if a == entids::DB_TX_INSTANT {
                txes_by_tx.insert(tx, Tx {
                    tx: tx,
                    tx_instant: DateTime::<Utc>::from_micros(v),
                    parts: Vec::new()
                });
            // Row represents part of a transaction. Our query statement above guarantees
            // that we've already processed corresponding transaction at this point.
            } else {
                if let Entry::Occupied(o) = txes_by_tx.entry(tx) {
                    *o.get_mut().parts.push(TxPart {
                        e: e,
                        a: a,
                        v: v, // TODO tx_instant conversion implied that this value is i64... but it can be many things
                        added: added,
                        value_type_tag: value_type_tag
                    });
                } else {
                    // Shouldn't happen if our query is correct.
                }
            }
        })?;

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
