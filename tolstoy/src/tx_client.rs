// Copyright 2016 Mozilla
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

use std::collections::BTreeMap;
use std::collections::btree_map::Entry;

use rusqlite;

use errors::{
    Result,
    ErrorKind
};

use mentat_db::types::{
    Entid
};

use mentat_db::{
    entids,
    TypedSQLValue
};

use mentat_core::{
    TypedValue
};

#[derive(Debug)]
pub struct TxPart {
    pub e: Entid,
    pub a: i64,
    pub v: TypedValue,
    pub added: i32
}

#[derive(Debug)]
pub struct Tx {
    pub tx: Entid,
    pub tx_instant: TypedValue,
    pub parts: Vec<TxPart>
}

struct RawTx {
    e: Entid,
    a: i64,
    v: TypedValue,
    tx: Entid,
    added: i32
}

pub trait TxReader {
    fn all(sqlite: &rusqlite::Connection) -> Result<Vec<Tx>>;
}

pub struct TxClient {}

impl TxReader for TxClient {
    fn all(sqlite: &rusqlite::Connection) -> Result<Vec<Tx>> {
        // Make sure a=txInstant rows are first, so that we process
        // all transactions before we process any transaction parts.
        let mut stmt = sqlite.prepare(
            "SELECT
                e, a, v, tx, added, value_type_tag,
                CASE a WHEN :txInstant THEN 1 ELSE 0 END is_transaction
            FROM transactions ORDER BY is_transaction DESC, tx ASC"
        )?;
        let rows: Vec<Result<RawTx>> = stmt.query_and_then_named(&[(":txInstant", &entids::DB_TX_INSTANT)], |row| -> Result<RawTx> {
            Ok(RawTx {
                e: row.get(0),
                a: row.get(1),
                v: TypedValue::from_sql_value_pair(row.get(2), row.get(5))?,
                tx: row.get(3),
                added: row.get(4)
            })
        })?.collect();

        // It's convenient to have a consistently ordered set of results,
        // so we use a sorting map.
        let mut txes_by_tx = BTreeMap::new();
        for row_result in rows {
            let row = row_result?;
            // Row represents a transaction.
            if row.a == entids::DB_TX_INSTANT {
                txes_by_tx.insert(row.tx, Tx {
                    tx: row.tx,
                    tx_instant: row.v,
                    parts: Vec::new()
                });
            // Row represents part of a transaction. Our query statement above guarantees
            // that we've already processed corresponding transaction at this point.
            } else {
                if let Entry::Occupied(mut t) = txes_by_tx.entry(row.tx) {
                    t.get_mut().parts.push(TxPart {
                        e: row.e,
                        a: row.a,
                        v: row.v,
                        added: row.added
                    });
                } else {
                    bail!(ErrorKind::UnexpectedState(format!("Encountered transaction part before transaction {:?}", row.tx)))
                }
            }
        }

        Ok(txes_by_tx.into_iter().map(|(_, tx)| tx).collect())
    }
}
