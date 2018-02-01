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
use std::collections::HashMap;

use rusqlite;

use errors::{
    Result,
    ErrorKind,
};

use mentat_db::{
    entids,
    TypedSQLValue,
};

use mentat_core::{
    TypedValue,
    Entid,
};

#[derive(Debug)]
pub struct TxPart {
    pub e: Entid,
    pub a: Entid,
    pub v: TypedValue,
    pub added: bool,
}

// Notes on 'parts' representation:
// Currently it's suitable for uses which necessitate pulling in the entire tx into memory,
// and don't require efficient querying/monitoring by attributes of parts.
//
// Example: streaming transactions to/from the server.
//
// In the future, consider:
// - A structure that makes typical tx-listener questions — "does this transaction mention
//   an attribute or entity I care about?" — efficient. That might be a trie, it might be a
//   bunch of extra data structures (e.g., a set of mentioned attributes), or something else.
//   With an unsorted Vec<TxPart>, looking for a mentioned attribute requires linear search of the entire vector.
// - A structure that doesn't require pulling the entire tx into memory. This might be a cursor,
//   a rowid range, or something else that's scoped to the lifetime of a particular database transaction,
//   in order to preserve isolation.
#[derive(Debug)]
pub struct Tx {
    pub tx: Entid,
    pub tx_instant: TypedValue,
    pub parts: Vec<TxPart>,
}

struct RawDatom {
    e: Entid,
    a: Entid,
    v: TypedValue, // composite of 'v' and 'value_type_tag'
    tx: Entid,
    added: bool,
    is_transaction: bool
}

pub trait TxReader {
    fn all(sqlite: &rusqlite::Connection) -> Result<Vec<Tx>>;
}

pub struct TxClient {}

impl TxReader for TxClient {
    fn all(sqlite: &rusqlite::Connection) -> Result<Vec<Tx>> {
        let mut stmt = sqlite.prepare(
            "SELECT
                e, a, v, tx, added, value_type_tag,
                CASE a WHEN :txInstant THEN 1 ELSE 0 END is_transaction
            FROM transactions"
        )?;
        let datoms: Vec<Result<RawDatom>> = stmt.query_and_then_named(&[(":txInstant", &entids::DB_TX_INSTANT)], |row| -> Result<RawDatom> {
            Ok(RawDatom {
                e: row.get(0),
                a: row.get(1),
                v: TypedValue::from_sql_value_pair(row.get(2), row.get(5))?,
                tx: row.get(3),
                added: row.get(4),
                is_transaction: row.get(6),
            })
        })?.collect();

        // It's convenient to have a consistently ordered set of results,
        // so we use a sorting map.
        let mut txes_by_tx = BTreeMap::new();
        let mut tx_parts_by_tx = HashMap::new();

        // On first pass, build our Txes and TxParts for each.
        for datom_result in datoms {
            let datom = datom_result?;
            // Datom represents a transaction.
            if datom.is_transaction {
                txes_by_tx.insert(datom.tx, Tx {
                    tx: datom.tx,
                    tx_instant: datom.v,
                    parts: Vec::new(),
                });
            // Datom represents part of a transaction. Our query statement above guarantees
            // that we've already processed corresponding transaction at this point.
            } else {
                let parts = tx_parts_by_tx.entry(datom.tx).or_insert(Vec::new());
                parts.push(TxPart {
                    e: datom.e,
                    a: datom.a,
                    v: datom.v,
                    added: datom.added,
                });
            }
        }

        // On second pass, consume TxParts map and associate parts with corresponding Txes.
        for (e, tx_parts) in tx_parts_by_tx.into_iter() {
            if let Entry::Occupied(mut tx) = txes_by_tx.entry(e) {
                tx.get_mut().parts = tx_parts;
            } else {
                bail!(ErrorKind::UnexpectedState(format!("Missing transactions datoms for tx={:?}", e)));
            }
        }

        // Finally, consume the Tx map and a Vec of its values.
        Ok(txes_by_tx.into_iter().map(|(_, tx)| tx).collect())
    }
}
