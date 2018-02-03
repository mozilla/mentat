// Copyright 2016 Mozilla
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

// use itertools::structs::Batching;
use itertools::Itertools;

use rusqlite;

use errors::{
    Result,
};

use mentat_db::{
    entids,
    TypedSQLValue,
};

use mentat_core::{
    TypedValue,
    Entid,
};

#[derive(Debug, Clone)]
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
#[derive(Debug, Clone)]
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
}

// type TxIter<I, F> = Batching<I, F>;

pub trait TxReader {
    fn all(sqlite: &rusqlite::Connection) -> Result<Vec<Tx>>;
}

pub struct TxClient {}

impl TxReader for TxClient {
    // TODO what should a type signature look like for this to return the
    // batching iterator?
    fn all(sqlite: &rusqlite::Connection) -> Result<Vec<Tx>> {
        let mut stmt = sqlite.prepare(
            "SELECT e, a, v, tx, added, value_type_tag FROM transactions ORDER BY tx"
        )?;

        let row_iterator = stmt.query_and_then(&[], |row| -> Result<RawDatom> {
            Ok(RawDatom {
                e: row.get(0),
                a: row.get(1),
                v: TypedValue::from_sql_value_pair(row.get(2), row.get(5))?,
                tx: row.get(3),
                added: row.get(4),
            })
        })?;

        let txes_iterator = row_iterator.batching(|rows| {
            let mut has_next_tx = false;
            let mut next_tx: Option<Box<Tx>> = None;

            // Our rows are partitioned into batches that represent transactions,
            // thanks to ORDER BY clause above. We come up with a transaction by
            // iterating through rows until we have its full representation.
            // TODO place limits to safeguard against bad data?
            loop {
                if let Some(datom) = rows.next() {
                    let datom = match datom {
                        Ok(d) => d,
                        Err(_) => break // TODO propagate error from from_sql_value_pair above
                    };
                    let part = TxPart {
                        e: datom.e,
                        a: datom.a,
                        v: datom.v.clone(),
                        added: datom.added,
                    };
                    if !has_next_tx {
                        next_tx = Some(Box::new(Tx {
                            tx: datom.tx,
                            tx_instant: datom.v.clone(),
                            parts: vec![part],
                        }));
                        has_next_tx = true;
                    } else {
                        // Datom represents a transaction, we're done with this chunk of rows.
                        if datom.a == entids::DB_TX_INSTANT && datom.tx == datom.e {
                            match next_tx {
                                Some(ref mut t) => {t.tx_instant = part.v;},
                                None => break // TODO bad state
                            }
                            break;
                        // Datom represents a transaction part - take a note of it, continue iterating.
                        } else {
                            match next_tx {
                                Some(ref mut t) => {t.parts.push(part);},
                                None => break // TODO bad state
                            }
                        }
                    }
                } else {
                    break;
                }
            }

            // TODO due to TODOs above, this is ambiguous:
            // either there's no transaction, or something went wrong!
            next_tx
        }).map(|t| *t);

        // TODO just return the iterator...
        Ok(txes_iterator.collect())
    }
}
