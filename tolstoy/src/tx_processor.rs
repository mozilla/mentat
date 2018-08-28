// Copyright 2018 Mozilla
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.
use std::iter::Peekable;

use rusqlite;

use mentat_db::{
    TypedSQLValue,
};

use core_traits::{
    Entid,
    TypedValue,
};

use public_traits::errors::{
    Result,
};

use types::{
    TxPart,
};

/// Implementors must specify type of the "receiver report" which
/// they will produce once processor is finished.
pub trait TxReceiver<RR> {
    /// Called for each transaction, with an iterator over its datoms.
    fn tx<T: Iterator<Item=TxPart>>(&mut self, tx_id: Entid, d: &mut T) -> Result<()>;
    /// Called once processor is finished, consuming this receiver and producing a report.
    fn done(self) -> RR;
}

pub struct Processor {}

pub struct DatomsIterator<'dbtx, 't, T>
where T: Sized + Iterator<Item=Result<TxPart>> + 't {
    at_first: bool,
    at_last: bool,
    first: &'dbtx TxPart,
    rows: &'t mut Peekable<T>,
}

impl<'dbtx, 't, T> DatomsIterator<'dbtx, 't, T>
where T: Sized + Iterator<Item=Result<TxPart>> + 't {
    fn new(first: &'dbtx TxPart, rows: &'t mut Peekable<T>) -> DatomsIterator<'dbtx, 't, T>
    {
        DatomsIterator {
            at_first: true,
            at_last: false,
            first: first,
            rows: rows,
        }
    }
}

impl<'dbtx, 't, T> Iterator for DatomsIterator<'dbtx, 't, T>
where T: Sized + Iterator<Item=Result<TxPart>> + 't {
    type Item = TxPart;

    fn next(&mut self) -> Option<Self::Item> {
        if self.at_last {
            return None;
        }

        if self.at_first {
            self.at_first = false;
            return Some(self.first.clone());
        }

        // Look ahead to see if we're about to cross into
        // the next partition.
        {
            let next_option = self.rows.peek();
            match next_option {
                Some(&Ok(ref next)) => {
                    if next.tx != self.first.tx {
                        self.at_last = true;
                        return None;
                    }
                },
                // Empty, or error. Either way, this iterator's done.
                _ => {
                    self.at_last = true;
                    return None;
                }
            }
        }

        // We're in the correct partition, return a TxPart.
        if let Some(result) = self.rows.next() {
            match result {
                Err(_) => None,
                Ok(datom) => {
                    Some(TxPart {
                        partitions: None,
                        e: datom.e,
                        a: datom.a,
                        v: datom.v.clone(),
                        tx: datom.tx,
                        added: datom.added,
                    })
                },
            }
        } else {
            self.at_last = true;
            None
        }
    }
}

fn to_tx_part(row: &rusqlite::Row) -> Result<TxPart> {
    Ok(TxPart {
        partitions: None,
        e: row.get_checked(0)?,
        a: row.get_checked(1)?,
        v: TypedValue::from_sql_value_pair(row.get_checked(2)?, row.get_checked(3)?)?,
        tx: row.get_checked(4)?,
        added: row.get_checked(5)?,
    })
}

impl Processor {
    pub fn process<RR, R: TxReceiver<RR>>
        (sqlite: &rusqlite::Transaction, from_tx: Option<Entid>, mut receiver: R) -> Result<RR> {

        let tx_filter = match from_tx {
            Some(tx) => format!(" WHERE timeline = 0 AND tx > {} ", tx),
            None => format!("WHERE timeline = 0")
        };
        let select_query = format!("SELECT e, a, v, value_type_tag, tx, added FROM timelined_transactions {} ORDER BY tx", tx_filter);
        let mut stmt = sqlite.prepare(&select_query)?;

        let mut rows = stmt.query_and_then(&[], to_tx_part)?.peekable();

        // Walk the transaction table, keeping track of the current "tx".
        // Whenever "tx" changes, construct a datoms iterator and pass it to the receiver.
        // NB: this logic depends on data coming out of the rows iterator to be sorted by "tx".
        let mut current_tx = None;
        while let Some(row) = rows.next() {
            let datom = row?;

            match current_tx {
                Some(tx) => {
                    if tx != datom.tx {
                        current_tx = Some(datom.tx);
                        receiver.tx(
                            datom.tx,
                            &mut DatomsIterator::new(&datom, &mut rows)
                        )?;
                    }
                },
                None => {
                    current_tx = Some(datom.tx);
                    receiver.tx(
                        datom.tx,
                        &mut DatomsIterator::new(&datom, &mut rows)
                    )?;
                }
            }
        }
        // Consume the receiver, letting it produce a "receiver report"
        // as defined by generic type RR.
        Ok(receiver.done())
    }
}
