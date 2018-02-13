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

use errors::{
    Result,
};

use mentat_db::{
    TypedSQLValue,
};

use mentat_core::{
    Entid,
    TypedValue,
};

#[derive(Debug,Clone,Serialize,Deserialize)]
pub struct TxPart {
    pub e: Entid,
    pub a: Entid,
    pub v: TypedValue,
    pub tx: Entid,
    pub added: bool,
}

pub trait TxReceiver {
    fn tx<T>(&mut self, tx_id: Entid, d: &mut T) -> Result<()>
        where T: Iterator<Item=TxPart>;
    fn done(&mut self) -> Result<()>;
}

pub struct Processor {}

pub struct DatomsIterator<'dbtx, 't, T>
where T: Sized + Iterator<Item=Result<TxPart>> + 't {
    at_first: bool,
    at_last: bool,
    first: &'dbtx TxPart,
    rows: &'t mut Peekable<T>,
}

use std::os::raw::c_char;
use std::os::raw::c_int;
use std::ffi::CString;
pub const ANDROID_LOG_DEBUG: i32 = 3;
#[cfg(all(target_os="android", not(test)))]
extern { pub fn __android_log_write(prio: c_int, tag: *const c_char, text: *const c_char) -> c_int; }

#[cfg(all(target_os="android", not(test)))]
pub fn d(message: &str) {
    let tag = "mentat_db::tx_processor";
    let message = CString::new(message).unwrap();
    let message = message.as_ptr();
    let tag = CString::new(tag).unwrap();
    let tag = tag.as_ptr();
    unsafe { __android_log_write(ANDROID_LOG_DEBUG, tag, message) };
}

#[cfg(all(not(target_os="android")))]
pub fn d(message: &str) {
    let tag = "mentat_db::tx_processor";
    println!("d: {}: {}", tag, message);
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
        e: row.get(0),
        a: row.get(1),
        v: TypedValue::from_sql_value_pair(row.get(2), row.get(3))?,
        tx: row.get(4),
        added: row.get(5),
    })
}

impl Processor {
    pub fn process<R>(sqlite: &rusqlite::Transaction, from_tx: Option<Entid>, receiver: &mut R) -> Result<()>
    where R: TxReceiver {
        let tx_filter = match from_tx {
            Some(tx) => format!("WHERE tx > {}", tx),
            None => format!("")
        };
        // If no 'from_tx' is provided, get everything but skip over the first (bootstrap) transaction.
        let skip_first_tx = match from_tx {
            Some(_) => false,
            None => true
        };
        let select_query = format!("SELECT e, a, v, value_type_tag, tx, added FROM transactions {} ORDER BY tx", tx_filter);
        let mut stmt = sqlite.prepare(&select_query)?;

        let mut rows = stmt.query_and_then(&[], to_tx_part)?.peekable();
        let mut at_tx = 1;
        let mut current_tx = None;

        while let Some(row) = rows.next() {
            let datom = row?;
            d(&format!("datom! {:?}", datom));
            match current_tx {
                Some(tx) => {
                    if tx != datom.tx {
                        d(&format!("new tx! {:?}", datom.tx));
                        at_tx = at_tx + 1;
                        d(&format!("at_tx! {:?}", at_tx));
                        current_tx = Some(datom.tx);
                        if at_tx <= 2 && skip_first_tx {
                            d(&format!("skipping subsequent"));
                            continue;
                        }
                        d(&format!("Some: calling receiver.tx"));
                        receiver.tx(
                            datom.tx,
                            &mut DatomsIterator::new(&datom, &mut rows)
                        )?;
                        d(&format!("Some: returned from receiver.tx"));
                    } else {
                        d(&format!("skipping over datom in current tx block"));
                    }
                },
                None => {
                    current_tx = Some(datom.tx);
                    if at_tx <= 3 && skip_first_tx {
                        d(&format!("skipping first"));
                        continue;
                    }
                    d(&format!("None: calling receiver.tx"));
                    receiver.tx(
                        datom.tx,
                        &mut DatomsIterator::new(&datom, &mut rows)
                    )?;
                }
            }
        }
        d(&format!("calling receiver.done"));
        receiver.done()?;
        Ok(())
    }
}

