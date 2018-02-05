// // Copyright 2016 Mozilla
// //
// // Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// // this file except in compliance with the License. You may obtain a copy of the
// // License at http://www.apache.org/licenses/LICENSE-2.0
// // Unless required by applicable law or agreed to in writing, software distributed
// // under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// // CONDITIONS OF ANY KIND, either express or implied. See the License for the
// // specific language governing permissions and limitations under the License.

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

#[derive(Debug, Clone)]
pub struct Tx {
    pub tx: Entid,
    pub tx_instant: TypedValue,
}

pub trait TxReceiver {
    fn tx(&mut self, tx_id: Entid, d: &mut DatomsIterator) -> Result<()>;
    fn done(&mut self) -> Result<()>;
}

pub struct Processor {}

struct RawDatom {
    e: Entid,
    a: Entid,
    v: TypedValue, // composite of 'v' and 'value_type_tag'
    tx: Entid,
    added: bool,
}

pub struct DatomsIterator<'conn> {
    at_first: bool,
    at_last: bool,
    first: &'conn RawDatom,
    rows: &'conn mut Iterator<Item=Result<RawDatom>>,
}

impl<'conn> DatomsIterator<'conn> {
    fn new(first: &'conn RawDatom, rows: &'conn mut Iterator<Item=Result<RawDatom>>) -> DatomsIterator<'conn> {
        DatomsIterator {
            at_first: true,
            at_last: false,
            first: first,
            rows: rows
        }
    }
}

impl<'conn> Iterator for DatomsIterator<'conn> {
    type Item = Result<TxPart>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.at_last {
            return None;
        }

        if self.at_first {
            self.at_first = false;
            return Some(Ok(TxPart {
                e: self.first.e,
                a: self.first.a,
                v: self.first.v.clone(),
                added: self.first.added,
            }));
        }

        if let Some(row) = self.rows.next() {
            let datom = match row {
                Ok(r) => r,
                Err(e) => {
                    self.at_last = true;
                    return Some(Err(e));
                }
            };

            if datom.a == entids::DB_TX_INSTANT && datom.tx == datom.e {
                self.at_last = true;
            }

            return Some(Ok(TxPart {
                e: datom.e,
                a: datom.a,
                v: datom.v.clone(),
                added: datom.added,
            }));
        } else {
            self.at_last = true;
            return None;
        }
    }
}

impl Processor {
    pub fn process(sqlite: &rusqlite::Connection, receiver: &mut TxReceiver) -> Result<()> {
        let mut stmt = sqlite.prepare(
            "SELECT e, a, v, tx, added, value_type_tag FROM transactions ORDER BY tx"
        )?;

        let mut rows = stmt.query_and_then(&[], |row| -> Result<RawDatom> {
            Ok(RawDatom {
                e: row.get(0),
                a: row.get(1),
                v: TypedValue::from_sql_value_pair(row.get(2), row.get(5))?,
                tx: row.get(3),
                added: row.get(4),
            })
        })?;

        let mut current_tx = None;

        while let Some(row) = rows.next() {
            let datom = row?;
            
            match current_tx {
                Some(tx) => {
                    if tx == datom.tx {
                        continue;
                    } else {
                        current_tx = Some(datom.tx);
                        receiver.tx(
                            datom.tx,
                            &mut DatomsIterator::new(&datom, &mut rows)
                        )?;
                        continue;
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
        receiver.done()?;
        Ok(())
    }
}
