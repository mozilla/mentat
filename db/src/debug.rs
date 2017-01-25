// Copyright 2016 Mozilla
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

#![allow(dead_code)]

/// Low-level functions for testing.

use rusqlite;

use {to_namespaced_keyword};
use edn::types::{Value};
use mentat_tx::entities::{Entid};
use types::{DB, TypedValue};
use errors::Result;

/// Represents an assertion (*datom*) in the store.
#[derive(Clone,Debug,Eq,Hash,Ord,PartialOrd,PartialEq)]
pub struct Datom {
    // TODO: generalize this.
    e: Entid,
    a: Entid,
    v: Value,
    tx: Option<i64>,
}

/// Return the complete set of datoms in the store, ordered by (e, a, v).
pub fn datoms(conn: &rusqlite::Connection, db: &DB) -> Result<Vec<Datom>> {
    // TODO: fewer magic numbers!
    datoms_after(conn, db, &0x10000000)
}

/// Return the set of datoms in the store with transaction ID strictly
/// greater than the given `tx`, ordered by (tx, e, a, v).
pub fn datoms_after(conn: &rusqlite::Connection, db: &DB, tx: &i32) -> Result<Vec<Datom>> {
    let mut stmt: rusqlite::Statement = conn.prepare("SELECT e, a, v, value_type_tag FROM datoms WHERE tx > ? ORDER BY tx, e, a, v")?;

    // Convert numeric entid to entity Entid.
    let to_entid = |x| {
        db.schema.get_ident(&x).and_then(|y| to_namespaced_keyword(&y)).map(Entid::Ident).unwrap_or(Entid::Entid(x))
    };

    let datoms = stmt.query_and_then(&[tx], |row| {
        let e: i64 = row.get_checked(0)?;
        let a: i64 = row.get_checked(1)?;
        let v: rusqlite::types::Value = row.get_checked(2)?;
        let value_type_tag: i32 = row.get_checked(3)?;

        let typed_value = TypedValue::from_sql_value_pair(&v, &value_type_tag)?;
        let (value, _) = typed_value.to_edn_value_pair();

        Ok(Datom {
            e: to_entid(e),
            a: to_entid(a),
            v: value,
            tx: None,
        })
    })?.collect();
    datoms
}
