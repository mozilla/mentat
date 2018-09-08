// Copyright 2018 Mozilla
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

// TODO could hide this behind #[cfg(test)], since this is only for test use.

use rusqlite;

use uuid::Uuid;

use edn::entities::{
    EntidOrIdent,
};

use core_traits::{
    Entid,
    TypedValue,
};

use mentat_core::{
    HasSchema,
    Schema,
};

use mentat_db::{
    TypedSQLValue,
};

use mentat_db::debug::{
    Datom,
    Datoms,
    transactions_after,
};

use types::{
    Tx,
    TxPart,
};

/// A rough equivalent of mentat_db::debug::transactions_after
/// for Tolstoy's Tx type.
pub fn txs_after(sqlite: &rusqlite::Connection, schema: &Schema, after: Entid) -> Vec<Tx> {
    let transactions = transactions_after(
        sqlite, schema, after
    ).expect("remote transactions");
    
    let mut txs = vec![];

    for transaction in transactions.0 {
        let mut tx = Tx {
            tx: Uuid::new_v4(),
            parts: vec![],
        };

        for datom in &transaction.0 {
            let e = match datom.e {
                EntidOrIdent::Entid(ref e) => *e,
                _ => panic!(),
            };
            let a = match datom.a {
                EntidOrIdent::Entid(ref a) => *a,
                EntidOrIdent::Ident(ref a) => schema.get_entid(a).unwrap().0,
            };

            tx.parts.push(TxPart {
                partitions: None,
                e: e,
                a: a,
                v: TypedValue::from_edn_value(&datom.v).unwrap(),
                tx: datom.tx,
                added: datom.added.unwrap()
            });
        }

        txs.push(tx);
    }

    txs
}

pub fn part_to_datom(schema: &Schema, part: &TxPart) -> Datom {
    Datom {
        e: match schema.get_ident(part.e) {
            Some(ident) => EntidOrIdent::Ident(ident.clone()),
            None => EntidOrIdent::Entid(part.e),
        },
        a: match schema.get_ident(part.a) {
            Some(ident) => EntidOrIdent::Ident(ident.clone()),
            None => EntidOrIdent::Entid(part.a),
        },
        v: TypedValue::to_edn_value_pair(&part.v).0,
        tx: part.tx,
        added: Some(part.added),
    }
}

pub fn parts_to_datoms(schema: &Schema, parts: &Vec<TxPart>) -> Datoms {
    Datoms(parts.iter().map(|p| part_to_datom(schema, p)).collect())
}
