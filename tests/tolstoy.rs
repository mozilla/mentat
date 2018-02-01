// Copyright 2016 Mozilla
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

extern crate mentat;
extern crate mentat_core;
extern crate mentat_tolstoy;

use mentat::conn::Conn;

use mentat::new_connection;
use mentat_tolstoy::tx_client::{
    TxReader,
    TxClient
};
use mentat_core::{
    ValueType,
    TypedValue
};

#[test]
fn test_reader() {
    let mut c = new_connection("").expect("Couldn't open conn.");
    let mut conn = Conn::connect(&mut c).expect("Couldn't open DB.");

    let txes = TxClient::all(&c).expect("bootstrap transactions");

    // Don't inspect the bootstrap, but we'd like to see it's there.
    assert_eq!(1, txes.len());
    assert_eq!(76, txes[0].parts.len());

    let ids = conn.transact(&mut c, r#"[
        [:db/add "s" :db/ident :foo/numba]
        [:db/add "s" :db/valueType :db.type/long]
        [:db/add "s" :db/cardinality :db.cardinality/one]
    ]"#).expect("successful transaction").tempids;
    let numba_entity_id = ids.get("s").unwrap();

    let txes = TxClient::all(&c).expect("got transactions");

    // Expect to see one more transaction of three parts.
    assert_eq!(2, txes.len());
    assert_eq!(3, txes[1].parts.len());

    println!("{:?}", txes[1]);

    let ids = conn.transact(&mut c, r#"[
        [:db/add "b" :foo/numba 123]
    ]"#).expect("successful transaction").tempids;
    let asserted_e = ids.get("b").unwrap();

    let txes = TxClient::all(&c).expect("got transactions");

    // Expect to see a single part transactions
    // TODO verify that tx itself looks sane
    assert_eq!(3, txes.len());
    assert_eq!(1, txes[2].parts.len());

    // Inspect the transaction part.
    let part = &txes[2].parts[0];

    assert_eq!(asserted_e, &part.e);
    assert_eq!(numba_entity_id, &part.a);
    assert!(part.v.matches_type(ValueType::Long));
    assert_eq!(TypedValue::Long(123), part.v);
    assert_eq!(true, part.added);

    // TODO retractions
}
