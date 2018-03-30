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

use std::collections::BTreeMap;

use mentat::conn::Conn;

use mentat::new_connection;
use mentat_tolstoy::tx_processor::{
    Processor,
    TxReceiver,
    TxPart,
};
use mentat_tolstoy::errors::Result;
use mentat_core::{
    Entid,
    TypedValue,
    ValueType,
};

struct TxCountingReceiver {
    pub tx_count: usize,
    pub is_done: bool,
}

impl TxCountingReceiver {
    fn new() -> TxCountingReceiver {
        TxCountingReceiver {
            tx_count: 0,
            is_done: false,
        }
    }
}

impl TxReceiver for TxCountingReceiver {
    fn tx<T>(&mut self, _tx_id: Entid, _d: &mut T) -> Result<()>
        where T: Iterator<Item=TxPart> {
        self.tx_count = self.tx_count + 1;
        Ok(())
    }

    fn done(&mut self) -> Result<()> {
        self.is_done = true;
        Ok(())
    }
}

#[derive(Debug)]
struct TestingReceiver {
    pub txes: BTreeMap<Entid, Vec<TxPart>>,
    pub is_done: bool,
}

impl TestingReceiver {
    fn new() -> TestingReceiver {
        TestingReceiver {
            txes: BTreeMap::new(),
            is_done: false,
        }
    }
}

impl TxReceiver for TestingReceiver {
    fn tx<T>(&mut self, tx_id: Entid, d: &mut T) -> Result<()>
        where T: Iterator<Item=TxPart> {
        let datoms = self.txes.entry(tx_id).or_insert(vec![]);
        datoms.extend(d);
        Ok(())
    }

    fn done(&mut self) -> Result<()> {
        self.is_done = true;
        Ok(())
    }
}

fn assert_tx_datoms_count(receiver: &TestingReceiver, tx_num: usize, expected_datoms: usize) {
    let tx = receiver.txes.keys().nth(tx_num).expect("first tx");
    let datoms = receiver.txes.get(tx).expect("datoms");
    assert_eq!(expected_datoms, datoms.len());
}

#[test]
fn test_reader() {
    let mut c = new_connection("").expect("Couldn't open conn.");
    let mut conn = Conn::connect(&mut c).expect("Couldn't open DB.");
    {
        let db_tx = c.transaction().expect("db tx");
        // Don't inspect the bootstrap transaction, but we'd like to see it's there.
        let mut receiver = TxCountingReceiver::new();
        assert_eq!(false, receiver.is_done);
        Processor::process(&db_tx, None, &mut receiver).expect("processor");
        assert_eq!(true, receiver.is_done);
        assert_eq!(1, receiver.tx_count);
    }

    let ids = conn.transact(&mut c, r#"[
        [:db/add "s" :db/ident :foo/numba]
        [:db/add "s" :db/valueType :db.type/long]
        [:db/add "s" :db/cardinality :db.cardinality/one]
    ]"#).expect("successful transaction").tempids;
    let numba_entity_id = ids.get("s").unwrap();

    let bootstrap_tx;
    {
        let db_tx = c.transaction().expect("db tx");
        // Expect to see one more transaction of four parts (one for tx datom itself).
        let mut receiver = TestingReceiver::new();
        Processor::process(&db_tx, None, &mut receiver).expect("processor");

        println!("{:#?}", receiver);

        assert_eq!(2, receiver.txes.keys().count());
        assert_tx_datoms_count(&receiver, 1, 4);

        bootstrap_tx = Some(*receiver.txes.keys().nth(0).expect("bootstrap tx"));
    }

    let ids = conn.transact(&mut c, r#"[
        [:db/add "b" :foo/numba 123]
    ]"#).expect("successful transaction").tempids;
    let asserted_e = ids.get("b").unwrap();

    {
        let db_tx = c.transaction().expect("db tx");

        // Expect to see a single two part transaction
        let mut receiver = TestingReceiver::new();

        // Note that we're asking for the bootstrap tx to be skipped by the processor.
        Processor::process(&db_tx, bootstrap_tx, &mut receiver).expect("processor");

        assert_eq!(2, receiver.txes.keys().count());
        assert_tx_datoms_count(&receiver, 1, 2);

        // Inspect the transaction part.
        let tx_id = receiver.txes.keys().nth(1).expect("tx");
        let datoms = receiver.txes.get(tx_id).expect("datoms");
        let part = &datoms[0];

        assert_eq!(asserted_e, &part.e);
        assert_eq!(numba_entity_id, &part.a);
        assert!(part.v.matches_type(ValueType::Long));
        assert_eq!(TypedValue::Long(123), part.v);
        assert_eq!(true, part.added);
    }
}
