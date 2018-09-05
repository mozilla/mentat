// Copyright 2016 Mozilla
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

extern crate uuid;
extern crate mentat;
extern crate edn;
extern crate core_traits;
extern crate public_traits;

extern crate log;
#[macro_use] extern crate mentat_db;

#[cfg(feature = "syncable")]
extern crate mentat_tolstoy;

#[cfg(feature = "syncable")]
extern crate tolstoy_traits;

// Run with 'cargo test tolstoy_tests' from top-level.
#[cfg(feature = "syncable")]
mod tolstoy_tests {
    use std::collections::HashMap;
    use std::collections::BTreeMap;

    use std::collections::hash_map::Entry;

    use std::borrow::Borrow;

    use edn;

    use uuid::Uuid;

    use mentat::conn::Conn;

    use mentat::new_connection;

    use mentat_db::TX0;

    use mentat_tolstoy::{
        Tx,
        TxPart,
        GlobalTransactionLog,
        SyncReport,
        SyncFollowup,
        Syncer,
    };

    use mentat_tolstoy::debug::{
        parts_to_datoms,
        txs_after,
    };

    use mentat_tolstoy::tx_processor::{
        Processor,
        TxReceiver,
    };
    use core_traits::{
        Entid,
        TypedValue,
        ValueType,
    };
    use public_traits::errors::{
        Result,
        MentatError,
    };
    use tolstoy_traits::errors::{
        TolstoyError,
    };

    struct TxCountingReceiver {
        tx_count: usize,
    }

    impl TxCountingReceiver {
        fn new() -> TxCountingReceiver {
            TxCountingReceiver {
                tx_count: 0,
            }
        }
    }

    impl TxReceiver<usize> for TxCountingReceiver {
        fn tx<T>(&mut self, _tx_id: Entid, _d: &mut T) -> Result<()>
            where T: Iterator<Item=TxPart> {
            self.tx_count = self.tx_count + 1;
            Ok(())
        }

        fn done(self) -> usize {
            self.tx_count
        }
    }

    #[derive(Debug)]
    struct TestingReceiver {
        txes: BTreeMap<Entid, Vec<TxPart>>,
    }

    impl TestingReceiver {
        fn new() -> TestingReceiver {
            TestingReceiver {
                txes: BTreeMap::new(),
            }
        }
    }

    impl TxReceiver<BTreeMap<Entid, Vec<TxPart>>> for TestingReceiver {
        fn tx<T>(&mut self, tx_id: Entid, d: &mut T) -> Result<()>
            where T: Iterator<Item=TxPart> {
            let datoms = self.txes.entry(tx_id).or_insert(vec![]);
            datoms.extend(d);
            Ok(())
        }

        fn done(self) -> BTreeMap<Entid, Vec<TxPart>> {
            self.txes
        }
    }

    fn assert_tx_datoms_count(txes: &BTreeMap<Entid, Vec<TxPart>>, tx_num: usize, expected_datoms: usize) {
        let tx = txes.keys().nth(tx_num).expect("first tx");
        let datoms = txes.get(tx).expect("datoms");
        assert_eq!(expected_datoms, datoms.len());
    }

    #[derive(Debug)]
    struct TestRemoteClient {
        pub head: Uuid,
        pub chunks: HashMap<Uuid, TxPart>,
        pub transactions: HashMap<Uuid, Vec<TxPart>>,
        // Keep transactions in order:
        pub tx_rowid: HashMap<Uuid, usize>,
        pub rowid_tx: Vec<Uuid>,
    }

    impl TestRemoteClient {
        fn new() -> TestRemoteClient {
            TestRemoteClient {
                head: Uuid::nil(),
                chunks: HashMap::default(),
                transactions: HashMap::default(),
                tx_rowid: HashMap::default(),
                rowid_tx: vec![],
            }
        }
    }

    impl GlobalTransactionLog for TestRemoteClient {
        fn head(&self) -> Result<Uuid> {
            Ok(self.head)
        }

        fn transactions_after(&self, tx: &Uuid) -> Result<Vec<Tx>> {
            let rowid_range;
            if tx == &Uuid::nil() {
                rowid_range = 0..;
            } else {
                rowid_range = self.tx_rowid[tx] + 1 ..;
            }

            let mut txs = vec![];
            for tx_uuid in &self.rowid_tx[rowid_range] {
                txs.push(Tx {
                    tx: tx_uuid.clone(),
                    parts: self.transactions.get(tx_uuid).unwrap().clone(),
                });
            }
            Ok(txs)
        }

        fn set_head(&mut self, tx: &Uuid) -> Result<()> {
            self.head = tx.clone();
            Ok(())
        }

        fn put_chunk(&mut self, tx: &Uuid, payload: &TxPart) -> Result<()> {
            match self.chunks.entry(tx.clone()) {
                Entry::Occupied(_) => panic!("trying to overwrite chunk"),
                Entry::Vacant(entry) => {
                    entry.insert(payload.clone());
                    ()
                },
            }
            Ok(())
        }

        fn put_transaction(&mut self, tx: &Uuid, _parent_tx: &Uuid, chunk_txs: &Vec<Uuid>) -> Result<()> {
            let mut parts = vec![];
            for chunk_tx in chunk_txs {
                parts.push(self.chunks.get(chunk_tx).unwrap().clone());
            }
            self.transactions.insert(tx.clone(), parts);
            self.rowid_tx.push(tx.clone());
            self.tx_rowid.insert(tx.clone(), self.rowid_tx.len() - 1);
            Ok(())
        }
    }

    macro_rules! assert_sync {
        ( $report: pat, $conn: expr, $sqlite: expr, $remote: expr ) => {{
            let mut ip = $conn.begin_transaction(&mut $sqlite).expect("begun successfully");
            match Syncer::sync(&mut ip, &mut $remote).expect("sync report") {
                $report => (),
                wr => panic!("Wrong sync report: {:?}", wr),
            }
            ip.commit().expect("committed");
        }};
        ( error => $error: pat, $conn: expr, $sqlite: expr, $remote: expr ) => {{
            let mut ip = $conn.begin_transaction(&mut $sqlite).expect("begun successfully");
            match Syncer::sync(&mut ip, &mut $remote).expect_err("expected sync to fail, but did not") {
                $error => (),
                we => panic!("Failed with wrong error: {:?}", we),
            }
        }};
    }

    macro_rules! assert_transactions {
        ($sqlite:expr, $conn:expr, $($tx:expr),+) => {
            let txs = txs_after(&$sqlite, &$conn.current_schema(), TX0);

            let mut index = 1;
            $(
                assert_matches!(parts_to_datoms(&$conn.current_schema(), &txs[index].parts), $tx);
                index = index + 1;
            )*

            assert_eq!(index, txs.len());
        };

        ($sqlite:expr, $conn:expr, schema => $schema:expr, $($tx:expr),*) => {
            let txs = txs_after(&$sqlite, &$conn.current_schema(), TX0);

            // Schema assumed to be first transaction.
            assert_matches!(parts_to_datoms(&$conn.current_schema(), &txs[0].parts), $schema);

            let mut index = 1;
            $(
                assert_matches!(parts_to_datoms(&$conn.current_schema(), &txs[index].parts), $tx);
                index = index + 1;
            )*

            assert_eq!(index, txs.len());
        };
    }

    #[test]
    fn test_reader() {
        let mut c = new_connection("").expect("Couldn't open conn.");
        let mut conn = Conn::connect(&mut c).expect("Couldn't open DB.");
        {
            let db_tx = c.transaction().expect("db tx");
            // Ensure that we see a bootstrap transaction.
            assert_eq!(1, Processor::process(
                &db_tx, None, TxCountingReceiver::new()
            ).expect("processor"));
        }

        let ids = conn.transact(&mut c, r#"[
            [:db/add "s" :db/ident :foo/numba]
            [:db/add "s" :db/valueType :db.type/long]
            [:db/add "s" :db/cardinality :db.cardinality/one]
        ]"#).expect("successful transaction").tempids;
        let numba_entity_id = ids.get("s").unwrap();

        let ids = conn.transact(&mut c, r#"[
            [:db/add "b" :foo/numba 123]
        ]"#).expect("successful transaction").tempids;
        let _asserted_e = ids.get("b").unwrap();

        let first_tx;
        {
            let db_tx = c.transaction().expect("db tx");
            // Expect to see one more transaction of four parts (one for tx datom itself).
            let receiver = TestingReceiver::new();
            let txes = Processor::process(&db_tx, None, receiver).expect("processor");

            println!("{:#?}", txes);

            // Three transactions: bootstrap, vocab, assertion.
            assert_eq!(3, txes.keys().count());
            assert_tx_datoms_count(&txes, 2, 2);

            first_tx = txes.keys().nth(1).expect("first non-bootstrap tx").clone();
        }

        let ids = conn.transact(&mut c, r#"[
            [:db/add "b" :foo/numba 123]
        ]"#).expect("successful transaction").tempids;
        let asserted_e = ids.get("b").unwrap();

        {
            let db_tx = c.transaction().expect("db tx");

            // Expect to see a single two part transaction
            let receiver = TestingReceiver::new();

            // Note that we're asking for the first transacted tx to be skipped by the processor.
            let txes = Processor::process(&db_tx, Some(first_tx), receiver).expect("processor");

            // Vocab, assertion.
            assert_eq!(2, txes.keys().count());
            // Assertion datoms.
            assert_tx_datoms_count(&txes, 1, 2);

            // Inspect the assertion.
            let tx_id = txes.keys().nth(1).expect("tx");
            let datoms = txes.get(tx_id).expect("datoms");
            let part = datoms.iter().find(|&part| &part.e == asserted_e).expect("to find asserted datom");

            assert_eq!(numba_entity_id, &part.a);
            assert!(part.v.matches_type(ValueType::Long));
            assert_eq!(TypedValue::Long(123), part.v);
            assert_eq!(true, part.added);
        }
    }

    #[test]
    fn test_bootstrap_upload() {
        let mut sqlite = new_connection("").unwrap();
        let mut conn = Conn::connect(&mut sqlite).unwrap();
        let mut remote_client = TestRemoteClient::new();

        // Fast forward empty remote with a bootstrap transaction.
        assert_sync!(SyncReport::RemoteFastForward, conn, sqlite, remote_client);

        let bootstrap_tx_parts = remote_client.transactions.get(&remote_client.rowid_tx[0]).unwrap();

        assert_matches!(parts_to_datoms(&conn.current_schema(), &bootstrap_tx_parts), "[
            [:db.schema/core :db.schema/attribute 1 ?tx true]
            [:db.schema/core :db.schema/attribute 3 ?tx true]
            [:db.schema/core :db.schema/attribute 4 ?tx true]
            [:db.schema/core :db.schema/attribute 5 ?tx true]
            [:db.schema/core :db.schema/attribute 6 ?tx true]
            [:db.schema/core :db.schema/attribute 7 ?tx true]
            [:db.schema/core :db.schema/attribute 8 ?tx true]
            [:db.schema/core :db.schema/attribute 9 ?tx true]
            [:db.schema/core :db.schema/attribute 10 ?tx true]
            [:db.schema/core :db.schema/attribute 11 ?tx true]
            [:db.schema/core :db.schema/attribute 12 ?tx true]
            [:db.schema/core :db.schema/attribute 13 ?tx true]
            [:db.schema/core :db.schema/attribute 22 ?tx true]
            [:db.schema/core :db.schema/attribute 37 ?tx true]
            [:db.schema/core :db.schema/attribute 38 ?tx true]
            [:db.schema/core :db.schema/attribute 39 ?tx true]
            [:db/ident :db/ident :db/ident ?tx true]
            [:db.part/db :db/ident :db.part/db ?tx true]
            [:db/txInstant :db/ident :db/txInstant ?tx true]
            [:db.install/partition :db/ident :db.install/partition ?tx true]
            [:db.install/valueType :db/ident :db.install/valueType ?tx true]
            [:db.install/attribute :db/ident :db.install/attribute ?tx true]
            [:db/valueType :db/ident :db/valueType ?tx true]
            [:db/cardinality :db/ident :db/cardinality ?tx true]
            [:db/unique :db/ident :db/unique ?tx true]
            [:db/isComponent :db/ident :db/isComponent ?tx true]
            [:db/index :db/ident :db/index ?tx true]
            [:db/fulltext :db/ident :db/fulltext ?tx true]
            [:db/noHistory :db/ident :db/noHistory ?tx true]
            [:db/add :db/ident :db/add ?tx true]
            [:db/retract :db/ident :db/retract ?tx true]
            [:db.part/user :db/ident :db.part/user ?tx true]
            [:db.part/tx :db/ident :db.part/tx ?tx true]
            [:db/excise :db/ident :db/excise ?tx true]
            [:db.excise/attrs :db/ident :db.excise/attrs ?tx true]
            [:db.excise/beforeT :db/ident :db.excise/beforeT ?tx true]
            [:db.excise/before :db/ident :db.excise/before ?tx true]
            [:db.alter/attribute :db/ident :db.alter/attribute ?tx true]
            [:db.type/ref :db/ident :db.type/ref ?tx true]
            [:db.type/keyword :db/ident :db.type/keyword ?tx true]
            [:db.type/long :db/ident :db.type/long ?tx true]
            [:db.type/double :db/ident :db.type/double ?tx true]
            [:db.type/string :db/ident :db.type/string ?tx true]
            [:db.type/uuid :db/ident :db.type/uuid ?tx true]
            [:db.type/uri :db/ident :db.type/uri ?tx true]
            [:db.type/boolean :db/ident :db.type/boolean ?tx true]
            [:db.type/instant :db/ident :db.type/instant ?tx true]
            [:db.type/bytes :db/ident :db.type/bytes ?tx true]
            [:db.cardinality/one :db/ident :db.cardinality/one ?tx true]
            [:db.cardinality/many :db/ident :db.cardinality/many ?tx true]
            [:db.unique/value :db/ident :db.unique/value ?tx true]
            [:db.unique/identity :db/ident :db.unique/identity ?tx true]
            [:db/doc :db/ident :db/doc ?tx true]
            [:db.schema/version :db/ident :db.schema/version ?tx true]
            [:db.schema/attribute :db/ident :db.schema/attribute ?tx true]
            [:db.schema/core :db/ident :db.schema/core ?tx true]
            [?tx :db/txInstant ?ms ?tx true]
            [:db/ident :db/valueType 24 ?tx true]
            [:db/txInstant :db/valueType 31 ?tx true]
            [:db.install/partition :db/valueType 23 ?tx true]
            [:db.install/valueType :db/valueType 23 ?tx true]
            [:db.install/attribute :db/valueType 23 ?tx true]
            [:db/valueType :db/valueType 23 ?tx true]
            [:db/cardinality :db/valueType 23 ?tx true]
            [:db/unique :db/valueType 23 ?tx true]
            [:db/isComponent :db/valueType 30 ?tx true]
            [:db/index :db/valueType 30 ?tx true]
            [:db/fulltext :db/valueType 30 ?tx true]
            [:db/noHistory :db/valueType 30 ?tx true]
            [:db.alter/attribute :db/valueType 23 ?tx true]
            [:db/doc :db/valueType 27 ?tx true]
            [:db.schema/version :db/valueType 25 ?tx true]
            [:db.schema/attribute :db/valueType 23 ?tx true]
            [:db/ident :db/cardinality 33 ?tx true]
            [:db/txInstant :db/cardinality 33 ?tx true]
            [:db.install/partition :db/cardinality 34 ?tx true]
            [:db.install/valueType :db/cardinality 34 ?tx true]
            [:db.install/attribute :db/cardinality 34 ?tx true]
            [:db/valueType :db/cardinality 33 ?tx true]
            [:db/cardinality :db/cardinality 33 ?tx true]
            [:db/unique :db/cardinality 33 ?tx true]
            [:db/isComponent :db/cardinality 33 ?tx true]
            [:db/index :db/cardinality 33 ?tx true]
            [:db/fulltext :db/cardinality 33 ?tx true]
            [:db/noHistory :db/cardinality 33 ?tx true]
            [:db.alter/attribute :db/cardinality 34 ?tx true]
            [:db/doc :db/cardinality 33 ?tx true]
            [:db.schema/version :db/cardinality 33 ?tx true]
            [:db.schema/attribute :db/cardinality 34 ?tx true]
            [:db/ident :db/unique 36 ?tx true]
            [:db.schema/attribute :db/unique 35 ?tx true]
            [:db/ident :db/index true ?tx true]
            [:db/txInstant :db/index true ?tx true]
            [:db.schema/attribute :db/index true ?tx true]
            [:db.schema/core :db.schema/version 1 ?tx true]]");
    }

    #[test]
    fn test_against_bootstrap() {
        let mut sqlite_1 = new_connection("").unwrap();
        let mut sqlite_2 = new_connection("").unwrap();

        let mut conn_1 = Conn::connect(&mut sqlite_1).unwrap();
        let mut conn_2 = Conn::connect(&mut sqlite_2).unwrap();

        let mut remote_client = TestRemoteClient::new();

        // Fast forward empty remote with a bootstrap transaction from 1.
        assert_sync!(SyncReport::RemoteFastForward, conn_1, sqlite_1, remote_client);

        // Merge 1 and 2 bootstrap transactions.
        assert_sync!(SyncReport::Merge(SyncFollowup::None), conn_2, sqlite_2, remote_client);

        // Assert that nothing besides a bootstrap transaction is present after a sync on 2.
        let synced_txs_2 = txs_after(&sqlite_2, &conn_2.current_schema(), TX0);
        assert_eq!(0, synced_txs_2.len());

        // Assert that 1's sync didn't affect remote.
        assert_sync!(SyncReport::NoChanges, conn_1, sqlite_1, remote_client);

        // Assert that nothing besides a bootstrap transaction is present after a sync on 1.
        let synced_txs_1 = txs_after(&sqlite_1, &conn_1.current_schema(), TX0);
        assert_eq!(0, synced_txs_1.len());
    }

    #[test]
    fn test_empty_merge() {
        let mut sqlite_1 = new_connection("").unwrap();
        let mut sqlite_2 = new_connection("").unwrap();

        let mut conn_1 = Conn::connect(&mut sqlite_1).unwrap();
        let mut conn_2 = Conn::connect(&mut sqlite_2).unwrap();

        let mut remote_client = TestRemoteClient::new();

        conn_1.transact(&mut sqlite_1, "[
            {:db/ident :person/name
              :db/valueType :db.type/string
              :db/cardinality :db.cardinality/one}]").expect("transacted");

        // Fast forward empty remote with a bootstrap and schema transactions from 1.
        assert_sync!(SyncReport::RemoteFastForward, conn_1, sqlite_1, remote_client);

        // Merge bootstrap+schema transactions from 1 into 2.
        assert_sync!(SyncReport::Merge(SyncFollowup::None), conn_2, sqlite_2, remote_client);

        // Assert that we end up with the same schema on 2 as we had on 1.
        assert_transactions!(sqlite_2, conn_2,
            schema =>
            "[[:person/name :db/ident :person/name ?tx true]
            [:person/name :db/valueType :db.type/string ?tx true]
            [:person/name :db/cardinality :db.cardinality/one ?tx true]
            [?tx :db/txInstant ?ms ?tx true]]",
        );

        // Assert that 2's sync didn't affect remote state.
        assert_sync!(SyncReport::NoChanges, conn_1, sqlite_1, remote_client);
    }

    #[test]
    fn test_non_conflicting_merge_exact() {
        let mut sqlite_1 = new_connection("").unwrap();
        let mut sqlite_2 = new_connection("").unwrap();

        let mut conn_1 = Conn::connect(&mut sqlite_1).unwrap();
        let mut conn_2 = Conn::connect(&mut sqlite_2).unwrap();

        let mut remote_client = TestRemoteClient::new();

        // Both 1 and 2 define the same schema.
        conn_1.transact(&mut sqlite_1, "[
            {:db/ident :person/name
              :db/valueType :db.type/string
              :db/cardinality :db.cardinality/one}]").expect("transacted");

        conn_2.transact(&mut sqlite_2, "[
            {:db/ident :person/name
              :db/valueType :db.type/string
              :db/cardinality :db.cardinality/one}]").expect("transacted");

        // Fast forward empty remote with a bootstrap and schema transactions.
        assert_sync!(SyncReport::RemoteFastForward, conn_1, sqlite_1, remote_client);

        // Merge bootstrap+schema transactions from 1 into 2.
        assert_sync!(SyncReport::Merge(SyncFollowup::None), conn_2, sqlite_2, remote_client);

        // Assert that 2's schema didn't change after sync.
        assert_transactions!(sqlite_2, conn_2,
            schema =>
            "[[:person/name :db/ident :person/name ?tx true]
            [:person/name :db/valueType :db.type/string ?tx true]
            [:person/name :db/cardinality :db.cardinality/one ?tx true]
            [?tx :db/txInstant ?ms ?tx true]]",
        );

        // Assert that 2's sync didn't change remote state.
        assert_sync!(SyncReport::NoChanges, conn_1, sqlite_1, remote_client);
    }

    #[test]
    fn test_non_conflicting_merge_subset() {
        let mut sqlite_1 = new_connection("").unwrap();
        let mut sqlite_2 = new_connection("").unwrap();

        let mut conn_1 = Conn::connect(&mut sqlite_1).unwrap();
        let mut conn_2 = Conn::connect(&mut sqlite_2).unwrap();

        let mut remote_client = TestRemoteClient::new();

        // Both 1 and 2 define the same schema.
        conn_1.transact(&mut sqlite_1, "[
            {:db/ident :person/name
              :db/valueType :db.type/string
              :db/cardinality :db.cardinality/one}]").expect("transacted");

        // But 1 also has an assertion against its schema.
        conn_1.transact(&mut sqlite_1, r#"[{:person/name "Ivan"}]"#).expect("transacted");

        conn_2.transact(&mut sqlite_2, "[
            {:db/ident :person/name
              :db/valueType :db.type/string
              :db/cardinality :db.cardinality/one}]").expect("transacted");

        // Fast forward empty remote with a bootstrap and schema transactions.
        assert_sync!(SyncReport::RemoteFastForward, conn_1, sqlite_1, remote_client);

        // Merge bootstrap+schema transactions from 1 into 2.
        assert_sync!(SyncReport::Merge(SyncFollowup::None), conn_2, sqlite_2, remote_client);

        assert_transactions!(sqlite_2, conn_2,
            // Assert that 2's schema is the same as before the sync.
            schema =>
            "[[:person/name :db/ident :person/name ?tx true]
            [:person/name :db/valueType :db.type/string ?tx true]
            [:person/name :db/cardinality :db.cardinality/one ?tx true]
            [?tx :db/txInstant ?ms ?tx true]]",

            // Assert that 2 has an additional transaction from 1 (name=Ivan).
            r#"[[?e :person/name "Ivan" ?tx true]
            [?tx :db/txInstant ?ms1 ?tx true]]"#
        );

        // Assert that 2's sync didn't change remote state.
        assert_sync!(SyncReport::NoChanges, conn_1, sqlite_1, remote_client);
    }

    #[test]
    fn test_schema_merge() {
        let mut sqlite_1 = new_connection("").unwrap();
        let mut sqlite_2 = new_connection("").unwrap();

        let mut conn_1 = Conn::connect(&mut sqlite_1).unwrap();
        let mut conn_2 = Conn::connect(&mut sqlite_2).unwrap();

        let mut remote_client = TestRemoteClient::new();

        // 1 defines a richer schema than 2.
        conn_1.transact(&mut sqlite_1, "[
            {:db/ident :person/name
              :db/valueType :db.type/string
              :db/cardinality :db.cardinality/one}
            {:db/ident :person/age
              :db/valueType :db.type/long
              :db/cardinality :db.cardinality/one}]").expect("transacted");

        conn_2.transact(&mut sqlite_2, "[
            {:db/ident :person/name
              :db/valueType :db.type/string
              :db/cardinality :db.cardinality/one}]").expect("transacted");

        // Fast forward empty remote with a bootstrap and schema transactions.
        assert_sync!(SyncReport::RemoteFastForward, conn_1, sqlite_1, remote_client);

        // Merge bootstrap+schema transactions from 1 into 2.
        assert_sync!(SyncReport::Merge(SyncFollowup::None), conn_2, sqlite_2, remote_client);

        // Assert that 2's schema has been augmented with 1's.
        assert_transactions!(sqlite_2, conn_2,
            schema =>
            "[[:person/name :db/ident :person/name ?tx true]
            [:person/name :db/valueType :db.type/string ?tx true]
            [:person/name :db/cardinality :db.cardinality/one ?tx true]
            [:person/age :db/ident :person/age ?tx true]
            [:person/age :db/valueType :db.type/long ?tx true]
            [:person/age :db/cardinality :db.cardinality/one ?tx true]
            [?tx :db/txInstant ?ms ?tx true]]",
        );

        // Assert that 2's sync didn't change remote state.
        assert_sync!(SyncReport::NoChanges, conn_1, sqlite_1, remote_client);
    }

    #[test]
    fn test_entity_merge_unique_identity() {
        let mut sqlite_1 = new_connection("").unwrap();
        let mut sqlite_2 = new_connection("").unwrap();

        let mut conn_1 = Conn::connect(&mut sqlite_1).unwrap();
        let mut conn_2 = Conn::connect(&mut sqlite_2).unwrap();

        let mut remote_client = TestRemoteClient::new();

        // Both have the same schema with a unique/identity attribute.
        conn_1.transact(&mut sqlite_1, "[
            {:db/ident :person/name
              :db/valueType :db.type/string
              :db/cardinality :db.cardinality/one
              :db/unique :db.unique/identity
              :db/index true}]").expect("transacted");

        // Both have the same assertion against the schema.
        conn_1.transact(&mut sqlite_1, r#"[{:person/name "Ivan"}]"#).expect("transacted");

        conn_2.transact(&mut sqlite_2, "[
            {:db/ident :person/name
              :db/valueType :db.type/string
              :db/cardinality :db.cardinality/one
              :db/unique :db.unique/identity
              :db/index true}]").expect("transacted");

        conn_2.transact(&mut sqlite_2, r#"[{:person/name "Ivan"}]"#).expect("transacted");

        // Fast forward empty remote with a bootstrap and schema transactions.
        assert_sync!(SyncReport::RemoteFastForward, conn_1, sqlite_1, remote_client);

        // Merge bootstrap+schema transactions from 1 into 2.
        assert_sync!(SyncReport::Merge(SyncFollowup::None), conn_2, sqlite_2, remote_client);

        assert_transactions!(sqlite_2, conn_2,
            // Assert that 2's schema is unchanged.
            schema =>
            "[[:person/name :db/ident :person/name ?tx true]
            [:person/name :db/valueType :db.type/string ?tx true]
            [:person/name :db/cardinality :db.cardinality/one ?tx true]
            [:person/name :db/unique :db.unique/identity ?tx true]
            [:person/name :db/index true ?tx true]
            [?tx :db/txInstant ?ms ?tx true]]",

            // Assert that 2's unique entity got smushed with 1's.
            r#"[[?e :person/name "Ivan" ?tx true]
            [?tx :db/txInstant ?ms ?tx true]]"#
        );

        // Assert that 2's sync didn't change remote state.
        assert_sync!(SyncReport::NoChanges, conn_1, sqlite_1, remote_client);
    }

    #[test]
    fn test_entity_merge_unique_identity_conflict() {
        let mut sqlite_1 = new_connection("").unwrap();
        let mut sqlite_2 = new_connection("").unwrap();

        let mut conn_1 = Conn::connect(&mut sqlite_1).unwrap();
        let mut conn_2 = Conn::connect(&mut sqlite_2).unwrap();

        let mut remote_client = TestRemoteClient::new();

        // Both start off with the same schema (a single unique/identity attribute) and an assertion.
        conn_1.transact(&mut sqlite_1, "[
            {:db/ident :person/name
              :db/valueType :db.type/string
              :db/cardinality :db.cardinality/one
              :db/unique :db.unique/identity
              :db/index true}]").expect("transacted");

        conn_1.transact(&mut sqlite_1, r#"[{:person/name "Ivan"}]"#).expect("transacted");

        conn_2.transact(&mut sqlite_2, "[
            {:db/ident :person/name
              :db/valueType :db.type/string
              :db/cardinality :db.cardinality/one
              :db/unique :db.unique/identity
              :db/index true}]").expect("transacted");

        conn_2.transact(&mut sqlite_2, r#"[{:person/name "Ivan"}]"#).expect("transacted");

        // Fast forward empty remote with a bootstrap and schema transactions.
        assert_sync!(SyncReport::RemoteFastForward, conn_1, sqlite_1, remote_client);

        // Merge bootstrap+schema transactions from 1 into 2.
        assert_sync!(SyncReport::Merge(SyncFollowup::None), conn_2, sqlite_2, remote_client);

        // First removes the entity.
        conn_1.transact(&mut sqlite_1, r#"[
            [:db/retract (lookup-ref :person/name "Ivan") :person/name "Ivan"]]"#).expect("transacted");

        // Second changes the entitiy.
        conn_2.transact(&mut sqlite_2, r#"[
            {:db/id (lookup-ref :person/name "Ivan") :person/name "Vanya"}
        ]"#).expect("transacted");

        // First syncs first.
        assert_sync!(SyncReport::RemoteFastForward, conn_1, sqlite_1, remote_client);

        // And now, merge!
        assert_sync!(SyncReport::Merge(SyncFollowup::FullSync), conn_2, sqlite_2, remote_client);

        // We currently have a primitive conflict resolution strategy,
        // ending up with a new "Vanya" entity.
        assert_transactions!(sqlite_2, conn_2,
            // These hard-coded entids are brittle but deterministic.
            // They signify that we end up with a new entity Vanya, separate from the one
            // that was renamed.
            r#"[[65537 :person/name "Ivan" ?tx true]
            [?tx :db/txInstant ?ms ?tx true]]"#,

            r#"[[65537 :person/name "Ivan" ?tx false]
            [?tx :db/txInstant ?ms ?tx true]]"#,

            r#"[[65538 :person/name "Vanya" ?tx true]
            [?tx :db/txInstant ?ms ?tx true]]"#
        );
    }

    #[test]
    fn test_entity_merge_unique_identity_conflict_reversed() {
        let mut sqlite_1 = new_connection("").unwrap();
        let mut sqlite_2 = new_connection("").unwrap();

        let mut conn_1 = Conn::connect(&mut sqlite_1).unwrap();
        let mut conn_2 = Conn::connect(&mut sqlite_2).unwrap();

        let mut remote_client = TestRemoteClient::new();

        // Both start off with the same schema (a single unique/identity attribute) and an assertion.
        conn_1.transact(&mut sqlite_1, "[
            {:db/ident :person/name
              :db/valueType :db.type/string
              :db/cardinality :db.cardinality/one
              :db/unique :db.unique/identity
              :db/index true}]").expect("transacted");

        conn_1.transact(&mut sqlite_1, r#"[{:person/name "Ivan"}]"#).expect("transacted");

        conn_2.transact(&mut sqlite_2, "[
            {:db/ident :person/name
              :db/valueType :db.type/string
              :db/cardinality :db.cardinality/one
              :db/unique :db.unique/identity
              :db/index true}]").expect("transacted");

        conn_2.transact(&mut sqlite_2, r#"[{:person/name "Ivan"}]"#).expect("transacted");

        // Fast forward empty remote with a bootstrap and schema transactions.
        assert_sync!(SyncReport::RemoteFastForward, conn_1, sqlite_1, remote_client);

        // Merge bootstrap+schema transactions from 1 into 2.
        assert_sync!(SyncReport::Merge(SyncFollowup::None), conn_2, sqlite_2, remote_client);

        // First removes the entity.
        conn_1.transact(&mut sqlite_1, r#"[
            [:db/retract (lookup-ref :person/name "Ivan") :person/name "Ivan"]]"#).expect("transacted");

        // Second changes the entitiy.
        conn_2.transact(&mut sqlite_2, r#"[
            [:db/add (lookup-ref :person/name "Ivan") :person/name "Vanya"]
        ]"#).expect("transacted");

        // Second syncs first.
        assert_sync!(SyncReport::RemoteFastForward, conn_2, sqlite_2, remote_client);

        // And now, merge!
        assert_sync!(SyncReport::Merge(SyncFollowup::None), conn_1, sqlite_1, remote_client);

        // Deletion of "Ivan" will be dropped on the floor, since there's no such
        // entity anymore (it's "Vanya").
        assert_transactions!(sqlite_1, conn_1,
            // These hard-coded entids are brittle but deterministic.
            r#"[[65537 :person/name "Ivan" ?tx true]
            [?tx :db/txInstant ?ms ?tx true]]"#,

            r#"[[65537 :person/name "Ivan" ?tx false]
            [65537 :person/name "Vanya" ?tx true]
            [?tx :db/txInstant ?ms ?tx true]]"#
        );
    }

    #[test]
    fn test_entity_merge_unique_identity_conflict_simple() {
        let mut sqlite_1 = new_connection("").unwrap();
        let mut sqlite_2 = new_connection("").unwrap();

        let mut conn_1 = Conn::connect(&mut sqlite_1).unwrap();
        let mut conn_2 = Conn::connect(&mut sqlite_2).unwrap();

        let mut remote_client = TestRemoteClient::new();

        // Both start off with the same schema (a single unique/identity attribute) and an assertion.
        conn_1.transact(&mut sqlite_1, "[
            {:db/ident :person/name
              :db/valueType :db.type/string
              :db/cardinality :db.cardinality/one
              :db/unique :db.unique/identity
              :db/index true}]").expect("transacted");

        conn_1.transact(&mut sqlite_1, r#"[{:person/name "Ivan"}]"#).expect("transacted");

        conn_2.transact(&mut sqlite_2, "[
            {:db/ident :person/name
              :db/valueType :db.type/string
              :db/cardinality :db.cardinality/one
              :db/unique :db.unique/identity
              :db/index true}]").expect("transacted");

        conn_2.transact(&mut sqlite_2, r#"[{:person/name "Ivan"}]"#).expect("transacted");

        // Fast forward empty remote with a bootstrap and schema transactions.
        assert_sync!(SyncReport::RemoteFastForward, conn_1, sqlite_1, remote_client);

        // Merge bootstrap+schema transactions from 1 into 2.
        assert_sync!(SyncReport::Merge(SyncFollowup::None), conn_2, sqlite_2, remote_client);

        // First renames the entity.
        conn_1.transact(&mut sqlite_1, r#"[
            [:db/add (lookup-ref :person/name "Ivan") :person/name "Vanechka"]]"#).expect("transacted");

        // Second also renames the entitiy.
        conn_2.transact(&mut sqlite_2, r#"[
            [:db/add (lookup-ref :person/name "Ivan") :person/name "Vanya"]
        ]"#).expect("transacted");

        // Second syncs first.
        assert_sync!(SyncReport::RemoteFastForward, conn_2, sqlite_2, remote_client);

        // And now, merge!
        assert_sync!(SyncReport::Merge(SyncFollowup::FullSync), conn_1, sqlite_1, remote_client);

        // These hard-coded entids are brittle but deterministic.
        // They signify that we end up with a new entity Vanechka, separate from the one
        // that was renamed.
        assert_transactions!(sqlite_1, conn_1,
            r#"[[65537 :person/name "Ivan" ?tx true]
            [?tx :db/txInstant ?ms ?tx true]]"#,

            r#"[[65537 :person/name "Ivan" ?tx false]
            [65537 :person/name "Vanya" ?tx true]
            [?tx :db/txInstant ?ms ?tx true]]"#,

            // A new entity is created for the second rename.
            r#"[[65538 :person/name "Vanechka" ?tx true]
            [?tx :db/txInstant ?ms ?tx true]]"#
        );
    }

    #[test]
    fn test_conflicting_schema() {
        let mut sqlite_1 = new_connection("").unwrap();
        let mut sqlite_2 = new_connection("").unwrap();

        let mut conn_1 = Conn::connect(&mut sqlite_1).unwrap();
        let mut conn_2 = Conn::connect(&mut sqlite_2).unwrap();

        let mut remote_client = TestRemoteClient::new();

        conn_1.transact(&mut sqlite_1, "[
            {:db/ident :person/name
              :db/valueType :db.type/string
              :db/cardinality :db.cardinality/one}]").expect("transacted");

        conn_2.transact(&mut sqlite_2, "[
            {:db/ident :person/name
              :db/valueType :db.type/string
              :db/cardinality :db.cardinality/many}]").expect("transacted");

        assert_sync!(SyncReport::RemoteFastForward, conn_1, sqlite_1, remote_client);
        assert_sync!(
            error => MentatError::TolstoyError(TolstoyError::NotYetImplemented(_)),
            conn_2, sqlite_2, remote_client);
    }


    #[test]
    fn test_schema_with_non_matching_entids() {
        let mut sqlite_1 = new_connection("").unwrap();
        let mut sqlite_2 = new_connection("").unwrap();

        let mut conn_1 = Conn::connect(&mut sqlite_1).unwrap();
        let mut conn_2 = Conn::connect(&mut sqlite_2).unwrap();

        let mut remote_client = TestRemoteClient::new();

        // :person/name will be e=65536.
        conn_1.transact(&mut sqlite_1, "[
            {:db/ident :person/name
              :db/valueType :db.type/string
              :db/cardinality :db.cardinality/one}]").expect("transacted");

        // This entity will be e=65537.
        conn_1.transact(&mut sqlite_1, r#"[{:person/name "Ivan"}]"#).expect("transacted");

        // :person/name will be e=65536, :person/age will be e=65537 (NB conflict w/ above entity).
        conn_2.transact(&mut sqlite_2, "[
            {:db/ident :person/name
              :db/valueType :db.type/string
              :db/cardinality :db.cardinality/one}
            {:db/ident :person/age
              :db/valueType :db.type/long
              :db/cardinality :db.cardinality/one}]").expect("transacted");

        assert_sync!(SyncReport::RemoteFastForward, conn_1, sqlite_1, remote_client);
        assert_sync!(SyncReport::Merge(SyncFollowup::FullSync), conn_2, sqlite_2, remote_client);
        assert_sync!(SyncReport::RemoteFastForward, conn_2, sqlite_2, remote_client);
        assert_sync!(SyncReport::LocalFastForward, conn_1, sqlite_1, remote_client);

        assert_transactions!(sqlite_1, conn_1,
            schema =>
            "[[:person/name :db/ident :person/name ?tx true]
            [:person/name :db/valueType :db.type/string ?tx true]
            [:person/name :db/cardinality :db.cardinality/one ?tx true]
            [?tx :db/txInstant ?ms ?tx true]]",

            // Assert that 2's unique entity got smushed with 1's.
            r#"[[?e :person/name "Ivan" ?tx true]
            [?tx :db/txInstant ?ms ?tx true]]"#,

            // Assert that 2's extra vocabulary is present.
            "[[:person/age :db/ident :person/age ?tx true]
            [:person/age :db/valueType :db.type/long ?tx true]
            [:person/age :db/cardinality :db.cardinality/one ?tx true]
            [?tx :db/txInstant ?ms ?tx true]]"
        );
    }

    #[test]
    fn test_entity_merge_non_unique_entity_conflict() {
        let mut sqlite_1 = new_connection("").unwrap();
        let mut sqlite_2 = new_connection("").unwrap();

        let mut conn_1 = Conn::connect(&mut sqlite_1).unwrap();
        let mut conn_2 = Conn::connect(&mut sqlite_2).unwrap();

        let mut remote_client = TestRemoteClient::new();

        // Both start off with the same schema (a single unique/identity attribute) and an assertion.
        conn_1.transact(&mut sqlite_1, "[
            {:db/ident :person/name
              :db/valueType :db.type/string
              :db/cardinality :db.cardinality/one}]").expect("transacted");

        conn_1.transact(&mut sqlite_1, r#"[{:person/name "Ivan"}]"#).expect("transacted");

        conn_2.transact(&mut sqlite_2, "[
            {:db/ident :person/name
              :db/valueType :db.type/string
              :db/cardinality :db.cardinality/one}]").expect("transacted");

        conn_2.transact(&mut sqlite_2, r#"[{:person/name "Ivan"}]"#).expect("transacted");

        // Fast forward empty remote with a bootstrap and schema transactions.
        assert_sync!(SyncReport::RemoteFastForward, conn_1, sqlite_1, remote_client);

        // Merge bootstrap+schema transactions from 1 into 2.
        // Will result in two Ivans.
        assert_sync!(SyncReport::Merge(SyncFollowup::FullSync), conn_2, sqlite_2, remote_client);
        // Upload the second Ivan.
        assert_sync!(SyncReport::RemoteFastForward, conn_2, sqlite_2, remote_client);

        // Get the second Ivan.
        assert_sync!(SyncReport::LocalFastForward, conn_1, sqlite_1, remote_client);

        // These entids are determenistic. We can't use lookup-refs because :person/name is
        // a non-unique attribute.
        // First removes an Ivan.
        conn_1.transact(&mut sqlite_1, r#"[
            [:db/retract 65537 :person/name "Ivan"]]"#).expect("transacted");

        // Second renames an Ivan.
        conn_2.transact(&mut sqlite_2, r#"[
            {:db/id 65537 :person/name "Vanya"}]"#).expect("transacted");

        // First syncs first.
        assert_sync!(SyncReport::RemoteFastForward, conn_1, sqlite_1, remote_client);

        // And now, merge!
        assert_sync!(SyncReport::Merge(SyncFollowup::FullSync), conn_2, sqlite_2, remote_client);

        // We currently have a primitive conflict resolution strategy,
        // ending up with a new "Vanya" entity.

        // These hard-coded entids are brittle but deterministic.
        // They signify that we end up with a new entity Vanya, separate from the one
        // that was renamed.
        assert_transactions!(sqlite_2, conn_2,
            r#"[[65537 :person/name "Ivan" ?tx true]
            [?tx :db/txInstant ?ms ?tx true]]"#,

            r#"[[65538 :person/name "Ivan" ?tx true]
            [?tx :db/txInstant ?ms ?tx true]]"#,

            r#"[[65537 :person/name "Ivan" ?tx false]
            [?tx :db/txInstant ?ms ?tx true]]"#,

            r#"[[65538 :person/name "Ivan" ?tx false]
            [65538 :person/name "Vanya" ?tx true]
            [?tx :db/txInstant ?ms ?tx true]]"#
        );
    }

    #[test]
    fn test_entity_merge_non_unique_entity_conflict_reversed() {
        let mut sqlite_1 = new_connection("").unwrap();
        let mut sqlite_2 = new_connection("").unwrap();

        let mut conn_1 = Conn::connect(&mut sqlite_1).unwrap();
        let mut conn_2 = Conn::connect(&mut sqlite_2).unwrap();

        let mut remote_client = TestRemoteClient::new();

        // Both start off with the same schema (a single unique/identity attribute) and an assertion.
        conn_1.transact(&mut sqlite_1, "[
            {:db/ident :person/name
              :db/valueType :db.type/string
              :db/cardinality :db.cardinality/one}]").expect("transacted");

        conn_1.transact(&mut sqlite_1, r#"[{:person/name "Ivan"}]"#).expect("transacted");

        conn_2.transact(&mut sqlite_2, "[
            {:db/ident :person/name
              :db/valueType :db.type/string
              :db/cardinality :db.cardinality/one}]").expect("transacted");

        conn_2.transact(&mut sqlite_2, r#"[{:person/name "Ivan"}]"#).expect("transacted");

        // Fast forward empty remote with a bootstrap and schema transactions.
        assert_sync!(SyncReport::RemoteFastForward, conn_1, sqlite_1, remote_client);

        // Merge bootstrap+schema transactions from 1 into 2.
        // Merge will result in two Ivans.
        assert_sync!(SyncReport::Merge(SyncFollowup::FullSync), conn_2, sqlite_2, remote_client);
        // Upload the second Ivan.
        assert_sync!(SyncReport::RemoteFastForward, conn_2, sqlite_2, remote_client);

        // Get the second Ivan.
        assert_sync!(SyncReport::LocalFastForward, conn_1, sqlite_1, remote_client);

        // These entids are determenistic. We can't use lookup-refs because :person/name is
        // a non-unique attribute.
        // First removes an Ivan.
        conn_1.transact(&mut sqlite_1, r#"[
            [:db/retract 65537 :person/name "Ivan"]]"#).expect("transacted");

        // Second renames an Ivan.
        conn_2.transact(&mut sqlite_2, r#"[
            [:db/add 65537 :person/name "Vanya"]]"#).expect("transacted");

        // Second wins the sync race.
        assert_sync!(SyncReport::RemoteFastForward, conn_2, sqlite_2, remote_client);

        // First merges its changes with second's.
        assert_sync!(SyncReport::Merge(SyncFollowup::None), conn_1, sqlite_1, remote_client);

        // We currently have a primitive conflict resolution strategy,
        // ending up dropping first's removal of "Ivan".
        // Internally that happens because :person/name is not :db/unique.

        // These hard-coded entids are brittle but deterministic.
        // They signify that we end up with a new entity Vanya, separate from the one
        // that was renamed.
        assert_transactions!(sqlite_1, conn_1,
            r#"[[65537 :person/name "Ivan" ?tx true]
            [?tx :db/txInstant ?ms ?tx true]]"#,

            r#"[[65538 :person/name "Ivan" ?tx true]
            [?tx :db/txInstant ?ms ?tx true]]"#,

            // Just the rename left, removal is dropped on the floor.
            r#"[[65537 :person/name "Ivan" ?tx false]
            [65537 :person/name "Vanya" ?tx true]
            [?tx :db/txInstant ?ms ?tx true]]"#
        );
    }

    #[test]
    fn test_entity_merge_non_unique() {
        let mut sqlite_1 = new_connection("").unwrap();
        let mut sqlite_2 = new_connection("").unwrap();

        let mut conn_1 = Conn::connect(&mut sqlite_1).unwrap();
        let mut conn_2 = Conn::connect(&mut sqlite_2).unwrap();

        let mut remote_client = TestRemoteClient::new();

        // 1 defines the same schema as 2.
        conn_1.transact(&mut sqlite_1, "[
            {:db/ident :world/city
              :db/valueType :db.type/string
              :db/cardinality :db.cardinality/one}]").expect("transacted");

        // Vancouver, BC
        conn_1.transact(&mut sqlite_1, r#"[{:world/city "Vancouver"}]"#).expect("transacted");

        conn_2.transact(&mut sqlite_2, "[
            {:db/ident :world/city
              :db/valueType :db.type/string
              :db/cardinality :db.cardinality/one}]").expect("transacted");

        // Vancouver, WA
        conn_2.transact(&mut sqlite_2, r#"[{:world/city "Vancouver"}]"#).expect("transacted");

        // Fast forward empty remote with a bootstrap and schema transactions.
        assert_sync!(SyncReport::RemoteFastForward, conn_1, sqlite_1, remote_client);

        // Since :world/city is not unique, we elect not to smush these entities.
        assert_sync!(SyncReport::Merge(SyncFollowup::FullSync), conn_2, sqlite_2, remote_client);

        assert_transactions!(sqlite_2, conn_2,
            schema =>
            "[[?e :db/ident :world/city ?tx true]
            [?e :db/valueType :db.type/string ?tx true]
            [?e :db/cardinality :db.cardinality/one ?tx true]
            [?tx :db/txInstant ?ms ?tx true]]",

            // Assert that we didn't try smushing non-unique entities.
            r#"[[?e :world/city "Vancouver" ?tx true]
            [?tx :db/txInstant ?ms ?tx true]]"#,

            r#"[[?e :world/city "Vancouver" ?tx true]
            [?tx :db/txInstant ?ms ?tx true]]"#
        );

        // Since follow-up must be manually triggered, 1 shouldn't observe any changes yet.
        assert_sync!(SyncReport::NoChanges, conn_1, sqlite_1, remote_client);

        // Follow-up sync.
        assert_sync!(SyncReport::RemoteFastForward, conn_2, sqlite_2, remote_client);

        // 2 should now observe merge results from 1.
        assert_sync!(SyncReport::LocalFastForward, conn_1, sqlite_1, remote_client);

        assert_transactions!(sqlite_1, conn_1,
            // Assert that 1's schema is unchanged.
            schema =>
            "[[?e :db/ident :world/city ?tx true]
            [?e :db/valueType :db.type/string ?tx true]
            [?e :db/cardinality :db.cardinality/one ?tx true]
            [?tx :db/txInstant ?ms ?tx true]]",

            // Assert that we didn't try smushing non-unique entities.
            r#"[[?e :world/city "Vancouver" ?tx true]
            [?tx :db/txInstant ?ms ?tx true]]"#,

            r#"[[?e :world/city "Vancouver" ?tx true]
            [?tx :db/txInstant ?ms ?tx true]]"#
        );
    }

    #[test]
    fn test_schema_with_assertions_merge() {
        let mut sqlite_1 = new_connection("").unwrap();
        let mut sqlite_2 = new_connection("").unwrap();

        let mut conn_1 = Conn::connect(&mut sqlite_1).unwrap();
        let mut conn_2 = Conn::connect(&mut sqlite_2).unwrap();

        let mut remote_client = TestRemoteClient::new();

        // 1 defines a richer schema than 2.
        conn_1.transact(&mut sqlite_1, "[
            {:db/ident :person/name
              :db/valueType :db.type/string
              :db/cardinality :db.cardinality/one}
            {:db/ident :person/age
              :db/valueType :db.type/long
              :db/cardinality :db.cardinality/one}]").expect("transacted");

        conn_1.transact(&mut sqlite_1, r#"[{:person/name "Ivan" :person/age 28}]"#).expect("transacted");

        conn_2.transact(&mut sqlite_2, "[
            {:db/ident :person/name
              :db/valueType :db.type/string
              :db/cardinality :db.cardinality/one}]").expect("transacted");

        conn_2.transact(&mut sqlite_2, r#"[{:person/name "Ivan"}]"#).expect("transacted");

        // Fast forward empty remote with a bootstrap and schema transactions.
        assert_sync!(SyncReport::RemoteFastForward, conn_1, sqlite_1, remote_client);

        // Merge bootstrap+schema transactions from 1 into 2.
        assert_sync!(SyncReport::Merge(SyncFollowup::FullSync), conn_2, sqlite_2, remote_client);

        assert_transactions!(sqlite_2, conn_2,
            // Assert that 2's schema has been augmented with 1's.
            schema =>
            "[[:person/name :db/ident :person/name ?tx true]
            [:person/name :db/valueType :db.type/string ?tx true]
            [:person/name :db/cardinality :db.cardinality/one ?tx true]
            [:person/age :db/ident :person/age ?tx true]
            [:person/age :db/valueType :db.type/long ?tx true]
            [:person/age :db/cardinality :db.cardinality/one ?tx true]
            [?tx :db/txInstant ?ms ?tx true]]",

            r#"[[?e :person/name "Ivan" ?tx true]
            [?e :person/age 28 ?tx true]
            [?tx :db/txInstant ?ms ?tx true]]"#,

            r#"[[?e :person/name "Ivan" ?tx true]
            [?tx :db/txInstant ?ms ?tx true]]"#
        );

        // Follow-up sync after merge.
        assert_sync!(SyncReport::RemoteFastForward, conn_2, sqlite_2, remote_client);

        // Assert that 2's sync fast-forwarded remote.
        assert_sync!(SyncReport::LocalFastForward, conn_1, sqlite_1, remote_client);

        assert_transactions!(sqlite_1, conn_1,
            // Assert that 1's schema remains the same, and it sees the extra Ivan.
            schema =>
            "[[:person/name :db/ident :person/name ?tx true]
            [:person/name :db/valueType :db.type/string ?tx true]
            [:person/name :db/cardinality :db.cardinality/one ?tx true]
            [:person/age :db/ident :person/age ?tx true]
            [:person/age :db/valueType :db.type/long ?tx true]
            [:person/age :db/cardinality :db.cardinality/one ?tx true]
            [?tx :db/txInstant ?ms ?tx true]]",

            r#"[[?e :person/name "Ivan" ?tx true]
            [?e :person/age 28 ?tx true]
            [?tx :db/txInstant ?ms ?tx true]]"#,

            r#"[[?e :person/name "Ivan" ?tx true]
            [?tx :db/txInstant ?ms ?tx true]]"#
        );
    }

    #[test]
    fn test_non_subset_merge() {
        let mut sqlite_1 = new_connection("").unwrap();
        let mut sqlite_2 = new_connection("").unwrap();

        let mut conn_1 = Conn::connect(&mut sqlite_1).unwrap();
        let mut conn_2 = Conn::connect(&mut sqlite_2).unwrap();

        let mut remote_client = TestRemoteClient::new();

        // 1 and 2 define different schemas.
        conn_1.transact(&mut sqlite_1, "[
            {:db/ident :person/name
              :db/valueType :db.type/string
              :db/cardinality :db.cardinality/one}
            {:db/ident :person/age
              :db/valueType :db.type/long
              :db/cardinality :db.cardinality/one}]").expect("transacted");

        conn_2.transact(&mut sqlite_2, "[
            {:db/ident :person/name
              :db/valueType :db.type/string
              :db/cardinality :db.cardinality/one}
            {:db/ident :person/sin
              :db/valueType :db.type/long
              :db/cardinality :db.cardinality/one}]").expect("transacted");

        // Fast forward empty remote with a bootstrap and schema transactions.
        assert_sync!(SyncReport::RemoteFastForward, conn_1, sqlite_1, remote_client);

        // Merge bootstrap+schema transactions from 1 into 2.
        assert_sync!(SyncReport::Merge(SyncFollowup::FullSync), conn_2, sqlite_2, remote_client);

        assert_transactions!(sqlite_2, conn_2,
            schema =>
            "[[:person/name :db/ident :person/name ?tx true]
            [:person/name :db/valueType :db.type/string ?tx true]
            [:person/name :db/cardinality :db.cardinality/one ?tx true]
            [:person/age :db/ident :person/age ?tx true]
            [:person/age :db/valueType :db.type/long ?tx true]
            [:person/age :db/cardinality :db.cardinality/one ?tx true]
            [?tx :db/txInstant ?ms ?tx true]]",

            "[[:person/sin :db/ident :person/sin ?tx true]
            [:person/sin :db/valueType :db.type/long ?tx true]
            [:person/sin :db/cardinality :db.cardinality/one ?tx true]
            [?tx :db/txInstant ?ms ?tx true]]"
        );

        // Follow-up sync after merge.
        assert_sync!(SyncReport::RemoteFastForward, conn_2, sqlite_2, remote_client);

        // Assert that 2's sync moved forward the remote state.
        assert_sync!(SyncReport::LocalFastForward, conn_1, sqlite_1, remote_client);

        assert_transactions!(sqlite_1, conn_1,
            // Assert that 1's schema is intact, and has been augmented with 2's.
            schema =>
            "[[:person/name :db/ident :person/name ?tx true]
            [:person/name :db/valueType :db.type/string ?tx true]
            [:person/name :db/cardinality :db.cardinality/one ?tx true]
            [:person/age :db/ident :person/age ?tx true]
            [:person/age :db/valueType :db.type/long ?tx true]
            [:person/age :db/cardinality :db.cardinality/one ?tx true]
            [?tx :db/txInstant ?ms ?tx true]]",

            "[[:person/sin :db/ident :person/sin ?tx true]
            [:person/sin :db/valueType :db.type/long ?tx true]
            [:person/sin :db/cardinality :db.cardinality/one ?tx true]
            [?tx :db/txInstant ?ms ?tx true]]"
        );
    }

    #[test]
    fn test_merge_schema_with_different_attribute_definitions() {
        let mut sqlite_1 = new_connection("").unwrap();
        let mut sqlite_2 = new_connection("").unwrap();

        let mut conn_1 = Conn::connect(&mut sqlite_1).unwrap();
        let mut conn_2 = Conn::connect(&mut sqlite_2).unwrap();

        let mut remote_client = TestRemoteClient::new();

        // 1 and 2 define same idents but with different cardinality.
        conn_1.transact(&mut sqlite_1, "[
            {:db/ident :person/name
              :db/valueType :db.type/string
              :db/cardinality :db.cardinality/one}
            {:db/ident :person/bff
              :db/valueType :db.type/string
              :db/cardinality :db.cardinality/one}]").expect("transacted");

        conn_2.transact(&mut sqlite_2, "[
            {:db/ident :person/name
              :db/valueType :db.type/string
              :db/cardinality :db.cardinality/one}
            {:db/ident :person/bff
              :db/valueType :db.type/string
              :db/cardinality :db.cardinality/many}]").expect("transacted");

        // Fast forward empty remote with a bootstrap and schema transactions.
        assert_sync!(SyncReport::RemoteFastForward, conn_1, sqlite_1, remote_client);

        // Merge bootstrap+schema transactions from 1 into 2.
        assert_sync!(
            error => MentatError::TolstoyError(TolstoyError::NotYetImplemented(_)),
            conn_2, sqlite_2, remote_client
        );
    }
}
