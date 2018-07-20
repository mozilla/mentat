// Copyright 2016 Mozilla
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

use std::ops::RangeFrom;

use rusqlite;

use errors::{
    DbErrorKind,
    Result,
};

use mentat_core::{
    Entid,
    Schema,
    TypedValue,
    KnownEntid,
};

use edn::{
    InternSet,
};

use edn::entities::OpType;

use db;
use db::{
    TypedSQLValue,
};

use tx::{
    transact_terms_with_action,
    TransactorAction,
};

use types::{
    PartitionMap,
};

use internal_types::{
    Term,
    TermWithoutTempIds,
};

use watcher::{
    NullWatcher,
};

/// Collects a supplied tx range into an DESC ordered Vec of valid txs,
/// ensuring they all belong to the same timeline.
fn collect_ordered_txs_to_move(conn: &rusqlite::Connection, txs_from: RangeFrom<Entid>, timeline: Entid) -> Result<Vec<Entid>> {
    let mut stmt = conn.prepare("SELECT tx, timeline FROM timelined_transactions WHERE tx >= ? AND timeline = ? GROUP BY tx ORDER BY tx DESC")?;
    let mut rows = stmt.query_and_then(&[&txs_from.start, &timeline], |row: &rusqlite::Row| -> Result<(Entid, Entid)>{
        Ok((row.get_checked(0)?, row.get_checked(1)?))
    })?;

    let mut txs = vec![];

    // TODO do this in SQL instead?
    let timeline = match rows.next() {
        Some(t) => {
            let t = t?;
            txs.push(t.0);
            t.1
        },
        None => bail!(DbErrorKind::TimelinesInvalidRange)
    };

    while let Some(t) = rows.next() {
        let t = t?;
        txs.push(t.0);
        if t.1 != timeline {
            bail!(DbErrorKind::TimelinesMixed);
        }
    }

    Ok(txs)
}

fn move_transactions_to(conn: &rusqlite::Connection, tx_ids: &[Entid], new_timeline: Entid) -> Result<()> {
    // Move specified transactions over to a specified timeline.
    conn.execute(&format!(
        "UPDATE timelined_transactions SET timeline = {} WHERE tx IN {}",
            new_timeline,
            ::repeat_values(tx_ids.len(), 1)
        ), &(tx_ids.iter().map(|x| x as &rusqlite::types::ToSql).collect::<Vec<_>>())
    )?;
    Ok(())
}

fn is_timeline_empty(conn: &rusqlite::Connection, timeline: Entid) -> Result<bool> {
    let mut stmt = conn.prepare("SELECT timeline FROM timelined_transactions WHERE timeline = ? GROUP BY timeline")?;
    let rows = stmt.query_and_then(&[&timeline], |row| -> Result<i64> {
        Ok(row.get_checked(0)?)
    })?;
    Ok(rows.count() == 0)
}

/// Get terms for tx_id, reversing them in meaning (swap add & retract).
fn reversed_terms_for(conn: &rusqlite::Connection, tx_id: Entid) -> Result<Vec<TermWithoutTempIds>> {
    let mut stmt = conn.prepare("SELECT e, a, v, value_type_tag, tx, added FROM timelined_transactions WHERE tx = ? AND timeline = ? ORDER BY tx DESC")?;
    let mut rows = stmt.query_and_then(&[&tx_id, &::TIMELINE_MAIN], |row| -> Result<TermWithoutTempIds> {
        let op = match row.get_checked(5)? {
            true => OpType::Retract,
            false => OpType::Add
        };
        Ok(Term::AddOrRetract(
            op,
            KnownEntid(row.get_checked(0)?),
            row.get_checked(1)?,
            TypedValue::from_sql_value_pair(row.get_checked(2)?, row.get_checked(3)?)?,
        ))
    })?;

    let mut terms = vec![];

    while let Some(row) = rows.next() {
        terms.push(row?);
    }
    Ok(terms)
}

/// Move specified transaction RangeFrom off of main timeline.
pub fn move_from_main_timeline(conn: &rusqlite::Connection, schema: &Schema,
    partition_map: PartitionMap, txs_from: RangeFrom<Entid>, new_timeline: Entid) -> Result<(Option<Schema>, PartitionMap)> {

    if new_timeline == ::TIMELINE_MAIN {
        bail!(DbErrorKind::NotYetImplemented(format!("Can't move transactions to main timeline")));
    }

    // We don't currently ensure that moving transactions onto a non-empty timeline
    // will result in sensible end-state for that timeline.
    // Let's remove that foot gun by prohibiting moving transactions to a non-empty timeline.
    if !is_timeline_empty(conn, new_timeline)? {
        bail!(DbErrorKind::TimelinesMoveToNonEmpty);
    }

    let txs_to_move = collect_ordered_txs_to_move(conn, txs_from, ::TIMELINE_MAIN)?;

    let mut last_schema = None;
    for tx_id in &txs_to_move {
        let reversed_terms = reversed_terms_for(conn, *tx_id)?;

        // Rewind schema and datoms.
        let (_, _, new_schema, _) = transact_terms_with_action(
            conn, partition_map.clone(), schema, schema, NullWatcher(),
            reversed_terms.into_iter().map(|t| t.rewrap()),
            InternSet::new(), TransactorAction::Materialize
        )?;
        last_schema = new_schema;
    }

    // Move transactions over to the target timeline.
    move_transactions_to(conn, &txs_to_move, new_timeline)?;

    Ok((last_schema, db::read_partition_map(conn)?))
}

#[cfg(test)]
mod tests {
    use super::*;

    use edn;

    use std::borrow::{
        Borrow,
    };

    use debug::{
        TestConn,
    };

    use bootstrap;

    // For convenience during testing.
    // Real consumers will perform similar operations when appropriate.
    fn update_conn(conn: &mut TestConn, schema: &Option<Schema>, pmap: &PartitionMap) {
        match schema {
            &Some(ref s) => conn.schema = s.clone(),
            &None => ()
        };
        conn.partition_map = pmap.clone();
    }
    
    #[test]
    fn test_pop_simple() {
        let mut conn = TestConn::default();
        conn.sanitized_partition_map();

        let t = r#"
            [{:db/id :db/doc :db/doc "test"}]
        "#;

        let partition_map0 = conn.partition_map.clone();

        let report1 = assert_transact!(conn, t);
        let partition_map1 = conn.partition_map.clone();

        let (new_schema, new_partition_map) = move_from_main_timeline(
            &conn.sqlite, &conn.schema, conn.partition_map.clone(),
            conn.last_tx_id().., 1
        ).expect("moved single tx");
        update_conn(&mut conn, &new_schema, &new_partition_map);

        assert_matches!(conn.datoms(), "[]");
        assert_matches!(conn.transactions(), "[]");
        assert_eq!(new_partition_map, partition_map0);

        conn.partition_map = partition_map0.clone();
        let report2 = assert_transact!(conn, t);
        let partition_map2 = conn.partition_map.clone();

        // Ensure that we can't move transactions to a non-empty timeline:
        move_from_main_timeline(
            &conn.sqlite, &conn.schema, conn.partition_map.clone(),
            conn.last_tx_id().., 1
        ).expect_err("Can't move transactions to a non-empty timeline");

        assert_eq!(report1.tx_id, report2.tx_id);
        assert_eq!(partition_map1, partition_map2);

        assert_matches!(conn.datoms(), r#"
            [[37 :db/doc "test"]]
        "#);
        assert_matches!(conn.transactions(), r#"
            [[[37 :db/doc "test" ?tx true]
              [?tx :db/txInstant ?ms ?tx true]]]
        "#);
    }

    #[test]
    fn test_pop_ident() {
        let mut conn = TestConn::default();
        conn.sanitized_partition_map();

        let t = r#"
            [{:db/ident :test/entid :db/doc "test" :db.schema/version 1}]
        "#;

        let partition_map0 = conn.partition_map.clone();
        let schema0 = conn.schema.clone();

        let report1 = assert_transact!(conn, t);
        let partition_map1 = conn.partition_map.clone();
        let schema1 = conn.schema.clone();

        let (new_schema, new_partition_map) = move_from_main_timeline(
            &conn.sqlite, &conn.schema, conn.partition_map.clone(),
            conn.last_tx_id().., 1
        ).expect("moved single tx");
        update_conn(&mut conn, &new_schema, &new_partition_map);

        assert_matches!(conn.datoms(), "[]");
        assert_matches!(conn.transactions(), "[]");
        assert_eq!(conn.partition_map, partition_map0);
        assert_eq!(conn.schema, schema0);

        let report2 = assert_transact!(conn, t);

        assert_eq!(report1.tx_id, report2.tx_id);
        assert_eq!(conn.partition_map, partition_map1);
        assert_eq!(conn.schema, schema1);

        assert_matches!(conn.datoms(), r#"
            [[?e :db/ident :test/entid]
             [?e :db/doc "test"]
             [?e :db.schema/version 1]]
        "#);
        assert_matches!(conn.transactions(), r#"
            [[[?e :db/ident :test/entid ?tx true]
              [?e :db/doc "test" ?tx true]
              [?e :db.schema/version 1 ?tx true]
              [?tx :db/txInstant ?ms ?tx true]]]
        "#);
    }

    
    #[test]
    fn test_pop_schema() {
        let mut conn = TestConn::default();
        conn.sanitized_partition_map();

        let t = r#"
            [{:db/id "e" :db/ident :test/one :db/valueType :db.type/long :db/cardinality :db.cardinality/one}
             {:db/id "f" :db/ident :test/many :db/valueType :db.type/long :db/cardinality :db.cardinality/many}]
        "#;

        let partition_map0 = conn.partition_map.clone();
        let schema0 = conn.schema.clone();

        let report1 = assert_transact!(conn, t);
        let partition_map1 = conn.partition_map.clone();
        let schema1 = conn.schema.clone();

        let (new_schema, new_partition_map) = move_from_main_timeline(
            &conn.sqlite, &conn.schema, conn.partition_map.clone(),
            report1.tx_id.., 1).expect("moved single tx");
        update_conn(&mut conn, &new_schema, &new_partition_map);

        assert_matches!(conn.datoms(), "[]");
        assert_matches!(conn.transactions(), "[]");
        assert_eq!(conn.partition_map, partition_map0);
        assert_eq!(conn.schema, schema0);

        let report2 = assert_transact!(conn, t);
        let partition_map2 = conn.partition_map.clone();
        let schema2 = conn.schema.clone();

        assert_eq!(report1.tx_id, report2.tx_id);
        assert_eq!(partition_map1, partition_map2);
        assert_eq!(schema1, schema2);

        assert_matches!(conn.datoms(), r#"
            [[?e1 :db/ident :test/one]
             [?e1 :db/valueType :db.type/long]
             [?e1 :db/cardinality :db.cardinality/one]
             [?e2 :db/ident :test/many]
             [?e2 :db/valueType :db.type/long]
             [?e2 :db/cardinality :db.cardinality/many]]
        "#);
        assert_matches!(conn.transactions(), r#"
            [[[?e1 :db/ident :test/one ?tx1 true]
             [?e1 :db/valueType :db.type/long ?tx1 true]
             [?e1 :db/cardinality :db.cardinality/one ?tx1 true]
             [?e2 :db/ident :test/many ?tx1 true]
             [?e2 :db/valueType :db.type/long ?tx1 true]
             [?e2 :db/cardinality :db.cardinality/many ?tx1 true]
             [?tx1 :db/txInstant ?ms ?tx1 true]]]
        "#);
    }

    #[test]
    fn test_pop_schema_all_attributes() {
        let mut conn = TestConn::default();
        conn.sanitized_partition_map();

        let t = r#"
            [{
                :db/id "e"
                :db/ident :test/one
                :db/valueType :db.type/string
                :db/cardinality :db.cardinality/one
                :db/unique :db.unique/value
                :db/index true
                :db/fulltext true
            }]
        "#;

        let partition_map0 = conn.partition_map.clone();
        let schema0 = conn.schema.clone();

        let report1 = assert_transact!(conn, t);
        let partition_map1 = conn.partition_map.clone();
        let schema1 = conn.schema.clone();

        let (new_schema, new_partition_map) = move_from_main_timeline(
            &conn.sqlite, &conn.schema, conn.partition_map.clone(),
            report1.tx_id.., 1).expect("moved single tx");
        update_conn(&mut conn, &new_schema, &new_partition_map);

        assert_matches!(conn.datoms(), "[]");
        assert_matches!(conn.transactions(), "[]");
        assert_eq!(conn.partition_map, partition_map0);
        assert_eq!(conn.schema, schema0);

        let report2 = assert_transact!(conn, t);
        let partition_map2 = conn.partition_map.clone();
        let schema2 = conn.schema.clone();

        assert_eq!(report1.tx_id, report2.tx_id);
        assert_eq!(partition_map1, partition_map2);
        assert_eq!(schema1, schema2);

        assert_matches!(conn.datoms(), r#"
            [[?e1 :db/ident :test/one]
             [?e1 :db/valueType :db.type/string]
             [?e1 :db/cardinality :db.cardinality/one]
             [?e1 :db/unique :db.unique/value]
             [?e1 :db/index true]
             [?e1 :db/fulltext true]]
        "#);
        assert_matches!(conn.transactions(), r#"
            [[[?e1 :db/ident :test/one ?tx1 true]
             [?e1 :db/valueType :db.type/string ?tx1 true]
             [?e1 :db/cardinality :db.cardinality/one ?tx1 true]
             [?e1 :db/unique :db.unique/value ?tx1 true]
             [?e1 :db/index true ?tx1 true]
             [?e1 :db/fulltext true ?tx1 true]
             [?tx1 :db/txInstant ?ms ?tx1 true]]]
        "#);
    }

        #[test]
    fn test_pop_schema_all_attributes_component() {
        let mut conn = TestConn::default();
        conn.sanitized_partition_map();

        let t = r#"
            [{
                :db/id "e"
                :db/ident :test/one
                :db/valueType :db.type/ref
                :db/cardinality :db.cardinality/one
                :db/unique :db.unique/value
                :db/index true
                :db/isComponent true
            }]
        "#;

        let partition_map0 = conn.partition_map.clone();
        let schema0 = conn.schema.clone();

        let report1 = assert_transact!(conn, t);
        let partition_map1 = conn.partition_map.clone();
        let schema1 = conn.schema.clone();

        let (new_schema, new_partition_map) = move_from_main_timeline(
            &conn.sqlite, &conn.schema, conn.partition_map.clone(),
            report1.tx_id.., 1).expect("moved single tx");
        update_conn(&mut conn, &new_schema, &new_partition_map);

        assert_matches!(conn.datoms(), "[]");
        assert_matches!(conn.transactions(), "[]");
        assert_eq!(conn.partition_map, partition_map0);
        
        // Assert all of schema's components individually, for some guidance in case of failures:
        assert_eq!(conn.schema.entid_map, schema0.entid_map);
        assert_eq!(conn.schema.ident_map, schema0.ident_map);
        assert_eq!(conn.schema.attribute_map, schema0.attribute_map);
        assert_eq!(conn.schema.component_attributes, schema0.component_attributes);
        // Assert the whole schema, just in case we missed something:
        assert_eq!(conn.schema, schema0);

        let report2 = assert_transact!(conn, t);
        let partition_map2 = conn.partition_map.clone();
        let schema2 = conn.schema.clone();

        assert_eq!(report1.tx_id, report2.tx_id);
        assert_eq!(partition_map1, partition_map2);
        assert_eq!(schema1, schema2);

        assert_matches!(conn.datoms(), r#"
            [[?e1 :db/ident :test/one]
             [?e1 :db/valueType :db.type/ref]
             [?e1 :db/cardinality :db.cardinality/one]
             [?e1 :db/unique :db.unique/value]
             [?e1 :db/isComponent true]
             [?e1 :db/index true]]
        "#);
        assert_matches!(conn.transactions(), r#"
            [[[?e1 :db/ident :test/one ?tx1 true]
             [?e1 :db/valueType :db.type/ref ?tx1 true]
             [?e1 :db/cardinality :db.cardinality/one ?tx1 true]
             [?e1 :db/unique :db.unique/value ?tx1 true]
             [?e1 :db/isComponent true ?tx1 true]
             [?e1 :db/index true ?tx1 true]
             [?tx1 :db/txInstant ?ms ?tx1 true]]]
        "#);
    }

    #[test]
    fn test_pop_in_sequence() {
        let mut conn = TestConn::default();
        conn.sanitized_partition_map();

        let partition_map_after_bootstrap = conn.partition_map.clone();

        assert_eq!((65536..65538),
                   conn.partition_map.allocate_entids(":db.part/user", 2));
        let tx_report0 = assert_transact!(conn, r#"[
            {:db/id 65536 :db/ident :test/one :db/valueType :db.type/long :db/cardinality :db.cardinality/one :db/unique :db.unique/identity :db/index true}
            {:db/id 65537 :db/ident :test/many :db/valueType :db.type/long :db/cardinality :db.cardinality/many}
        ]"#);

        let first = "[
            [65536 :db/ident :test/one]
            [65536 :db/valueType :db.type/long]
            [65536 :db/cardinality :db.cardinality/one]
            [65536 :db/unique :db.unique/identity]
            [65536 :db/index true]
            [65537 :db/ident :test/many]
            [65537 :db/valueType :db.type/long]
            [65537 :db/cardinality :db.cardinality/many]
        ]";
        assert_matches!(conn.datoms(), first);

        let partition_map0 = conn.partition_map.clone();

        assert_eq!((65538..65539),
                   conn.partition_map.allocate_entids(":db.part/user", 1));
        let tx_report1 = assert_transact!(conn, r#"[
            [:db/add 65538 :test/one 1]
            [:db/add 65538 :test/many 2]
            [:db/add 65538 :test/many 3]
        ]"#);
        let schema1 = conn.schema.clone();
        let partition_map1 = conn.partition_map.clone();

        assert_matches!(conn.last_transaction(),
                        "[[65538 :test/one 1 ?tx true]
                          [65538 :test/many 2 ?tx true]
                          [65538 :test/many 3 ?tx true]
                          [?tx :db/txInstant ?ms ?tx true]]");

        let second = "[
            [65536 :db/ident :test/one]
            [65536 :db/valueType :db.type/long]
            [65536 :db/cardinality :db.cardinality/one]
            [65536 :db/unique :db.unique/identity]
            [65536 :db/index true]
            [65537 :db/ident :test/many]
            [65537 :db/valueType :db.type/long]
            [65537 :db/cardinality :db.cardinality/many]
            [65538 :test/one 1]
            [65538 :test/many 2]
            [65538 :test/many 3]
        ]";
        assert_matches!(conn.datoms(), second);

        let tx_report2 = assert_transact!(conn, r#"[
            [:db/add 65538 :test/one 2]
            [:db/add 65538 :test/many 2]
            [:db/retract 65538 :test/many 3]
            [:db/add 65538 :test/many 4]
        ]"#);
        let schema2 = conn.schema.clone();

        assert_matches!(conn.last_transaction(),
                        "[[65538 :test/one 1 ?tx false]
                          [65538 :test/one 2 ?tx true]
                          [65538 :test/many 3 ?tx false]
                          [65538 :test/many 4 ?tx true]
                          [?tx :db/txInstant ?ms ?tx true]]");

        let third = "[
            [65536 :db/ident :test/one]
            [65536 :db/valueType :db.type/long]
            [65536 :db/cardinality :db.cardinality/one]
            [65536 :db/unique :db.unique/identity]
            [65536 :db/index true]
            [65537 :db/ident :test/many]
            [65537 :db/valueType :db.type/long]
            [65537 :db/cardinality :db.cardinality/many]
            [65538 :test/one 2]
            [65538 :test/many 2]
            [65538 :test/many 4]
        ]";
        assert_matches!(conn.datoms(), third);

        let (new_schema, new_partition_map) = move_from_main_timeline(
            &conn.sqlite, &conn.schema, conn.partition_map.clone(),
            tx_report2.tx_id.., 1).expect("moved timeline");
        update_conn(&mut conn, &new_schema, &new_partition_map);

        assert_matches!(conn.datoms(), second);
        // Moving didn't change the schema.
        assert_eq!(None, new_schema);
        assert_eq!(conn.schema, schema2);
        // But it did change the partition map.
        assert_eq!(conn.partition_map, partition_map1);

        let (new_schema, new_partition_map) = move_from_main_timeline(
            &conn.sqlite, &conn.schema, conn.partition_map.clone(),
            tx_report1.tx_id.., 2).expect("moved timeline");
        update_conn(&mut conn, &new_schema, &new_partition_map);
        assert_matches!(conn.datoms(), first);
        assert_eq!(None, new_schema);
        assert_eq!(schema1, conn.schema);
        assert_eq!(conn.partition_map, partition_map0);

        let (new_schema, new_partition_map) = move_from_main_timeline(
            &conn.sqlite, &conn.schema, conn.partition_map.clone(),
            tx_report0.tx_id.., 3).expect("moved timeline");
        update_conn(&mut conn, &new_schema, &new_partition_map);
        assert_eq!(true, new_schema.is_some());
        assert_eq!(bootstrap::bootstrap_schema(), conn.schema);
        assert_eq!(partition_map_after_bootstrap, conn.partition_map);
        assert_matches!(conn.datoms(), "[]");
        assert_matches!(conn.transactions(), "[]");
    }

    #[test]
    fn test_move_range() {
        let mut conn = TestConn::default();
        conn.sanitized_partition_map();

        let partition_map_after_bootstrap = conn.partition_map.clone();

        assert_eq!((65536..65539),
                   conn.partition_map.allocate_entids(":db.part/user", 3));
        let tx_report0 = assert_transact!(conn, r#"[
            {:db/id 65536 :db/ident :test/one :db/valueType :db.type/long :db/cardinality :db.cardinality/one}
            {:db/id 65537 :db/ident :test/many :db/valueType :db.type/long :db/cardinality :db.cardinality/many}
        ]"#);

        assert_transact!(conn, r#"[
            [:db/add 65538 :test/one 1]
            [:db/add 65538 :test/many 2]
            [:db/add 65538 :test/many 3]
        ]"#);

        assert_transact!(conn, r#"[
            [:db/add 65538 :test/one 2]
            [:db/add 65538 :test/many 2]
            [:db/retract 65538 :test/many 3]
            [:db/add 65538 :test/many 4]
        ]"#);

        // Remove all of these transactions from the main timeline,
        // ensure we get back to a "just bootstrapped" state.
        let (new_schema, new_partition_map) = move_from_main_timeline(
            &conn.sqlite, &conn.schema, conn.partition_map.clone(),
            tx_report0.tx_id.., 1).expect("moved timeline");
        update_conn(&mut conn, &new_schema, &new_partition_map);

        update_conn(&mut conn, &new_schema, &new_partition_map);
        assert_eq!(true, new_schema.is_some());
        assert_eq!(bootstrap::bootstrap_schema(), conn.schema);
        assert_eq!(partition_map_after_bootstrap, conn.partition_map);
        assert_matches!(conn.datoms(), "[]");
        assert_matches!(conn.transactions(), "[]");
    }
}
