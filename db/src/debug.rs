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
#![allow(unused_macros)]

/// Low-level functions for testing.

// Macro to parse a `Borrow<str>` to an `edn::Value` and assert the given `edn::Value` `matches`
// against it.
//
// This is a macro only to give nice line numbers when tests fail.
#[macro_export]
macro_rules! assert_matches {
    ( $input: expr, $expected: expr ) => {{
        // Failure to parse the expected pattern is a coding error, so we unwrap.
        let pattern_value = edn::parse::value($expected.borrow())
            .expect(format!("to be able to parse expected {}", $expected).as_str())
            .without_spans();
        let input_value = $input.to_edn();
        assert!(input_value.matches(&pattern_value),
                "Expected value:\n{}\nto match pattern:\n{}\n",
                input_value.to_pretty(120).unwrap(),
                pattern_value.to_pretty(120).unwrap());
    }}
}

// Transact $input against the given $conn, expecting success or a `Result<TxReport, String>`.
//
// This unwraps safely and makes asserting errors pleasant.
#[macro_export]
macro_rules! assert_transact {
    ( $conn: expr, $input: expr, $expected: expr ) => {{
        trace!("assert_transact: {}", $input);
        let result = $conn.transact($input).map_err(|e| e.to_string());
        assert_eq!(result, $expected.map_err(|e| e.to_string()));
    }};
    ( $conn: expr, $input: expr ) => {{
        trace!("assert_transact: {}", $input);
        let result = $conn.transact($input);
        assert!(result.is_ok(), "Expected Ok(_), got `{}`", result.unwrap_err());
        result.unwrap()
    }};
}

use std::borrow::Borrow;
use std::collections::BTreeMap;
use std::io::{Write};

use itertools::Itertools;
use rusqlite;
use rusqlite::{TransactionBehavior};
use rusqlite::types::{ToSql};
use tabwriter::TabWriter;

use bootstrap;
use db::*;
use db::{read_attribute_map,read_ident_map};
use edn;
use entids;
use errors::Result;
use mentat_core::{
    HasSchema,
    SQLValueType,
    TxReport,
    TypedValue,
    ValueType,
};
use edn::{
    InternSet,
};
use edn::entities::{
    EntidOrIdent,
    TempId,
};
use internal_types::{
    TermWithTempIds,
};
use schema::{
    SchemaBuilding,
};
use types::*;
use tx::{
    transact,
    transact_terms,
};
use watcher::NullWatcher;

/// Represents a *datom* (assertion) in the store.
#[derive(Clone,Debug,Eq,Hash,Ord,PartialOrd,PartialEq)]
pub struct Datom {
    // TODO: generalize this.
    pub e: EntidOrIdent,
    pub a: EntidOrIdent,
    pub v: edn::Value,
    pub tx: i64,
    pub added: Option<bool>,
}

/// Represents a set of datoms (assertions) in the store.
///
/// To make comparision easier, we deterministically order.  The ordering is the ascending tuple
/// ordering determined by `(e, a, (value_type_tag, v), tx)`, where `value_type_tag` is an internal
/// value that is not exposed but is deterministic.
pub struct Datoms(pub Vec<Datom>);

/// Represents an ordered sequence of transactions in the store.
///
/// To make comparision easier, we deterministically order.  The ordering is the ascending tuple
/// ordering determined by `(e, a, (value_type_tag, v), tx, added)`, where `value_type_tag` is an
/// internal value that is not exposed but is deterministic, and `added` is ordered such that
/// retracted assertions appear before added assertions.
pub struct Transactions(pub Vec<Datoms>);

/// Represents the fulltext values in the store.
pub struct FulltextValues(pub Vec<(i64, String)>);

impl Datom {
    pub fn to_edn(&self) -> edn::Value {
        let f = |entid: &EntidOrIdent| -> edn::Value {
            match *entid {
                EntidOrIdent::Entid(ref y) => edn::Value::Integer(y.clone()),
                EntidOrIdent::Ident(ref y) => edn::Value::Keyword(y.clone()),
            }
        };

        let mut v = vec![f(&self.e), f(&self.a), self.v.clone()];
        if let Some(added) = self.added {
            v.push(edn::Value::Integer(self.tx));
            v.push(edn::Value::Boolean(added));
        }

        edn::Value::Vector(v)
    }
}

impl Datoms {
    pub fn to_edn(&self) -> edn::Value {
        edn::Value::Vector((&self.0).into_iter().map(|x| x.to_edn()).collect())
    }
}

impl Transactions {
    pub fn to_edn(&self) -> edn::Value {
        edn::Value::Vector((&self.0).into_iter().map(|x| x.to_edn()).collect())
    }
}

impl FulltextValues {
    pub fn to_edn(&self) -> edn::Value {
        edn::Value::Vector((&self.0).into_iter().map(|&(x, ref y)| edn::Value::Vector(vec![edn::Value::Integer(x), edn::Value::Text(y.clone())])).collect())
    }
}

/// Turn TypedValue::Ref into TypedValue::Keyword when it is possible.
trait ToIdent {
  fn map_ident(self, schema: &Schema) -> Self;
}

impl ToIdent for TypedValue {
    fn map_ident(self, schema: &Schema) -> Self {
        if let TypedValue::Ref(e) = self {
            schema.get_ident(e).cloned().map(|i| i.into()).unwrap_or(TypedValue::Ref(e))
        } else {
            self
        }
    }
}

/// Convert a numeric entid to an ident `Entid` if possible, otherwise a numeric `Entid`.
pub fn to_entid(schema: &Schema, entid: i64) -> EntidOrIdent {
    schema.get_ident(entid).map_or(EntidOrIdent::Entid(entid), |ident| EntidOrIdent::Ident(ident.clone()))
}

// /// Convert a symbolic ident to an ident `Entid` if possible, otherwise a numeric `Entid`.
// pub fn to_ident(schema: &Schema, entid: i64) -> Entid {
//     schema.get_ident(entid).map_or(Entid::Entid(entid), |ident| Entid::Ident(ident.clone()))
// }

/// Return the set of datoms in the store, ordered by (e, a, v, tx), but not including any datoms of
/// the form [... :db/txInstant ...].
pub fn datoms<S: Borrow<Schema>>(conn: &rusqlite::Connection, schema: &S) -> Result<Datoms> {
    datoms_after(conn, schema, bootstrap::TX0 - 1)
}

/// Return the set of datoms in the store with transaction ID strictly greater than the given `tx`,
/// ordered by (e, a, v, tx).
///
/// The datom set returned does not include any datoms of the form [... :db/txInstant ...].
pub fn datoms_after<S: Borrow<Schema>>(conn: &rusqlite::Connection, schema: &S, tx: i64) -> Result<Datoms> {
    let borrowed_schema = schema.borrow();

    let mut stmt: rusqlite::Statement = conn.prepare("SELECT e, a, v, value_type_tag, tx FROM datoms WHERE tx > ? ORDER BY e ASC, a ASC, value_type_tag ASC, v ASC, tx ASC")?;

    let r: Result<Vec<_>> = stmt.query_and_then(&[&tx], |row| {
        let e: i64 = row.get_checked(0)?;
        let a: i64 = row.get_checked(1)?;

        if a == entids::DB_TX_INSTANT {
            return Ok(None);
        }

        let v: rusqlite::types::Value = row.get_checked(2)?;
        let value_type_tag: i32 = row.get_checked(3)?;

        let attribute = borrowed_schema.require_attribute_for_entid(a)?;
        let value_type_tag = if !attribute.fulltext { value_type_tag } else { ValueType::Long.value_type_tag() };

        let typed_value = TypedValue::from_sql_value_pair(v, value_type_tag)?.map_ident(borrowed_schema);
        let (value, _) = typed_value.to_edn_value_pair();

        let tx: i64 = row.get_checked(4)?;

        Ok(Some(Datom {
            e: EntidOrIdent::Entid(e),
            a: to_entid(borrowed_schema, a),
            v: value,
            tx: tx,
            added: None,
        }))
    })?.collect();

    Ok(Datoms(r?.into_iter().filter_map(|x| x).collect()))
}

/// Return the sequence of transactions in the store with transaction ID strictly greater than the
/// given `tx`, ordered by (tx, e, a, v).
///
/// Each transaction returned includes the [(transaction-tx) :db/txInstant ...] datom.
pub fn transactions_after<S: Borrow<Schema>>(conn: &rusqlite::Connection, schema: &S, tx: i64) -> Result<Transactions> {
    let borrowed_schema = schema.borrow();

    let mut stmt: rusqlite::Statement = conn.prepare("SELECT e, a, v, value_type_tag, tx, added FROM transactions WHERE tx > ? ORDER BY tx ASC, e ASC, a ASC, value_type_tag ASC, v ASC, added ASC")?;

    let r: Result<Vec<_>> = stmt.query_and_then(&[&tx], |row| {
        let e: i64 = row.get_checked(0)?;
        let a: i64 = row.get_checked(1)?;

        let v: rusqlite::types::Value = row.get_checked(2)?;
        let value_type_tag: i32 = row.get_checked(3)?;

        let attribute = borrowed_schema.require_attribute_for_entid(a)?;
        let value_type_tag = if !attribute.fulltext { value_type_tag } else { ValueType::Long.value_type_tag() };

        let typed_value = TypedValue::from_sql_value_pair(v, value_type_tag)?.map_ident(borrowed_schema);
        let (value, _) = typed_value.to_edn_value_pair();

        let tx: i64 = row.get_checked(4)?;
        let added: bool = row.get_checked(5)?;

        Ok(Datom {
            e: EntidOrIdent::Entid(e),
            a: to_entid(borrowed_schema, a),
            v: value,
            tx: tx,
            added: Some(added),
        })
    })?.collect();

    // Group by tx.
    let r: Vec<Datoms> = r?.into_iter().group_by(|x| x.tx).into_iter().map(|(_key, group)| Datoms(group.collect())).collect();
    Ok(Transactions(r))
}

/// Return the set of fulltext values in the store, ordered by rowid.
pub fn fulltext_values(conn: &rusqlite::Connection) -> Result<FulltextValues> {
    let mut stmt: rusqlite::Statement = conn.prepare("SELECT rowid, text FROM fulltext_values ORDER BY rowid")?;

    let r: Result<Vec<_>> = stmt.query_and_then(&[], |row| {
        let rowid: i64 = row.get_checked(0)?;
        let text: String = row.get_checked(1)?;
        Ok((rowid, text))
    })?.collect();

    r.map(FulltextValues)
}

/// Execute the given `sql` query with the given `params` and format the results as a
/// tab-and-newline formatted string suitable for debug printing.
///
/// The query is printed followed by a newline, then the returned columns followed by a newline, and
/// then the data rows and columns.  All columns are aligned.
pub fn dump_sql_query(conn: &rusqlite::Connection, sql: &str, params: &[&ToSql]) -> Result<String> {
    let mut stmt: rusqlite::Statement = conn.prepare(sql)?;

    let mut tw = TabWriter::new(Vec::new()).padding(2);
    write!(&mut tw, "{}\n", sql).unwrap();

    for column_name in stmt.column_names() {
        write!(&mut tw, "{}\t", column_name).unwrap();
    }
    write!(&mut tw, "\n").unwrap();

    let r: Result<Vec<_>> = stmt.query_and_then(params, |row| {
        for i in 0..row.column_count() {
            let value: rusqlite::types::Value = row.get_checked(i)?;
            write!(&mut tw, "{:?}\t", value).unwrap();
        }
        write!(&mut tw, "\n").unwrap();
        Ok(())
    })?.collect();
    r?;

    let dump = String::from_utf8(tw.into_inner().unwrap()).unwrap();
    Ok(dump)
}

// A connection that doesn't try to be clever about possibly sharing its `Schema`.  Compare to
// `mentat::Conn`.
pub struct TestConn {
    pub sqlite: rusqlite::Connection,
    pub partition_map: PartitionMap,
    pub schema: Schema,
}

impl TestConn {
    fn assert_materialized_views(&self) {
        let materialized_ident_map = read_ident_map(&self.sqlite).expect("ident map");
        let materialized_attribute_map = read_attribute_map(&self.sqlite).expect("schema map");

        let materialized_schema = Schema::from_ident_map_and_attribute_map(materialized_ident_map, materialized_attribute_map).expect("schema");
        assert_eq!(materialized_schema, self.schema);
    }

    pub fn transact<I>(&mut self, transaction: I) -> Result<TxReport> where I: Borrow<str> {
        // Failure to parse the transaction is a coding error, so we unwrap.
        let entities = edn::parse::entities(transaction.borrow()).expect(format!("to be able to parse {} into entities", transaction.borrow()).as_str());

        let details = {
            // The block scopes the borrow of self.sqlite.
            // We're about to write, so go straight ahead and get an IMMEDIATE transaction.
            let tx = self.sqlite.transaction_with_behavior(TransactionBehavior::Immediate)?;
            // Applying the transaction can fail, so we don't unwrap.
            let details = transact(&tx, self.partition_map.clone(), &self.schema, &self.schema, NullWatcher(), entities)?;
            tx.commit()?;
            details
        };

        let (report, next_partition_map, next_schema, _watcher) = details;
        self.partition_map = next_partition_map;
        if let Some(next_schema) = next_schema {
            self.schema = next_schema;
        }

        // Verify that we've updated the materialized views during transacting.
        self.assert_materialized_views();

        Ok(report)
    }

    pub fn transact_simple_terms<I>(&mut self, terms: I, tempid_set: InternSet<TempId>) -> Result<TxReport> where I: IntoIterator<Item=TermWithTempIds> {
        let details = {
            // The block scopes the borrow of self.sqlite.
            // We're about to write, so go straight ahead and get an IMMEDIATE transaction.
            let tx = self.sqlite.transaction_with_behavior(TransactionBehavior::Immediate)?;
            // Applying the transaction can fail, so we don't unwrap.
            let details = transact_terms(&tx, self.partition_map.clone(), &self.schema, &self.schema, NullWatcher(), terms, tempid_set)?;
            tx.commit()?;
            details
        };

        let (report, next_partition_map, next_schema, _watcher) = details;
        self.partition_map = next_partition_map;
        if let Some(next_schema) = next_schema {
            self.schema = next_schema;
        }

        // Verify that we've updated the materialized views during transacting.
        self.assert_materialized_views();

        Ok(report)
    }

    pub fn last_tx_id(&self) -> Entid {
        self.partition_map.get(&":db.part/tx".to_string()).unwrap().next_entid() - 1
    }

    pub fn last_transaction(&self) -> Datoms {
        transactions_after(&self.sqlite, &self.schema, self.last_tx_id() - 1).expect("last_transaction").0.pop().unwrap()
    }

    pub fn transactions(&self) -> Transactions {
        transactions_after(&self.sqlite, &self.schema, bootstrap::TX0).expect("transactions")
    }

    pub fn datoms(&self) -> Datoms {
        datoms_after(&self.sqlite, &self.schema, bootstrap::TX0).expect("datoms")
    }

    pub fn fulltext_values(&self) -> FulltextValues {
        fulltext_values(&self.sqlite).expect("fulltext_values")
    }

    pub fn with_sqlite(mut conn: rusqlite::Connection) -> TestConn {
        let db = ensure_current_version(&mut conn).unwrap();

        // Does not include :db/txInstant.
        let datoms = datoms_after(&conn, &db.schema, 0).unwrap();
        assert_eq!(datoms.0.len(), 94);

        // Includes :db/txInstant.
        let transactions = transactions_after(&conn, &db.schema, 0).unwrap();
        assert_eq!(transactions.0.len(), 1);
        assert_eq!(transactions.0[0].0.len(), 95);

        let mut parts = db.partition_map;

        // Add a fake partition to allow tests to do things like
        // [:db/add 111 :foo/bar 222]
        {
            let fake_partition = Partition::new(100, 2000, 1000, true);
            parts.insert(":db.part/fake".into(), fake_partition);
        }

        let test_conn = TestConn {
            sqlite: conn,
            partition_map: parts,
            schema: db.schema,
        };

        // Verify that we've created the materialized views during bootstrapping.
        test_conn.assert_materialized_views();

        test_conn
    }

    pub fn sanitized_partition_map(&mut self) {
        self.partition_map.remove(":db.part/fake");
    }
}

impl Default for TestConn {
    fn default() -> TestConn {
        TestConn::with_sqlite(new_connection("").expect("Couldn't open in-memory db"))
    }
}

pub struct TempIds(edn::Value);

impl TempIds {
    pub fn to_edn(&self) -> edn::Value {
        self.0.clone()
    }
}

pub fn tempids(report: &TxReport) -> TempIds {
    let mut map: BTreeMap<edn::Value, edn::Value> = BTreeMap::default();
    for (tempid, &entid) in report.tempids.iter() {
        map.insert(edn::Value::Text(tempid.clone()), edn::Value::Integer(entid));
    }
    TempIds(edn::Value::Map(map))
}
