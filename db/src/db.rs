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

use std::borrow::Borrow;
use std::collections::HashMap;
use std::fmt::Display;
use std::iter::{once, repeat};
use std::ops::Range;
use std::path::Path;
use std::rc::Rc;

use itertools;
use itertools::Itertools;
use rusqlite;
use rusqlite::TransactionBehavior;
use rusqlite::limits::Limit;
use rusqlite::types::{ToSql, ToSqlOutput};

use ::{repeat_values, to_namespaced_keyword};
use bootstrap;

use edn::{
    DateTime,
    Utc,
    Uuid,
    Value,
};

use entids;
use mentat_core::{
    attribute,
    Attribute,
    AttributeBitFlags,
    Entid,
    FromMicros,
    IdentMap,
    Schema,
    SchemaMap,
    TypedValue,
    ToMicros,
    ValueType,
};
use errors::{ErrorKind, Result, ResultExt};
use metadata;
use schema::{
    SchemaBuilding,
};
use types::{
    AVMap,
    AVPair,
    DB,
    Partition,
    PartitionMap,
};
use tx::transact;

pub fn new_connection<T>(uri: T) -> rusqlite::Result<rusqlite::Connection> where T: AsRef<Path> {
    let conn = match uri.as_ref().to_string_lossy().len() {
        0 => rusqlite::Connection::open_in_memory()?,
        _ => rusqlite::Connection::open(uri)?,
    };

    // See https://github.com/mozilla/mentat/issues/505 for details on temp_store
    // pragma and how it might interact together with consumers such as Firefox.
    // temp_store=2 is currently present to force SQLite to store temp files in memory.
    // Some of the platforms we support do not have a tmp partition (e.g. Android)
    // necessary to store temp files on disk. Ideally, consumers should be able to
    // override this behaviour (see issue 505).
    conn.execute_batch("
        PRAGMA page_size=32768;
        PRAGMA journal_mode=wal;
        PRAGMA wal_autocheckpoint=32;
        PRAGMA journal_size_limit=3145728;
        PRAGMA foreign_keys=ON;
        PRAGMA temp_store=2;
    ")?;

    Ok(conn)
}

/// Version history:
///
/// 1: initial Rust Mentat schema.
pub const CURRENT_VERSION: i32 = 1;

/// MIN_SQLITE_VERSION should be changed when there's a new minimum version of sqlite required
/// for the project to work.
const MIN_SQLITE_VERSION: i32 = 3008000;

const TRUE: &'static bool = &true;
const FALSE: &'static bool = &false;

/// Turn an owned bool into a static reference to a bool.
///
/// `rusqlite` is designed around references to values; this lets us use computed bools easily.
#[inline(always)]
fn to_bool_ref(x: bool) -> &'static bool {
    if x { TRUE } else { FALSE }
}

lazy_static! {
    /// SQL statements to be executed, in order, to create the Mentat SQL schema (version 1).
    #[cfg_attr(rustfmt, rustfmt_skip)]
    static ref V1_STATEMENTS: Vec<&'static str> = { vec![
        r#"CREATE TABLE datoms (e INTEGER NOT NULL, a SMALLINT NOT NULL, v BLOB NOT NULL, tx INTEGER NOT NULL,
                                value_type_tag SMALLINT NOT NULL,
                                index_avet TINYINT NOT NULL DEFAULT 0, index_vaet TINYINT NOT NULL DEFAULT 0,
                                index_fulltext TINYINT NOT NULL DEFAULT 0,
                                unique_value TINYINT NOT NULL DEFAULT 0)"#,
        r#"CREATE UNIQUE INDEX idx_datoms_eavt ON datoms (e, a, value_type_tag, v)"#,
        r#"CREATE UNIQUE INDEX idx_datoms_aevt ON datoms (a, e, value_type_tag, v)"#,

        // Opt-in index: only if a has :db/index true.
        r#"CREATE UNIQUE INDEX idx_datoms_avet ON datoms (a, value_type_tag, v, e) WHERE index_avet IS NOT 0"#,

        // Opt-in index: only if a has :db/valueType :db.type/ref.  No need for tag here since all
        // indexed elements are refs.
        r#"CREATE UNIQUE INDEX idx_datoms_vaet ON datoms (v, a, e) WHERE index_vaet IS NOT 0"#,

        // Opt-in index: only if a has :db/fulltext true; thus, it has :db/valueType :db.type/string,
        // which is not :db/valueType :db.type/ref.  That is, index_vaet and index_fulltext are mutually
        // exclusive.
        r#"CREATE INDEX idx_datoms_fulltext ON datoms (value_type_tag, v, a, e) WHERE index_fulltext IS NOT 0"#,

        // TODO: possibly remove this index.  :db.unique/{value,identity} should be asserted by the
        // transactor in all cases, but the index may speed up some of SQLite's query planning.  For now,
        // it serves to validate the transactor implementation.  Note that tag is needed here to
        // differentiate, e.g., keywords and strings.
        r#"CREATE UNIQUE INDEX idx_datoms_unique_value ON datoms (a, value_type_tag, v) WHERE unique_value IS NOT 0"#,

        r#"CREATE TABLE transactions (e INTEGER NOT NULL, a SMALLINT NOT NULL, v BLOB NOT NULL, tx INTEGER NOT NULL, added TINYINT NOT NULL DEFAULT 1, value_type_tag SMALLINT NOT NULL)"#,
        r#"CREATE INDEX idx_transactions_tx ON transactions (tx, added)"#,

        // Fulltext indexing.
        // A fulltext indexed value v is an integer rowid referencing fulltext_values.

        // Optional settings:
        // tokenize="porter"#,
        // prefix='2,3'
        // By default we use Unicode-aware tokenizing (particularly for case folding), but preserve
        // diacritics.
        r#"CREATE VIRTUAL TABLE fulltext_values
             USING FTS4 (text NOT NULL, searchid INT, tokenize=unicode61 "remove_diacritics=0")"#,

        // This combination of view and triggers allows you to transparently
        // update-or-insert into FTS. Just INSERT INTO fulltext_values_view (text, searchid).
        r#"CREATE VIEW fulltext_values_view AS SELECT * FROM fulltext_values"#,
        r#"CREATE TRIGGER replace_fulltext_searchid
             INSTEAD OF INSERT ON fulltext_values_view
             WHEN EXISTS (SELECT 1 FROM fulltext_values WHERE text = new.text)
             BEGIN
               UPDATE fulltext_values SET searchid = new.searchid WHERE text = new.text;
             END"#,
        r#"CREATE TRIGGER insert_fulltext_searchid
             INSTEAD OF INSERT ON fulltext_values_view
             WHEN NOT EXISTS (SELECT 1 FROM fulltext_values WHERE text = new.text)
             BEGIN
               INSERT INTO fulltext_values (text, searchid) VALUES (new.text, new.searchid);
             END"#,

        // A view transparently interpolating fulltext indexed values into the datom structure.
        r#"CREATE VIEW fulltext_datoms AS
             SELECT e, a, fulltext_values.text AS v, tx, value_type_tag, index_avet, index_vaet, index_fulltext, unique_value
               FROM datoms, fulltext_values
               WHERE datoms.index_fulltext IS NOT 0 AND datoms.v = fulltext_values.rowid"#,

        // A view transparently interpolating all entities (fulltext and non-fulltext) into the datom structure.
        r#"CREATE VIEW all_datoms AS
             SELECT e, a, v, tx, value_type_tag, index_avet, index_vaet, index_fulltext, unique_value
               FROM datoms
               WHERE index_fulltext IS 0
             UNION ALL
             SELECT e, a, v, tx, value_type_tag, index_avet, index_vaet, index_fulltext, unique_value
               FROM fulltext_datoms"#,

        // Materialized views of the metadata.
        r#"CREATE TABLE idents (e INTEGER NOT NULL, a SMALLINT NOT NULL, v BLOB NOT NULL, value_type_tag SMALLINT NOT NULL)"#,
        r#"CREATE INDEX idx_idents_unique ON idents (e, a, v, value_type_tag)"#,
        r#"CREATE TABLE schema (e INTEGER NOT NULL, a SMALLINT NOT NULL, v BLOB NOT NULL, value_type_tag SMALLINT NOT NULL)"#,
        r#"CREATE INDEX idx_schema_unique ON schema (e, a, v, value_type_tag)"#,
        // TODO: store entid instead of ident for partition name.
        r#"CREATE TABLE parts (part TEXT NOT NULL PRIMARY KEY, start INTEGER NOT NULL, idx INTEGER NOT NULL)"#,
        ]
    };
}

/// Set the SQLite user version.
///
/// Mentat manages its own SQL schema version using the user version.  See the [SQLite
/// documentation](https://www.sqlite.org/pragma.html#pragma_user_version).
fn set_user_version(conn: &rusqlite::Connection, version: i32) -> Result<()> {
    conn.execute(&format!("PRAGMA user_version = {}", version), &[])
        .chain_err(|| "Could not set_user_version")
        .map(|_| ())
}

/// Get the SQLite user version.
///
/// Mentat manages its own SQL schema version using the user version.  See the [SQLite
/// documentation](https://www.sqlite.org/pragma.html#pragma_user_version).
fn get_user_version(conn: &rusqlite::Connection) -> Result<i32> {
    conn.query_row("PRAGMA user_version", &[], |row| {
        row.get(0)
    })
        .chain_err(|| "Could not get_user_version")
}

// TODO: rename "SQL" functions to align with "datoms" functions.
pub fn create_current_version(conn: &mut rusqlite::Connection) -> Result<DB> {
    let tx = conn.transaction_with_behavior(TransactionBehavior::Exclusive)?;

    for statement in (&V1_STATEMENTS).iter() {
        tx.execute(statement, &[])?;
    }

    let bootstrap_partition_map = bootstrap::bootstrap_partition_map();
    // TODO: think more carefully about allocating new parts and bitmasking part ranges.
    // TODO: install these using bootstrap assertions.  It's tricky because the part ranges are implicit.
    // TODO: one insert, chunk into 999/3 sections, for safety.
    // This is necessary: `transact` will only UPDATE parts, not INSERT them if they're missing.
    for (part, partition) in bootstrap_partition_map.iter() {
        // TODO: Convert "keyword" part to SQL using Value conversion.
        tx.execute("INSERT INTO parts VALUES (?, ?, ?)", &[part, &partition.start, &partition.index])?;
    }

    // TODO: return to transact_internal to self-manage the encompassing SQLite transaction.
    let bootstrap_schema = bootstrap::bootstrap_schema();
    let bootstrap_schema_for_mutation = Schema::default(); // The bootstrap transaction will populate this schema.
    let (_report, next_partition_map, next_schema) = transact(&tx, bootstrap_partition_map, &bootstrap_schema_for_mutation, &bootstrap_schema, bootstrap::bootstrap_entities())?;
    // TODO: validate metadata mutations that aren't schema related, like additional partitions.
    if let Some(next_schema) = next_schema {
        if next_schema != bootstrap_schema {
            // TODO Use custom ErrorKind https://github.com/brson/error-chain/issues/117
            bail!(ErrorKind::NotYetImplemented(format!("Initial bootstrap transaction did not produce expected bootstrap schema")));
        }
    }

    set_user_version(&tx, CURRENT_VERSION)?;

    // TODO: use the drop semantics to do this automagically?
    tx.commit()?;

    let bootstrap_db = DB::new(next_partition_map, bootstrap_schema);
    Ok(bootstrap_db)
}

// (def v2-statements v1-statements)

// (defn create-temp-tx-lookup-statement [table-name]
//   // n.b., v0/value_type_tag0 can be NULL, in which case we look up v from datoms;
//   // and the datom columns are NULL into the LEFT JOIN fills them in.
//   // The table-name is not escaped in any way, in order to allow r#"temp.dotted" names.
//   // TODO: update comment about sv.
//   [(str r#"CREATE TABLE IF NOT EXISTS r#" table-name
//         r#" (e0 INTEGER NOT NULL, a0 SMALLINT NOT NULL, v0 BLOB NOT NULL, tx0 INTEGER NOT NULL, added0 TINYINT NOT NULL,
//            value_type_tag0 SMALLINT NOT NULL,
//            index_avet0 TINYINT, index_vaet0 TINYINT,
//            index_fulltext0 TINYINT,
//            unique_value0 TINYINT,
//            sv BLOB,
//            svalue_type_tag SMALLINT,
//            rid INTEGER,
//            e INTEGER, a SMALLINT, v BLOB, tx INTEGER, value_type_tag SMALLINT)")])

// (defn create-temp-tx-lookup-eavt-statement [idx-name table-name]
//   // Note that the consuming code creates and drops the indexes
//   // manually, which makes insertion slightly faster.
//   // This index prevents overlapping transactions.
//   // The idx-name and table-name are not escaped in any way, in order
//   // to allow r#"temp.dotted" names.
//   // TODO: drop added0?
//   [(str r#"CREATE UNIQUE INDEX IF NOT EXISTS r#"#,
//         idx-name
//         r#" ON r#"#,
//         table-name
//         r#" (e0, a0, v0, added0, value_type_tag0) WHERE sv IS NOT NULL")])

// (defn <create-current-version
//   [db bootstrapper]
//   (println r#"Creating database at" current-version)
//   (s/in-transaction!
//     db
//     #(go-pair
//        (doseq [statement v2-statements]
//          (try
//            (<? (s/execute! db [statement]))
//            (catch #?(:clj Throwable :cljs js/Error) e
//              (throw (ex-info r#"Failed to execute statement" {:statement statement} e)))))
//        (<? (bootstrapper db 0))
//        (<? (s/set-user-version db current-version))
//        [0 (<? (s/get-user-version db))])))


// (defn <update-from-version
//   [db from-version bootstrapper]
//   {:pre [(> from-version 0)]} // Or we'd create-current-version instead.
//   {:pre [(< from-version current-version)]} // Or we wouldn't need to update-from-version.
//   (println r#"Upgrading database from" from-version r#"to" current-version)
//   (s/in-transaction!
//     db
//     #(go-pair
//        // We must only be migrating from v1 to v2.
//        (let [statement r#"UPDATE parts SET idx = idx + 2 WHERE part = ?"]
//          (try
//            (<? (s/execute!
//                  db
//                  [statement :db.part/db]))
//            (catch #?(:clj Throwable :cljs js/Error) e
//              (throw (ex-info r#"Failed to execute statement" {:statement statement} e)))))
//        (<? (bootstrapper db from-version))
//        (<? (s/set-user-version db current-version))
//        [from-version (<? (s/get-user-version db))])))

// (defn <ensure-current-version
//   r#"Returns a pair: [previous-version current-version]."#,
//   [db bootstrapper]
//   (go-pair
//     (let [v (<? (s/get-user-version db))]
//       (cond
//         (= v current-version)
//         [v v]

//         (= v 0)
//         (<? (<create-current-version db bootstrapper))

//         (< v current-version)
//         (<? (<update-from-version db v bootstrapper))))))
// */

pub fn ensure_current_version(conn: &mut rusqlite::Connection) -> Result<DB> {
    if rusqlite::version_number() < MIN_SQLITE_VERSION {
        panic!("Mentat requires at least sqlite {}", MIN_SQLITE_VERSION);
    }

    let user_version = get_user_version(&conn)?;
    match user_version {
        0               => create_current_version(conn),
        CURRENT_VERSION => read_db(conn),

        // TODO: support updating an existing store.
        v => bail!(ErrorKind::NotYetImplemented(format!("Opening databases with Mentat version: {}", v))),
    }
}

pub trait TypedSQLValue {
    fn from_sql_value_pair(value: rusqlite::types::Value, value_type_tag: i32) -> Result<TypedValue>;
    fn to_sql_value_pair<'a>(&'a self) -> (ToSqlOutput<'a>, i32);
    fn from_edn_value(value: &Value) -> Option<TypedValue>;
    fn to_edn_value_pair(&self) -> (Value, ValueType);
}

impl TypedSQLValue for TypedValue {
    /// Given a SQLite `value` and a `value_type_tag`, return the corresponding `TypedValue`.
    fn from_sql_value_pair(value: rusqlite::types::Value, value_type_tag: i32) -> Result<TypedValue> {
        match (value_type_tag, value) {
            (0, rusqlite::types::Value::Integer(x)) => Ok(TypedValue::Ref(x)),
            (1, rusqlite::types::Value::Integer(x)) => Ok(TypedValue::Boolean(0 != x)),

            // Negative integers are simply times before 1970.
            (4, rusqlite::types::Value::Integer(x)) => Ok(TypedValue::Instant(DateTime::<Utc>::from_micros(x))),

            // SQLite distinguishes integral from decimal types, allowing long and double to
            // share a tag.
            (5, rusqlite::types::Value::Integer(x)) => Ok(TypedValue::Long(x)),
            (5, rusqlite::types::Value::Real(x)) => Ok(TypedValue::Double(x.into())),
            (10, rusqlite::types::Value::Text(x)) => Ok(TypedValue::String(Rc::new(x))),
            (11, rusqlite::types::Value::Blob(x)) => {
                let u = Uuid::from_bytes(x.as_slice());
                if u.is_err() {
                    // Rather than exposing Uuid's ParseError…
                    bail!(ErrorKind::BadSQLValuePair(rusqlite::types::Value::Blob(x),
                                                     value_type_tag));
                }
                Ok(TypedValue::Uuid(u.unwrap()))
            },
            (13, rusqlite::types::Value::Text(x)) => {
                to_namespaced_keyword(&x).map(|k| TypedValue::Keyword(Rc::new(k)))
            },
            (_, value) => bail!(ErrorKind::BadSQLValuePair(value, value_type_tag)),
        }
    }

    /// Given an EDN `value`, return a corresponding Mentat `TypedValue`.
    ///
    /// An EDN `Value` does not encode a unique Mentat `ValueType`, so the composition
    /// `from_edn_value(first(to_edn_value_pair(...)))` loses information.  Additionally, there are
    /// EDN values which are not Mentat typed values.
    ///
    /// This function is deterministic.
    fn from_edn_value(value: &Value) -> Option<TypedValue> {
        match value {
            &Value::Boolean(x) => Some(TypedValue::Boolean(x)),
            &Value::Instant(x) => Some(TypedValue::Instant(x)),
            &Value::Integer(x) => Some(TypedValue::Long(x)),
            &Value::Uuid(x) => Some(TypedValue::Uuid(x)),
            &Value::Float(ref x) => Some(TypedValue::Double(x.clone())),
            &Value::Text(ref x) => Some(TypedValue::String(Rc::new(x.clone()))),
            &Value::NamespacedKeyword(ref x) => Some(TypedValue::Keyword(Rc::new(x.clone()))),
            _ => None
        }
    }

    /// Return the corresponding SQLite `value` and `value_type_tag` pair.
    fn to_sql_value_pair<'a>(&'a self) -> (ToSqlOutput<'a>, i32) {
        match self {
            &TypedValue::Ref(x) => (rusqlite::types::Value::Integer(x).into(), 0),
            &TypedValue::Boolean(x) => (rusqlite::types::Value::Integer(if x { 1 } else { 0 }).into(), 1),
            &TypedValue::Instant(x) => (rusqlite::types::Value::Integer(x.to_micros()).into(), 4),
            // SQLite distinguishes integral from decimal types, allowing long and double to share a tag.
            &TypedValue::Long(x) => (rusqlite::types::Value::Integer(x).into(), 5),
            &TypedValue::Double(x) => (rusqlite::types::Value::Real(x.into_inner()).into(), 5),
            &TypedValue::String(ref x) => (rusqlite::types::ValueRef::Text(x.as_str()).into(), 10),
            &TypedValue::Uuid(ref u) => (rusqlite::types::Value::Blob(u.as_bytes().to_vec()).into(), 11),
            &TypedValue::Keyword(ref x) => (rusqlite::types::ValueRef::Text(&x.to_string()).into(), 13),
        }
    }

    /// Return the corresponding EDN `value` and `value_type` pair.
    fn to_edn_value_pair(&self) -> (Value, ValueType) {
        match self {
            &TypedValue::Ref(x) => (Value::Integer(x), ValueType::Ref),
            &TypedValue::Boolean(x) => (Value::Boolean(x), ValueType::Boolean),
            &TypedValue::Instant(x) => (Value::Instant(x), ValueType::Instant),
            &TypedValue::Long(x) => (Value::Integer(x), ValueType::Long),
            &TypedValue::Double(x) => (Value::Float(x), ValueType::Double),
            &TypedValue::String(ref x) => (Value::Text(x.as_ref().clone()), ValueType::String),
            &TypedValue::Uuid(ref u) => (Value::Uuid(u.clone()), ValueType::Uuid),
            &TypedValue::Keyword(ref x) => (Value::NamespacedKeyword(x.as_ref().clone()), ValueType::Keyword),
        }
    }
}

/// Read an arbitrary [e a v value_type_tag] materialized view from the given table in the SQL
/// store.
fn read_materialized_view(conn: &rusqlite::Connection, table: &str) -> Result<Vec<(Entid, Entid, TypedValue)>> {
    let mut stmt: rusqlite::Statement = conn.prepare(format!("SELECT e, a, v, value_type_tag FROM {}", table).as_str())?;
    let m: Result<Vec<(Entid, Entid, TypedValue)>> = stmt.query_and_then(&[], |row| {
        let e: Entid = row.get_checked(0)?;
        let a: Entid = row.get_checked(1)?;
        let v: rusqlite::types::Value = row.get_checked(2)?;
        let value_type_tag: i32 = row.get_checked(3)?;
        let typed_value = TypedValue::from_sql_value_pair(v, value_type_tag)?;
        Ok((e, a, typed_value))
    })?.collect();
    m
}

/// Read the partition map materialized view from the given SQL store.
fn read_partition_map(conn: &rusqlite::Connection) -> Result<PartitionMap> {
    let mut stmt: rusqlite::Statement = conn.prepare("SELECT part, start, idx FROM parts")?;
    let m = stmt.query_and_then(&[], |row| -> Result<(String, Partition)> {
        Ok((row.get_checked(0)?, Partition::new(row.get_checked(1)?, row.get_checked(2)?)))
    })?.collect();
    m
}

/// Read the ident map materialized view from the given SQL store.
fn read_ident_map(conn: &rusqlite::Connection) -> Result<IdentMap> {
    let v = read_materialized_view(conn, "idents")?;
    v.into_iter().map(|(e, a, typed_value)| {
        if a != entids::DB_IDENT {
            bail!(ErrorKind::NotYetImplemented(format!("bad idents materialized view: expected :db/ident but got {}", a)));
        }
        if let TypedValue::Keyword(keyword) = typed_value {
            Ok((keyword.as_ref().clone(), e))
        } else {
            bail!(ErrorKind::NotYetImplemented(format!("bad idents materialized view: expected [entid :db/ident keyword] but got [entid :db/ident {:?}]", typed_value)));
        }
    }).collect()
}

/// Read the schema materialized view from the given SQL store.
fn read_schema_map(conn: &rusqlite::Connection) -> Result<SchemaMap> {
    let entid_triples = read_materialized_view(conn, "schema")?;
    let mut schema_map = SchemaMap::default();
    metadata::update_schema_map_from_entid_triples(&mut schema_map, entid_triples)?;
    Ok(schema_map)
}

/// Read the materialized views from the given SQL store and return a Mentat `DB` for querying and
/// applying transactions.
pub fn read_db(conn: &rusqlite::Connection) -> Result<DB> {
    let partition_map = read_partition_map(conn)?;
    let ident_map = read_ident_map(conn)?;
    let schema_map = read_schema_map(conn)?;
    let schema = Schema::from_ident_map_and_schema_map(ident_map, schema_map)?;
    Ok(DB::new(partition_map, schema))
}

/// Internal representation of an [e a v added] datom, ready to be transacted against the store.
pub type ReducedEntity<'a> = (Entid, Entid, &'a Attribute, TypedValue, bool);

#[derive(Clone,Debug,Eq,Hash,Ord,PartialOrd,PartialEq)]
pub enum SearchType {
    Exact,
    Inexact,
}

/// `MentatStoring` will be the trait that encapsulates the storage layer.  It is consumed by the
/// transaction processing layer.
///
/// Right now, the only implementation of `MentatStoring` is the SQLite-specific SQL schema.  In the
/// future, we might consider other SQL engines (perhaps with different fulltext indexing), or
/// entirely different data stores, say ones shaped like key-value stores.
pub trait MentatStoring {
    /// Given a slice of [a v] lookup-refs, look up the corresponding [e a v] triples.
    ///
    /// It is assumed that the attribute `a` in each lookup-ref is `:db/unique`, so that at most one
    /// matching [e a v] triple exists.  (If this is not true, some matching entid `e` will be
    /// chosen non-deterministically, if one exists.)
    ///
    /// Returns a map &(a, v) -> e, to avoid cloning potentially large values.  The keys of the map
    /// are exactly those (a, v) pairs that have an assertion [e a v] in the store.
    fn resolve_avs<'a>(&self, avs: &'a [&'a AVPair]) -> Result<AVMap<'a>>;

    /// Begin (or prepare) the underlying storage layer for a new Mentat transaction.
    ///
    /// Use this to create temporary tables, prepare indices, set pragmas, etc, before the initial
    /// `insert_non_fts_searches` invocation.
    fn begin_tx_application(&self) -> Result<()>;

    // TODO: this is not a reasonable abstraction, but I don't want to really consider non-SQL storage just yet.
    fn insert_non_fts_searches<'a>(&self, entities: &'a [ReducedEntity], search_type: SearchType) -> Result<()>;
    fn insert_fts_searches<'a>(&self, entities: &'a [ReducedEntity], search_type: SearchType) -> Result<()>;

    /// Finalize the underlying storage layer after a Mentat transaction.
    ///
    /// Use this to finalize temporary tables, complete indices, revert pragmas, etc, after the
    /// final `insert_non_fts_searches` invocation.
    fn commit_transaction(&self, tx_id: Entid) -> Result<()>;

    /// Extract metadata-related [e a typed_value added] datoms committed in the given transaction.
    fn committed_metadata_assertions(&self, tx_id: Entid) -> Result<Vec<(Entid, Entid, TypedValue, bool)>>;
}

/// Take search rows and complete `temp.search_results`.
///
/// See https://github.com/mozilla/mentat/wiki/Transacting:-entity-to-SQL-translation.
fn search(conn: &rusqlite::Connection) -> Result<()> {
    // First is fast, only one table walk: lookup by exact eav.
    // Second is slower, but still only one table walk: lookup old value by ea.
    let s = r#"
      INSERT INTO temp.search_results
      SELECT t.e0, t.a0, t.v0, t.value_type_tag0, t.added0, t.flags0, ':db.cardinality/many', d.rowid, d.v
      FROM temp.exact_searches AS t
      LEFT JOIN datoms AS d
      ON t.e0 = d.e AND
         t.a0 = d.a AND
         t.value_type_tag0 = d.value_type_tag AND
         t.v0 = d.v

      UNION ALL

      SELECT t.e0, t.a0, t.v0, t.value_type_tag0, t.added0, t.flags0, ':db.cardinality/one', d.rowid, d.v
      FROM temp.inexact_searches AS t
      LEFT JOIN datoms AS d
      ON t.e0 = d.e AND
         t.a0 = d.a"#;

    let mut stmt = conn.prepare_cached(s)?;
    stmt.execute(&[])
        .map(|_c| ())
        .chain_err(|| "Could not search!")
}

/// Insert the new transaction into the `transactions` table.
///
/// This turns the contents of `search_results` into a new transaction.
///
/// See https://github.com/mozilla/mentat/wiki/Transacting:-entity-to-SQL-translation.
fn insert_transaction(conn: &rusqlite::Connection, tx: Entid) -> Result<()> {
    let s = r#"
      INSERT INTO transactions (e, a, v, tx, added, value_type_tag)
      SELECT e0, a0, v0, ?, 1, value_type_tag0
      FROM temp.search_results
      WHERE added0 IS 1 AND ((rid IS NULL) OR ((rid IS NOT NULL) AND (v0 IS NOT v)))"#;

    let mut stmt = conn.prepare_cached(s)?;
    stmt.execute(&[&tx])
        .map(|_c| ())
        .chain_err(|| "Could not insert transaction: failed to add datoms not already present")?;

    let s = r#"
      INSERT INTO transactions (e, a, v, tx, added, value_type_tag)
      SELECT e0, a0, v, ?, 0, value_type_tag0
      FROM temp.search_results
      WHERE rid IS NOT NULL AND
            ((added0 IS 0) OR
             (added0 IS 1 AND search_type IS ':db.cardinality/one' AND v0 IS NOT v))"#;

    let mut stmt = conn.prepare_cached(s)?;
    stmt.execute(&[&tx])
        .map(|_c| ())
        .chain_err(|| "Could not insert transaction: failed to retract datoms already present")?;

    Ok(())
}

/// Update the contents of the `datoms` materialized view with the new transaction.
///
/// This applies the contents of `search_results` to the `datoms` table (in place).
///
/// See https://github.com/mozilla/mentat/wiki/Transacting:-entity-to-SQL-translation.
fn update_datoms(conn: &rusqlite::Connection, tx: Entid) -> Result<()> {
    // Delete datoms that were retracted, or those that were :db.cardinality/one and will be
    // replaced.
    let s = r#"
        WITH ids AS (SELECT rid
                     FROM temp.search_results
                     WHERE rid IS NOT NULL AND
                           ((added0 IS 0) OR
                            (added0 IS 1 AND search_type IS ':db.cardinality/one' AND v0 IS NOT v)))
        DELETE FROM datoms WHERE rowid IN ids"#;

    let mut stmt = conn.prepare_cached(s)?;
    stmt.execute(&[])
        .map(|_c| ())
        .chain_err(|| "Could not update datoms: failed to retract datoms already present")?;

    // Insert datoms that were added and not already present. We also must
    // expand our bitfield into flags.
    let s = format!(r#"
      INSERT INTO datoms (e, a, v, tx, value_type_tag, index_avet, index_vaet, index_fulltext, unique_value)
      SELECT e0, a0, v0, ?, value_type_tag0,
             flags0 & {} IS NOT 0,
             flags0 & {} IS NOT 0,
             flags0 & {} IS NOT 0,
             flags0 & {} IS NOT 0
      FROM temp.search_results
      WHERE added0 IS 1 AND ((rid IS NULL) OR ((rid IS NOT NULL) AND (v0 IS NOT v)))"#,
      AttributeBitFlags::IndexAVET as u8,
      AttributeBitFlags::IndexVAET as u8,
      AttributeBitFlags::IndexFulltext as u8,
      AttributeBitFlags::UniqueValue as u8);

    let mut stmt = conn.prepare_cached(&s)?;
    stmt.execute(&[&tx])
        .map(|_c| ())
        .chain_err(|| "Could not update datoms: failed to add datoms not already present")?;

    Ok(())
}

impl MentatStoring for rusqlite::Connection {
    fn resolve_avs<'a>(&self, avs: &'a [&'a AVPair]) -> Result<AVMap<'a>> {
        // Start search_id's at some identifiable number.
        let initial_search_id = 2000;
        let bindings_per_statement = 4;

        // We map [a v] -> numeric search_id -> e, and then we use the search_id lookups to finally
        // produce the map [a v] -> e.
        //
        // TODO: `collect` into a HashSet so that any (a, v) is resolved at most once.
        let max_vars = self.limit(Limit::SQLITE_LIMIT_VARIABLE_NUMBER) as usize;
        let chunks: itertools::IntoChunks<_> = avs.into_iter().enumerate().chunks(max_vars / 4);

        // We'd like to `flat_map` here, but it's not obvious how to `flat_map` across `Result`.
        // Alternatively, this is a `fold`, and it might be wise to express it as such.
        let results: Result<Vec<Vec<_>>> = chunks.into_iter().map(|chunk| -> Result<Vec<_>> {
            let mut count = 0;

            // We must keep these computed values somewhere to reference them later, so we can't
            // combine this `map` and the subsequent `flat_map`.
            let block: Vec<(i64, i64, ToSqlOutput<'a>, i32)> = chunk.map(|(index, &&(a, ref v))| {
                count += 1;
                let search_id: i64 = initial_search_id + index as i64;
                let (value, value_type_tag) = v.to_sql_value_pair();
                (search_id, a, value, value_type_tag)
            }).collect();

            // `params` reference computed values in `block`.
            let params: Vec<&ToSql> = block.iter().flat_map(|&(ref searchid, ref a, ref value, ref value_type_tag)| {
                // Avoid inner heap allocation.
                once(searchid as &ToSql)
                    .chain(once(a as &ToSql)
                           .chain(once(value as &ToSql)
                                  .chain(once(value_type_tag as &ToSql))))
            }).collect();

            // TODO: cache these statements for selected values of `count`.
            // TODO: query against `datoms` and UNION ALL with `fulltext_datoms` rather than
            // querying against `all_datoms`.  We know all the attributes, and in the common case,
            // where most unique attributes will not be fulltext-indexed, we'll be querying just
            // `datoms`, which will be much faster.ˇ
            assert!(bindings_per_statement * count < max_vars, "Too many values: {} * {} >= {}", bindings_per_statement, count, max_vars);

            let values: String = repeat_values(bindings_per_statement, count);
            let s: String = format!("WITH t(search_id, a, v, value_type_tag) AS (VALUES {}) SELECT t.search_id, d.e \
                                     FROM t, all_datoms AS d \
                                     WHERE d.index_avet IS NOT 0 AND d.a = t.a AND d.value_type_tag = t.value_type_tag AND d.v = t.v",
                                    values);
            let mut stmt: rusqlite::Statement = self.prepare(s.as_str())?;

            let m: Result<Vec<(i64, Entid)>> = stmt.query_and_then(&params, |row| -> Result<(i64, Entid)> {
                Ok((row.get_checked(0)?, row.get_checked(1)?))
            })?.collect();
            m
        }).collect::<Result<Vec<Vec<(i64, Entid)>>>>();

        // Flatten.
        let results: Vec<(i64, Entid)> = results?.as_slice().concat();

        // Create map [a v] -> e.
        let m: HashMap<&'a AVPair, Entid> = results.into_iter().map(|(search_id, entid)| {
            let index: usize = (search_id - initial_search_id) as usize;
            (avs[index], entid)
        }).collect();
        Ok(m)
    }

    /// Create empty temporary tables for search parameters and search results.
    fn begin_tx_application(&self) -> Result<()> {
        // We can't do this in one shot, since we can't prepare a batch statement.
        let statements = [
            r#"DROP TABLE IF EXISTS temp.exact_searches"#,
            // Note that `flags0` is a bitfield of several flags compressed via
            // `AttributeBitFlags.flags()` in the temporary search tables, later
            // expanded in the `datoms` insertion.
            r#"CREATE TABLE temp.exact_searches (
               e0 INTEGER NOT NULL,
               a0 SMALLINT NOT NULL,
               v0 BLOB NOT NULL,
               value_type_tag0 SMALLINT NOT NULL,
               added0 TINYINT NOT NULL,
               flags0 TINYINT NOT NULL)"#,
            // There's no real need to split exact and inexact searches, so long as we keep things
            // in the correct place and performant.  Splitting has the advantage of being explicit
            // and slightly easier to read, so we'll do that to start.
            r#"DROP TABLE IF EXISTS temp.inexact_searches"#,
            r#"CREATE TABLE temp.inexact_searches (
               e0 INTEGER NOT NULL,
               a0 SMALLINT NOT NULL,
               v0 BLOB NOT NULL,
               value_type_tag0 SMALLINT NOT NULL,
               added0 TINYINT NOT NULL,
               flags0 TINYINT NOT NULL)"#,
            r#"DROP TABLE IF EXISTS temp.search_results"#,
            // TODO: don't encode search_type as a STRING.  This is explicit and much easier to read
            // than another flag, so we'll do it to start, and optimize later.
            r#"CREATE TABLE temp.search_results (
               e0 INTEGER NOT NULL,
               a0 SMALLINT NOT NULL,
               v0 BLOB NOT NULL,
               value_type_tag0 SMALLINT NOT NULL,
               added0 TINYINT NOT NULL,
               flags0 TINYINT NOT NULL,
               search_type STRING NOT NULL,
               rid INTEGER,
               v BLOB)"#,
            // It is an error to transact the same [e a v] twice in one transaction.  This index will
            // cause insertion to fail if a transaction tries to do that.  (Sadly, the failure is
            // opaque.)
            //
            // N.b.: temp goes on index name, not table name.  See http://stackoverflow.com/a/22308016.
            r#"CREATE UNIQUE INDEX IF NOT EXISTS temp.search_results_unique ON search_results (e0, a0, v0, value_type_tag0)"#,
        ];

        for statement in &statements {
            let mut stmt = self.prepare_cached(statement)?;
            stmt.execute(&[])
                .map(|_c| ())
                .chain_err(|| "Failed to create temporary tables")?;
        }

        Ok(())
    }

    /// Insert search rows into temporary search tables.
    ///
    /// Eventually, the details of this approach will be captured in
    /// https://github.com/mozilla/mentat/wiki/Transacting:-entity-to-SQL-translation.
    fn insert_non_fts_searches<'a>(&self, entities: &'a [ReducedEntity<'a>], search_type: SearchType) -> Result<()> {
        let bindings_per_statement = 6;

        let max_vars = self.limit(Limit::SQLITE_LIMIT_VARIABLE_NUMBER) as usize;
        let chunks: itertools::IntoChunks<_> = entities.into_iter().chunks(max_vars / bindings_per_statement);

        // We'd like to flat_map here, but it's not obvious how to flat_map across Result.
        let results: Result<Vec<()>> = chunks.into_iter().map(|chunk| -> Result<()> {
            let mut count = 0;

            // We must keep these computed values somewhere to reference them later, so we can't
            // combine this map and the subsequent flat_map.
            // (e0, a0, v0, value_type_tag0, added0, flags0)
            let block: Result<Vec<(i64 /* e */,
                                   i64 /* a */,
                                   ToSqlOutput<'a> /* value */,
                                   i32 /* value_type_tag */,
                                   bool, /* added0 */
                                   u8 /* flags0 */)>> = chunk.map(|&(e, a, ref attribute, ref typed_value, added)| {
                count += 1;

                // Now we can represent the typed value as an SQL value.
                let (value, value_type_tag): (ToSqlOutput, i32) = typed_value.to_sql_value_pair();

                Ok((e, a, value, value_type_tag, added, attribute.flags()))
            }).collect();
            let block = block?;

            // `params` reference computed values in `block`.
            let params: Vec<&ToSql> = block.iter().flat_map(|&(ref e, ref a, ref value, ref value_type_tag, added, ref flags)| {
                // Avoid inner heap allocation.
                // TODO: extract some finite length iterator to make this less indented!
                once(e as &ToSql)
                    .chain(once(a as &ToSql)
                           .chain(once(value as &ToSql)
                                  .chain(once(value_type_tag as &ToSql)
                                         .chain(once(to_bool_ref(added) as &ToSql)
                                                .chain(once(flags as &ToSql))))))
            }).collect();

            // TODO: cache this for selected values of count.
            assert!(bindings_per_statement * count < max_vars, "Too many values: {} * {} >= {}", bindings_per_statement, count, max_vars);
            let values: String = repeat_values(bindings_per_statement, count);
            let s: String = if search_type == SearchType::Exact {
                format!("INSERT INTO temp.exact_searches (e0, a0, v0, value_type_tag0, added0, flags0) VALUES {}", values)
            } else {
                format!("INSERT INTO temp.inexact_searches (e0, a0, v0, value_type_tag0, added0, flags0) VALUES {}", values)
            };

            // TODO: consider ensuring we inserted the expected number of rows.
            let mut stmt = self.prepare_cached(s.as_str())?;
            stmt.execute(&params)
                .map(|_c| ())
                .chain_err(|| "Could not insert non-fts one statements into temporary search table!")
        }).collect::<Result<Vec<()>>>();

        results.map(|_| ())
    }

    /// Insert search rows into temporary search tables.
    ///
    /// Eventually, the details of this approach will be captured in
    /// https://github.com/mozilla/mentat/wiki/Transacting:-entity-to-SQL-translation.
    fn insert_fts_searches<'a>(&self, entities: &'a [ReducedEntity<'a>], search_type: SearchType) -> Result<()> {
        let max_vars = self.limit(Limit::SQLITE_LIMIT_VARIABLE_NUMBER) as usize;
        let bindings_per_statement = 6;

        let mut outer_searchid = 2000;

        let chunks: itertools::IntoChunks<_> = entities.into_iter().chunks(max_vars / bindings_per_statement);

        // We'd like to flat_map here, but it's not obvious how to flat_map across Result.
        let results: Result<Vec<()>> = chunks.into_iter().map(|chunk| -> Result<()> {
            let mut count = 0;

            // We must keep these computed values somewhere to reference them later, so we can't
            // combine this map and the subsequent flat_map.
            // (e0, a0, v0, value_type_tag0, added0, flags0)
            let block: Result<Vec<(i64 /* e */,
                                   i64 /* a */,
                                   ToSqlOutput<'a> /* value */,
                                   i32 /* value_type_tag */,
                                   bool /* added0 */,
                                   u8 /* flags0 */,
                                   i64 /* searchid */)>> = chunk.map(|&(e, a, ref attribute, ref typed_value, added)| {
                if typed_value.value_type() != ValueType::String {
                    bail!("Cannot transact a fulltext assertion with a typed value that is not :db/valueType :db.type/string");
                }

                count += 1;
                outer_searchid += 1;

                // Now we can represent the typed value as an SQL value.
                let (value, value_type_tag): (ToSqlOutput, i32) = typed_value.to_sql_value_pair();

                Ok((e, a, value, value_type_tag, added, attribute.flags(), outer_searchid))
            }).collect();
            let block = block?;

            // First, insert all fulltext string values.
            // `fts_params` reference computed values in `block`.
            let fts_params: Vec<&ToSql> = block.iter().flat_map(|&(ref _e, ref _a, ref value, ref _value_type_tag, _added, ref _flags, ref searchid)| {
                // Avoid inner heap allocation.
                once(value as &ToSql)
                    .chain(once(searchid as &ToSql))
            }).collect();

            // TODO: make this maximally efficient.  It's not terribly inefficient right now.
            let fts_values: String = repeat_values(2, count);
            let fts_s: String = format!("INSERT INTO fulltext_values_view (text, searchid) VALUES {}", fts_values);

            // TODO: consider ensuring we inserted the expected number of rows.
            let mut stmt = self.prepare_cached(fts_s.as_str())?;
            stmt.execute(&fts_params)
                .map(|_c| ())
                .chain_err(|| "Could not insert fts values into fts table!")?;

            // Second, insert searches.
            // `params` reference computed values in `block`.
            let params: Vec<&ToSql> = block.iter().flat_map(|&(ref e, ref a, ref _value, ref value_type_tag, added, ref flags, ref searchid)| {
                // Avoid inner heap allocation.
                // TODO: extract some finite length iterator to make this less indented!
                once(e as &ToSql)
                    .chain(once(a as &ToSql)
                           .chain(once(searchid as &ToSql)
                                  .chain(once(value_type_tag as &ToSql)
                                         .chain(once(to_bool_ref(added) as &ToSql)
                                                .chain(once(flags as &ToSql))))))
            }).collect();

            // TODO: cache this for selected values of count.
            assert!(bindings_per_statement * count < max_vars, "Too many values: {} * {} >= {}", bindings_per_statement, count, max_vars);
            let inner = "(?, ?, (SELECT rowid FROM fulltext_values WHERE searchid = ?), ?, ?, ?)".to_string();
            // Like "(?, ?, (SELECT rowid FROM fulltext_values WHERE searchid = ?), ?, ?, ?), (?, ?, (SELECT rowid FROM fulltext_values WHERE searchid = ?), ?, ?, ?)".
            let fts_values: String = repeat(inner).take(count).join(", ");
            let s: String = if search_type == SearchType::Exact {
                format!("INSERT INTO temp.exact_searches (e0, a0, v0, value_type_tag0, added0, flags0) VALUES {}", fts_values)
            } else {
                format!("INSERT INTO temp.inexact_searches (e0, a0, v0, value_type_tag0, added0, flags0) VALUES {}", fts_values)
            };

            // TODO: consider ensuring we inserted the expected number of rows.
            let mut stmt = self.prepare_cached(s.as_str())?;
            stmt.execute(&params)
                .map(|_c| ())
                .chain_err(|| "Could not insert fts statements into temporary search table!")
        }).collect::<Result<Vec<()>>>();

        // Finally, clean up temporary searchids.
        let mut stmt = self.prepare_cached("UPDATE fulltext_values SET searchid = NULL WHERE searchid IS NOT NULL")?;
        stmt.execute(&[])
            .map(|_c| ())
            .chain_err(|| "Could not drop fts search ids!")?;

        results.map(|_| ())
    }

    fn commit_transaction(&self, tx_id: Entid) -> Result<()> {
        search(&self)?;
        insert_transaction(&self, tx_id)?;
        update_datoms(&self, tx_id)?;
        Ok(())
    }

    fn committed_metadata_assertions(&self, tx_id: Entid) -> Result<Vec<(Entid, Entid, TypedValue, bool)>> {
        // TODO: use concat! to avoid creating String instances.
        let mut stmt = self.prepare_cached(format!("SELECT e, a, v, value_type_tag, added FROM transactions WHERE tx = ? AND a IN {} ORDER BY e, a, v, value_type_tag, added", entids::METADATA_SQL_LIST.as_str()).as_str())?;
        let params = [&tx_id as &ToSql];
        let m: Result<Vec<_>> = stmt.query_and_then(&params[..], |row| -> Result<(Entid, Entid, TypedValue, bool)> {
            Ok((row.get_checked(0)?,
                row.get_checked(1)?,
                TypedValue::from_sql_value_pair(row.get_checked(2)?, row.get_checked(3)?)?,
                row.get_checked(4)?))
        })?.collect();
        m
    }
}

/// Update the current partition map materialized view.
// TODO: only update changed partitions.
pub fn update_partition_map(conn: &rusqlite::Connection, partition_map: &PartitionMap) -> Result<()> {
    let values_per_statement = 2;
    let max_vars = conn.limit(Limit::SQLITE_LIMIT_VARIABLE_NUMBER) as usize;
    let max_partitions = max_vars / values_per_statement;
    if partition_map.len() > max_partitions {
        bail!(ErrorKind::NotYetImplemented(format!("No more than {} partitions are supported", max_partitions)));
    }

    // Like "UPDATE parts SET idx = CASE WHEN part = ? THEN ? WHEN part = ? THEN ? ELSE idx END".
    let s = format!("UPDATE parts SET idx = CASE {} ELSE idx END",
                    repeat("WHEN part = ? THEN ?").take(partition_map.len()).join(" "));

    let params: Vec<&ToSql> = partition_map.iter().flat_map(|(name, partition)| {
        once(name as &ToSql)
            .chain(once(&partition.index as &ToSql))
    }).collect();

    // TODO: only cache the latest of these statements.  Changing the set of partitions isn't
    // supported in the Clojure implementation at all, and might not be supported in Mentat soon,
    // so this is very low priority.
    let mut stmt = conn.prepare_cached(s.as_str())?;
    stmt.execute(&params[..])
        .map(|_c| ())
        .chain_err(|| "Could not update partition map")
}

/// Update the metadata materialized views based on the given metadata report.
///
/// This updates the "entids", "idents", and "schema" materialized views, copying directly from the
/// "datoms" and "transactions" table as appropriate.
pub fn update_metadata(conn: &rusqlite::Connection, _old_schema: &Schema, new_schema: &Schema, metadata_report: &metadata::MetadataReport) -> Result<()>
{
    use metadata::AttributeAlteration::*;

    // Populate the materialized view directly from datoms (and, potentially in the future,
    // transactions).  This might generalize nicely as we expand the set of materialized views.
    // TODO: consider doing this in fewer SQLite execute() invocations.
    // TODO: use concat! to avoid creating String instances.
    if !metadata_report.idents_altered.is_empty() {
        // Idents is the materialized view of the [entid :db/ident ident] slice of datoms.
        conn.execute(format!("DELETE FROM idents").as_str(),
                     &[])?;
        conn.execute(format!("INSERT INTO idents SELECT e, a, v, value_type_tag FROM datoms WHERE a IN {}", entids::IDENTS_SQL_LIST.as_str()).as_str(),
                     &[])?;
    }


    let mut stmt = conn.prepare(format!("INSERT INTO schema SELECT e, a, v, value_type_tag FROM datoms WHERE e = ? AND a IN {}", entids::SCHEMA_SQL_LIST.as_str()).as_str())?;
    for &entid in &metadata_report.attributes_installed {
        stmt.execute(&[&entid as &ToSql])?;
    }

    let mut delete_stmt = conn.prepare(format!("DELETE FROM schema WHERE e = ? AND a IN {}", entids::SCHEMA_SQL_LIST.as_str()).as_str())?;
    let mut insert_stmt = conn.prepare(format!("INSERT INTO schema SELECT e, a, v, value_type_tag FROM datoms WHERE e = ? AND a IN {}", entids::SCHEMA_SQL_LIST.as_str()).as_str())?;
    let mut index_stmt = conn.prepare("UPDATE datoms SET index_avet = ? WHERE a = ?")?;
    let mut unique_value_stmt = conn.prepare("UPDATE datoms SET unique_value = ? WHERE a = ?")?;
    let mut cardinality_stmt = conn.prepare(r#"
SELECT EXISTS
    (SELECT 1
        FROM datoms AS left, datoms AS right
        WHERE left.a = ? AND
        left.a = right.a AND
        left.e = right.e AND
        left.v <> right.v)"#)?;

    for (&entid, alterations) in &metadata_report.attributes_altered {
        delete_stmt.execute(&[&entid as &ToSql])?;
        insert_stmt.execute(&[&entid as &ToSql])?;

        let attribute = new_schema.require_attribute_for_entid(entid)?;

        for alteration in alterations {
            match alteration {
                &Index => {
                    // This should always succeed.
                    index_stmt.execute(&[&attribute.index, &entid as &ToSql])?;
                },
                &Unique => {
                    // TODO: This can fail if there are conflicting values; give a more helpful
                    // error message in this case.
                    if unique_value_stmt.execute(&[to_bool_ref(attribute.unique.is_some()), &entid as &ToSql]).is_err() {
                        match attribute.unique {
                            Some(attribute::Unique::Value) => bail!(ErrorKind::NotYetImplemented(format!("Cannot alter schema attribute {} to be :db.unique/value", entid))),
                            Some(attribute::Unique::Identity) => bail!(ErrorKind::NotYetImplemented(format!("Cannot alter schema attribute {} to be :db.unique/identity", entid))),
                            None => unreachable!(), // This shouldn't happen, even after we support removing :db/unique.
                        }
                    }
                },
                &Cardinality => {
                    // We can always go from :db.cardinality/one to :db.cardinality many.  It's
                    // :db.cardinality/many to :db.cardinality/one that can fail.
                    //
                    // TODO: improve the failure message.  Perhaps try to mimic what Datomic says in
                    // this case?
                    if !attribute.multival {
                        let mut rows = cardinality_stmt.query(&[&entid as &ToSql])?;
                        if rows.next().is_some() {
                            bail!(ErrorKind::NotYetImplemented(format!("Cannot alter schema attribute {} to be :db.cardinality/one", entid)));
                        }
                    }
                },
                &NoHistory | &IsComponent => {
                    // There's no on disk change required for either of these.
                },
            }
        }
    }

    Ok(())
}

pub trait PartitionMapping {
    fn allocate_entid<S: ?Sized + Ord + Display>(&mut self, partition: &S) -> i64 where String: Borrow<S>;
    fn allocate_entids<S: ?Sized + Ord + Display>(&mut self, partition: &S, n: usize) -> Range<i64> where String: Borrow<S>;
    fn contains_entid(&self, entid: Entid) -> bool;
}

impl PartitionMapping for PartitionMap {
    /// Allocate a single fresh entid in the given `partition`.
    fn allocate_entid<S: ?Sized + Ord + Display>(&mut self, partition: &S) -> i64 where String: Borrow<S> {
        self.allocate_entids(partition, 1).start
    }

    /// Allocate `n` fresh entids in the given `partition`.
    fn allocate_entids<S: ?Sized + Ord + Display>(&mut self, partition: &S, n: usize) -> Range<i64> where String: Borrow<S> {
        match self.get_mut(partition) {
            Some(partition) => {
                let idx = partition.index;
                partition.index += n as i64;
                idx..partition.index
            },
            // This is a programming error.
            None => panic!("Cannot allocate entid from unknown partition: {}", partition),
        }
    }

    fn contains_entid(&self, entid: Entid) -> bool {
        self.values().any(|partition| partition.contains_entid(entid))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bootstrap;
    use debug;
    use edn;
    use mentat_core::{
        attribute,
    };
    use mentat_tx_parser;
    use rusqlite;
    use std::collections::{
        BTreeMap,
    };
    use types::TxReport;

    // Macro to parse a `Borrow<str>` to an `edn::Value` and assert the given `edn::Value` `matches`
    // against it.
    //
    // This is a macro only to give nice line numbers when tests fail.
    macro_rules! assert_matches {
        ( $input: expr, $expected: expr ) => {{
            // Failure to parse the expected pattern is a coding error, so we unwrap.
            let pattern_value = edn::parse::value($expected.borrow())
                .expect(format!("to be able to parse expected {}", $expected).as_str())
                .without_spans();
            assert!($input.matches(&pattern_value),
                    "Expected value:\n{}\nto match pattern:\n{}\n",
                    $input.to_pretty(120).unwrap(),
                    pattern_value.to_pretty(120).unwrap());
        }}
    }

    // Transact $input against the given $conn, expecting success or a `Result<TxReport, String>`.
    //
    // This unwraps safely and makes asserting errors pleasant.
    macro_rules! assert_transact {
        ( $conn: expr, $input: expr, $expected: expr ) => {{
            let result = $conn.transact($input).map_err(|e| e.to_string());
            assert_eq!(result, $expected.map_err(|e| e.to_string()));
        }};
        ( $conn: expr, $input: expr ) => {{
            let result = $conn.transact($input);
            assert!(result.is_ok(), "Expected Ok(_), got `{}`", result.unwrap_err());
            result.unwrap()
        }};
    }

    // A connection that doesn't try to be clever about possibly sharing its `Schema`.  Compare to
    // `mentat::Conn`.
    struct TestConn {
        sqlite: rusqlite::Connection,
        partition_map: PartitionMap,
        schema: Schema,
    }

    impl TestConn {
        fn assert_materialized_views(&self) {
            let materialized_ident_map = read_ident_map(&self.sqlite).expect("ident map");
            let materialized_schema_map = read_schema_map(&self.sqlite).expect("schema map");

            let materialized_schema = Schema::from_ident_map_and_schema_map(materialized_ident_map, materialized_schema_map).expect("schema");
            assert_eq!(materialized_schema, self.schema);
        }

        fn transact<I>(&mut self, transaction: I) -> Result<TxReport> where I: Borrow<str> {
            // Failure to parse the transaction is a coding error, so we unwrap.
            let assertions = edn::parse::value(transaction.borrow()).expect(format!("to be able to parse {} into EDN", transaction.borrow()).as_str());
            let entities: Vec<_> = mentat_tx_parser::Tx::parse(&assertions).expect(format!("to be able to parse {} into entities", assertions).as_str());

            let details = {
                // The block scopes the borrow of self.sqlite.
                // We're about to write, so go straight ahead and get an IMMEDIATE transaction.
                let tx = self.sqlite.transaction_with_behavior(TransactionBehavior::Immediate)?;
                // Applying the transaction can fail, so we don't unwrap.
                let details = transact(&tx, self.partition_map.clone(), &self.schema, &self.schema, entities)?;
                tx.commit()?;
                details
            };

            let (report, next_partition_map, next_schema) = details;
            self.partition_map = next_partition_map;
            if let Some(next_schema) = next_schema {
                self.schema = next_schema;
            }

            // Verify that we've updated the materialized views during transacting.
            self.assert_materialized_views();

            Ok(report)
        }

        fn last_tx_id(&self) -> Entid {
            self.partition_map.get(&":db.part/tx".to_string()).unwrap().index - 1
        }

        fn last_transaction(&self) -> edn::Value {
            debug::transactions_after(&self.sqlite, &self.schema, self.last_tx_id() - 1).expect("last_transaction").0[0].into_edn()
        }

        fn datoms(&self) -> edn::Value {
            debug::datoms_after(&self.sqlite, &self.schema, bootstrap::TX0).expect("datoms").into_edn()
        }

        fn fulltext_values(&self) -> edn::Value {
            debug::fulltext_values(&self.sqlite).expect("fulltext_values").into_edn()
        }
    }

    impl Default for TestConn {
        fn default() -> TestConn {
            let mut conn = new_connection("").expect("Couldn't open in-memory db");
            let db = ensure_current_version(&mut conn).unwrap();

            // Does not include :db/txInstant.
            let datoms = debug::datoms_after(&conn, &db.schema, 0).unwrap();
            assert_eq!(datoms.0.len(), 76);

            // Includes :db/txInstant.
            let transactions = debug::transactions_after(&conn, &db.schema, 0).unwrap();
            assert_eq!(transactions.0.len(), 1);
            assert_eq!(transactions.0[0].0.len(), 77);

            let mut parts = db.partition_map;

            // Add a fake partition to allow tests to do things like
            // [:db/add 111 :foo/bar 222]
            {
                let fake_partition = Partition { start: 100, index: 1000 };
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
    }

    fn tempids(report: &TxReport) -> edn::Value {
        let mut map: BTreeMap<edn::Value, edn::Value> = BTreeMap::default();
        for (tempid, &entid) in report.tempids.iter() {
            map.insert(edn::Value::Text(tempid.clone()), edn::Value::Integer(entid));
        }
        edn::Value::Map(map)
    }

    #[test]
    fn test_add() {
        let mut conn = TestConn::default();

        // Test inserting :db.cardinality/one elements.
        assert_transact!(conn, "[[:db/add 100 :db.schema/version 1]
                                 [:db/add 101 :db.schema/version 2]]");
        assert_matches!(conn.last_transaction(),
                        "[[100 :db.schema/version 1 ?tx true]
                          [101 :db.schema/version 2 ?tx true]
                          [?tx :db/txInstant ?ms ?tx true]]");
        assert_matches!(conn.datoms(),
                       "[[100 :db.schema/version 1]
                         [101 :db.schema/version 2]]");

        // Test inserting :db.cardinality/many elements.
        assert_transact!(conn, "[[:db/add 200 :db.schema/attribute 100]
                                 [:db/add 200 :db.schema/attribute 101]]");
        assert_matches!(conn.last_transaction(),
                        "[[200 :db.schema/attribute 100 ?tx true]
                          [200 :db.schema/attribute 101 ?tx true]
                          [?tx :db/txInstant ?ms ?tx true]]");
        assert_matches!(conn.datoms(),
                        "[[100 :db.schema/version 1]
                          [101 :db.schema/version 2]
                          [200 :db.schema/attribute 100]
                          [200 :db.schema/attribute 101]]");

        // Test replacing existing :db.cardinality/one elements.
        assert_transact!(conn, "[[:db/add 100 :db.schema/version 11]
                                 [:db/add 101 :db.schema/version 22]]");
        assert_matches!(conn.last_transaction(),
                        "[[100 :db.schema/version 1 ?tx false]
                          [100 :db.schema/version 11 ?tx true]
                          [101 :db.schema/version 2 ?tx false]
                          [101 :db.schema/version 22 ?tx true]
                          [?tx :db/txInstant ?ms ?tx true]]");
        assert_matches!(conn.datoms(),
                        "[[100 :db.schema/version 11]
                          [101 :db.schema/version 22]
                          [200 :db.schema/attribute 100]
                          [200 :db.schema/attribute 101]]");


        // Test that asserting existing :db.cardinality/one elements doesn't change the store.
        assert_transact!(conn, "[[:db/add 100 :db.schema/version 11]
                                 [:db/add 101 :db.schema/version 22]]");
        assert_matches!(conn.last_transaction(),
                        "[[?tx :db/txInstant ?ms ?tx true]]");
        assert_matches!(conn.datoms(),
                        "[[100 :db.schema/version 11]
                          [101 :db.schema/version 22]
                          [200 :db.schema/attribute 100]
                          [200 :db.schema/attribute 101]]");


        // Test that asserting existing :db.cardinality/many elements doesn't change the store.
        assert_transact!(conn, "[[:db/add 200 :db.schema/attribute 100]
                                 [:db/add 200 :db.schema/attribute 101]]");
        assert_matches!(conn.last_transaction(),
                        "[[?tx :db/txInstant ?ms ?tx true]]");
        assert_matches!(conn.datoms(),
                        "[[100 :db.schema/version 11]
                          [101 :db.schema/version 22]
                          [200 :db.schema/attribute 100]
                          [200 :db.schema/attribute 101]]");
    }

    #[test]
    fn test_retract() {
        let mut conn = TestConn::default();

        // Insert a few :db.cardinality/one elements.
        assert_transact!(conn, "[[:db/add 100 :db.schema/version 1]
                                 [:db/add 101 :db.schema/version 2]]");
        assert_matches!(conn.last_transaction(),
                        "[[100 :db.schema/version 1 ?tx true]
                          [101 :db.schema/version 2 ?tx true]
                          [?tx :db/txInstant ?ms ?tx true]]");
        assert_matches!(conn.datoms(),
                        "[[100 :db.schema/version 1]
                          [101 :db.schema/version 2]]");

        // And a few :db.cardinality/many elements.
        assert_transact!(conn, "[[:db/add 200 :db.schema/attribute 100]
                                 [:db/add 200 :db.schema/attribute 101]]");
        assert_matches!(conn.last_transaction(),
                        "[[200 :db.schema/attribute 100 ?tx true]
                          [200 :db.schema/attribute 101 ?tx true]
                          [?tx :db/txInstant ?ms ?tx true]]");
        assert_matches!(conn.datoms(),
                        "[[100 :db.schema/version 1]
                          [101 :db.schema/version 2]
                          [200 :db.schema/attribute 100]
                          [200 :db.schema/attribute 101]]");

        // Test that we can retract :db.cardinality/one elements.
        assert_transact!(conn, "[[:db/retract 100 :db.schema/version 1]]");
        assert_matches!(conn.last_transaction(),
                        "[[100 :db.schema/version 1 ?tx false]
                          [?tx :db/txInstant ?ms ?tx true]]");
        assert_matches!(conn.datoms(),
                        "[[101 :db.schema/version 2]
                          [200 :db.schema/attribute 100]
                          [200 :db.schema/attribute 101]]");

        // Test that we can retract :db.cardinality/many elements.
        assert_transact!(conn, "[[:db/retract 200 :db.schema/attribute 100]]");
        assert_matches!(conn.last_transaction(),
                        "[[200 :db.schema/attribute 100 ?tx false]
                          [?tx :db/txInstant ?ms ?tx true]]");
        assert_matches!(conn.datoms(),
                        "[[101 :db.schema/version 2]
                          [200 :db.schema/attribute 101]]");

        // Verify that retracting :db.cardinality/{one,many} elements that are not present doesn't
        // change the store.
        assert_transact!(conn, "[[:db/retract 100 :db.schema/version 1]
                                 [:db/retract 200 :db.schema/attribute 100]]");
        assert_matches!(conn.last_transaction(),
                        "[[?tx :db/txInstant ?ms ?tx true]]");
        assert_matches!(conn.datoms(),
                        "[[101 :db.schema/version 2]
                          [200 :db.schema/attribute 101]]");
    }

    // TODO: don't use :db/ident to test upserts!
    #[test]
    fn test_upsert_vector() {
        let mut conn = TestConn::default();

        // Insert some :db.unique/identity elements.
        assert_transact!(conn, "[[:db/add 100 :db/ident :name/Ivan]
                                 [:db/add 101 :db/ident :name/Petr]]");
        assert_matches!(conn.last_transaction(),
                        "[[100 :db/ident :name/Ivan ?tx true]
                          [101 :db/ident :name/Petr ?tx true]
                          [?tx :db/txInstant ?ms ?tx true]]");
        assert_matches!(conn.datoms(),
                        "[[100 :db/ident :name/Ivan]
                          [101 :db/ident :name/Petr]]");

        // Upserting two tempids to the same entid works.
        let report = assert_transact!(conn, "[[:db/add \"t1\" :db/ident :name/Ivan]
                                              [:db/add \"t1\" :db.schema/attribute 100]
                                              [:db/add \"t2\" :db/ident :name/Petr]
                                              [:db/add \"t2\" :db.schema/attribute 101]]");
        assert_matches!(conn.last_transaction(),
                        "[[100 :db.schema/attribute :name/Ivan ?tx true]
                          [101 :db.schema/attribute :name/Petr ?tx true]
                          [?tx :db/txInstant ?ms ?tx true]]");
        assert_matches!(conn.datoms(),
                        "[[100 :db/ident :name/Ivan]
                          [100 :db.schema/attribute :name/Ivan]
                          [101 :db/ident :name/Petr]
                          [101 :db.schema/attribute :name/Petr]]");
        assert_matches!(tempids(&report),
                        "{\"t1\" 100
                          \"t2\" 101}");

        // Upserting a tempid works.  The ref doesn't have to exist (at this time), but we can't
        // reuse an existing ref due to :db/unique :db.unique/value.
        let report = assert_transact!(conn, "[[:db/add \"t1\" :db/ident :name/Ivan]
                                              [:db/add \"t1\" :db.schema/attribute 102]]");
        assert_matches!(conn.last_transaction(),
                        "[[100 :db.schema/attribute 102 ?tx true]
                          [?true :db/txInstant ?ms ?tx true]]");
        assert_matches!(conn.datoms(),
                        "[[100 :db/ident :name/Ivan]
                          [100 :db.schema/attribute :name/Ivan]
                          [100 :db.schema/attribute 102]
                          [101 :db/ident :name/Petr]
                          [101 :db.schema/attribute :name/Petr]]");
        assert_matches!(tempids(&report),
                        "{\"t1\" 100}");

        // A single complex upsert allocates a new entid.
        let report = assert_transact!(conn, "[[:db/add \"t1\" :db.schema/attribute \"t2\"]]");
        assert_matches!(conn.last_transaction(),
                        "[[65536 :db.schema/attribute 65537 ?tx true]
                          [?tx :db/txInstant ?ms ?tx true]]");
        assert_matches!(tempids(&report),
                        "{\"t1\" 65536
                          \"t2\" 65537}");

        // Conflicting upserts fail.
        assert_transact!(conn, "[[:db/add \"t1\" :db/ident :name/Ivan]
                                [:db/add \"t1\" :db/ident :name/Petr]]",
                         Err("not yet implemented: Conflicting upsert: tempid \'t1\' resolves to more than one entid: 100, 101"));

        // tempids in :db/retract that don't upsert fail.
        assert_transact!(conn, "[[:db/retract \"t1\" :db/ident :name/Anonymous]]",
                         Err("not yet implemented: [:db/retract ...] entity referenced tempid that did not upsert: t1"));

        // tempids in :db/retract that do upsert are retracted.  The ref given doesn't exist, so the
        // assertion will be ignored.
        let report = assert_transact!(conn, "[[:db/add \"t1\" :db/ident :name/Ivan]
                                              [:db/retract \"t1\" :db.schema/attribute 103]]");
        assert_matches!(conn.last_transaction(),
                        "[[?tx :db/txInstant ?ms ?tx true]]");
        assert_matches!(tempids(&report),
                        "{\"t1\" 100}");

        // A multistep upsert.  The upsert algorithm will first try to resolve "t1", fail, and then
        // allocate both "t1" and "t2".
        let report = assert_transact!(conn, "[[:db/add \"t1\" :db/ident :name/Josef]
                                              [:db/add \"t2\" :db.schema/attribute \"t1\"]]");
        assert_matches!(conn.last_transaction(),
                        "[[65538 :db/ident :name/Josef ?tx true]
                          [65539 :db.schema/attribute :name/Josef ?tx true]
                          [?tx :db/txInstant ?ms ?tx true]]");
        assert_matches!(tempids(&report),
                        "{\"t1\" 65538
                          \"t2\" 65539}");

        // A multistep insert.  This time, we can resolve both, but we have to try "t1", succeed,
        // and then resolve "t2".
        // TODO: We can't quite test this without more schema elements.
        // conn.transact("[[:db/add \"t1\" :db/ident :name/Josef]
        //                 [:db/add \"t2\" :db/ident \"t1\"]]");
        // assert_matches!(conn.last_transaction(),
        //                 "[[65538 :db/ident :name/Josef]
        //                   [65538 :db/ident :name/Karl]
        //                   [?tx :db/txInstant ?ms ?tx true]]");
    }

    #[test]
    fn test_sqlite_limit() {
        let conn = new_connection("").expect("Couldn't open in-memory db");
        let initial = conn.limit(Limit::SQLITE_LIMIT_VARIABLE_NUMBER);
        // Sanity check.
        assert!(initial > 500);

        // Make sure setting works.
        conn.set_limit(Limit::SQLITE_LIMIT_VARIABLE_NUMBER, 222);
        assert_eq!(222, conn.limit(Limit::SQLITE_LIMIT_VARIABLE_NUMBER));
    }

    #[test]
    fn test_db_install() {
        let mut conn = TestConn::default();

        // We can assert a new attribute.
        assert_transact!(conn, "[[:db/add 100 :db/ident :test/ident]
                                 [:db/add 100 :db/valueType :db.type/long]
                                 [:db/add 100 :db/cardinality :db.cardinality/many]]");

        assert_eq!(conn.schema.entid_map.get(&100).cloned().unwrap(), to_namespaced_keyword(":test/ident").unwrap());
        assert_eq!(conn.schema.ident_map.get(&to_namespaced_keyword(":test/ident").unwrap()).cloned().unwrap(), 100);
        let attribute = conn.schema.attribute_for_entid(100).unwrap().clone();
        assert_eq!(attribute.value_type, ValueType::Long);
        assert_eq!(attribute.multival, true);
        assert_eq!(attribute.fulltext, false);

        assert_matches!(conn.last_transaction(),
                        "[[100 :db/ident :test/ident ?tx true]
                          [100 :db/valueType :db.type/long ?tx true]
                          [100 :db/cardinality :db.cardinality/many ?tx true]
                          [?tx :db/txInstant ?ms ?tx true]]");
        assert_matches!(conn.datoms(),
                        "[[100 :db/ident :test/ident]
                          [100 :db/valueType :db.type/long]
                          [100 :db/cardinality :db.cardinality/many]]");

        // Let's check we actually have the schema characteristics we expect.
        let attribute = conn.schema.attribute_for_entid(100).unwrap().clone();
        assert_eq!(attribute.value_type, ValueType::Long);
        assert_eq!(attribute.multival, true);
        assert_eq!(attribute.fulltext, false);

        // Let's check that we can use the freshly installed attribute.
        assert_transact!(conn, "[[:db/add 101 100 -10]
                                 [:db/add 101 :test/ident -9]]");

        assert_matches!(conn.last_transaction(),
                        "[[101 :test/ident -10 ?tx true]
                          [101 :test/ident -9 ?tx true]
                          [?tx :db/txInstant ?ms ?tx true]]");

        // Cannot retract a characteristic of an installed attribute.
        assert_transact!(conn,
                         "[[:db/retract 100 :db/cardinality :db.cardinality/many]]",
                         Err("not yet implemented: Retracting metadata attribute assertions not yet implemented: retracted [e a] pairs [[100 8]]"));

        // Trying to install an attribute without a :db/ident is allowed.
        assert_transact!(conn, "[[:db/add 101 :db/valueType :db.type/long]
                                 [:db/add 101 :db/cardinality :db.cardinality/many]]");
    }

    #[test]
    fn test_db_alter() {
        let mut conn = TestConn::default();

        // Start by installing a :db.cardinality/one attribute.
        assert_transact!(conn, "[[:db/add 100 :db/ident :test/ident]
                                 [:db/add 100 :db/valueType :db.type/keyword]
                                 [:db/add 100 :db/cardinality :db.cardinality/one]]");

        // Trying to alter the :db/valueType will fail.
        assert_transact!(conn, "[[:db/add 100 :db/valueType :db.type/long]]",
                         Err("bad schema assertion: Schema alteration for existing attribute with entid 100 is not valid"));

        // But we can alter the cardinality.
        assert_transact!(conn, "[[:db/add 100 :db/cardinality :db.cardinality/many]]");

        assert_matches!(conn.last_transaction(),
                        "[[100 :db/cardinality :db.cardinality/one ?tx false]
                          [100 :db/cardinality :db.cardinality/many ?tx true]
                          [?tx :db/txInstant ?ms ?tx true]]");
        assert_matches!(conn.datoms(),
                        "[[100 :db/ident :test/ident]
                          [100 :db/valueType :db.type/keyword]
                          [100 :db/cardinality :db.cardinality/many]]");

        // Let's check we actually have the schema characteristics we expect.
        let attribute = conn.schema.attribute_for_entid(100).unwrap().clone();
        assert_eq!(attribute.value_type, ValueType::Keyword);
        assert_eq!(attribute.multival, true);
        assert_eq!(attribute.fulltext, false);

        // Let's check that we can use the freshly altered attribute's new characteristic.
        assert_transact!(conn, "[[:db/add 101 100 :test/value1]
                                 [:db/add 101 :test/ident :test/value2]]");

        assert_matches!(conn.last_transaction(),
                        "[[101 :test/ident :test/value1 ?tx true]
                          [101 :test/ident :test/value2 ?tx true]
                          [?tx :db/txInstant ?ms ?tx true]]");
    }

    #[test]
    fn test_db_ident() {
        let mut conn = TestConn::default();

        // We can assert a new :db/ident.
        assert_transact!(conn, "[[:db/add 100 :db/ident :name/Ivan]]");
        assert_matches!(conn.last_transaction(),
                        "[[100 :db/ident :name/Ivan ?tx true]
                          [?tx :db/txInstant ?ms ?tx true]]");
        assert_matches!(conn.datoms(),
                        "[[100 :db/ident :name/Ivan]]");
        assert_eq!(conn.schema.entid_map.get(&100).cloned().unwrap(), to_namespaced_keyword(":name/Ivan").unwrap());
        assert_eq!(conn.schema.ident_map.get(&to_namespaced_keyword(":name/Ivan").unwrap()).cloned().unwrap(), 100);

        // We can re-assert an existing :db/ident.
        assert_transact!(conn, "[[:db/add 100 :db/ident :name/Ivan]]");
        assert_matches!(conn.last_transaction(),
                        "[[?tx :db/txInstant ?ms ?tx true]]");
        assert_matches!(conn.datoms(),
                        "[[100 :db/ident :name/Ivan]]");
        assert_eq!(conn.schema.entid_map.get(&100).cloned().unwrap(), to_namespaced_keyword(":name/Ivan").unwrap());
        assert_eq!(conn.schema.ident_map.get(&to_namespaced_keyword(":name/Ivan").unwrap()).cloned().unwrap(), 100);

        // We can alter an existing :db/ident to have a new keyword.
        assert_transact!(conn, "[[:db/add :name/Ivan :db/ident :name/Petr]]");
        assert_matches!(conn.last_transaction(),
                        "[[100 :db/ident :name/Ivan ?tx false]
                          [100 :db/ident :name/Petr ?tx true]
                          [?tx :db/txInstant ?ms ?tx true]]");
        assert_matches!(conn.datoms(),
                        "[[100 :db/ident :name/Petr]]");
        // Entid map is updated.
        assert_eq!(conn.schema.entid_map.get(&100).cloned().unwrap(), to_namespaced_keyword(":name/Petr").unwrap());
        // Ident map contains the new ident.
        assert_eq!(conn.schema.ident_map.get(&to_namespaced_keyword(":name/Petr").unwrap()).cloned().unwrap(), 100);
        // Ident map no longer contains the old ident.
        assert!(conn.schema.ident_map.get(&to_namespaced_keyword(":name/Ivan").unwrap()).is_none());

        // We can re-purpose an old ident.
        assert_transact!(conn, "[[:db/add 101 :db/ident :name/Ivan]]");
        assert_matches!(conn.last_transaction(),
                        "[[101 :db/ident :name/Ivan ?tx true]
                          [?tx :db/txInstant ?ms ?tx true]]");
        assert_matches!(conn.datoms(),
                        "[[100 :db/ident :name/Petr]
                          [101 :db/ident :name/Ivan]]");
        // Entid map contains both entids.
        assert_eq!(conn.schema.entid_map.get(&100).cloned().unwrap(), to_namespaced_keyword(":name/Petr").unwrap());
        assert_eq!(conn.schema.entid_map.get(&101).cloned().unwrap(), to_namespaced_keyword(":name/Ivan").unwrap());
        // Ident map contains the new ident.
        assert_eq!(conn.schema.ident_map.get(&to_namespaced_keyword(":name/Petr").unwrap()).cloned().unwrap(), 100);
        // Ident map contains the old ident, but re-purposed to the new entid.
        assert_eq!(conn.schema.ident_map.get(&to_namespaced_keyword(":name/Ivan").unwrap()).cloned().unwrap(), 101);

        // We can retract an existing :db/ident.
        assert_transact!(conn, "[[:db/retract :name/Petr :db/ident :name/Petr]]");
        // It's really gone.
        assert!(conn.schema.entid_map.get(&100).is_none());
        assert!(conn.schema.ident_map.get(&to_namespaced_keyword(":name/Petr").unwrap()).is_none());
    }

    #[test]
    fn test_db_alter_cardinality() {
        let mut conn = TestConn::default();

        // Start by installing a :db.cardinality/one attribute.
        assert_transact!(conn, "[[:db/add 100 :db/ident :test/ident]
                                 [:db/add 100 :db/valueType :db.type/long]
                                 [:db/add 100 :db/cardinality :db.cardinality/one]]");

        assert_transact!(conn, "[[:db/add 200 :test/ident 1]]");

        // We can always go from :db.cardinality/one to :db.cardinality/many.
        assert_transact!(conn, "[[:db/add 100 :db/cardinality :db.cardinality/many]]");

        assert_transact!(conn, "[[:db/add 200 :test/ident 2]]");

        assert_matches!(conn.datoms(),
                        "[[100 :db/ident :test/ident]
                          [100 :db/valueType :db.type/long]
                          [100 :db/cardinality :db.cardinality/many]
                          [200 :test/ident 1]
                          [200 :test/ident 2]]");

        // We can't always go from :db.cardinality/many to :db.cardinality/one.
        assert_transact!(conn, "[[:db/add 100 :db/cardinality :db.cardinality/one]]",
                         // TODO: give more helpful error details.
                         Err("not yet implemented: Cannot alter schema attribute 100 to be :db.cardinality/one"));
    }

    #[test]
    fn test_db_alter_unique_value() {
        let mut conn = TestConn::default();

        // Start by installing a :db.cardinality/one attribute.
        assert_transact!(conn, "[[:db/add 100 :db/ident :test/ident]
                                 [:db/add 100 :db/valueType :db.type/long]
                                 [:db/add 100 :db/cardinality :db.cardinality/one]]");

        assert_transact!(conn, "[[:db/add 200 :test/ident 1]
                                 [:db/add 201 :test/ident 1]]");

        // We can't always migrate to be :db.unique/value.
        assert_transact!(conn, "[[:db/add :test/ident :db/unique :db.unique/value]]",
                         // TODO: give more helpful error details.
                         Err("not yet implemented: Cannot alter schema attribute 100 to be :db.unique/value"));

        // Not even indirectly!
        assert_transact!(conn, "[[:db/add :test/ident :db/unique :db.unique/identity]]",
                         // TODO: give more helpful error details.
                         Err("not yet implemented: Cannot alter schema attribute 100 to be :db.unique/identity"));

        // But we can if we make sure there's no repeated [a v] pair.
        assert_transact!(conn, "[[:db/add 201 :test/ident 2]]");

        assert_transact!(conn, "[[:db/add :test/ident :db/index true]
                                 [:db/add :test/ident :db/unique :db.unique/value]
                                 [:db/add :db.part/db :db.alter/attribute 100]]");
    }

    /// Verify that we can't alter :db/fulltext schema characteristics at all.
    #[test]
    fn test_db_alter_fulltext() {
        let mut conn = TestConn::default();

        // Start by installing a :db/fulltext true and a :db/fulltext unset attribute.
        assert_transact!(conn, "[[:db/add 111 :db/ident :test/fulltext]
                                 [:db/add 111 :db/valueType :db.type/string]
                                 [:db/add 111 :db/unique :db.unique/identity]
                                 [:db/add 111 :db/index true]
                                 [:db/add 111 :db/fulltext true]
                                 [:db/add 222 :db/ident :test/string]
                                 [:db/add 222 :db/cardinality :db.cardinality/one]
                                 [:db/add 222 :db/valueType :db.type/string]
                                 [:db/add 222 :db/index true]]");

        assert_transact!(conn,
                         "[[:db/retract 111 :db/fulltext true]]",
                         Err("not yet implemented: Retracting metadata attribute assertions not yet implemented: retracted [e a] pairs [[111 12]]"));

        assert_transact!(conn,
                         "[[:db/add 222 :db/fulltext true]]",
                         Err("bad schema assertion: Schema alteration for existing attribute with entid 222 is not valid"));
    }

    #[test]
    fn test_db_fulltext() {
        let mut conn = TestConn::default();

        // Start by installing a few :db/fulltext true attributes.
        assert_transact!(conn, "[[:db/add 111 :db/ident :test/fulltext]
                                 [:db/add 111 :db/valueType :db.type/string]
                                 [:db/add 111 :db/unique :db.unique/identity]
                                 [:db/add 111 :db/index true]
                                 [:db/add 111 :db/fulltext true]
                                 [:db/add 222 :db/ident :test/other]
                                 [:db/add 222 :db/cardinality :db.cardinality/one]
                                 [:db/add 222 :db/valueType :db.type/string]
                                 [:db/add 222 :db/index true]
                                 [:db/add 222 :db/fulltext true]]");

        // Let's check we actually have the schema characteristics we expect.
        let fulltext = conn.schema.attribute_for_entid(111).cloned().expect(":test/fulltext");
        assert_eq!(fulltext.value_type, ValueType::String);
        assert_eq!(fulltext.fulltext, true);
        assert_eq!(fulltext.multival, false);
        assert_eq!(fulltext.unique, Some(attribute::Unique::Identity));

        let other = conn.schema.attribute_for_entid(222).cloned().expect(":test/other");
        assert_eq!(other.value_type, ValueType::String);
        assert_eq!(other.fulltext, true);
        assert_eq!(other.multival, false);
        assert_eq!(other.unique, None);

        // We can add fulltext indexed datoms.
        assert_transact!(conn, "[[:db/add 301 :test/fulltext \"test this\"]]");
        // value column is rowid into fulltext table.
        assert_matches!(conn.fulltext_values(),
                        "[[1 \"test this\"]]");
        assert_matches!(conn.last_transaction(),
                        "[[301 :test/fulltext 1 ?tx true]
                          [?tx :db/txInstant ?ms ?tx true]]");
        assert_matches!(conn.datoms(),
                        "[[111 :db/ident :test/fulltext]
                          [111 :db/valueType :db.type/string]
                          [111 :db/unique :db.unique/identity]
                          [111 :db/index true]
                          [111 :db/fulltext true]
                          [222 :db/ident :test/other]
                          [222 :db/valueType :db.type/string]
                          [222 :db/cardinality :db.cardinality/one]
                          [222 :db/index true]
                          [222 :db/fulltext true]
                          [301 :test/fulltext 1]]");

        // We can replace existing fulltext indexed datoms.
        assert_transact!(conn, "[[:db/add 301 :test/fulltext \"alternate thing\"]]");
        // value column is rowid into fulltext table.
        assert_matches!(conn.fulltext_values(),
                        "[[1 \"test this\"]
                          [2 \"alternate thing\"]]");
        assert_matches!(conn.last_transaction(),
                        "[[301 :test/fulltext 1 ?tx false]
                          [301 :test/fulltext 2 ?tx true]
                          [?tx :db/txInstant ?ms ?tx true]]");
        assert_matches!(conn.datoms(),
                        "[[111 :db/ident :test/fulltext]
                          [111 :db/valueType :db.type/string]
                          [111 :db/unique :db.unique/identity]
                          [111 :db/index true]
                          [111 :db/fulltext true]
                          [222 :db/ident :test/other]
                          [222 :db/valueType :db.type/string]
                          [222 :db/cardinality :db.cardinality/one]
                          [222 :db/index true]
                          [222 :db/fulltext true]
                          [301 :test/fulltext 2]]");

        // We can upsert keyed by fulltext indexed datoms.
        assert_transact!(conn, "[[:db/add \"t\" :test/fulltext \"alternate thing\"]
                                 [:db/add \"t\" :test/other \"other\"]]");
        // value column is rowid into fulltext table.
        assert_matches!(conn.fulltext_values(),
                        "[[1 \"test this\"]
                          [2 \"alternate thing\"]
                          [3 \"other\"]]");
        assert_matches!(conn.last_transaction(),
                        "[[301 :test/other 3 ?tx true]
                          [?tx :db/txInstant ?ms ?tx true]]");
        assert_matches!(conn.datoms(),
                        "[[111 :db/ident :test/fulltext]
                          [111 :db/valueType :db.type/string]
                          [111 :db/unique :db.unique/identity]
                          [111 :db/index true]
                          [111 :db/fulltext true]
                          [222 :db/ident :test/other]
                          [222 :db/valueType :db.type/string]
                          [222 :db/cardinality :db.cardinality/one]
                          [222 :db/index true]
                          [222 :db/fulltext true]
                          [301 :test/fulltext 2]
                          [301 :test/other 3]]");

        // We can re-use fulltext values; they won't be added to the fulltext values table twice.
        assert_transact!(conn, "[[:db/add 302 :test/other \"alternate thing\"]]");
        // value column is rowid into fulltext table.
        assert_matches!(conn.fulltext_values(),
                        "[[1 \"test this\"]
                          [2 \"alternate thing\"]
                          [3 \"other\"]]");
        assert_matches!(conn.last_transaction(),
                        "[[302 :test/other 2 ?tx true]
                          [?tx :db/txInstant ?ms ?tx true]]");
        assert_matches!(conn.datoms(),
                        "[[111 :db/ident :test/fulltext]
                          [111 :db/valueType :db.type/string]
                          [111 :db/unique :db.unique/identity]
                          [111 :db/index true]
                          [111 :db/fulltext true]
                          [222 :db/ident :test/other]
                          [222 :db/valueType :db.type/string]
                          [222 :db/cardinality :db.cardinality/one]
                          [222 :db/index true]
                          [222 :db/fulltext true]
                          [301 :test/fulltext 2]
                          [301 :test/other 3]
                          [302 :test/other 2]]");

        // We can retract fulltext indexed datoms.  The underlying fulltext value remains -- indeed,
        // it might still be in use.
        assert_transact!(conn, "[[:db/retract 302 :test/other \"alternate thing\"]]");
        // value column is rowid into fulltext table.
        assert_matches!(conn.fulltext_values(),
                        "[[1 \"test this\"]
                          [2 \"alternate thing\"]
                          [3 \"other\"]]");
        assert_matches!(conn.last_transaction(),
                        "[[302 :test/other 2 ?tx false]
                          [?tx :db/txInstant ?ms ?tx true]]");
        assert_matches!(conn.datoms(),
                        "[[111 :db/ident :test/fulltext]
                          [111 :db/valueType :db.type/string]
                          [111 :db/unique :db.unique/identity]
                          [111 :db/index true]
                          [111 :db/fulltext true]
                          [222 :db/ident :test/other]
                          [222 :db/valueType :db.type/string]
                          [222 :db/cardinality :db.cardinality/one]
                          [222 :db/index true]
                          [222 :db/fulltext true]
                          [301 :test/fulltext 2]
                          [301 :test/other 3]]");
    }

    #[test]
    fn test_lookup_refs_entity_column() {
        let mut conn = TestConn::default();

        // Start by installing a few attributes.
        assert_transact!(conn, "[[:db/add 111 :db/ident :test/unique_value]
                                 [:db/add 111 :db/valueType :db.type/string]
                                 [:db/add 111 :db/unique :db.unique/value]
                                 [:db/add 111 :db/index true]
                                 [:db/add 222 :db/ident :test/unique_identity]
                                 [:db/add 222 :db/valueType :db.type/long]
                                 [:db/add 222 :db/unique :db.unique/identity]
                                 [:db/add 222 :db/index true]
                                 [:db/add 333 :db/ident :test/not_unique]
                                 [:db/add 333 :db/cardinality :db.cardinality/one]
                                 [:db/add 333 :db/valueType :db.type/keyword]
                                 [:db/add 333 :db/index true]]");

        // And a few datoms to match against.
        assert_transact!(conn, "[[:db/add 501 :test/unique_value \"test this\"]
                                 [:db/add 502 :test/unique_value \"other\"]
                                 [:db/add 503 :test/unique_identity -10]
                                 [:db/add 504 :test/unique_identity -20]
                                 [:db/add 505 :test/not_unique :test/keyword]
                                 [:db/add 506 :test/not_unique :test/keyword]]");

        // We can resolve lookup refs in the entity column, referring to the attribute as an entid or an ident.
        assert_transact!(conn, "[[:db/add (lookup-ref :test/unique_value \"test this\") :test/not_unique :test/keyword]
                                 [:db/add (lookup-ref 111 \"other\") :test/not_unique :test/keyword]
                                 [:db/add (lookup-ref :test/unique_identity -10) :test/not_unique :test/keyword]
                                 [:db/add (lookup-ref 222 -20) :test/not_unique :test/keyword]]");
        assert_matches!(conn.last_transaction(),
                        "[[501 :test/not_unique :test/keyword ?tx true]
                          [502 :test/not_unique :test/keyword ?tx true]
                          [503 :test/not_unique :test/keyword ?tx true]
                          [504 :test/not_unique :test/keyword ?tx true]
                          [?tx :db/txInstant ?ms ?tx true]]");

        // We cannot resolve lookup refs that aren't :db/unique.
        assert_transact!(conn,
                         "[[:db/add (lookup-ref :test/not_unique :test/keyword) :test/not_unique :test/keyword]]",
                         Err("not yet implemented: Cannot resolve (lookup-ref 333 :test/keyword) with attribute that is not :db/unique"));

        // We type check the lookup ref's value against the lookup ref's attribute.
        assert_transact!(conn,
                         "[[:db/add (lookup-ref :test/unique_value :test/not_a_string) :test/not_unique :test/keyword]]",
                         Err("EDN value \':test/not_a_string\' is not the expected Mentat value type String"));

        // Each lookup ref in the entity column must resolve
        assert_transact!(conn,
                         "[[:db/add (lookup-ref :test/unique_value \"unmatched string value\") :test/not_unique :test/keyword]]",
                         Err("no entid found for ident: couldn\'t lookup [a v]: (111, String(\"unmatched string value\"))"));
    }

    #[test]
    fn test_lookup_refs_value_column() {
        let mut conn = TestConn::default();

        // Start by installing a few attributes.
        assert_transact!(conn, "[[:db/add 111 :db/ident :test/unique_value]
                                 [:db/add 111 :db/valueType :db.type/string]
                                 [:db/add 111 :db/unique :db.unique/value]
                                 [:db/add 111 :db/index true]
                                 [:db/add 222 :db/ident :test/unique_identity]
                                 [:db/add 222 :db/valueType :db.type/long]
                                 [:db/add 222 :db/unique :db.unique/identity]
                                 [:db/add 222 :db/index true]
                                 [:db/add 333 :db/ident :test/not_unique]
                                 [:db/add 333 :db/cardinality :db.cardinality/one]
                                 [:db/add 333 :db/valueType :db.type/keyword]
                                 [:db/add 333 :db/index true]
                                 [:db/add 444 :db/ident :test/ref]
                                 [:db/add 444 :db/valueType :db.type/ref]
                                 [:db/add 444 :db/unique :db.unique/identity]
                                 [:db/add 444 :db/index true]]");

        // And a few datoms to match against.
        assert_transact!(conn, "[[:db/add 501 :test/unique_value \"test this\"]
                                 [:db/add 502 :test/unique_value \"other\"]
                                 [:db/add 503 :test/unique_identity -10]
                                 [:db/add 504 :test/unique_identity -20]
                                 [:db/add 505 :test/not_unique :test/keyword]
                                 [:db/add 506 :test/not_unique :test/keyword]]");

        // We can resolve lookup refs in the entity column, referring to the attribute as an entid or an ident.
        assert_transact!(conn, "[[:db/add 601 :test/ref (lookup-ref :test/unique_value \"test this\")]
                                 [:db/add 602 :test/ref (lookup-ref 111 \"other\")]
                                 [:db/add 603 :test/ref (lookup-ref :test/unique_identity -10)]
                                 [:db/add 604 :test/ref (lookup-ref 222 -20)]]");
        assert_matches!(conn.last_transaction(),
                        "[[601 :test/ref 501 ?tx true]
                          [602 :test/ref 502 ?tx true]
                          [603 :test/ref 503 ?tx true]
                          [604 :test/ref 504 ?tx true]
                          [?tx :db/txInstant ?ms ?tx true]]");

        // We cannot resolve lookup refs for attributes that aren't :db/ref.
        assert_transact!(conn,
                         "[[:db/add \"t\" :test/not_unique (lookup-ref :test/unique_value \"test this\")]]",
                         Err("not yet implemented: Cannot resolve value lookup ref for attribute 333 that is not :db/valueType :db.type/ref"));

        // If a value column lookup ref resolves, we can upsert against it.  Here, the lookup ref
        // resolves to 501, which upserts "t" to 601.
        assert_transact!(conn, "[[:db/add \"t\" :test/ref (lookup-ref :test/unique_value \"test this\")]
                                 [:db/add \"t\" :test/not_unique :test/keyword]]");
        assert_matches!(conn.last_transaction(),
                        "[[601 :test/not_unique :test/keyword ?tx true]
                          [?tx :db/txInstant ?ms ?tx true]]");

        // Each lookup ref in the value column must resolve
        assert_transact!(conn,
                         "[[:db/add \"t\" :test/ref (lookup-ref :test/unique_value \"unmatched string value\")]]",
                         Err("no entid found for ident: couldn\'t lookup [a v]: (111, String(\"unmatched string value\"))"));
    }

    #[test]
    fn test_explode_value_lists() {
        let mut conn = TestConn::default();

        // Start by installing a few attributes.
        assert_transact!(conn, "[[:db/add 111 :db/ident :test/many]
                                 [:db/add 111 :db/valueType :db.type/long]
                                 [:db/add 111 :db/cardinality :db.cardinality/many]
                                 [:db/add 222 :db/ident :test/one]
                                 [:db/add 222 :db/valueType :db.type/long]
                                 [:db/add 222 :db/cardinality :db.cardinality/one]]");

        // Check that we can explode vectors for :db.cardinality/many attributes.
        assert_transact!(conn, "[[:db/add 501 :test/many [1]]
                                 [:db/add 502 :test/many [2 3]]
                                 [:db/add 503 :test/many [4 5 6]]]");
        assert_matches!(conn.last_transaction(),
                        "[[501 :test/many 1 ?tx true]
                          [502 :test/many 2 ?tx true]
                          [502 :test/many 3 ?tx true]
                          [503 :test/many 4 ?tx true]
                          [503 :test/many 5 ?tx true]
                          [503 :test/many 6 ?tx true]
                          [?tx :db/txInstant ?ms ?tx true]]");

        // Check that we can explode nested vectors for :db.cardinality/many attributes.
        assert_transact!(conn, "[[:db/add 600 :test/many [1 [2] [[3] [4]] []]]]");
        assert_matches!(conn.last_transaction(),
                        "[[600 :test/many 1 ?tx true]
                          [600 :test/many 2 ?tx true]
                          [600 :test/many 3 ?tx true]
                          [600 :test/many 4 ?tx true]
                          [?tx :db/txInstant ?ms ?tx true]]");

        // Check that we cannot explode vectors for :db.cardinality/one attributes.
        assert_transact!(conn,
                         "[[:db/add 501 :test/one [1]]]",
                         Err("not yet implemented: Cannot explode vector value for attribute 222 that is not :db.cardinality :db.cardinality/many"));
        assert_transact!(conn,
                         "[[:db/add 501 :test/one [2 3]]]",
                         Err("not yet implemented: Cannot explode vector value for attribute 222 that is not :db.cardinality :db.cardinality/many"));
    }

    #[test]
    fn test_explode_map_notation() {
        let mut conn = TestConn::default();

        // Start by installing a few attributes.
        assert_transact!(conn, "[[:db/add 111 :db/ident :test/many]
                                 [:db/add 111 :db/valueType :db.type/long]
                                 [:db/add 111 :db/cardinality :db.cardinality/many]
                                 [:db/add 222 :db/ident :test/component]
                                 [:db/add 222 :db/isComponent true]
                                 [:db/add 222 :db/valueType :db.type/ref]
                                 [:db/add 333 :db/ident :test/unique]
                                 [:db/add 333 :db/unique :db.unique/identity]
                                 [:db/add 333 :db/index true]
                                 [:db/add 333 :db/valueType :db.type/long]
                                 [:db/add 444 :db/ident :test/dangling]
                                 [:db/add 444 :db/valueType :db.type/ref]]");

        // Check that we can explode map notation without :db/id.
        let report = assert_transact!(conn, "[{:test/many 1}]");
        assert_matches!(conn.last_transaction(),
                        "[[?e :test/many 1 ?tx true]
                          [?tx :db/txInstant ?ms ?tx true]]");
        assert_matches!(tempids(&report),
                        "{}");

        // Check that we can explode map notation with :db/id, as an entid, ident, and tempid.
        let report = assert_transact!(conn, "[{:db/id :db/ident :test/many 1}
                                              {:db/id 500 :test/many 2}
                                              {:db/id \"t\" :test/many 3}]");
        assert_matches!(conn.last_transaction(),
                        "[[1 :test/many 1 ?tx true]
                          [500 :test/many 2 ?tx true]
                          [?e :test/many 3 ?tx true]
                          [?tx :db/txInstant ?ms ?tx true]]");
        assert_matches!(tempids(&report),
                        "{\"t\" 65537}");

        // Check that we can explode map notation with nested vector values.
        let report = assert_transact!(conn, "[{:test/many [1 2]}]");
        assert_matches!(conn.last_transaction(),
                        "[[?e :test/many 1 ?tx true]
                          [?e :test/many 2 ?tx true]
                          [?tx :db/txInstant ?ms ?tx true]]");
        assert_matches!(tempids(&report),
                        "{}");

        // Check that we can explode map notation with nested maps if the attribute is
        // :db/isComponent true.
        let report = assert_transact!(conn, "[{:test/component {:test/many 1}}]");
        assert_matches!(conn.last_transaction(),
                        "[[?e :test/component ?f ?tx true]
                          [?f :test/many 1 ?tx true]
                          [?tx :db/txInstant ?ms ?tx true]]");
        assert_matches!(tempids(&report),
                        "{}");

        // Check that we can explode map notation with nested maps if the inner map contains a
        // :db/unique :db.unique/identity attribute.
        let report = assert_transact!(conn, "[{:test/dangling {:test/unique 10}}]");
        assert_matches!(conn.last_transaction(),
                        "[[?e :test/dangling ?f ?tx true]
                          [?f :test/unique 10 ?tx true]
                          [?tx :db/txInstant ?ms ?tx true]]");
        assert_matches!(tempids(&report),
                        "{}");

        // Verify that we can't explode map notation with nested maps if the inner map would be
        // dangling.
        assert_transact!(conn,
                         "[{:test/dangling {:test/many 11}}]",
                         Err("not yet implemented: Cannot explode nested map value that would lead to dangling entity for attribute 444"));

        // Verify that we can explode map notation with nested maps, even if the inner map would be
        // dangling, if we give a :db/id explicitly.
        assert_transact!(conn, "[{:test/dangling {:db/id \"t\" :test/many 12}}]");
    }

    #[test]
    fn test_explode_reversed_notation() {
        let mut conn = TestConn::default();

        // Start by installing a few attributes.
        assert_transact!(conn, "[[:db/add 111 :db/ident :test/many]
                                 [:db/add 111 :db/valueType :db.type/long]
                                 [:db/add 111 :db/cardinality :db.cardinality/many]
                                 [:db/add 222 :db/ident :test/component]
                                 [:db/add 222 :db/isComponent true]
                                 [:db/add 222 :db/valueType :db.type/ref]
                                 [:db/add 333 :db/ident :test/unique]
                                 [:db/add 333 :db/unique :db.unique/identity]
                                 [:db/add 333 :db/index true]
                                 [:db/add 333 :db/valueType :db.type/long]
                                 [:db/add 444 :db/ident :test/dangling]
                                 [:db/add 444 :db/valueType :db.type/ref]]");

        // Check that we can explode direct reversed notation, entids.
        let report = assert_transact!(conn, "[[:db/add 100 :test/_dangling 200]]");
        assert_matches!(conn.last_transaction(),
                        "[[200 :test/dangling 100 ?tx true]
                          [?tx :db/txInstant ?ms ?tx true]]");
        assert_matches!(tempids(&report),
                        "{}");

        // Check that we can explode direct reversed notation, idents.
        let report = assert_transact!(conn, "[[:db/add :test/many :test/_dangling :test/unique]]");
        assert_matches!(conn.last_transaction(),
                        "[[333 :test/dangling :test/many ?tx true]
                          [?tx :db/txInstant ?ms ?tx true]]");
        assert_matches!(tempids(&report),
                        "{}");

        // Check that we can explode direct reversed notation, tempids.
        let report = assert_transact!(conn, "[[:db/add \"s\" :test/_dangling \"t\"]]");
        assert_matches!(conn.last_transaction(),
                        "[[65537 :test/dangling 65536 ?tx true]
                          [?tx :db/txInstant ?ms ?tx true]]");
        // This is implementation specific, but it should be deterministic.
        assert_matches!(tempids(&report),
                        "{\"s\" 65536
                          \"t\" 65537}");

        // Check that we can explode reversed notation in map notation without :db/id.
        let report = assert_transact!(conn, "[{:test/_dangling 501}
                                              {:test/_dangling :test/many}
                                              {:test/_dangling \"t\"}]");
        assert_matches!(conn.last_transaction(),
                        "[[111 :test/dangling ?e1 ?tx true]
                          [501 :test/dangling ?e2 ?tx true]
                          [65538 :test/dangling ?e3 ?tx true]
                          [?tx :db/txInstant ?ms ?tx true]]");
        assert_matches!(tempids(&report),
                        "{\"t\" 65538}");

        // Check that we can explode reversed notation in map notation with :db/id, entid.
        let report = assert_transact!(conn, "[{:db/id 600 :test/_dangling 601}]");
        assert_matches!(conn.last_transaction(),
                        "[[601 :test/dangling 600 ?tx true]
                          [?tx :db/txInstant ?ms ?tx true]]");
        assert_matches!(tempids(&report),
                        "{}");

        // Check that we can explode reversed notation in map notation with :db/id, ident.
        let report = assert_transact!(conn, "[{:db/id :test/component :test/_dangling :test/component}]");
        assert_matches!(conn.last_transaction(),
                        "[[222 :test/dangling :test/component ?tx true]
                          [?tx :db/txInstant ?ms ?tx true]]");
        assert_matches!(tempids(&report),
                        "{}");

        // Check that we can explode reversed notation in map notation with :db/id, tempid.
        let report = assert_transact!(conn, "[{:db/id \"s\" :test/_dangling \"t\"}]");
        assert_matches!(conn.last_transaction(),
                        "[[65543 :test/dangling 65542 ?tx true]
                          [?tx :db/txInstant ?ms ?tx true]]");
        // This is implementation specific, but it should be deterministic.
        assert_matches!(tempids(&report),
                        "{\"s\" 65542
                          \"t\" 65543}");

        // Check that we can use the same attribute in both forward and backward form in the same
        // transaction.
        let report = assert_transact!(conn, "[[:db/add 888 :test/dangling 889]
                                              [:db/add 888 :test/_dangling 889]]");
        assert_matches!(conn.last_transaction(),
                        "[[888 :test/dangling 889 ?tx true]
                          [889 :test/dangling 888 ?tx true]
                          [?tx :db/txInstant ?ms ?tx true]]");
        assert_matches!(tempids(&report),
                        "{}");

        // Check that we can use the same attribute in both forward and backward form in the same
        // transaction in map notation.
        let report = assert_transact!(conn, "[{:db/id 998 :test/dangling 999 :test/_dangling 999}]");
        assert_matches!(conn.last_transaction(),
                        "[[998 :test/dangling 999 ?tx true]
                          [999 :test/dangling 998 ?tx true]
                          [?tx :db/txInstant ?ms ?tx true]]");
        assert_matches!(tempids(&report),
                        "{}");

    }

    #[test]
    fn test_explode_reversed_notation_errors() {
        let mut conn = TestConn::default();

        // Start by installing a few attributes.
        assert_transact!(conn, "[[:db/add 111 :db/ident :test/many]
                                 [:db/add 111 :db/valueType :db.type/long]
                                 [:db/add 111 :db/cardinality :db.cardinality/many]
                                 [:db/add 222 :db/ident :test/component]
                                 [:db/add 222 :db/isComponent true]
                                 [:db/add 222 :db/valueType :db.type/ref]
                                 [:db/add 333 :db/ident :test/unique]
                                 [:db/add 333 :db/unique :db.unique/identity]
                                 [:db/add 333 :db/index true]
                                 [:db/add 333 :db/valueType :db.type/long]
                                 [:db/add 444 :db/ident :test/dangling]
                                 [:db/add 444 :db/valueType :db.type/ref]]");

        // `tx-parser` should fail to parse direct reverse notation with nested value maps and
        // nested value vectors, so we only test things that "get through" to the map notation
        // dynamic processor here.

        // Verify that we can't explode reverse notation in map notation with nested value maps.
        assert_transact!(conn,
                         "[{:test/_dangling {:test/many 14}}]",
                         Err("not yet implemented: Cannot explode map notation value in :attr/_reversed notation for attribute 444"));

        // Verify that we can't explode reverse notation in map notation with nested value vectors.
        assert_transact!(conn,
                         "[{:test/_dangling [:test/many]}]",
                         Err("not yet implemented: Cannot explode vector value in :attr/_reversed notation for attribute 444"));

        // Verify that we can't use reverse notation with non-:db.type/ref attributes.
        assert_transact!(conn,
                         "[{:test/_unique 500}]",
                         Err("not yet implemented: Cannot use :attr/_reversed notation for attribute 333 that is not :db/valueType :db.type/ref"));

        // Verify that we can't use reverse notation with unrecognized attributes.
        assert_transact!(conn,
                         "[{:test/_unknown 500}]",
                         Err("no entid found for ident: :test/unknown")); // TODO: make this error reference the original :test/_unknown.

        // Verify that we can't use reverse notation with bad value types: here, an unknown keyword
        // that can't be coerced to a ref.
        assert_transact!(conn,
                         "[{:test/_dangling :test/unknown}]",
                         Err("no entid found for ident: :test/unknown"));
        // And here, a float.
        assert_transact!(conn,
                         "[{:test/_dangling 1.23}]",
                         Err("EDN value \'1.23\' is not the expected Mentat value type Ref"));
    }
}
