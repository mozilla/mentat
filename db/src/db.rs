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

use itertools;
use itertools::Itertools;
use rusqlite;
use rusqlite::types::{ToSql, ToSqlOutput};

use ::{now, repeat_values, to_namespaced_keyword};
use bootstrap;
use edn::types::Value;
use edn::symbols;
use mentat_core::{
    Attribute,
    AttributeBitFlags,
    Entid,
    IdentMap,
    Schema,
    TypedValue,
    ValueType,
};
use mentat_tx::entities::Entity;
use errors::{ErrorKind, Result, ResultExt};
use schema::SchemaBuilding;
use types::{
    AVMap,
    AVPair,
    DB,
    Partition,
    PartitionMap,
    TxReport,
};
use tx::Tx;

pub fn new_connection<T>(uri: T) -> rusqlite::Result<rusqlite::Connection> where T: AsRef<Path> {
    let conn = match uri.as_ref().to_string_lossy().len() {
        0 => rusqlite::Connection::open_in_memory()?,
        _ => rusqlite::Connection::open(uri)?,
    };

    conn.execute_batch("
        PRAGMA page_size=32768;
        PRAGMA journal_mode=wal;
        PRAGMA wal_autocheckpoint=32;
        PRAGMA journal_size_limit=3145728;
        PRAGMA foreign_keys=ON;
    ")?;

    Ok(conn)
}

/// Version history:
///
/// 1: initial schema.
/// 2: added :db.schema/version and /attribute in bootstrap; assigned idents 36 and 37, so we bump
///    the part range here; tie bootstrapping to the SQLite user_version.
pub const CURRENT_VERSION: i32 = 2;

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
    /// SQL statements to be executed, in order, to create the Mentat SQL schema (version 2).
    #[cfg_attr(rustfmt, rustfmt_skip)]
    static ref V2_STATEMENTS: Vec<&'static str> = { vec![
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

        // Materialized views of the schema.
        r#"CREATE TABLE idents (ident TEXT NOT NULL PRIMARY KEY, entid INTEGER UNIQUE NOT NULL)"#,
        r#"CREATE TABLE schema (ident TEXT NOT NULL, attr TEXT NOT NULL, value BLOB NOT NULL, value_type_tag SMALLINT NOT NULL,
             FOREIGN KEY (ident) REFERENCES idents (ident))"#,
        r#"CREATE INDEX idx_schema_unique ON schema (ident, attr, value, value_type_tag)"#,
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
    let tx = conn.transaction()?;

    for statement in (&V2_STATEMENTS).iter() {
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
    let mut bootstrap_db = DB::new(bootstrap_partition_map, bootstrap::bootstrap_schema());
    bootstrap_db.transact(&tx, bootstrap::bootstrap_entities())?;

    set_user_version(&tx, CURRENT_VERSION)?;

    // TODO: use the drop semantics to do this automagically?
    tx.commit()?;
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

pub fn update_from_version(conn: &mut rusqlite::Connection, current_version: i32) -> Result<i32> {
    if current_version < 0 || CURRENT_VERSION <= current_version {
        bail!(ErrorKind::BadSQLiteStoreVersion(current_version))
    }

    let tx = conn.transaction()?;
    // TODO: actually implement upgrade.
    set_user_version(&tx, CURRENT_VERSION)?;
    let user_version = get_user_version(&tx)?;
    // TODO: use the drop semantics to do this automagically?
    tx.commit()?;

    Ok(user_version)
}

pub fn ensure_current_version(conn: &mut rusqlite::Connection) -> Result<DB> {
    let user_version = get_user_version(&conn)?;
    match user_version {
        0 => create_current_version(conn),
        // TODO: support updating or re-opening an existing store.
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
            // SQLite distinguishes integral from decimal types, allowing long and double to
            // share a tag.
            (5, rusqlite::types::Value::Integer(x)) => Ok(TypedValue::Long(x)),
            (5, rusqlite::types::Value::Real(x)) => Ok(TypedValue::Double(x.into())),
            (10, rusqlite::types::Value::Text(x)) => Ok(TypedValue::String(x)),
            (13, rusqlite::types::Value::Text(x)) => {
                to_namespaced_keyword(&x).map(|k| TypedValue::Keyword(k))
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
            &Value::Integer(x) => Some(TypedValue::Long(x)),
            &Value::Float(ref x) => Some(TypedValue::Double(x.clone())),
            &Value::Text(ref x) => Some(TypedValue::String(x.clone())),
            &Value::NamespacedKeyword(ref x) => Some(TypedValue::Keyword(x.clone())),
            _ => None
        }
    }

    /// Return the corresponding SQLite `value` and `value_type_tag` pair.
    fn to_sql_value_pair<'a>(&'a self) -> (ToSqlOutput<'a>, i32) {
        match self {
            &TypedValue::Ref(x) => (rusqlite::types::Value::Integer(x).into(), 0),
            &TypedValue::Boolean(x) => (rusqlite::types::Value::Integer(if x { 1 } else { 0 }).into(), 1),
            // SQLite distinguishes integral from decimal types, allowing long and double to share a tag.
            &TypedValue::Long(x) => (rusqlite::types::Value::Integer(x).into(), 5),
            &TypedValue::Double(x) => (rusqlite::types::Value::Real(x.into_inner()).into(), 5),
            &TypedValue::String(ref x) => (rusqlite::types::ValueRef::Text(x.as_str()).into(), 10),
            &TypedValue::Keyword(ref x) => (rusqlite::types::ValueRef::Text(&x.to_string()).into(), 13),
        }
    }

    /// Return the corresponding EDN `value` and `value_type` pair.
    fn to_edn_value_pair(&self) -> (Value, ValueType) {
        match self {
            &TypedValue::Ref(x) => (Value::Integer(x), ValueType::Ref),
            &TypedValue::Boolean(x) => (Value::Boolean(x), ValueType::Boolean),
            &TypedValue::Long(x) => (Value::Integer(x), ValueType::Long),
            &TypedValue::Double(x) => (Value::Float(x), ValueType::Double),
            &TypedValue::String(ref x) => (Value::Text(x.clone()), ValueType::String),
            &TypedValue::Keyword(ref x) => (Value::NamespacedKeyword(x.clone()), ValueType::Keyword),
        }
    }
}

/// Read the ident map materialized view from the given SQL store.
pub fn read_ident_map(conn: &rusqlite::Connection) -> Result<IdentMap> {
    let mut stmt: rusqlite::Statement = conn.prepare("SELECT ident, entid FROM idents")?;
    let m = stmt.query_and_then(&[], |row| -> Result<(symbols::NamespacedKeyword, Entid)> {
        let ident: String = row.get(0);
        to_namespaced_keyword(&ident).map(|i| (i, row.get(1)))
    })?.collect();
    m
}

/// Read the partition map materialized view from the given SQL store.
pub fn read_partition_map(conn: &rusqlite::Connection) -> Result<PartitionMap> {
    let mut stmt: rusqlite::Statement = conn.prepare("SELECT part, start, idx FROM parts")?;
    let m = stmt.query_and_then(&[], |row| -> Result<(String, Partition)> {
        Ok((row.get_checked(0)?, Partition::new(row.get_checked(1)?, row.get_checked(2)?)))
    })?.collect();
    m
}

/// Read the schema materialized view from the given SQL store.
pub fn read_schema(conn: &rusqlite::Connection, ident_map: &IdentMap) -> Result<Schema> {
    let mut stmt: rusqlite::Statement = conn.prepare("SELECT ident, attr, value, value_type_tag FROM schema")?;
    let r: Result<Vec<(symbols::NamespacedKeyword, symbols::NamespacedKeyword, TypedValue)>> = stmt.query_and_then(&[], |row| {
        // Each row looks like :db/index|:db/valueType|28|0.  Observe that 28|0 represents a
        // :db.type/ref to entid 28, which needs to be converted to a TypedValue.
        // TODO: don't use textual ident and attr; just use entids directly.
        let symbolic_ident: String = row.get_checked(0)?;
        let symbolic_attr: String = row.get_checked(1)?;
        let v: rusqlite::types::Value = row.get_checked(2)?;
        let value_type_tag: i32 = row.get_checked(3)?;
        let typed_value = TypedValue::from_sql_value_pair(v, value_type_tag)?;

        let ident = to_namespaced_keyword(&symbolic_ident);
        let attr = to_namespaced_keyword(&symbolic_attr);
        match (ident, attr, typed_value) {
            (Ok(ident), Ok(attr), typed_value) => Ok((ident, attr, typed_value)),
            (Err(e), _, _) => Err(e),
            (_, Err(e), _) => Err(e),
        }
    })?.collect();

    r.and_then(|triples| Schema::from_ident_map_and_triples(ident_map.clone(), triples))
}

/// Read the materialized views from the given SQL store and return a Mentat `DB` for querying and
/// applying transactions.
pub fn read_db(conn: &rusqlite::Connection) -> Result<DB> {
    let partition_map = read_partition_map(conn)?;
    let ident_map = read_ident_map(conn)?;
    let schema = read_schema(conn, &ident_map)?;
    Ok(DB::new(partition_map, schema))
}

/// Internal representation of an [e a v added] datom, ready to be transacted against the store.
pub type ReducedEntity = (i64, i64, TypedValue, bool);

#[derive(Clone,Debug,Eq,Hash,Ord,PartialOrd,PartialEq)]
pub enum SearchType {
    Exact,
    Inexact,
}

impl DB {
    /// Do schema-aware typechecking and coercion.
    ///
    /// Either assert that the given value is in the attribute's value set, or (in limited cases)
    /// coerce the given value into the attribute's value set.
    pub fn to_typed_value(&self, value: &Value, attribute: &Attribute) -> Result<TypedValue> {
        // TODO: encapsulate entid-ident-attribute for better error messages.
        match TypedValue::from_edn_value(value) {
            // We don't recognize this EDN at all.  Get out!
            None => bail!(ErrorKind::BadEDNValuePair(value.clone(), attribute.value_type.clone())),
            Some(typed_value) => match (&attribute.value_type, typed_value) {
                // Most types don't coerce at all.
                (&ValueType::Boolean, tv @ TypedValue::Boolean(_)) => Ok(tv),
                (&ValueType::Long, tv @ TypedValue::Long(_)) => Ok(tv),
                (&ValueType::Double, tv @ TypedValue::Double(_)) => Ok(tv),
                (&ValueType::String, tv @ TypedValue::String(_)) => Ok(tv),
                (&ValueType::Keyword, tv @ TypedValue::Keyword(_)) => Ok(tv),
                // Ref coerces a little: we interpret some things depending on the schema as a Ref.
                (&ValueType::Ref, TypedValue::Long(x)) => Ok(TypedValue::Ref(x)),
                (&ValueType::Ref, TypedValue::Keyword(ref x)) => {
                    self.schema.require_entid(&x).map(|entid| TypedValue::Ref(entid))
                }
                // Otherwise, we have a type mismatch.
                (value_type, _) => bail!(ErrorKind::BadEDNValuePair(value.clone(), value_type.clone())),
            }
        }
    }

    /// Given a slice of [a v] lookup-refs, look up the corresponding [e a v] triples.
    ///
    /// It is assumed that the attribute `a` in each lookup-ref is `:db/unique`, so that at most one
    /// matching [e a v] triple exists.  (If this is not true, some matching entid `e` will be
    /// chosen non-deterministically, if one exists.)
    ///
    /// Returns a map &(a, v) -> e, to avoid cloning potentially large values.  The keys of the map
    /// are exactly those (a, v) pairs that have an assertion [e a v] in the datom store.
    pub fn resolve_avs<'a>(&self, conn: &rusqlite::Connection, avs: &'a [&'a AVPair]) -> Result<AVMap<'a>> {
        // Start search_id's at some identifiable number.
        let initial_search_id = 2000;
        let bindings_per_statement = 4;

        // We map [a v] -> numeric search_id -> e, and then we use the search_id lookups to finally
        // produce the map [a v] -> e.
        //
        // TODO: `collect` into a HashSet so that any (a, v) is resolved at most once.
        let chunks: itertools::IntoChunks<_> = avs.into_iter().enumerate().chunks(::SQLITE_MAX_VARIABLE_NUMBER / 4);

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
            // `datoms`, which will be much faster.
            let values: String = repeat_values(bindings_per_statement, count);
            let s: String = format!("WITH t(search_id, a, v, value_type_tag) AS (VALUES {}) SELECT t.search_id, d.e \
                                     FROM t, all_datoms AS d \
                                     WHERE d.index_avet IS NOT 0 AND d.a = t.a AND d.value_type_tag = t.value_type_tag AND d.v = t.v",
                                    values);
            let mut stmt: rusqlite::Statement = conn.prepare(s.as_str())?;

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
    fn create_temp_tables(&self, conn: &rusqlite::Connection) -> Result<()> {
        // We can't do this in one shot, since we can't prepare a batch statement.
        let statements = [
            r#"DROP TABLE IF EXISTS temp.exact_searches"#,
            // Note that `flags0` is a bitfield of several flags compressed via
            // `AttributeBitFlags.flags()` in the temporary search tables, later
            // expanded in the `datoms` insertion.
            // TODO: drop tx0 entirely.
            r#"CREATE TABLE temp.exact_searches (
               e0 INTEGER NOT NULL,
               a0 SMALLINT NOT NULL,
               v0 BLOB NOT NULL,
               value_type_tag0 SMALLINT NOT NULL,
               tx0 INTEGER NOT NULL,
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
               tx0 INTEGER NOT NULL,
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
               tx0 INTEGER NOT NULL,
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
            let mut stmt = conn.prepare_cached(statement)?;
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
    pub fn insert_non_fts_searches<'a>(&self, conn: &rusqlite::Connection, entities: &'a [ReducedEntity], tx: Entid, search_type: SearchType) -> Result<()> {
        let bindings_per_statement = 7;

        let chunks: itertools::IntoChunks<_> = entities.into_iter().chunks(::SQLITE_MAX_VARIABLE_NUMBER / bindings_per_statement);

        // We'd like to flat_map here, but it's not obvious how to flat_map across Result.
        let results: Result<Vec<()>> = chunks.into_iter().map(|chunk| -> Result<()> {
            let mut count = 0;

            // We must keep these computed values somewhere to reference them later, so we can't
            // combine this map and the subsequent flat_map.
            // (e0, a0, v0, value_type_tag0, added0, flags0)
            let block: Result<Vec<(i64 /* e */, i64 /* a */,
                                   ToSqlOutput<'a> /* value */, /* value_type_tag */ i32,
                                   /* added0 */ bool,
                                   /* flags0 */ u8)>> = chunk.map(|&(e, a, ref typed_value, added)| {
                count += 1;
                let attribute: &Attribute = self.schema.require_attribute_for_entid(a)?;

                // Now we can represent the typed value as an SQL value.
                let (value, value_type_tag): (ToSqlOutput, i32) = typed_value.to_sql_value_pair();

                let flags = attribute.flags();

                Ok((e, a, value, value_type_tag,
                    added,
                    flags))
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
                                         .chain(once(&tx as &ToSql)
                                                .chain(once(to_bool_ref(added) as &ToSql)
                                                       .chain(once(flags as &ToSql)))))))
            }).collect();

            // TODO: cache this for selected values of count.
            let values: String = repeat_values(bindings_per_statement, count);
            let s: String = if search_type == SearchType::Exact {
                format!("INSERT INTO temp.exact_searches (e0, a0, v0, value_type_tag0, tx0, added0, flags0) VALUES {}", values)
            } else {
                format!("INSERT INTO temp.inexact_searches (e0, a0, v0, value_type_tag0, tx0, added0, flags0) VALUES {}", values)
            };

            // TODO: consider ensuring we inserted the expected number of rows.
            let mut stmt = conn.prepare_cached(s.as_str())?;
            stmt.execute(&params)
                .map(|_c| ())
                .chain_err(|| "Could not insert non-fts one statements into temporary search table!")
        }).collect::<Result<Vec<()>>>();

        results.map(|_| ())
    }

    /// Take search rows and complete `temp.search_results`.
    ///
    /// See https://github.com/mozilla/mentat/wiki/Transacting:-entity-to-SQL-translation.
    pub fn search(&self, conn: &rusqlite::Connection) -> Result<()> {
        // First is fast, only one table walk: lookup by exact eav.
        // Second is slower, but still only one table walk: lookup old value by ea.
        let s = r#"
          INSERT INTO temp.search_results
          SELECT t.e0, t.a0, t.v0, t.value_type_tag0, t.tx0, t.added0, t.flags0, ':db.cardinality/many', d.rowid, d.v
          FROM temp.exact_searches AS t
          LEFT JOIN datoms AS d
          ON t.e0 = d.e AND
             t.a0 = d.a AND
             t.value_type_tag0 = d.value_type_tag AND
             t.v0 = d.v

          UNION ALL

          SELECT t.e0, t.a0, t.v0, t.value_type_tag0, t.tx0, t.added0, t.flags0, ':db.cardinality/one', d.rowid, d.v
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
    // TODO: capture `conn` in a `TxInternal` structure.
    pub fn insert_transaction(&self, conn: &rusqlite::Connection, tx: Entid) -> Result<()> {
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
    // TODO: capture `conn` in a `TxInternal` structure.
    pub fn update_datoms(&self, conn: &rusqlite::Connection, tx: Entid) -> Result<()> {
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

    /// Update the current partition map materialized view.
    // TODO: only update changed partitions.
    pub fn update_partition_map(&self, conn: &rusqlite::Connection) -> Result<()> {
        let values_per_statement = 2;
        let max_partitions = ::SQLITE_MAX_VARIABLE_NUMBER / values_per_statement;
        if self.partition_map.len() > max_partitions {
            bail!(ErrorKind::NotYetImplemented(format!("No more than {} partitions are supported", max_partitions)));
        }

        // Like "UPDATE parts SET idx = CASE WHEN part = ? THEN ? WHEN part = ? THEN ? ELSE idx END".
        let s = format!("UPDATE parts SET idx = CASE {} ELSE idx END",
                        repeat("WHEN part = ? THEN ?").take(self.partition_map.len()).join(" "));

        let params: Vec<&ToSql> = self.partition_map.iter().flat_map(|(name, partition)| {
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

    /// Allocate a single fresh entid in the given `partition`.
    pub fn allocate_entid<S: ?Sized + Ord + Display>(&mut self, partition: &S) -> i64 where String: Borrow<S> {
        self.allocate_entids(partition, 1).start
    }

    /// Allocate `n` fresh entids in the given `partition`.
    pub fn allocate_entids<S: ?Sized + Ord + Display>(&mut self, partition: &S, n: usize) -> Range<i64> where String: Borrow<S> {
        match self.partition_map.get_mut(partition) {
            Some(mut partition) => {
                let idx = partition.index;
                partition.index += n as i64;
                idx..partition.index
            },
            // This is a programming error.
            None => panic!("Cannot allocate entid from unknown partition: {}", partition),
        }
    }

    /// Transact the given `entities` against the given SQLite `conn`, using the metadata in
    /// `self.DB`.
    ///
    /// This approach is explained in https://github.com/mozilla/mentat/wiki/Transacting.
    // TODO: move this to the transactor layer.
    pub fn transact<I>(&mut self, conn: &rusqlite::Connection, entities: I) -> Result<TxReport> where I: IntoIterator<Item=Entity> {
        // Eventually, this function will be responsible for managing a SQLite transaction.  For
        // now, it's just about the tx details.

        let tx_instant = now(); // Label the transaction with the timestamp when we first see it: leading edge.
        let tx_id = self.allocate_entid(":db.part/tx");

        self.create_temp_tables(conn)?;

        let mut tx = Tx::new(self, conn, tx_id, tx_instant);
        tx.transact_entities(entities)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bootstrap;
    use debug;
    use edn;
    use edn::symbols;
    use mentat_tx_parser;
    use rusqlite;
    use types::*;

    #[test]
    fn test_open_current_version() {
        // TODO: figure out how to reference the fixtures directory for real.  For now, assume we're
        // executing `cargo test` in `db/`.
        let conn = rusqlite::Connection::open("../fixtures/v2empty.db").unwrap();

        let ident_map = read_ident_map(&conn).unwrap();
        assert_eq!(ident_map, bootstrap::bootstrap_ident_map());

        let schema = read_schema(&conn, &ident_map).unwrap();
        assert_eq!(schema, bootstrap::bootstrap_schema());

        let db = read_db(&conn).unwrap();

        // Does not include :db/txInstant.
        let datoms = debug::datoms_after(&conn, &db, 0).unwrap();
        assert_eq!(datoms.0.len(), 88);

        // Includes :db/txInstant.
        let transactions = debug::transactions_after(&conn, &db, 0).unwrap();
        assert_eq!(transactions.0.len(), 1);
        assert_eq!(transactions.0[0].0.len(), 89);
    }

    /// Assert that a sequence of transactions meets expectations.
    ///
    /// The transactions, expectations, and optional labels, are given in a simple EDN format; see
    /// https://github.com/mozilla/mentat/wiki/Transacting:-EDN-test-format.
    ///
    /// There is some magic here about transaction numbering that I don't want to commit to or
    /// document just yet.  The end state might be much more general pattern matching syntax, rather
    /// than the targeted transaction ID and timestamp replacement we have right now.
    fn assert_transactions(conn: &rusqlite::Connection, db: &mut DB, transactions: &Vec<edn::Value>) {
        for (index, transaction) in transactions.into_iter().enumerate() {
            let index = index as i64;
            let transaction = transaction.as_map().unwrap();
            let label: edn::Value = transaction.get(&edn::Value::NamespacedKeyword(symbols::NamespacedKeyword::new("test", "label"))).unwrap().clone();
            let assertions: edn::Value = transaction.get(&edn::Value::NamespacedKeyword(symbols::NamespacedKeyword::new("test", "assertions"))).unwrap().clone();
            let expected_transaction: Option<&edn::Value> = transaction.get(&edn::Value::NamespacedKeyword(symbols::NamespacedKeyword::new("test", "expected-transaction")));
            let expected_datoms: Option<&edn::Value> = transaction.get(&edn::Value::NamespacedKeyword(symbols::NamespacedKeyword::new("test", "expected-datoms")));
            let expected_error_message: Option<&edn::Value> = transaction.get(&edn::Value::NamespacedKeyword(symbols::NamespacedKeyword::new("test", "expected-error-message")));

            let entities: Vec<_> = mentat_tx_parser::Tx::parse(&[assertions][..]).unwrap();

            let maybe_report = db.transact(&conn, entities);

            if let Some(expected_transaction) = expected_transaction {
                if expected_transaction.is_nil() {
                    assert!(maybe_report.is_err());

                    if let Some(expected_error_message) = expected_error_message {
                        let expected_error_message = expected_error_message.as_text();
                        assert!(expected_error_message.is_some(), "Expected error message to be text:\n{:?}", expected_error_message);
                        let error_message = maybe_report.unwrap_err().to_string();
                        assert!(error_message.contains(expected_error_message.unwrap()), "Expected error message:\n{}\nto contain:\n{}", error_message, expected_error_message.unwrap());
                    }
                    continue
                }

                let report = maybe_report.unwrap();
                assert_eq!(report.tx_id, bootstrap::TX0 + index + 1);

                let transactions = debug::transactions_after(&conn, &db, bootstrap::TX0 + index).unwrap();
                assert_eq!(transactions.0.len(), 1);
                assert_eq!(*expected_transaction,
                           transactions.0[0].into_edn(),
                           "\n{} - expected transaction:\n{}\nbut got transaction:\n{}", label, *expected_transaction, transactions.0[0].into_edn());
            }

            if let Some(expected_datoms) = expected_datoms {
                let datoms = debug::datoms_after(&conn, &db, bootstrap::TX0).unwrap();
                assert_eq!(*expected_datoms,
                           datoms.into_edn(),
                           "\n{} - expected datoms:\n{}\nbut got datoms:\n{}", label, *expected_datoms, datoms.into_edn())
            }

            // Don't allow empty tests.  This will need to change if we allow transacting schema
            // fragments in a preamble, but for now it might catch malformed tests.
            assert_ne!((expected_transaction, expected_datoms), (None, None),
                       "Transaction test must include at least one of :test/expected-transaction or :test/expected-datoms");
        }
    }

    #[test]
    fn test_add() {
        let mut conn = new_connection("").expect("Couldn't open in-memory db");
        let mut db = ensure_current_version(&mut conn).unwrap();

        // Does not include :db/txInstant.
        let datoms = debug::datoms_after(&conn, &db, 0).unwrap();
        assert_eq!(datoms.0.len(), 88);

        // Includes :db/txInstant.
        let transactions = debug::transactions_after(&conn, &db, 0).unwrap();
        assert_eq!(transactions.0.len(), 1);
        assert_eq!(transactions.0[0].0.len(), 89);

        // TODO: extract a test macro simplifying this boilerplate yet further.
        let value = edn::parse::value(include_str!("../../tx/fixtures/test_add.edn")).unwrap().without_spans();

        let transactions = value.as_vector().unwrap();
        assert_transactions(&conn, &mut db, transactions);
    }

    #[test]
    fn test_retract() {
        let mut conn = new_connection("").expect("Couldn't open in-memory db");
        let mut db = ensure_current_version(&mut conn).unwrap();

        // Does not include :db/txInstant.
        let datoms = debug::datoms_after(&conn, &db, 0).unwrap();
        assert_eq!(datoms.0.len(), 88);

        // Includes :db/txInstant.
        let transactions = debug::transactions_after(&conn, &db, 0).unwrap();
        assert_eq!(transactions.0.len(), 1);
        assert_eq!(transactions.0[0].0.len(), 89);

        let value = edn::parse::value(include_str!("../../tx/fixtures/test_retract.edn")).unwrap().without_spans();

        let transactions = value.as_vector().unwrap();
        assert_transactions(&conn, &mut db, transactions);
    }

    #[test]
    fn test_upsert_vector() {
        let mut conn = new_connection("").expect("Couldn't open in-memory db");
        let mut db = ensure_current_version(&mut conn).unwrap();

        // Does not include :db/txInstant.
        let datoms = debug::datoms_after(&conn, &db, 0).unwrap();
        assert_eq!(datoms.0.len(), 88);

        // Includes :db/txInstant.
        let transactions = debug::transactions_after(&conn, &db, 0).unwrap();
        assert_eq!(transactions.0.len(), 1);
        assert_eq!(transactions.0[0].0.len(), 89);

        let value = edn::parse::value(include_str!("../../tx/fixtures/test_upsert_vector.edn")).unwrap().without_spans();

        let transactions = value.as_vector().unwrap();
        assert_transactions(&conn, &mut db, transactions);
    }
}
