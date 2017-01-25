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

use rusqlite;
use rusqlite::types::{ToSql, ToSqlOutput};

use bootstrap;
use edn::types::Value;
use errors::*;
use mentat_tx::entities as entmod;
use mentat_tx::entities::Entity;
use types::*;

pub fn new_connection() -> rusqlite::Connection {
    return rusqlite::Connection::open_in_memory().unwrap();
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
pub fn create_current_version(conn: &mut rusqlite::Connection) -> Result<i32> {
    let tx = conn.transaction()?;

    for statement in (&V2_STATEMENTS).iter() {
        tx.execute(statement, &[])?;
    }

    let bootstrap_partition_map = bootstrap::bootstrap_partition_map();
    // TODO: think more carefully about allocating new parts and bitmasking part ranges.
    // TODO: install these using bootstrap assertions.  It's tricky because the part ranges are implicit.
    // TODO: one insert, chunk into 999/3 sections, for safety.
    for (part, partition) in bootstrap_partition_map.iter() {
        // TODO: Convert "keyword" part to SQL using Value conversion.
        tx.execute("INSERT INTO parts VALUES (?, ?, ?)", &[part, &partition.start, &partition.index])?;
    }

    let bootstrap_db = DB::new(bootstrap_partition_map, bootstrap::bootstrap_schema());
    bootstrap_db.transact_internal(&tx, &bootstrap::bootstrap_entities()[..])?;

    set_user_version(&tx, CURRENT_VERSION)?;
    let user_version = get_user_version(&tx)?;

    // TODO: use the drop semantics to do this automagically?
    tx.commit()?;
    Ok(user_version)
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

pub fn ensure_current_version(conn: &mut rusqlite::Connection) -> Result<i32> {
    let user_version = get_user_version(&conn)?;
    match user_version {
        CURRENT_VERSION => Ok(user_version),
        0 => create_current_version(conn),
        v => update_from_version(conn, v),
    }
}

impl TypedValue {
    /// Given a SQLite `value` and a `value_type_tag`, return the corresponding `TypedValue`.
    pub fn from_sql_value_pair(value: &rusqlite::types::Value, value_type_tag: &i32) -> Result<TypedValue> {
        match (*value_type_tag, value) {
            (0, &rusqlite::types::Value::Integer(ref x)) => Ok(TypedValue::Ref(*x)),
            (1, &rusqlite::types::Value::Integer(ref x)) => Ok(TypedValue::Boolean(0 != *x)),
            // SQLite distinguishes integral from decimal types, allowing long and double to
            // share a tag.
            (5, &rusqlite::types::Value::Integer(ref x)) => Ok(TypedValue::Long(*x)),
            (5, &rusqlite::types::Value::Real(ref x)) => Ok(TypedValue::Double((*x).into())),
            (10, &rusqlite::types::Value::Text(ref x)) => Ok(TypedValue::String(x.clone())),
            (13, &rusqlite::types::Value::Text(ref x)) => Ok(TypedValue::Keyword(x.clone())),
            (_, value) => bail!(ErrorKind::BadSQLValuePair(value.clone(), *value_type_tag)),
        }
    }

    /// Given an EDN `value`, return a corresponding Mentat `TypedValue`.
    ///
    /// An EDN `Value` does not encode a unique Mentat `ValueType`, so the composition
    /// `from_edn_value(first(to_edn_value_pair(...)))` loses information.  Additionally, there are
    /// EDN values which are not Mentat typed values.
    ///
    /// This function is deterministic.
    pub fn from_edn_value(value: &Value) -> Option<TypedValue> {
        match value {
            &Value::Boolean(x) => Some(TypedValue::Boolean(x)),
            &Value::Integer(x) => Some(TypedValue::Long(x)),
            &Value::Float(ref x) => Some(TypedValue::Double(x.clone())),
            &Value::Text(ref x) => Some(TypedValue::String(x.clone())),
            &Value::NamespacedKeyword(ref x) => Some(TypedValue::Keyword(x.to_string())),
            _ => None
        }
    }

    /// Return the corresponding SQLite `value` and `value_type_tag` pair.
    pub fn to_sql_value_pair<'a>(&'a self) -> (ToSqlOutput<'a>, i32) {
        match self {
            &TypedValue::Ref(x) => (rusqlite::types::Value::Integer(x).into(), 0),
            &TypedValue::Boolean(x) => (rusqlite::types::Value::Integer(if x { 1 } else { 0 }).into(), 1),
            // SQLite distinguishes integral from decimal types, allowing long and double to share a tag.
            &TypedValue::Long(x) => (rusqlite::types::Value::Integer(x).into(), 5),
            &TypedValue::Double(x) => (rusqlite::types::Value::Real(x.into_inner()).into(), 5),
            &TypedValue::String(ref x) => (rusqlite::types::ValueRef::Text(x.as_str()).into(), 10),
            &TypedValue::Keyword(ref x) => (rusqlite::types::ValueRef::Text(x.as_str()).into(), 13),
        }
    }

    /// Return the corresponding EDN `value` and `value_type` pair.
    pub fn to_edn_value_pair(&self) -> (Value, ValueType) {
        match self {
            &TypedValue::Ref(x) => (Value::Integer(x), ValueType::Ref),
            &TypedValue::Boolean(x) => (Value::Boolean(x), ValueType::Boolean),
            &TypedValue::Long(x) => (Value::Integer(x), ValueType::Long),
            &TypedValue::Double(x) => (Value::Float(x), ValueType::Double),
            &TypedValue::String(ref x) => (Value::Text(x.clone()), ValueType::String),
            &TypedValue::Keyword(ref x) => (Value::Text(x.clone()), ValueType::Keyword),
        }
    }
}

/// Read the ident map materialized view from the given SQL store.
pub fn read_ident_map(conn: &rusqlite::Connection) -> Result<IdentMap> {
    let mut stmt: rusqlite::Statement = conn.prepare("SELECT ident, entid FROM idents")?;
    let m = stmt.query_and_then(&[], |row| -> Result<(String, Entid)> {
        Ok((row.get(0), row.get(1)))
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
    let r: Result<Vec<(String, String, TypedValue)>> = stmt.query_and_then(&[], |row| {
        // Each row looks like :db/index|:db/valueType|28|0.  Observe that 28|0 represents a
        // :db.type/ref to entid 28, which needs to be converted to a TypedValue.
        // TODO: don't use textual ident and attr; just use entids directly.
        let symbolic_ident: String = row.get_checked(0)?;
        let symbolic_attr: String = row.get_checked(1)?;
        let v: rusqlite::types::Value = row.get_checked(2)?;
        let value_type_tag: i32 = row.get_checked(3)?;
        let typed_value = TypedValue::from_sql_value_pair(&v, &value_type_tag)?;

        Ok((symbolic_ident, symbolic_attr, typed_value))
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
                (&ValueType::Ref, TypedValue::Keyword(ref x)) => self.schema.require_entid(&x.to_string()).map(|&entid| TypedValue::Ref(entid)),
                // Otherwise, we have a type mismatch.
                (value_type, _) => bail!(ErrorKind::BadEDNValuePair(value.clone(), value_type.clone())),
            }
        }
    }

    // TODO: move this to the transactor layer.
    pub fn transact_internal(&self, conn: &rusqlite::Connection, entities: &[Entity]) -> Result<()>{
        // TODO: manage :db/tx, write :db/txInstant.
        let tx = 1;
        let r: Vec<Result<()>> = entities.into_iter().map(|entity: &Entity| -> Result<()> {
            match *entity {
                Entity::Add {
                    e: entmod::EntidOrLookupRef::Entid(entmod::Entid::Ident(ref e_)),
                    a: entmod::Entid::Ident(ref a_),
                    v: entmod::ValueOrLookupRef::Value(ref v_),
                    tx: _ } => {

                    // TODO: prepare and cache all these statements outside the transaction loop.
                    // XXX: Error types.
                    let mut stmt: rusqlite::Statement = conn.prepare("INSERT INTO datoms(e, a, v, tx, value_type_tag, index_avet, index_vaet, index_fulltext, unique_value) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)")?;
                    let e: i64 = *self.schema.require_entid(&e_.to_string())?;
                    let a: i64 = *self.schema.require_entid(&a_.to_string())?;
                    let attribute: &Attribute = self.schema.require_attribute_for_entid(&a)?;

                    // This is our chance to do schema-aware typechecking: to either assert that the
                    // given value is in the attribute's value set, or (in limited cases) to coerce
                    // the value into the attribute's value set.
                    let typed_value: TypedValue = self.to_typed_value(v_, &attribute)?;

                    // Now we can represent the typed value as an SQL value.
                    let (value, value_type_tag): (ToSqlOutput, i32) = typed_value.to_sql_value_pair();

                    // Fun times, type signatures.
                    let values: [&ToSql; 9] = [&e, &a, &value, &tx, &value_type_tag, &attribute.index, to_bool_ref(attribute.value_type == ValueType::Ref), &attribute.fulltext, &attribute.unique_value];
                    stmt.insert(&values[..])?;
                    Ok(())
                },
                // TODO: find a better error type for this.
                _ => panic!(format!("Transacting entity not yet supported: {:?}", entity))
            }
        }).collect();

        let x: Result<Vec<()>> = r.into_iter().collect();
        x.map(|_| ())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bootstrap;
    use debug;
    use rusqlite;
    use types::*;

    #[test]
    fn test_open_current_version() {
        // TODO: figure out how to reference the fixtures directory for real.  For now, assume we're
        // executing `cargo test` in `db/`.
        let conn = rusqlite::Connection::open("../fixtures/v2empty.db").unwrap();
        // assert_eq!(ensure_current_version(&mut conn).unwrap(), CURRENT_VERSION);

        // TODO: write :db/txInstant, bump :db.part/tx.
        // let partition_map = read_partition_map(&conn).unwrap();
        // assert_eq!(partition_map, bootstrap::bootstrap_partition_map());

        let ident_map = read_ident_map(&conn).unwrap();
        assert_eq!(ident_map, bootstrap::bootstrap_ident_map());

        let schema = read_schema(&conn, &ident_map).unwrap();
        assert_eq!(schema, bootstrap::bootstrap_schema()); // Schema::default());

        let db = read_db(&conn).unwrap();

        let datoms = debug::datoms_after(&conn, &db, &0).unwrap();
        assert_eq!(datoms.len(), 89); // The 89th is the :db/txInstant value.

        // // TODO: fewer magic numbers!
        // assert_eq!(debug::datoms_after(&conn, &db, &0x10000001).unwrap(), vec![]);
    }

    #[test]
    fn test_create_current_version() {
        // // assert_eq!(bootstrap_schema().unwrap(), Schema::default());

        // // Ignore result.
        // use std::fs;
        // let _ = fs::remove_file("/Users/nalexander/Mozilla/mentat/test.db");
        // let mut conn = rusqlite::Connection::open("file:///Users/nalexander/Mozilla/mentat/test.db").unwrap();

        let mut conn = new_connection();

        assert_eq!(ensure_current_version(&mut conn).unwrap(), CURRENT_VERSION);

        let bootstrap_db = DB::new(bootstrap::bootstrap_partition_map(), bootstrap::bootstrap_schema());
        // TODO: write materialized view of bootstrapped schema to SQL store.
        // let db = read_db(&conn).unwrap();
        // assert_eq!(db, bootstrap_db);

        let datoms = debug::datoms_after(&conn, &bootstrap_db, &0).unwrap();
        assert_eq!(datoms.len(), 88);
    }
}
