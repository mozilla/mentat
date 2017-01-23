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
use values;
use {to_namespaced_keyword};

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

/// Given a SQLite `value` and a `value_type_tag`, return the corresponding EDN `Value`.
pub fn to_edn(value: &rusqlite::types::Value, value_type_tag: &i32) -> Result<Value> {
    ValueType::from_value_type_tag(value_type_tag)
         .ok_or(ErrorKind::BadValueTypeTag(*value_type_tag).into())
         .and_then(|value_type| -> Result<Value> {
             match (value_type, value) {
                 (ValueType::Ref, &rusqlite::types::Value::Integer(ref x)) => Ok(Value::Integer(*x)),
                 (ValueType::Boolean, &rusqlite::types::Value::Integer(ref x)) => Ok(Value::Boolean(0 != *x)),
                 (ValueType::Instant, &rusqlite::types::Value::Integer(_)) => bail!(ErrorKind::NotYetImplemented(":db.type/instant".into())),
                 // SQLite distinguishes integral from decimal types, allowing long and double to
                 // share a tag.  That means the `value_type` above might be incorrect: we could see
                 // a `Long` tag and really have `Double` data.  We always trust the tag: Mentat
                 // should be managing its metadata correctly.
                 (ValueType::Long, &rusqlite::types::Value::Integer(ref x)) => Ok(Value::Integer(*x)),
                 (ValueType::Long, &rusqlite::types::Value::Real(ref x)) if value_type_tag == &ValueType::Double.value_type_tag() => Ok(Value::Float((*x).into())),
                 // This is spurious, since `value_type` never returns `Double`, but let's just
                 // leave it in case we change things.
                 (ValueType::Double, &rusqlite::types::Value::Real(ref x)) => Ok(Value::Float((*x).into())),
                 (ValueType::String, &rusqlite::types::Value::Text(ref x)) => Ok(Value::Text(x.clone())),
                 (ValueType::UUID, &rusqlite::types::Value::Text(_)) => bail!(ErrorKind::NotYetImplemented(":db.type/uuid".into())),
                 (ValueType::URI, &rusqlite::types::Value::Text(_)) => bail!(ErrorKind::NotYetImplemented(":db.type/uri".into())),
                 (ValueType::Keyword, &rusqlite::types::Value::Text(ref x)) => Ok(Value::Text(x.clone())),
                 (_, value) => bail!(ErrorKind::BadValueAndTagPair(value.clone(), *value_type_tag)),
             }
         })
}

/// A `Value` that can be converted `ToSql`.
///
/// This is just working around Rust's refusal to allow us to implement a trait we don't originate
/// for a type we don't originate.
///
/// TODO: &Value for efficiency.
struct SqlValueWrapper(Value);

impl ToSql for SqlValueWrapper {
    fn to_sql(&self) -> rusqlite::Result<ToSqlOutput> {
        match self.0 {
            Value::Nil => Ok(ToSqlOutput::from(rusqlite::types::Value::Null)),
            Value::Boolean(x) => Ok(ToSqlOutput::from(x)),
            Value::Integer(x) => Ok(ToSqlOutput::from(x)),
            // TODO: consider using a larger radix to save characters.
            Value::BigInteger(ref x) => Ok(ToSqlOutput::from(x.to_str_radix(10))),
            Value::Float(ref x) => Ok(ToSqlOutput::from(x.into_inner())),
            // TODO: try to avoid this clone.
            Value::Text(ref x) => Ok(ToSqlOutput::from(x.clone())),
            Value::PlainSymbol(ref x) => Ok(ToSqlOutput::from(x.to_string())),
            Value::NamespacedSymbol(ref x) => Ok(ToSqlOutput::from(x.to_string())),
            Value::Keyword(ref x) => Ok(ToSqlOutput::from(x.to_string())),
            Value::NamespacedKeyword(ref x) => Ok(ToSqlOutput::from(x.to_string())),
            Value::Vector(_) => Err(rusqlite::Error::InvalidColumnName(format!("Cannot convert to_sql: {:?}", self.0))),
            Value::List(_) => Err(rusqlite::Error::InvalidColumnName(format!("Cannot convert to_sql: {:?}", self.0))),
            Value::Set(_) => Err(rusqlite::Error::InvalidColumnName(format!("Cannot convert to_sql: {:?}", self.0))),
            Value::Map(_) => Err(rusqlite::Error::InvalidColumnName(format!("Cannot convert to_sql: {:?}", self.0))),
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
    // TODO: consider a less expensive way to do this inversion.
    let schema_for_idents = Schema::from(ident_map.clone(), SchemaMap::default())?;

    let mut stmt: rusqlite::Statement = conn.prepare("SELECT ident, attr, value, value_type_tag FROM schema")?;
    let r: Result<Vec<Value>> = stmt.query_and_then(&[], |row| {
        // Each row looks like :db/index|:db/valueType|28|0.  Observe that 28|0 represents a
        // :db.type/ref to entid 28, which needs to be converted to an ident.
        let symbolic_ident: String = row.get_checked(0)?;
        let attr: String = row.get_checked(1)?;
        let v: rusqlite::types::Value = row.get_checked(2)?;
        let value_type_tag = row.get_checked(3)?;

        // We want a symbolic schema, but most of our values are :db.type/ref attributes.  Map those
        // entids back to idents.  This is ad-hoc since we haven't yet a functional DB instance.
        let value = match to_edn(&v, &value_type_tag)? {
            Value::Integer(entid) if value_type_tag == ValueType::Ref.value_type_tag() => {
                schema_for_idents.get_ident(&entid)
                    .and_then(|x| to_namespaced_keyword(&x))
                    .map(Value::NamespacedKeyword)
                    .ok_or(rusqlite::Error::InvalidColumnName(format!("Could not map :db.type/ref {} to ident!", entid)))?
            },
            value => value
        };

        Ok(Value::Vector(vec![
            values::DB_ADD.clone(),
            to_namespaced_keyword(&symbolic_ident).map(Value::NamespacedKeyword).ok_or(rusqlite::Error::InvalidColumnName(format!("XX1!")))?,
            to_namespaced_keyword(&attr).map(Value::NamespacedKeyword).ok_or(rusqlite::Error::InvalidColumnName(format!("XX2!")))?,
            value]))
    })?.collect();

    r.and_then(|values| Schema::from_ident_map_and_assertions(ident_map.clone(), &Value::Vector(values)))
}

/// Read the materialized views from the given SQL store and return a Mentat `DB` for querying and
/// applying transactions.
pub fn read_db(conn: &rusqlite::Connection) -> Result<DB> {
    let partition_map = read_partition_map(conn)?;
    let ident_map = read_ident_map(conn)?;
    let schema = read_schema(conn, &ident_map)?;
    Ok(DB::new(partition_map, schema))
}

// pub fn bootstrap(conn: &mut rusqlite::Connection, from_version: i32) -> Result<()> {
//     match from_version {
//         0 => {
//             conn.execute(&format!("INSERT INTO parts VALUES = {}", "()"), &[][..])?;
//             Ok(())
//         },
//         // 1 => {
//         // },
//         // TODO: find a better error type for this.
//         _ => Err(rusqlite::Error::InvalidColumnName(format!("Cannot handle bootstrapping from version {}!", from_version)))
//     }
// }

impl DB {
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
                    let mut stmt: rusqlite::Statement = conn.prepare("INSERT INTO datoms(e, a, v, tx, value_type_tag, index_avet, index_vaet, index_fulltext, unique_value) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)")?;
                    let e: i64 = *self.schema.get_entid(&e_.to_string()).ok_or(rusqlite::Error::InvalidColumnName(format!("Could not find entid for ident: {:?}", e_)))?;
                    let a: i64 = *self.schema.get_entid(&a_.to_string()).ok_or(rusqlite::Error::InvalidColumnName(format!("Could not find entid for ident: {:?}", a_)))?;
                    let attributes: &Attribute = self.schema.schema_map.get(&a).ok_or(rusqlite::Error::InvalidColumnName(format!("Could not find attributes for entid: {:?}", a)))?;

                    let value_type_tag = attributes.value_type.value_type_tag();

                    // We can have [:db/ident :db/ref-attr :db/kw] and need to convert the ident
                    // :db/kw to an entid.  So we need some initial type-checking.
                    // TODO: do the type-checking and value-mapping comprehensively.
                    let mut v__ = v_.clone();
                    if attributes.value_type == ValueType::Ref {
                        match *v_ {
                            Value::Integer(_) => (),
                            Value::NamespacedKeyword(ref s) => { v__ = self.schema.get_entid(&s.to_string()).map(|&x| Value::Integer(x)).ok_or(rusqlite::Error::InvalidColumnName(format!("Could not find entid for ident: {:?}", e_)))? },
                            _ => panic!("bad type"),
                        }
                    }

                    // TODO: avoid spurious clone.
                    let v = SqlValueWrapper(v__.clone());

                    // Fun times, type signatures.
                    let values: [&ToSql; 9] = [&e, &a, &v, &tx, &value_type_tag, &attributes.index, to_bool_ref(attributes.value_type == ValueType::Ref), &attributes.fulltext, &attributes.unique_value];
                    stmt.insert(&values[..])?;
                    Ok(())
                },
                // TODO: find a better error type for this.
                _ => panic!(format!("Transacting entity not yet supported: {:?}", entity)) // rusqlite::Error::InvalidColumnName(format!("Not yet implented: entities of form ...")))
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
    use types::*;

    // #[test]
    // fn test_open_current_version() {
    //     let mut conn = rusqlite::Connection::open("file:///Users/nalexander/Mozilla/mentat/fixtures/v2empty.db").unwrap();
    //     // assert_eq!(ensure_current_version(&mut conn).unwrap(), CURRENT_VERSION);

    //     // let mut map = IdentMap::new();
    //     // assert_eq!(map.insert("a".into(), 1), None);

    //     let partition_map = read_partition_map(&conn).unwrap();
    //     // assert_eq!(partition_map, bootstrap_partition_map());

    //     let ident_map = read_ident_map(&conn).unwrap();
    //     assert_eq!(ident_map, bootstrap_ident_map());

    //     let schema = read_schema(&conn, &ident_map).unwrap();
    //     assert_eq!(schema, Schema::default());

    //     let db = DB {
    //         partition_map: partition_map,
    //         schema: schema,
    //     };

    //     // assert_eq!(ident_map, IdentMap::new());
    //     // assert_eq!(read_partition_map(&conn).unwrap(), PartitionMap::new());
    //     // assert_eq!(read_schema(&conn, &ident_map).unwrap(), Schema::default());

    //     // TODO: fewer magic numbers!
    //     assert_eq!(datoms_after(&conn, &db, &0x10000000).unwrap(), vec![]);
    // }

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
