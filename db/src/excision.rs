// Copyright 2018 Mozilla
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

use std::collections::{
    BTreeSet,
    BTreeMap,
};

use itertools::Itertools;

use rusqlite;

use mentat_core::{
    Attribute,
    Entid,
    HasSchema,
    Schema,
    SQLValueType,
    TypedValue,
    ValueType,
};

use db::{
    TypedSQLValue,
};

use entids;

use errors::{
    DbErrorKind,
    Result,
};

use internal_types::{
    AEVTrie,
    filter_aev_to_eav,
};

use schema::{
    SchemaBuilding,
};

use types::{
    PartitionMap,
};

/// Details about an excision:
/// - a target to excise (for now, an entid);
/// - a possibly empty set of attributes to excise (the empty set means all attributes, not no
///   attributes);
/// - and a possibly omitted transaction ID to limit the excision before.  (TODO: check whether
///   Datomic excises the last retraction before the first remaining assertion, and make our
///   behaviour agree.)
///
/// `:db/before` doesn't make sense globally, since in Mentat, monotonically increasing
/// transaction IDs don't guarantee monotonically increasing txInstant values.  Therefore, we
/// accept only `:db/beforeT` and allow consumers to turn `:db/before` timestamps into
/// transaction IDs in whatever way they see fit.
#[derive(Clone, Debug, Default, Eq, Hash, PartialEq)]
pub(crate) struct Excision {
    pub(crate) targets: BTreeSet<Entid>,
    pub(crate) attrs: Option<BTreeSet<Entid>>,
    pub(crate) before_tx: Option<Entid>,
}

/// Map from `entid` to excision details.  `entid` is not the excision `target`!
pub(crate) type ExcisionMap = BTreeMap<Entid, Excision>;

/// Extract excisions from the given transacted datoms.
pub(crate) fn excisions<'schema>(partition_map: &'schema PartitionMap, schema: &'schema Schema, aev_trie: &AEVTrie<'schema>) -> Result<Option<ExcisionMap>> {
    let pair = |a: Entid| -> Result<(Entid, &'schema Attribute)> {
        schema.require_attribute_for_entid(a).map(|attribute| (a, attribute))
    };

    if aev_trie.contains_key(&pair(entids::DB_EXCISE_BEFORE)?) {
        bail!(DbErrorKind::BadExcision(":db.excise/before".into())); // TODO: more details.
    }

    // TODO: Don't allow anything more than excisions in the excising transaction, except
    // additional facts about the (transaction-tx).
    let eav_trie = filter_aev_to_eav(aev_trie, |&(a, _)|
                                     a == entids::DB_EXCISE ||
                                     a == entids::DB_EXCISE_ATTRS ||
                                     a == entids::DB_EXCISE_BEFORE_T);

    let mut excisions = ExcisionMap::default();

    for (&e, avs) in eav_trie.iter() {
        for (&(_a, _attribute), ars) in avs {
            if !ars.retract.is_empty() {
                bail!(DbErrorKind::BadExcision("retraction".into())); // TODO: more details.
            }
        }

        let before_tx = avs.get(&pair(entids::DB_EXCISE_BEFORE_T)?)
            .and_then(|ars| {
                assert_eq!(ars.add.len(), 1, "witnessed more than one :db.excise/beforeT");
                assert!(ars.retract.is_empty(), "witnessed [:db/retract ... :db.excise/beforeT ...]");
                ars.add.iter().next().cloned()
            })
            .and_then(|v| v.into_entid());

        let attrs = avs.get(&pair(entids::DB_EXCISE_ATTRS)?)
            .map(|ars| ars.add.clone().into_iter().filter_map(|v| v.into_entid()).collect());

        let targets = avs.get(&pair(entids::DB_EXCISE)?)
            .map(|ars| {
                assert!(ars.retract.is_empty(), "witnessed [:db/retract ... :db/excise ...]");
                assert!(!ars.add.is_empty(), "witnessed empty :db/excise target set");
                let targets: BTreeSet<_> = ars.add.clone().into_iter().filter_map(|v| v.into_entid()).collect();
                assert_eq!(targets.len(), ars.add.len(), "witnessed non-entid :db/excise target");
                targets
            })
            .ok_or_else(|| DbErrorKind::BadExcision("no :db/excise".into()))?; // TODO: more details.

        for target in &targets {
            if schema.get_ident(*target).is_some() {
                bail!(DbErrorKind::BadExcision("cannot mutate schema".into())); // TODO: more details.
            }

            let (name, partition) = partition_map.partition_for_entid(*target)
                .ok_or_else(|| DbErrorKind::BadExcision("target has no partition".into()))?; // TODO: more details.
            if !partition.allow_excision {
                bail!(DbErrorKind::BadExcision(format!("cannot target entity in partition {}", name).into())); // TODO: more details.
            }
        }

        let excision = Excision {
            targets,
            attrs: attrs.clone(),
            before_tx,
        };

        excisions.insert(e, excision);
    }

    if excisions.is_empty() {
        Ok(None)
    } else {
        Ok(Some(excisions))
    }
}

fn excise_datoms(conn: &rusqlite::Connection, excision: &Excision) -> Result<()> {
    let targets = excision.targets.iter().join(", ");

    match (excision.before_tx, &excision.attrs) {
        (Some(before_tx), Some(ref attrs)) => {
            let s = attrs.iter().join(", ");
            conn.execute(format!("WITH ids AS (SELECT d.rowid FROM datoms AS d WHERE d.e IN ({}) AND d.a IN ({}) AND d.tx <= {}) DELETE FROM datoms WHERE rowid IN ids",
                                 targets, s, before_tx).as_ref(), &[])?;
        },
        (Some(before_tx), None) => {
            conn.execute(format!("WITH ids AS (SELECT d.rowid FROM datoms AS d WHERE d.e IN ({}) AND d.tx <= {}) DELETE FROM datoms WHERE rowid IN ids",
                                 targets, before_tx).as_ref(), &[])?;
        },
        (None, Some(ref attrs)) => {
            let s = attrs.iter().join(", ");
            conn.execute(format!("WITH ids AS (SELECT d.rowid FROM datoms AS d WHERE d.e IN ({}) AND d.a IN ({})) DELETE FROM datoms WHERE rowid IN ids",
                                 targets, s).as_ref(), &[])?;
        },
        (None, None) => {
            // TODO: Use AVET index to speed this up?
            conn.execute(format!("WITH ids AS (SELECT d.rowid FROM datoms AS d WHERE (d.e IN ({}) OR (d.v IN ({}) AND d.value_type_tag IS {} AND d.a IS NOT {}))) DELETE FROM datoms WHERE rowid IN ids",
                                 targets, targets, ValueType::Ref.value_type_tag(), entids::DB_EXCISE).as_ref(), &[])?;
        },
    }

    Ok(())
}

fn excise_transactions_before_tx(conn: &rusqlite::Connection, excision: &Excision, before_tx: Entid) -> Result<()> {
    let targets = excision.targets.iter().join(", ");

    match (excision.before_tx, &excision.attrs) {
        (Some(before_tx), Some(ref attrs)) => {
            let s = attrs.iter().join(", ");
            conn.execute(format!("WITH ids AS (SELECT t.rowid FROM transactions AS t WHERE t.e IN ({}) AND t.a IN ({}) AND t.tx <= {}) DELETE FROM transactions WHERE rowid IN ids",
                                 targets, s, before_tx).as_ref(), &[])?;
        },
        (Some(before_tx), None) => {
            conn.execute(format!("WITH ids AS (SELECT t.rowid FROM transactions AS t WHERE t.e IN ({}) AND t.tx <= {}) DELETE FROM transactions WHERE rowid IN ids",
                                 targets, before_tx).as_ref(), &[])?;
        },
        (None, Some(ref attrs)) => {
            let s = attrs.iter().join(", ");
            conn.execute(format!("WITH ids AS (SELECT t.rowid FROM transactions AS t WHERE t.e IN ({}) AND t.a IN ({})) DELETE FROM transactions WHERE rowid IN ids",
                                 targets, s).as_ref(), &[])?;
        },
        (None, None) => {
            conn.execute(format!("WITH ids AS (SELECT t.rowid FROM transactions AS t WHERE t.tx <= {} AND (t.e IN ({}) OR (t.v IN ({}) AND t.value_type_tag IS {} AND t.a IS NOT {}))) DELETE FROM transactions WHERE rowid IN ids",
                                 before_tx, targets, targets, ValueType::Ref.value_type_tag(), entids::DB_EXCISE).as_ref(), &[])?;
        },
    }

    Ok(())
}

pub(crate) fn enqueue_pending_excisions(conn: &rusqlite::Connection, schema: &Schema, tx_id: Entid, excisions: &ExcisionMap) -> Result<()> {
    let mut stmt1: rusqlite::Statement = conn.prepare("INSERT INTO excisions VALUES (?, ?, ?)")?;
    let mut stmt2: rusqlite::Statement = conn.prepare("INSERT INTO excision_targets VALUES (?, ?)")?;
    let mut stmt3: rusqlite::Statement = conn.prepare("INSERT INTO excision_attrs VALUES (?, ?)")?;

    for (entid, excision) in excisions {
        let status = excision.before_tx.unwrap_or(tx_id);
        stmt1.execute(&[entid, &excision.before_tx, &status])?;

        for target in &excision.targets {
            stmt2.execute(&[entid, target])?;
        }

        if let Some(ref attrs) = excision.attrs {
            for attr in attrs {
                stmt3.execute(&[entid, attr])?;
            }
        }
    }

    // Might as well not interleave writes to "excisions" and "excision_attrs" with writes to
    // "datoms".  This also leaves open the door for a more efficient bulk operation.
    for (_entid, excision) in excisions {
        excise_datoms(conn, &excision)?;
    }

    Ok(())
}

fn pending_excision_list(conn: &rusqlite::Connection, partition_map: &PartitionMap, schema: &Schema) -> Result<Vec<(Entid, Excision, Entid)>> {
    let mut stmt1: rusqlite::Statement = conn.prepare("SELECT e, before_tx, status FROM excisions WHERE status > 0 ORDER BY e")?;
    let mut stmt2: rusqlite::Statement = conn.prepare("SELECT target FROM excision_targets WHERE e IS ?")?;
    let mut stmt3: rusqlite::Statement = conn.prepare("SELECT a FROM excision_attrs WHERE e IS ?")?;

    let m: Result<Vec<(Entid, Excision, Entid)>> = stmt1.query_and_then(&[], |row| {
        let e: Entid = row.get_checked(0)?;
        let before_tx: Option<Entid> = row.get_checked(1)?;
        let status: Entid = row.get_checked(2)?;

        let targets: Result<BTreeSet<Entid>> = stmt2.query_and_then(&[&e], |row| {
            let target: Entid = row.get_checked(0)?;
            Ok(target)
        })?.collect();
        let targets = targets?;

        let attrs: Result<BTreeSet<Entid>> = stmt3.query_and_then(&[&e], |row| {
            let a: Entid = row.get_checked(0)?;
            Ok(a)
        })?.collect();
        let attrs = attrs.map(|attrs| {
            if attrs.is_empty() {
                None
            } else {
                Some(attrs)
            }
        })?;

        let excision = Excision {
            targets,
            before_tx,
            attrs,
        };

        Ok((e, excision, status))
    })?.collect();

    m
}

pub(crate) fn pending_excisions(conn: &rusqlite::Connection, partition_map: &PartitionMap, schema: &Schema) -> Result<ExcisionMap> {
    let list = pending_excision_list(conn, partition_map, schema)?;
    Ok(list.into_iter().map(|(entity, excision, _status)| (entity, excision)).collect())
}

pub(crate) fn ensure_no_pending_excisions(conn: &rusqlite::Connection, partition_map: &PartitionMap, schema: &Schema) -> Result<ExcisionMap> {
    let list = pending_excision_list(conn, partition_map, schema)?;

    for (_entid, excision, status) in &list {
        excise_transactions_before_tx(conn, &excision, *status)?;
    }

    delete_dangling_retractions(conn)?;

    conn.execute("UPDATE excisions SET status = 0", &[])?;

    // TODO: only vacuum fulltext if an excision (likely) impacted fulltext values, since this is
    // very expensive.  As always, correctness first, performance second.
    vacuum_fulltext_table(conn)?;

    Ok(list.into_iter().map(|(entity, excision, _status)| (entity, excision)).collect())
}


/// Delete fulltext values that are no longer refered to in the `datoms` or `transactions` table.
pub(crate) fn vacuum_fulltext_table(conn: &rusqlite::Connection) -> Result<()> {
    let (true_value, true_value_type_tag) = TypedValue::Boolean(true).to_sql_value_pair();

    // First, collect all `:db/fulltext true` attributes.  This is easier than extracting them from
    // a `Schema` (no need to execute multiple insertions for large collections), but less flexible.
    conn.execute(r#"CREATE TABLE temp.fulltext_as (a SMALLINT NOT NULL)"# , &[])?;
    conn.execute(r#"INSERT INTO temp.fulltext_as (a)
                    SELECT e FROM schema WHERE a = ? AND v = ? AND value_type_tag = ?"# ,
                 &[&entids::DB_FULLTEXT, &true_value, &true_value_type_tag])?;

    // Next, purge values that aren't referenced.  We're using that `:db/fulltext true` attributes
    // always have `:db/index true`, so that we can use the `avet` index.
    conn.execute(r#"DELETE FROM fulltext_values
                    WHERE rowid NOT IN
                      (SELECT v
                       FROM datoms
                       WHERE index_avet IS NOT 0 AND a IN temp.fulltext_as
                       UNION ALL
                       SELECT v
                       FROM transactions
                       WHERE a IN temp.fulltext_as)"#, &[])?;

    conn.execute(r#"DROP TABLE temp.fulltext_as"# , &[])?;

    Ok(())
}

/// Delete dangling retractions from the transaction log.
///
/// Suppose that `E` is a fixed entid and that the following transactions are transacted:
/// ```edn
/// [[:db/add E :db/doc "first"]]
/// [[:db/retract E :db/doc "first"]]
/// [[:db/add E :db/doc "second"]]
/// ```
///
/// If we excise just the first datom -- the datom corresponding to `[:db/add E :db/doc "first"]` --
/// then there will be a "dangling retraction" in the log, which will look like:
/// ```edn
/// [[E :db/doc "first" TX1 false]]
/// [[E :db/doc "second" TX2 true]]
/// ```
///
/// This function purges such dangling retractions, so that a datom is always asserted before it is
/// retracted.
pub(crate) fn delete_dangling_retractions(conn: &rusqlite::Connection) -> Result<()> {
    // We walk the transactions table.  For each `[e a v]`, we find all of the log entries
    // corresponding to the first transaction that it appeared in.  We delete any entries that are
    // retractions; it is not possible to retract an `[e a v]` not asserted in a prior transaction.
    conn.execute(r#"WITH ids AS
                    (SELECT rowid FROM
                     (SELECT MIN(tx), added, rowid
                      FROM transactions
                      GROUP BY e, a, v, value_type_tag)
                     WHERE added = 0)
                    DELETE FROM transactions
                    WHERE rowid IN ids"#,
                 &[])?;

    Ok(())
}
