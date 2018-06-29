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
    TypedValue,
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
    pub(crate) target: Entid,
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

        let target = avs.get(&pair(entids::DB_EXCISE)?)
            .and_then(|ars| ars.add.iter().next().cloned())
            .and_then(|v| v.into_entid())
            .ok_or_else(|| DbErrorKind::BadExcision("no :db/excise".into()))?; // TODO: more details.

        if schema.get_ident(target).is_some() {
            bail!(DbErrorKind::BadExcision("cannot mutate schema".into())); // TODO: more details.
        }

        let partition = partition_map.partition_for_entid(target)
            .ok_or_else(|| DbErrorKind::BadExcision("target has no partition".into()))?; // TODO: more details.
        // Right now, Mentat only supports `:db.part/{db,user,tx}`, and tests hack in `:db.part/fake`.
        if partition == ":db.part/db" || partition == ":db.part/tx" {
            bail!(DbErrorKind::BadExcision(format!("cannot target entity in partition {}", partition).into())); // TODO: more details.
        }

        let before_tx = avs.get(&pair(entids::DB_EXCISE_BEFORE_T)?)
            .and_then(|ars| ars.add.iter().next().cloned())
            .and_then(|v| v.into_entid());

        let attrs = avs.get(&pair(entids::DB_EXCISE_ATTRS)?)
            .map(|ars| ars.add.clone().into_iter().filter_map(|v| v.into_entid()).collect());

        let excision = Excision {
            target,
            attrs,
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
    match (excision.before_tx, &excision.attrs) {
        (Some(before_tx), Some(ref attrs)) => {
            let s = attrs.iter().join(", ");
            conn.execute(format!("WITH ids AS (SELECT d.rowid FROM datoms AS d WHERE d.e IS {} AND d.a IN ({}) AND d.tx <= {}) DELETE FROM datoms WHERE rowid IN ids",
                                 excision.target, s, before_tx).as_ref(), &[])?;
        },
        (Some(before_tx), None) => {
            conn.execute(format!("WITH ids AS (SELECT d.rowid FROM datoms AS d WHERE d.e IS {} AND d.tx <= {}) DELETE FROM datoms WHERE rowid IN ids",
                                 excision.target, before_tx).as_ref(), &[])?;
        },
        (None, Some(ref attrs)) => {
            let s = attrs.iter().join(", ");
            conn.execute(format!("WITH ids AS (SELECT d.rowid FROM datoms AS d WHERE d.e IS {} AND d.a IN ({})) DELETE FROM datoms WHERE rowid IN ids",
                                 excision.target, s).as_ref(), &[])?;
        },
        (None, None) => {
            conn.execute(format!("WITH ids AS (SELECT d.rowid FROM datoms AS d WHERE (d.e IS {} OR (d.v IS {} AND d.a IS NOT {}))) DELETE FROM datoms WHERE rowid IN ids",
                                 excision.target, excision.target, entids::DB_EXCISE).as_ref(), &[])?;
        },
    }

    Ok(())
}

fn excise_transactions_before_tx(conn: &rusqlite::Connection, excision: &Excision, before_tx: Entid) -> Result<()> {
    match (excision.before_tx, &excision.attrs) {
        (Some(before_tx), Some(ref attrs)) => {
            let s = attrs.iter().join(", ");
            conn.execute(format!("WITH ids AS (SELECT t.rowid FROM transactions AS t WHERE t.e IS {} AND t.a IN ({}) AND t.tx <= {}) DELETE FROM transactions WHERE rowid IN ids",
                                 excision.target, s, before_tx).as_ref(), &[])?;
        },
        (Some(before_tx), None) => {
            conn.execute(format!("WITH ids AS (SELECT t.rowid FROM transactions AS t WHERE t.e IS {} AND t.tx <= {}) DELETE FROM transactions WHERE rowid IN ids",
                                 excision.target, before_tx).as_ref(), &[])?;
        },
        (None, Some(ref attrs)) => {
            let s = attrs.iter().join(", ");
            conn.execute(format!("WITH ids AS (SELECT t.rowid FROM transactions AS t WHERE t.e IS {} AND t.a IN ({})) DELETE FROM transactions WHERE rowid IN ids",
                                 excision.target, s).as_ref(), &[])?;
        },
        (None, None) => {
            conn.execute(format!("WITH ids AS (SELECT t.rowid FROM transactions AS t WHERE t.tx <= {} AND (t.e IS {} OR (t.v IS {} AND t.a IS NOT {}))) DELETE FROM transactions WHERE rowid IN ids",
                                 before_tx, excision.target, excision.target, entids::DB_EXCISE).as_ref(), &[])?;
        },
    }

    Ok(())
}

pub(crate) fn enqueue_pending_excisions(conn: &rusqlite::Connection, schema: &Schema, tx_id: Entid, excisions: &ExcisionMap) -> Result<()> {
    let mut stmt1: rusqlite::Statement = conn.prepare("INSERT INTO excisions VALUES (?, ?, ?, ?)")?;
    let mut stmt2: rusqlite::Statement = conn.prepare("INSERT INTO excision_attrs VALUES (?, ?)")?;

    for (entid, excision) in excisions {
        let status = excision.before_tx.unwrap_or(tx_id);
        stmt1.execute(&[entid, &excision.target, &excision.before_tx, &status])?;

        if let Some(ref attrs) = excision.attrs {
            for attr in attrs {
                stmt2.execute(&[entid, attr])?;
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
    let mut stmt1: rusqlite::Statement = conn.prepare("SELECT e, target, before_tx, status FROM excisions WHERE status > 0 ORDER BY e")?;
    let mut stmt2: rusqlite::Statement = conn.prepare("SELECT a FROM excision_attrs WHERE e IS ?")?;

    let m: Result<Vec<(Entid, Excision, Entid)>> = stmt1.query_and_then(&[], |row| {
        let e: Entid = row.get_checked(0)?;
        let target: Entid = row.get_checked(1)?;
        let before_tx: Option<Entid> = row.get_checked(2)?;
        let status: Entid = row.get_checked(3)?;

        let attrs: Result<BTreeSet<Entid>> = stmt2.query_and_then(&[&e], |row| {
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
            target,
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
