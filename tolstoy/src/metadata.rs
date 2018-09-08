// Copyright 2018 Mozilla
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
use uuid::Uuid;

use core_traits::{
    Entid,
};

use schema;

use public_traits::errors::{
    Result,
};

use tolstoy_traits::errors::{
    TolstoyError,
};

use mentat_db::{
    Partition,
    PartitionMap,
    db,
};

use types::{
    LocalGlobalTxMapping,
};

use TxMapper;

// Could be Copy, but that might change
pub struct SyncMetadata {
    // Local head: latest transaction that we have in the store,
    // but with one caveat: its tx might will not be mapped if it's
    // never been synced successfully.
    // In other words: if latest tx isn't mapped, then HEAD moved
    // since last sync and server needs to be updated.
    pub root: Entid,
    pub head: Entid,
}

pub enum PartitionsTable {
    Core,
    Tolstoy,
}

impl SyncMetadata {
    pub fn new(root: Entid, head: Entid) -> SyncMetadata {
        SyncMetadata {
            root: root,
            head: head,
        }
    }

    pub fn remote_head(tx: &rusqlite::Transaction) -> Result<Uuid> {
        tx.query_row(
            "SELECT value FROM tolstoy_metadata WHERE key = ?",
            &[&schema::REMOTE_HEAD_KEY], |r| {
                let bytes: Vec<u8> = r.get(0);
                Uuid::from_bytes(bytes.as_slice())
            }
        )?.map_err(|e| e.into())
    }

    pub fn set_remote_head(tx: &rusqlite::Transaction, uuid: &Uuid) -> Result<()> {
        let uuid_bytes = uuid.as_bytes().to_vec();
        let updated = tx.execute("UPDATE tolstoy_metadata SET value = ? WHERE key = ?",
            &[&uuid_bytes, &schema::REMOTE_HEAD_KEY])?;
        if updated != 1 {
            bail!(TolstoyError::DuplicateMetadata(schema::REMOTE_HEAD_KEY.into()));
        }
        Ok(())
    }

    pub fn set_remote_head_and_map(tx: &mut rusqlite::Transaction, mapping: LocalGlobalTxMapping) -> Result<()> {
        SyncMetadata::set_remote_head(tx, mapping.remote)?;
        TxMapper::set_lg_mapping(tx, mapping)?;
        Ok(())
    }

    // TODO Functions below start to blur the line between mentat-proper and tolstoy...
    pub fn get_partitions(tx: &rusqlite::Transaction, parts_table: PartitionsTable) -> Result<PartitionMap> {
        match parts_table {
            PartitionsTable::Core => {
                db::read_partition_map(tx).map_err(|e| e.into())
            },
            PartitionsTable::Tolstoy => {
                let mut stmt: ::rusqlite::Statement = tx.prepare("SELECT part, start, end, idx, allow_excision FROM tolstoy_parts")?;
                let m: Result<PartitionMap> = stmt.query_and_then(&[], |row| -> Result<(String, Partition)> {
                    Ok((row.get_checked(0)?, Partition::new(row.get_checked(1)?, row.get_checked(2)?, row.get_checked(3)?, row.get_checked(4)?)))
                })?.collect();
                m
            }
        }
    }

    pub fn root_and_head_tx(tx: &rusqlite::Transaction) -> Result<(Entid, Entid)> {
        let mut stmt: ::rusqlite::Statement = tx.prepare("SELECT tx FROM timelined_transactions WHERE timeline = 0 GROUP BY tx ORDER BY tx")?;
        let txs: Vec<_> = stmt.query_and_then(&[], |row| -> Result<Entid> {
            Ok(row.get_checked(0)?)
        })?.collect();

        let mut txs = txs.into_iter();

        let root_tx = match txs.nth(0) {
            None => bail!(TolstoyError::UnexpectedState(format!("Could not get root tx"))),
            Some(t) => t?
        };

        match txs.last() {
            None => Ok((root_tx, root_tx)),
            Some(t) => Ok((root_tx, t?))
        }
    }

    pub fn local_txs(db_tx: &rusqlite::Transaction, after: Option<Entid>) -> Result<Vec<Entid>> {
        let after_clause = match after {
            Some(t) => format!("WHERE timeline = 0 AND tx > {}", t),
            None => format!("WHERE timeline = 0")
        };
        let mut stmt: ::rusqlite::Statement = db_tx.prepare(&format!("SELECT tx FROM timelined_transactions {} GROUP BY tx ORDER BY tx", after_clause))?;
        let txs: Vec<_> = stmt.query_and_then(&[], |row| -> Result<Entid> {
            Ok(row.get_checked(0)?)
        })?.collect();

        let mut all = Vec::with_capacity(txs.len());
        for tx in txs {
            all.push(tx?);
        }

        Ok(all)
    }

    pub fn is_tx_empty(db_tx: &rusqlite::Transaction, tx_id: Entid) -> Result<bool> {
        let count = db_tx.query_row("SELECT count(rowid) FROM timelined_transactions WHERE timeline = 0 AND tx = ? AND e != ?", &[&tx_id, &tx_id], |row| -> Result<i64> {
            Ok(row.get_checked(0)?)
        })?;
        Ok(count? == 0)
    }

    pub fn has_entity_assertions_in_tx(db_tx: &rusqlite::Transaction, e: Entid, tx_id: Entid) -> Result<bool> {
        let count = db_tx.query_row("SELECT count(rowid) FROM timelined_transactions WHERE timeline = 0 AND tx = ? AND e = ?", &[&tx_id, &e], |row| -> Result<i64> {
            Ok(row.get_checked(0)?)
        })?;
        Ok(count? > 0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use mentat_db::db;

    #[test]
    fn test_get_remote_head_default() {
        let mut conn = schema::tests::setup_conn_bare();
        let tx = schema::tests::setup_tx(&mut conn);
        assert_eq!(Uuid::nil(), SyncMetadata::remote_head(&tx).expect("fetch succeeded"));
    }

    #[test]
    fn test_set_and_get_remote_head() {
        let mut conn = schema::tests::setup_conn_bare();
        let tx = schema::tests::setup_tx(&mut conn);
        let uuid = Uuid::new_v4();
        SyncMetadata::set_remote_head(&tx, &uuid).expect("update succeeded");
        assert_eq!(uuid, SyncMetadata::remote_head(&tx).expect("fetch succeeded"));
    }

    #[test]
    fn test_root_and_head_tx() {
        let mut conn = schema::tests::setup_conn_bare();
        db::ensure_current_version(&mut conn).expect("mentat db init");
        let db_tx = conn.transaction().expect("transaction");

        let (root_tx, last_tx) = SyncMetadata::root_and_head_tx(&db_tx).expect("last tx");
        assert_eq!(268435456, root_tx);
        assert_eq!(268435456, last_tx);

        // These are determenistic, but brittle.
        // Inserting a tx 268435457 at time 1529971773701734
        // 268435457|3|1529971773701734|268435457|1|4
        // ... which defines entity ':person/name'...
        // 65536|1|:person/name|268435457|1|13
        // ... which has valueType of string
        // 65536|7|27|268435457|1|0
        // ... which is unique...
        // 65536|9|36|268435457|1|0
        // ... ident
        // 65536|11|1|268435457|1|1

        // last attribute is the timeline (0).

        db_tx.execute("INSERT INTO timelined_transactions VALUES (?, ?, ?, ?, ?, ?, ?)", &[&268435457, &3, &1529971773701734_i64, &268435457, &1, &4, &0]).expect("inserted");
        db_tx.execute("INSERT INTO timelined_transactions VALUES (?, ?, ?, ?, ?, ?, ?)", &[&65536, &1, &":person/name", &268435457, &1, &13, &0]).expect("inserted");
        db_tx.execute("INSERT INTO timelined_transactions VALUES (?, ?, ?, ?, ?, ?, ?)", &[&65536, &7, &27, &268435457, &1, &0, &0]).expect("inserted");
        db_tx.execute("INSERT INTO timelined_transactions VALUES (?, ?, ?, ?, ?, ?, ?)", &[&65536, &9, &36, &268435457, &1, &0, &0]).expect("inserted");
        db_tx.execute("INSERT INTO timelined_transactions VALUES (?, ?, ?, ?, ?, ?, ?)", &[&65536, &11, &1, &268435457, &1, &1, &0]).expect("inserted");

        let (root_tx, last_tx) = SyncMetadata::root_and_head_tx(&db_tx).expect("last tx");
        assert_eq!(268435456, root_tx);
        assert_eq!(268435457, last_tx);
    }
}
