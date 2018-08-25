// Copyright 2018 Mozilla
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

use std::fmt;

use std::collections::HashSet;

use rusqlite;
use uuid::Uuid;

use core_traits::{
    Entid,
    KnownEntid,
    TypedValue,
};

use edn::{
    PlainSymbol,
};
use edn::entities::{
    TxFunction,
    EntityPlace,
    LookupRef,
};
use mentat_db::{
    CORE_SCHEMA_VERSION,
    timelines,
    debug,
    entids,
    PartitionMap,
};
use mentat_transaction::{
    InProgress,
    TermBuilder,
    Queryable,
};

use mentat_transaction::entity_builder::{
    BuildTerms,
};

use mentat_transaction::query::{
    QueryInputs,
    Variable,
};

use bootstrap::{
    BootstrapHelper,
};

use public_traits::errors::{
    Result,
};

use tolstoy_traits::errors::{
    TolstoyError,
};
use metadata::{
    PartitionsTable,
    SyncMetadata,
};
use schema::{
    ensure_current_version,
};
use tx_uploader::TxUploader;
use tx_processor::{
    Processor,
    TxReceiver,
};
use tx_mapper::{
    TxMapper,
};
use types::{
    LocalTx,
    Tx,
    TxPart,
    GlobalTransactionLog,
};

use logger::d;

pub struct Syncer {}

#[derive(Debug,PartialEq,Clone)]
pub enum SyncFollowup {
    None,
    FullSync,
}

impl fmt::Display for SyncFollowup {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            SyncFollowup::None => write!(f, "None"),
            SyncFollowup::FullSync => write!(f, "Full sync"),
        }
    }
}

#[derive(Debug,PartialEq,Clone)]
pub enum SyncReport {
    IncompatibleRemoteBootstrap(i64, i64),
    BadRemoteState(String),
    NoChanges,
    RemoteFastForward,
    LocalFastForward,
    Merge(SyncFollowup),
}

pub enum SyncResult {
    Atomic(SyncReport),
    NonAtomic(Vec<SyncReport>),
}

impl fmt::Display for SyncReport {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            SyncReport::IncompatibleRemoteBootstrap(local, remote) => {
                write!(f, "Incompatible remote bootstrap transaction version. Local: {}, remote: {}.", local, remote)
            },
            SyncReport::BadRemoteState(err) => {
                write!(f, "Bad remote state: {}", err)
            },
            SyncReport::NoChanges => {
                write!(f, "Neither local nor remote have any new changes")
            },
            SyncReport::RemoteFastForward => {
                write!(f, "Fast-forwarded remote")
            },
            SyncReport::LocalFastForward => {
                write!(f, "Fast-forwarded local")
            },
            SyncReport::Merge(follow_up) => {
                write!(f, "Merged local and remote, requesting a follow-up: {}", follow_up)
            }
        }
    }
}

impl fmt::Display for SyncResult {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            SyncResult::Atomic(report) => write!(f, "Single atomic sync: {}", report),
            SyncResult::NonAtomic(reports) => {
                writeln!(f, "Series of atomic syncs ({})", reports.len())?;
                for report in reports {
                    writeln!(f, "{}", report)?;
                }
                writeln!(f, "\\o/")
            }
        }
    }
}

#[derive(Debug,PartialEq)]
enum SyncAction {
    NoOp,
    // TODO this is the same as remote fast-forward from local root.
    // It's currently distinguished from remote fast-forward for a more
    // path through the "first-sync against non-empty remote" flow.
    PopulateRemote,
    RemoteFastForward,
    LocalFastForward,
    // Generic name since we might merge, or rebase, or do something else.
    CombineChanges,
}

/// Represents remote state relative to previous sync.
/// On first sync, it's always "Changed" unless remote is "Empty".
pub enum RemoteDataState {
    Empty,
    Changed,
    Unchanged,
}

/// Remote state is expressed in terms of what "remote head" actually is,
/// and what we think it is.
impl<'a> From<(&'a Uuid, &'a Uuid)> for RemoteDataState {
    fn from((known_remote_head, actual_remote_head): (&Uuid, &Uuid)) -> RemoteDataState {
        if *actual_remote_head == Uuid::nil() {
            RemoteDataState::Empty
        } else if actual_remote_head != known_remote_head {
            RemoteDataState::Changed
        } else {
            RemoteDataState::Unchanged
        }
    }
}

/// Represents local state relative to previous sync.
/// On first sync it's always "Changed".
/// Local client can't be empty: there's always at least a bootstrap transaction.
pub enum LocalDataState {
    Changed,
    Unchanged,
}

/// Local state is expressed in terms of presence of a "mapping" for the local head.
/// Presence of a mapping means that we've uploaded our local head,
/// indicating that there's no local changes.
/// Absence of a mapping indicates that local head hasn't been uploaded
/// and that we have local changes.
impl From<Option<Uuid>> for LocalDataState {
    fn from(mapped_local_head: Option<Uuid>) -> LocalDataState {
        match mapped_local_head {
            Some(_) => LocalDataState::Unchanged,
            None => LocalDataState::Changed
        }
    }
}

// TODO rename this thing.
pub struct LocalTxSet {
    txs: Vec<LocalTx>,
}

impl LocalTxSet {
    pub fn new() -> LocalTxSet {
        LocalTxSet {
            txs: vec![]
        }
    }
}

impl TxReceiver<Vec<LocalTx>> for LocalTxSet {
    fn tx<T>(&mut self, tx_id: Entid, datoms: &mut T) -> Result<()>
    where T: Iterator<Item=TxPart> {
        self.txs.push(LocalTx {
            tx: tx_id,
            parts: datoms.collect()
        });
        Ok(())
    }

    fn done(self) -> Vec<LocalTx> {
        self.txs
    }
}

impl Syncer {
    /// Produces a SyncAction based on local and remote states.
    fn what_do(remote_state: RemoteDataState, local_state: LocalDataState) -> SyncAction {
        match remote_state {
            RemoteDataState::Empty => {
                SyncAction::PopulateRemote
            },

            RemoteDataState::Changed => {
                match local_state {
                    LocalDataState::Changed => {
                        SyncAction::CombineChanges
                    },

                    LocalDataState::Unchanged => {
                        SyncAction::LocalFastForward
                    },
                }
            },

            RemoteDataState::Unchanged => {
                match local_state {
                    LocalDataState::Changed => {
                        SyncAction::RemoteFastForward
                    },

                    LocalDataState::Unchanged => {
                        SyncAction::NoOp
                    },
                }
            },
        }
    }

    /// Upload local txs: (from_tx, HEAD]. Remote head is necessary here because we need to specify
    /// "parent" for each transaction we'll upload; remote head will be first transaction's parent.
    fn fast_forward_remote<R>(db_tx: &mut rusqlite::Transaction, from_tx: Option<Entid>, remote_client: &mut R, remote_head: &Uuid) -> Result<()>
        where R: GlobalTransactionLog {

        // TODO consider moving head manipulations into uploader?

        let report;

        // Scope to avoid double-borrowing mutable remote_client.
        {
            // Prepare an uploader.
            let uploader = TxUploader::new(
                remote_client,
                remote_head,
                SyncMetadata::get_partitions(db_tx, PartitionsTable::Tolstoy)?
            );
            // Walk the local transactions in the database and upload them.
            report = Processor::process(db_tx, from_tx, uploader)?;
        }

        if let Some(last_tx_uploaded) = report.head {
            // Upload remote head.
            remote_client.set_head(&last_tx_uploaded)?;

            // On success:
            // - persist local mappings from the receiver
            // - update our local "remote head".
            TxMapper::set_lg_mappings(
                db_tx,
                report.temp_uuids.iter().map(|v| (*v.0, v.1).into()).collect()
            )?;

            SyncMetadata::set_remote_head(db_tx, &last_tx_uploaded)?;
        }

        Ok(())
    }

    fn local_tx_for_uuid(db_tx: &rusqlite::Transaction, uuid: &Uuid) -> Result<Entid> {
        match TxMapper::get_tx_for_uuid(db_tx, uuid)? {
            Some(t) => Ok(t),
            None => bail!(TolstoyError::TxIncorrectlyMapped(0))
        }
    }

    fn remote_parts_to_builder(builder: &mut TermBuilder, parts: Vec<TxPart>) -> Result<()> {
        for part in parts {
            let e: EntityPlace<TypedValue>;
            let a = KnownEntid(part.a);
            let v = part.v;

            // Instead of providing a 'txInstant' datom directly, we map it
            // into a (transaction-tx) style assertion.
            // Transactor knows how to pick out a txInstant value out of these
            // assertions and use that value for the generated transaction's txInstant.
            if part.a == entids::DB_TX_INSTANT {
                e = EntityPlace::TxFunction(TxFunction { op: PlainSymbol("transaction-tx".to_string()) } ).into();
            } else {
                e = KnownEntid(part.e).into();
            }

            if part.added {
                builder.add(e, a, v)?;
            } else {
                builder.retract(e, a, v)?;
            }
        }

        Ok(())
    }

    /// In context of a "transaction to be applied", a PartitionMap supplied here
    /// represents what a PartitionMap will be once this transaction is applied.
    /// This works well for regular assertions: entids are supplied, and we need
    /// them to be allocated in the user partition space.
    /// However, we need to decrement 'tx' partition's index, so that the transactor's
    /// allocated tx will match what came off the wire.
    /// N.B.: this depends on absence of holes in the 'tx' partition!
    fn rewind_tx_partition_by_one(partition_map: &mut PartitionMap) -> Result<()> {
        if let Some(tx_part) = partition_map.get_mut(":db.part/tx") {
            assert_eq!(false, tx_part.allow_excision); // Sanity check.

            let next_entid = tx_part.next_entid() - 1;
            tx_part.set_next_entid(next_entid);
            Ok(())
        } else {
            bail!(TolstoyError::BadRemoteState("Missing tx partition in an incoming transaction".to_string()));
        }
    }

    fn fast_forward_local<'a, 'c>(in_progress: &mut InProgress<'a, 'c>, txs: Vec<Tx>) -> Result<SyncReport> {
        let mut last_tx = None;

        for tx in txs {
            let mut builder = TermBuilder::new();

            // TODO both here and in the merge scenario we're doing the same thing with the partition maps
            // and with the txInstant datom rewriting.
            // Figure out how to combine these operations into a resuable primitive(s).
            // See notes in 'merge' for why we're doing this stuff.
            let mut partition_map = match tx.parts[0].partitions.clone() {
                Some(parts) => parts,
                None => return Ok(SyncReport::BadRemoteState("Missing partition map in incoming transaction".to_string()))
            };

            // Make space in the provided tx partition for the transaction we're about to create.
            // See function's notes for details.
            Syncer::rewind_tx_partition_by_one(&mut partition_map)?;
            Syncer::remote_parts_to_builder(&mut builder, tx.parts)?;

            // Allocate space for the incoming entids.
            in_progress.partition_map = partition_map;
            let report = in_progress.transact_builder(builder)?;
            last_tx = Some((report.tx_id, tx.tx.clone()));
        }

        // We've just transacted a new tx, and generated a new tx entid.  Map it to the corresponding
        // incoming tx uuid, advance our "locally known remote head".
        if let Some((entid, uuid)) = last_tx {
            SyncMetadata::set_remote_head_and_map(&mut in_progress.transaction, (entid, &uuid).into())?;
        }

        Ok(SyncReport::LocalFastForward)
    }

    fn merge(ip: &mut InProgress, incoming_txs: Vec<Tx>, mut local_txs_to_merge: Vec<LocalTx>) -> Result<SyncReport> {
        d(&format!("Rewinding local transactions."));

        // 1) Rewind local to shared root.
        local_txs_to_merge.sort(); // TODO sort at the interface level?

        let (new_schema, new_partition_map) = timelines::move_from_main_timeline(
            &ip.transaction,
            &ip.schema,
            ip.partition_map.clone(),
            local_txs_to_merge[0].tx..,
            // A poor man's parent reference. This might be brittle, although
            // excisions are prohibited in the 'tx' partition, so this should hold...
            local_txs_to_merge[0].tx - 1
        )?;
        match new_schema {
            Some(schema) => ip.schema = schema,
            None => ()
        };
        ip.partition_map = new_partition_map;

        // 2) Transact incoming.
        // 2.1) Prepare remote tx tuples (TermBuilder, PartitionMap, Uuid), which represent
        // a remote transaction, its global identifier and partitions after it's applied.
        d(&format!("Transacting incoming..."));
        let mut builders = vec![];
        for remote_tx in incoming_txs {
            let mut builder = TermBuilder::new();

            let partition_map = match remote_tx.parts[0].partitions.clone() {
                Some(parts) => parts,
                None => return Ok(SyncReport::BadRemoteState("Missing partition map in incoming transaction".to_string()))
            };

            Syncer::remote_parts_to_builder(&mut builder, remote_tx.parts)?;

            builders.push((builder, partition_map, remote_tx.tx));
        }

        let mut remote_report = None;
        for (builder, mut partition_map, remote_tx) in builders {
            // Make space in the provided tx partition for the transaction we're about to create.
            // See function's notes for details.
            Syncer::rewind_tx_partition_by_one(&mut partition_map)?;

            // This allocates our incoming entids in each builder,
            // letting us just use KnownEntid in the builders.
            ip.partition_map = partition_map;
            remote_report = Some((ip.transact_builder(builder)?.tx_id, remote_tx));
        }

        d(&format!("Transacting local on top of incoming..."));
        // 3) Rebase local transactions on top of remote.
        let mut clean_rebase = true;
        for local_tx in local_txs_to_merge {
            let mut builder = TermBuilder::new();

            // This is the beginnings of entity merging.

            // An entid might be already known to the Schema, or it
            // might be allocated in this transaction.
            // In the former case, refer to it verbatim.
            // In the latter case, rewrite it as a tempid, and let the transactor allocate it.
            let mut entids_that_will_allocate = HashSet::new();

            // We currently support "strict schema merging": we'll smush attribute definitions,
            // but only if they're the same.
            // e.g. prohibited would be defining different cardinality for the same attribute.
            // Defining new attributes is allowed if:
            // - attribute is defined either on local or remote,
            // - attribute is defined on both local and remote in the same way.
            // Modifying an attribute is currently not supported (requires higher order schema migrations).
            // Note that "same" local and remote attributes might have different entids in the
            // two sets of transactions.

            // Set of entities that may alter "installed" attribute.
            // Since this is a rebase of local on top of remote, an "installed"
            // attribute might be one that was present in the root, or one that was
            // defined by remote.
            let mut might_alter_installed_attributes = HashSet::new();

            // Set of entities that describe a new attribute, not present in the root
            // or on the remote.
            let mut will_not_alter_installed_attribute = HashSet::new();

            // Note that at this point, remote and local have flipped - we're transacting
            // local on top of incoming (which are already in the schema).

            // Go through local datoms, and classify any schema-altering entids into
            // one of the two sets above.
            for part in &local_tx.parts {
                // If we have an ident definition locally, check if remote
                // already defined this ident. If it did, we'll need to ensure
                // both local and remote are defining it in the same way.
                if part.a == entids::DB_IDENT {
                    match part.v {
                        TypedValue::Keyword(ref local_kw) => {
                            // Remote did not define this ident. Make a note of it,
                            // so that we'll know to ignore its attribute datoms.
                            if !ip.schema.ident_map.contains_key(local_kw) {
                                will_not_alter_installed_attribute.insert(part.e);

                            // Otherwise, we'll need to ensure we have the same attribute definition
                            // for it.
                            } else {
                                might_alter_installed_attributes.insert(part.e);
                            }
                        },
                        _ => panic!("programming error: wrong value type for a local ident")
                    }
                } else if entids::is_a_schema_attribute(part.a) && !will_not_alter_installed_attribute.contains(&part.e) {
                    might_alter_installed_attributes.insert(part.e);
                }
            }

            for part in &local_tx.parts {
                match part.a {
                    // We'll be ignoring this datom later on (to be generated by the transactor).
                    // During a merge we're concerned with entities in the "user" partition,
                    // while this falls into the "tx" partition.
                    // We have preserved the original txInstant value on the alternate timeline.
                    entids::DB_TX_INSTANT => continue,

                    // 'e's will be replaced with tempids, letting transactor handle everything.
                    // Non-unique entities are "duplicated". Unique entities are upserted.
                    _ => {
                        // Retractions never allocated tempids in the transactor.
                        if part.added {
                            entids_that_will_allocate.insert(part.e);
                        }
                    },
                }
            }

            // :db/ident is a db.unique/identity attribute, which means transactor will upsert
            // attribute assertions. E.g. if a new attribute was defined on local and not on remote,
            // it will be inserted. If both local and remote defined the same attribute
            // with different entids, we'll converge and use remote's entid.

            // Same follows for other types of db.unique/identity attributes.
            // If user-defined attribute is db.unique/identity, we'll "smush" local and remote
            // assertions against it.
            // For example, {:p/name "Grisha"} assertion on local and
            // {:p/name "Grisha"} assertion on remote will result in a single entity.

            // If user-defined attribute is not unique, however, no smushing will be performed.
            // The above example will result in two entities.

            for part in local_tx.parts {
                // Skip the "tx instant" datom: it will be generated by our transactor.
                // We don't care about preserving exact state of these datoms: they're already
                // stashed away on the timeline we've created above.
                if part.a == entids::DB_TX_INSTANT {
                    continue;
                }

                let e: EntityPlace<TypedValue>;
                let a = KnownEntid(part.a);
                let v = part.v;

                // Rewrite entids if they will allocate (see entity merging notes above).
                if entids_that_will_allocate.contains(&part.e) {
                    e = builder.named_tempid(format!("{}", part.e)).into();
                // Otherwise, refer to existing entities.
                } else {
                    e = KnownEntid(part.e).into();
                }

                // TODO we need to do the same rewriting for part.v if it's a Ref.

                // N.b.: attribute can't refer to an unallocated entity, so it's always a KnownEntid.
                // To illustrate, this is not a valid transaction, and will fail ("no entid found for ident: :person/name"):
                // [
                //  {:db/ident :person/name :db/valueType :db.type/string :db/cardinality :db.cardinality/one}
                //  {:person/name "Grisha"}
                // ]
                // One would need to split that transaction into two,
                // at which point :person/name will refer to an allocated entity.

                match part.added {
                    true => builder.add(e, a, v)?,
                    false => {
                        if entids_that_will_allocate.contains(&part.e) {
                            builder.retract(e, a, v)?;
                            continue;
                        }

                        // TODO handle tempids in ValuePlace, as well.

                        // Retractions with non-upserting tempids are not currently supported.
                        // We work around this by using a lookup-ref instead of the entity tempid.
                        // However:
                        // - lookup-ref can only be used for attributes which are :db/unique,
                        // - a lookup-ref must resolve. If it doesn't, our transaction will fail.
                        // And so:
                        // - we skip retractions of non-unique attributes,
                        // - we "pre-run" a lookup-ref to ensure it will resolve,
                        //   and skip the retraction otherwise.
                        match ip.schema.attribute_map.get(&part.a) {
                            Some(attributes) => {
                                // A lookup-ref using a non-unique attribute will fail.
                                // Skip this retraction, since we can't make sense of it.
                                if attributes.unique.is_none() {
                                    continue;
                                }
                            },
                            None => panic!("programming error: missing attribute map for a known attribute")
                        }

                        // TODO prepare a query and re-use it for all retractions of this type
                        let pre_lookup = ip.q_once(
                            "[:find ?e . :in ?a ?v :where [?e ?a ?v]]",
                            QueryInputs::with_value_sequence(
                                vec![
                                    (Variable::from_valid_name("?a"), a.into()),
                                    (Variable::from_valid_name("?v"), v.clone()),
                                ]
                            )
                        )?;

                        if pre_lookup.is_empty() {
                            continue;
                        }

                        // TODO just use the value from the query instead of doing _another_ lookup-ref!

                        builder.retract(
                            EntityPlace::LookupRef(LookupRef {a: a.into(), v: v.clone()}), a, v
                        )?;
                    }
                }
            }

            // After all these checks, our builder might be empty: short-circuit.
            if builder.is_empty() {
                continue;
            }

            d(&format!("Savepoint before transacting a local tx..."));
            ip.savepoint("speculative_local")?;

            d(&format!("Transacting builder filled with local txs... {:?}", builder));

            let report = ip.transact_builder(builder)?;

            // Let's check that we didn't modify any schema attributes.
            // Our current attribute map in the schema isn't rich enough to allow
            // for this check: it's missing a notion of "attribute absence" - we can't
            // distinguish between a missing attribute and a default value.
            // Instead, we simply query the database, checking if transaction produced
            // any schema-altering datoms.
            for e in might_alter_installed_attributes.iter() {
                match report.tempids.get(&format!("{}", e)) {
                    Some(resolved_e) => {
                        if SyncMetadata::has_entity_assertions_in_tx(&ip.transaction, *resolved_e, report.tx_id)? {
                            bail!(TolstoyError::NotYetImplemented("Can't sync with schema alterations yet.".to_string()));
                        }
                    },
                    None => ()
                }
            }

            if !SyncMetadata::is_tx_empty(&ip.transaction, report.tx_id)? {
                d(&format!("tx {} is not a no-op", report.tx_id));
                clean_rebase = false;
                ip.release_savepoint("speculative_local")?;
            } else {
                d(&format!("Applied tx {} as a no-op. Rolling back the savepoint (empty tx clean-up).", report.tx_id));
                ip.rollback_savepoint("speculative_local")?;
            }
        }

        // TODO
        // At this point, we've rebased local transactions on top of remote.
        // This would be a good point to create a "merge commit" and upload our loosing timeline.

        // Since we don't upload during a merge (instead, we request a follow-up sync),
        // set the locally known remote HEAD to what we received from the 'remote'.
        if let Some((entid, uuid)) = remote_report {
            SyncMetadata::set_remote_head_and_map(&mut ip.transaction, (entid, &uuid).into())?;
        }

        // If necessary, request a full sync as a follow-up to fast-forward remote.
        if clean_rebase {
            Ok(SyncReport::Merge(SyncFollowup::None))
        } else {
            Ok(SyncReport::Merge(SyncFollowup::FullSync))
        }
    }

    fn first_sync_against_non_empty<R>(ip: &mut InProgress, remote_client: &R, local_metadata: &SyncMetadata) -> Result<SyncReport>
        where R: GlobalTransactionLog {

        d(&format!("remote non-empty on first sync, adopting remote state."));

        // 1) Download remote transactions.
        let incoming_txs = remote_client.transactions_after(&Uuid::nil())?;
        if incoming_txs.len() == 0 {
            return Ok(SyncReport::BadRemoteState("Remote specified non-root HEAD but gave no transactions".to_string()));
        }

        // 2) Process remote bootstrap.
        let remote_bootstrap = &incoming_txs[0];
        let local_bootstrap = local_metadata.root;
        let bootstrap_helper = BootstrapHelper::new(remote_bootstrap);

        if !bootstrap_helper.is_compatible()? {
            return Ok(SyncReport::IncompatibleRemoteBootstrap(CORE_SCHEMA_VERSION as i64, bootstrap_helper.core_schema_version()?));
        }

        d(&format!("mapping incoming bootstrap tx uuid to local bootstrap entid: {} -> {}", remote_bootstrap.tx, local_bootstrap));

        // Map incoming bootstrap tx uuid to local bootstrap entid.
        // If there's more work to do, we'll move the head again.
        SyncMetadata::set_remote_head_and_map(&mut ip.transaction, (local_bootstrap, &remote_bootstrap.tx).into())?;

        // 3) Determine new local and remote data states, now that bootstrap has been dealt with.
        let remote_state = if incoming_txs.len() > 1 {
            RemoteDataState::Changed
        } else {
            RemoteDataState::Unchanged
        };

        let local_state = if local_metadata.root != local_metadata.head {
            LocalDataState::Changed
        } else {
            LocalDataState::Unchanged
        };

        // 4) The rest of this flow isn't that special anymore.
        // Since we've "merged" with the remote bootstrap, the "no-op" and
        // "local fast-forward" cases are reported as merges.
        match Syncer::what_do(remote_state, local_state) {
            SyncAction::NoOp => {
                Ok(SyncReport::Merge(SyncFollowup::None))
            },

            SyncAction::PopulateRemote => {
                // This is a programming error.
                bail!(TolstoyError::UnexpectedState(format!("Remote state can't be empty on first sync against non-empty remote")))
            },

            SyncAction::RemoteFastForward => {
                bail!(TolstoyError::NotYetImplemented(format!("TODO fast-forward remote on first sync when remote is just bootstrap and local has more")))
            },

            SyncAction::LocalFastForward => {
                Syncer::fast_forward_local(ip, incoming_txs[1 ..].to_vec())?;
                Ok(SyncReport::Merge(SyncFollowup::None))
            },

            SyncAction::CombineChanges => {
                let local_txs = Processor::process(
                    &mut ip.transaction, Some(local_metadata.root), LocalTxSet::new())?;
                Syncer::merge(
                    ip,
                    incoming_txs[1 ..].to_vec(),
                    local_txs
                )
            }
        }
    }

    pub fn sync<R>(ip: &mut InProgress, remote_client: &mut R) -> Result<SyncReport>
        where R: GlobalTransactionLog {

        d(&format!("sync flowing"));

        ensure_current_version(&mut ip.transaction)?;

        let remote_head = remote_client.head()?;
        d(&format!("remote head {:?}", remote_head));

        let locally_known_remote_head = SyncMetadata::remote_head(&mut ip.transaction)?;
        d(&format!("local head {:?}", locally_known_remote_head));

        let (root, head) = SyncMetadata::root_and_head_tx(&mut ip.transaction)?;
        let local_metadata = SyncMetadata::new(root, head);

        // impl From ... vs ::new() calls to constuct "state" objects?
        let local_state = TxMapper::get(&mut ip.transaction, local_metadata.head)?.into();
        let remote_state = (&locally_known_remote_head, &remote_head).into();

        // Currently, first sync against a non-empty remote is special.
        if locally_known_remote_head == Uuid::nil() && remote_head != Uuid::nil() {
            return Syncer::first_sync_against_non_empty(ip, remote_client, &local_metadata);
        }

        match Syncer::what_do(remote_state, local_state) {
            SyncAction::NoOp => {
                d(&format!("local HEAD did not move. Nothing to do!"));
                Ok(SyncReport::NoChanges)
            },

            SyncAction::PopulateRemote => {
                d(&format!("empty remote!"));
                Syncer::fast_forward_remote(&mut ip.transaction, None, remote_client, &remote_head)?;
                Ok(SyncReport::RemoteFastForward)
            },

            SyncAction::RemoteFastForward => {
                d(&format!("local HEAD moved."));
                let upload_from_tx = Syncer::local_tx_for_uuid(
                    &mut ip.transaction, &locally_known_remote_head
                )?;

                d(&format!("Fast-forwarding the remote."));

                // TODO it's possible that we've successfully advanced remote head previously,
                // but failed to advance our own local head. If that's the case, and we can recognize it,
                // our sync becomes just bumping our local head. AFAICT below would currently fail.
                Syncer::fast_forward_remote(
                    &mut ip.transaction, Some(upload_from_tx), remote_client, &remote_head
                )?;
                Ok(SyncReport::RemoteFastForward)
            },

            SyncAction::LocalFastForward => {
                d(&format!("fast-forwarding local store."));
                Syncer::fast_forward_local(
                    ip,
                    remote_client.transactions_after(&locally_known_remote_head)?
                )?;
                Ok(SyncReport::LocalFastForward)
            },

            SyncAction::CombineChanges => {
                d(&format!("combining changes from local and remote stores."));
                // Get the starting point for out local set of txs to merge.
                let combine_local_from_tx = Syncer::local_tx_for_uuid(
                    &mut ip.transaction, &locally_known_remote_head
                )?;
                let local_txs = Processor::process(
                    &mut ip.transaction,
                    Some(combine_local_from_tx),
                    LocalTxSet::new()
                )?;
                // Merge!
                Syncer::merge(
                    ip,
                    // Remote txs to merge...
                    remote_client.transactions_after(&locally_known_remote_head)?,
                    // ... with the local txs.
                    local_txs
                )
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_what_do() {
        assert_eq!(SyncAction::PopulateRemote, Syncer::what_do(RemoteDataState::Empty, LocalDataState::Unchanged));
        assert_eq!(SyncAction::PopulateRemote, Syncer::what_do(RemoteDataState::Empty, LocalDataState::Changed));

        assert_eq!(SyncAction::NoOp,              Syncer::what_do(RemoteDataState::Unchanged, LocalDataState::Unchanged));
        assert_eq!(SyncAction::RemoteFastForward, Syncer::what_do(RemoteDataState::Unchanged, LocalDataState::Changed));

        assert_eq!(SyncAction::LocalFastForward, Syncer::what_do(RemoteDataState::Changed, LocalDataState::Unchanged));
        assert_eq!(SyncAction::CombineChanges,   Syncer::what_do(RemoteDataState::Changed, LocalDataState::Changed));
    }
}
