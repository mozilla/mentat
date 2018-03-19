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
    BTreeMap,
};
use std::sync::{
    Arc,
    Mutex,
    Weak,
};
use std::sync::mpsc::{
    channel,
    Receiver,
    RecvError,
    Sender,
};
use std::thread;

use indexmap::{
    IndexMap,
};

use mentat_core::{
    Entid,
    Schema,
    TypedValue,
};
use mentat_tx::entities::{
    OpType,
};

use errors::{
    Result,
};
use types::{
    AccumulatedTxids,
    AttributeSet,
};
use watcher::TransactWatcher;

pub struct TxObserver {
    notify_fn: Arc<Box<Fn(&str, BTreeMap<&Entid, &AttributeSet>) + Send + Sync>>,
    attributes: AttributeSet,
}

impl TxObserver {
    pub fn new<F>(attributes: AttributeSet, notify_fn: F) -> TxObserver where F: Fn(&str, BTreeMap<&Entid, &AttributeSet>) + 'static + Send + Sync {
        TxObserver {
            notify_fn: Arc::new(Box::new(notify_fn)),
            attributes,
        }
    }

    pub fn applicable_reports<'r>(&self, reports: &'r BTreeMap<Entid, AttributeSet>) -> BTreeMap<&'r Entid, &'r AttributeSet> {
        reports.into_iter().filter_map(|(txid, changeset)| {
            self.attributes.intersection(changeset)
                           .next()
                           .and_then(|_| Some((txid, changeset)))
        }).fold(BTreeMap::new(), |mut map, (txid, changeset)| {
            map.insert(txid, changeset);
            map
        })
    }

    fn notify(&self, key: &str, reports: BTreeMap<&Entid, &AttributeSet>) {
        (*self.notify_fn)(key, reports);
    }
}

pub trait Command {
    fn execute(&mut self);
}

pub struct TxCommand {
    reports: BTreeMap<Entid, AttributeSet>,
    observers: Weak<IndexMap<String, Arc<TxObserver>>>,
}

impl TxCommand {
    fn new(observers: &Arc<IndexMap<String, Arc<TxObserver>>>, reports: BTreeMap<Entid, AttributeSet>) -> Self {
        TxCommand {
            reports,
            observers: Arc::downgrade(observers),
        }
    }
}

impl Command for TxCommand {
    fn execute(&mut self) {
        self.observers.upgrade().map(|observers| {
            for (key, observer) in observers.iter() {
                let applicable_reports = observer.applicable_reports(&self.reports);
                if !applicable_reports.is_empty() {
                    observer.notify(&key, applicable_reports);
                }
            }
        });
    }
}

pub struct TxObservationService {
    observers: Arc<IndexMap<String, Arc<TxObserver>>>,
    transactions: BTreeMap<Entid, AttributeSet>,
    executor: Option<Sender<Box<Command + Send>>>,
}

impl TxObservationService {
    pub fn new() -> Self {
        TxObservationService {
            observers: Arc::new(IndexMap::new()),
            transactions: Default::default(),
            executor: None,
        }
    }

    // For testing purposes
    pub fn is_registered(&self, key: &String) -> bool {
        self.observers.contains_key(key)
    }

    pub fn register(&mut self, key: String, observer: Arc<TxObserver>) {
        Arc::make_mut(&mut self.observers).insert(key, observer);
    }

    pub fn deregister(&mut self, key: &String) {
        Arc::make_mut(&mut self.observers).remove(key);
    }

    pub fn has_observers(&self) -> bool {
        !self.observers.is_empty()
    }

    pub fn add_transaction(&mut self, tx_id: Entid, attributes: AttributeSet) {
        self.transactions.insert(tx_id, attributes);
    }

    pub fn transaction_did_commit(&mut self, txids: &AccumulatedTxids) {
        // collect the changesets relating to this commit
        let reports: BTreeMap<Entid, AttributeSet> = txids.into_iter().filter_map(|tx_id| {
                                        self.transactions.remove(&tx_id).map_or(None, |changeset| Some((tx_id, changeset)))
                                        })
                                        .fold(BTreeMap::new(), |mut map, (tx_id, changeset)| {
                                            map.insert(*tx_id, changeset);
                                            map
                                        });

        let executor = self.executor.get_or_insert_with(||{
            let (tx, rx): (Sender<Box<Command + Send>>, Receiver<Box<Command + Send>>) = channel();
            let mut worker = CommandExecutor::new(rx);

            thread::spawn(move || {
                worker.main();
            });

            tx
        });

        let cmd = Box::new(TxCommand::new(&self.observers, reports));
        executor.send(cmd).unwrap();
    }
}

impl Drop for TxObservationService {
    fn drop(&mut self) {
        self.executor = None;
    }
}

pub struct InProgressObserverTransactWatcher<'a> {
    collected_datoms: AttributeSet,
    observer_service: &'a Mutex<TxObservationService>,
    active: bool
}

impl<'a> InProgressObserverTransactWatcher<'a> {
    pub fn new(observer_service: &'a Mutex<TxObservationService>) -> InProgressObserverTransactWatcher {
        let mut w = InProgressObserverTransactWatcher {
            collected_datoms: Default::default(),
            observer_service,
            active: true
        };

        w.active = observer_service.lock().unwrap().has_observers();
        w
    }
}

impl<'a> TransactWatcher for InProgressObserverTransactWatcher<'a> {
    type Result = ();

    fn tx_id(&mut self) -> Option<Entid> {
        None
    }

    fn datom(&mut self, _op: OpType, _e: Entid, a: Entid, _v: &TypedValue) {
        if !self.active {
            return
        }
        self.collected_datoms.insert(a);
    }

    fn done(&mut self, t: &Entid, _schema: &Schema) -> Result<Self::Result> {
        let collected_datoms = ::std::mem::replace(&mut self.collected_datoms, Default::default());
        self.observer_service.lock().unwrap().add_transaction(t.clone(), collected_datoms);
        Ok(())
    }
}

struct CommandExecutor {
    receiver: Receiver<Box<Command + Send>>,
}

impl CommandExecutor {
    fn new(rx: Receiver<Box<Command + Send>>) -> Self {
        CommandExecutor {
            receiver: rx,
        }
    }

    fn main(&mut self) {
        loop {
            match self.receiver.recv() {
                Err(RecvError) => {
                    eprintln!("Disconnected, terminating CommandExecutor");
                    return
                },

                Ok(mut cmd) => {
                    cmd.execute()
                },
            }
        }
    }
}
