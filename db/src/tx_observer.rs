// Copyright 2018 Mozilla
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

use std::sync::{
    Arc,
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

use core_traits::{
    Entid,
    TypedValue,
};

use mentat_core::{
    Schema,
};

use edn::entities::{
    OpType,
};

use errors::{
    Result,
};

use types::{
    AttributeSet,
};

use watcher::TransactWatcher;

pub struct TxObserver {
    notify_fn: Arc<Box<Fn(&str, IndexMap<&Entid, &AttributeSet>) + Send + Sync>>,
    attributes: AttributeSet,
}

impl TxObserver {
    pub fn new<F>(attributes: AttributeSet, notify_fn: F) -> TxObserver where F: Fn(&str, IndexMap<&Entid, &AttributeSet>) + 'static + Send + Sync {
        TxObserver {
            notify_fn: Arc::new(Box::new(notify_fn)),
            attributes,
        }
    }

    pub fn applicable_reports<'r>(&self, reports: &'r IndexMap<Entid, AttributeSet>) -> IndexMap<&'r Entid, &'r AttributeSet> {
        reports.into_iter()
               .filter(|&(_txid, attrs)| !self.attributes.is_disjoint(attrs))
               .collect()
    }

    fn notify(&self, key: &str, reports: IndexMap<&Entid, &AttributeSet>) {
        (*self.notify_fn)(key, reports);
    }
}

pub trait Command {
    fn execute(&mut self);
}

pub struct TxCommand {
    reports: IndexMap<Entid, AttributeSet>,
    observers: Weak<IndexMap<String, Arc<TxObserver>>>,
}

impl TxCommand {
    fn new(observers: &Arc<IndexMap<String, Arc<TxObserver>>>, reports: IndexMap<Entid, AttributeSet>) -> Self {
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
    executor: Option<Sender<Box<Command + Send>>>,
}

impl TxObservationService {
    pub fn new() -> Self {
        TxObservationService {
            observers: Arc::new(IndexMap::new()),
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

    pub fn in_progress_did_commit(&mut self, txes: IndexMap<Entid, AttributeSet>) {
        // Don't spawn a thread only to say nothing.
        if !self.has_observers() {
            return;
        }

        let executor = self.executor.get_or_insert_with(|| {
            let (tx, rx): (Sender<Box<Command + Send>>, Receiver<Box<Command + Send>>) = channel();
            let mut worker = CommandExecutor::new(rx);

            thread::spawn(move || {
                worker.main();
            });

            tx
        });

        let cmd = Box::new(TxCommand::new(&self.observers, txes));
        executor.send(cmd).unwrap();
    }
}

impl Drop for TxObservationService {
    fn drop(&mut self) {
        self.executor = None;
    }
}

pub struct InProgressObserverTransactWatcher {
    collected_attributes: AttributeSet,
    pub txes: IndexMap<Entid, AttributeSet>,
}

impl InProgressObserverTransactWatcher {
    pub fn new() -> InProgressObserverTransactWatcher {
        InProgressObserverTransactWatcher {
            collected_attributes: Default::default(),
            txes: Default::default(),
        }
    }
}

impl TransactWatcher for InProgressObserverTransactWatcher {
    fn datom(&mut self, _op: OpType, _e: Entid, a: Entid, _v: &TypedValue) {
        self.collected_attributes.insert(a);
    }

    fn done(&mut self, t: &Entid, _schema: &Schema) -> Result<()> {
        let collected_attributes = ::std::mem::replace(&mut self.collected_attributes, Default::default());
        self.txes.insert(*t, collected_attributes);
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
                    // "The recv operation can only fail if the sending half of a channel (or
                    // sync_channel) is disconnected, implying that no further messages will ever be
                    // received."
                    // No need to log here.
                    return
                },

                Ok(mut cmd) => {
                    cmd.execute()
                },
            }
        }
    }
}
