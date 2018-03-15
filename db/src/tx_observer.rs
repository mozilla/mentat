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

use smallvec::{
    SmallVec,
};

use types::{
    AttributeSet,
    TxReport,
};

pub struct TxObserver {
    notify_fn: Arc<Box<Fn(&str, SmallVec<[&TxReport; 4]>) + Send + Sync>>,
    attributes: AttributeSet,
}

impl TxObserver {
    pub fn new<F>(attributes: AttributeSet, notify_fn: F) -> TxObserver where F: Fn(&str, SmallVec<[&TxReport; 4]>) + 'static + Send + Sync {
        TxObserver {
            notify_fn: Arc::new(Box::new(notify_fn)),
            attributes,
        }
    }

    pub fn applicable_reports<'r>(&self, reports: &'r SmallVec<[TxReport; 4]>) -> SmallVec<[&'r TxReport; 4]> {
        reports.into_iter().filter_map(|report| {
            self.attributes.intersection(&report.changeset)
                           .next()
                           .and_then(|_| Some(report))
        }).collect()
    }

    fn notify(&self, key: &str, reports: SmallVec<[&TxReport; 4]>) {
        (*self.notify_fn)(key, reports);
    }
}

pub trait Command {
    fn execute(&mut self);
}

pub struct TxCommand {
    reports: SmallVec<[TxReport; 4]>,
    observers: Weak<IndexMap<String, Arc<TxObserver>>>,
}

impl TxCommand {
    fn new(observers: &Arc<IndexMap<String, Arc<TxObserver>>>, reports: SmallVec<[TxReport; 4]>) -> Self {
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
    in_progress_count: i32,
}

impl TxObservationService {
    pub fn new() -> Self {
        TxObservationService {
            observers: Arc::new(IndexMap::new()),
            executor: None,
            in_progress_count: 0,
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

    pub fn transaction_did_start(&mut self) {
        self.in_progress_count += 1;
    }

    pub fn transaction_did_commit(&mut self, reports: SmallVec<[TxReport; 4]>) {
        {
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

        self.in_progress_count -= 1;

        if self.in_progress_count == 0 {
            self.executor = None;
        }
    }
}

struct CommandExecutor {
    reciever: Receiver<Box<Command + Send>>,
}

impl CommandExecutor {
    fn new(rx: Receiver<Box<Command + Send>>) -> Self {
        CommandExecutor {
            reciever: rx,
        }
    }

    fn main(&mut self) {
        loop {
            match self.reciever.recv() {
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
