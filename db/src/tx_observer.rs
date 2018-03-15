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
    VecDeque,
};
use std::sync::{
    Arc,
    Weak,
};
use std::thread;

use indexmap::{
    IndexMap,
};

use types::{
    AttributeSet,
    TxReport,
};

pub struct TxObserver {
    notify_fn: Arc<Box<Fn(String, Vec<&TxReport>) + Send + Sync>>,
    attributes: AttributeSet,
}

impl TxObserver {
    pub fn new<F>(attributes: AttributeSet, notify_fn: F) -> TxObserver where F: Fn(String, Vec<&TxReport>) + 'static + Send + Sync {
        TxObserver {
            notify_fn: Arc::new(Box::new(notify_fn)),
            attributes,
        }
    }

    pub fn applicable_reports<'r>(&self, reports: &'r Vec<TxReport>) -> Vec<&'r TxReport> {
        reports.into_iter().filter_map( |report| {
            self.attributes.intersection(&report.changeset)
                           .next()
                           .and_then(|_| Some(report))
        }).collect()
    }

    fn notify(&self, key: String, reports: Vec<&TxReport>) {
        (*self.notify_fn)(key, reports);
    }
}

pub trait Command {
    fn execute(&mut self);
}

pub struct AsyncTxExecutor {
    reports: Vec<TxReport>,
    observers: Weak<IndexMap<String, Arc<TxObserver>>>,
}

impl AsyncTxExecutor {
    fn new(observers: &Arc<IndexMap<String, Arc<TxObserver>>>, reports: Vec<TxReport>) -> Self {
        AsyncTxExecutor {
            reports,
            observers: Arc::downgrade(observers),
        }
    }
}

impl Command for AsyncTxExecutor {

    fn execute(&mut self) {
        let reports = ::std::mem::replace(&mut self.reports, Vec::new());
        let weak_observers = ::std::mem::replace(&mut self.observers, Default::default());
        thread::spawn (move || {
            weak_observers.upgrade().map(|observers| {
                for (key, observer) in observers.iter() {
                    let applicable_reports = observer.applicable_reports(&reports);
                    observer.notify(key.clone(), applicable_reports);
                }
            })
        });
    }
}

pub struct TxObservationService {
    observers: Arc<IndexMap<String, Arc<TxObserver>>>,
    pub command_queue: VecDeque<Box<Command + Send>>,
}

impl TxObservationService {
    pub fn new() -> Self {
        TxObservationService {
            observers: Arc::new(IndexMap::new()),
            command_queue: VecDeque::new(),
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

    pub fn transaction_did_commit(&mut self, reports: Vec<TxReport>) {
        self.command_queue.push_back(Box::new(AsyncTxExecutor::new(&self.observers, reports)));
    }

    pub fn run(&mut self) {
        let mut command = self.command_queue.pop_front();
        while command.is_some() {
            command.map(|mut c| c.execute());
            command = self.command_queue.pop_front();
        }
    }
}
