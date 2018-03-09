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
    notify_fn: Arc<Option<Box<Fn(String, Vec<TxReport>) + Send + Sync>>>,
    attributes: AttributeSet,
}

impl TxObserver {
    pub fn new<F>(attributes: AttributeSet, notify_fn: F) -> TxObserver where F: Fn(String, Vec<TxReport>) + 'static + Send + Sync {
        TxObserver {
            notify_fn: Arc::new(Some(Box::new(notify_fn))),
            attributes,
        }
    }

    pub fn applicable_reports(&self, reports: &Vec<TxReport>) -> Vec<TxReport> {
        reports.into_iter().filter_map( |report| {
            if self.attributes.intersection(&report.changeset).next().is_some(){
                Some(report.clone())
            } else {
                None
            }
        }).collect()
    }

    fn notify(&self, key: String, reports: Vec<TxReport>) {
        if let Some(ref notify_fn) = *self.notify_fn {
            (notify_fn)(key, reports);
        } else {
            eprintln!("no notify function specified for TxObserver");
        }
    }
}

pub trait CommandClone {
    fn clone_box(&self) -> Box<Command + Send>;
}

impl<T> CommandClone for T where T: 'static + Command + Clone + Send {
    fn clone_box(&self) -> Box<Command + Send> {
        Box::new(self.clone())
    }
}

pub trait Command: CommandClone {
    fn execute(&self);
}

impl Clone for Box<Command + Send> {
    fn clone(&self) -> Box<Command + Send> {
        self.clone_box()
    }
}

#[derive(Clone)]
pub struct NotifyTxObserver {
    key: String,
    reports: Vec<TxReport>,
    observer: Arc<TxObserver>,
}

impl NotifyTxObserver {
    pub fn new(key: String, reports: Vec<TxReport>, observer: Arc<TxObserver>) -> Self {
        NotifyTxObserver {
            key,
            reports,
            observer,
        }
    }
}

impl Command for NotifyTxObserver {
    fn execute(&self) {
        self.observer.notify(self.key.clone(), self.reports.clone());
    }
}

#[derive(Clone)]
pub struct AsyncBatchExecutor {
    commands: Vec<Box<Command + Send>>,
}

impl Command for AsyncBatchExecutor {
    fn execute(&self) {
        let command_queue = self.commands.clone();
        thread::spawn (move ||{
            for command in command_queue.iter() {
                command.execute();
            }
        });
    }
}

#[derive(Clone)]
pub struct TxObservationService {
    observers: IndexMap<String, Arc<TxObserver>>,
    pub command_queue: Vec<Box<Command + Send>>,
}

impl TxObservationService {
    pub fn new() -> Self {
        TxObservationService {
            observers: IndexMap::new(),
            command_queue: Vec::new(),
        }
    }
    // For testing purposes
    pub fn is_registered(&self, key: &String) -> bool {
        self.observers.contains_key(key)
    }

    pub fn register(&mut self, key: String, observer: Arc<TxObserver>) {
        self.observers.insert(key.clone(), observer);
    }

    pub fn deregister(&mut self, key: &String) {
        self.observers.remove(key);
    }

    pub fn has_observers(&self) -> bool {
        !self.observers.is_empty()
    }

    fn command_from_reports(&self, key: &String, reports: &Vec<TxReport>, observer: &Arc<TxObserver>) -> Option<Box<Command + Send>> {
        let applicable_reports = observer.applicable_reports(reports);
        if !applicable_reports.is_empty() {
            Some(Box::new(NotifyTxObserver::new(key.clone(), applicable_reports, Arc::clone(observer))))
        } else {
            None
        }
    }

    pub fn transaction_did_commit(&mut self, reports: Vec<TxReport>) {
        // notify all observers about their relevant transactions
        let commands: Vec<Box<Command + Send>> = self.observers
                                                .iter()
                                                .filter_map(|(key, observer)| { self.command_from_reports(&key, &reports, &observer) })
                                                .collect();
        self.command_queue.push(Box::new(AsyncBatchExecutor{ commands }));
    }

    pub fn run(&mut self) {
        for command in self.command_queue.iter() {
            command.execute();
        }

        self.command_queue.clear();
    }
}
