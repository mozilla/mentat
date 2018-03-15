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
    Mutex,
    Weak,
};
use std::sync::mpsc::{
    channel,
    Receiver,
    Sender,
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
    notify_fn: Arc<Box<Fn(&String, &Vec<Arc<TxReport>>) + Send + Sync>>,
    attributes: AttributeSet,
}

impl TxObserver {
    pub fn new<F>(attributes: AttributeSet, notify_fn: F) -> TxObserver where F: Fn(&String, &Vec<Arc<TxReport>>) + 'static + Send + Sync {
        TxObserver {
            notify_fn: Arc::new(Box::new(notify_fn)),
            attributes,
        }
    }

    pub fn applicable_reports(&self, reports: &Vec<Arc<TxReport>>) -> Vec<Arc<TxReport>> {
        reports.into_iter().filter_map( |report| {
            self.attributes.intersection(&report.changeset)
                           .next()
                           .and_then(|_| Some(Arc::clone(report)))
        }).collect()
    }

    fn notify(&self, key: &String, reports: &Vec<Arc<TxReport>>) {
        (*self.notify_fn)(key, reports);
    }
}

pub trait Command {
    fn execute(&mut self);
}

pub struct NotifyTxObserver {
    key: String,
    reports: Vec<Arc<TxReport>>,
    observer: Weak<TxObserver>,
}

impl NotifyTxObserver {
    pub fn new(key: String, reports: Vec<Arc<TxReport>>, observer: Weak<TxObserver>) -> Self {
        NotifyTxObserver {
            key,
            reports,
            observer,
        }
    }
}

impl Command for NotifyTxObserver {
    fn execute(&mut self) {
        self.observer.upgrade().map(|o| o.notify(&self.key, &self.reports));
    }
}

pub struct AsyncBatchExecutor {
    commands: Vec<Box<Command + Send>>,
}

impl Command for AsyncBatchExecutor {
    fn execute(&mut self) {
        // need to clone to move to a new thread.
        let command_queue = ::std::mem::replace(&mut self.commands, Vec::new());
        thread::spawn (move || {
            for mut command in command_queue.into_iter() {
                command.execute();
            }
        });
    }
}

pub struct TxObservationService {
    observers: IndexMap<String, Arc<TxObserver>>,
    pub command_queue: VecDeque<Box<Command + Send>>,
}

impl TxObservationService {
    pub fn new() -> Self {
        // let (tx, rx) = channel();
        // let worker = ThreadWorker::new(0, rx);
        // thread::spawn(move || worker.main());
        TxObservationService {
            observers: IndexMap::new(),
            command_queue: VecDeque::new(),
            // sender: tx,
        }
    }

    // For testing purposes
    pub fn is_registered(&self, key: &String) -> bool {
        self.observers.contains_key(key)
    }

    pub fn register(&mut self, key: String, observer: Arc<TxObserver>) {
        self.observers.insert(key, observer);
    }

    pub fn deregister(&mut self, key: &String) {
        self.observers.remove(key);
    }

    pub fn has_observers(&self) -> bool {
        !self.observers.is_empty()
    }

    fn command_from_reports(&self, key: &String, reports: &Vec<Arc<TxReport>>, observer: &Arc<TxObserver>) -> Option<Box<Command + Send>> {
        let applicable_reports = observer.applicable_reports(reports);
        if !applicable_reports.is_empty() {
            Some(Box::new(NotifyTxObserver::new(key.clone(), applicable_reports, Arc::downgrade(observer))))
        } else {
            None
        }
    }

    pub fn transaction_did_commit(&mut self, reports: Vec<Arc<TxReport>>) {
        // notify all observers about their relevant transactions
        let commands: Vec<Box<Command + Send>> = self.observers
                                                     .iter()
                                                     .filter_map(|(key, observer)| { self.command_from_reports(&key, &reports, &observer) })
                                                     .collect();
        self.command_queue.push_back(Box::new(AsyncBatchExecutor { commands }));
    }

    pub fn run(&mut self) {
        let mut command = self.command_queue.pop_front();
        while command.is_some() {
            command.map(|mut c| c.execute());
            command = self.command_queue.pop_front();
        }
    }
}

// impl CommandQueueObserver for TxObservationService {
//     fn inserted_item(&self, command: Command) {
//         command.execute();
//     }
// }

// pub trait CommandQueueObserver: Send + 'static {
//     fn inserted_item(&self, command: Command);
// }

// struct Inner<Observer: CommandQueueObserver> {
//     command_queue: VecDeque<Box<Command + Send>>,
//     observer: Observer,
// }

// impl<Observer: CommandQueueObserver> Inner<Observer> {
//     fn new(cq: VecDeque<Box<Command + Send>>, observer: Observer) -> Inner<Observer> {
//         Inner {
//             command_queue: cq,
//             observer: observer,
//         }
//     }
// }

// pub struct CommandQueueHandle(Vec<Sender<()>>);

// impl CommandQueueHandle {
//     pub fn new<Observer>(num_threads: usize, observer: Observer) -> (CommandQueueHandle, VecDeque<Box<Command + Send>>)
//         where Observer: CommandQueueObserver
//     {
//         let queue = VecDeque::new();
//         let inner = Arc::new(Mutex::new(Inner::new(queue.clone(), observer)));
//         let mut worker_channels = Vec::with_capacity(num_threads);

//         for i in 0..num_threads {
//             let (tx, rx) = mpsc::channel();
//             worker_channels.push(tx);

//             let worker = ThreadWorker::new(i, inner.clone(), rx);
//             thread::spawn(move || worker.main());
//         }
//         (ViewModelHandle(worker_channels), starting_vm)
//     }
// }

// struct ThreadWorker<Observer: CommandQueueObserver> {
//     inner: Arc<Mutex<Inner<Observer>>>,
//     thread_id: usize,
//     shutdown: Receiver<()>,
// }

// impl<Observer: CommandQueueObserver> ThreadWorker<Observer> {
//     fn new(thread_id: usize,
//            inner: Arc<Mutex<Inner<Observer>>>,
//            shutdown: Receiver<()>)
//            -> ThreadWorker<Observer> {
//         ThreadWorker {
//             inner: inner,
//             thread_id: thread_id,
//             shutdown: shutdown,
//         }
//     }

//     fn main(&self) {
//         let mut rng = rand::thread_rng();
//         let between = Range::new(0i32, 10);

//         loop {
//             thread::sleep(Duration::from_millis(1000 + rng.gen_range(0, 3000)));

//             if self.should_shutdown() {
//                 println!("thread {} exiting", self.thread_id);
//                 return;
//             }

//             let observer = self.inner.lock().unwrap();
//             let queue = self.inner.lock().unwrap().command_queue;

//             if queue.is_empty() {
//                 continue;
//             }

//             while !queue.is_empty() {
//                 let cmd = queue.front();

//             }

//             // // 20% of the time, add a new item.
//             // // 10% of the time, remove an item.
//             // // 70% of the time, modify an existing item.
//             // match between.ind_sample(&mut rng) {
//             //     0 | 1 => self.add_new_item(),
//             //     2 => self.remove_existing_item(&mut rng),
//             //     _ => self.modify_existing_item(&mut rng),
//             // }
//         }
//     }

//     fn should_shutdown(&self) -> bool {
//         match self.shutdown.try_recv() {
//             Err(mpsc::TryRecvError::Disconnected) => true,
//             Err(mpsc::TryRecvError::Empty) => false,
//             Ok(()) => unreachable!("thread worker channels should not be used directly"),
//         }
//     }
// }
