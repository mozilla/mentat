// Copyright 2018 Mozilla
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

extern crate mentat_db;
extern crate mentat_core;

use std::cell::{
    RefCell
};
use std::collections::{
    BTreeMap,
    BTreeSet,
};
use std::ops::Deref;
use std::rc::{
    Rc,
};

use mentat_core::{
    now,
};

use mentat_db::{
    TxObserver,
    TxObservationService,
};

use mentat_db::types::TxReport;

fn get_registered_observer_attributes() -> BTreeSet<i64> {
    let mut registered_attrs = BTreeSet::new();
    registered_attrs.insert(100);
    registered_attrs.insert(200);
    registered_attrs.insert(300);
    registered_attrs
}

fn tx_report(tx_id: i64, changes: BTreeSet<i64>) -> TxReport {
    TxReport {
        tx_id: tx_id,
        tx_instant: now(),
        tempids: BTreeMap::new(),
        changeset: changes,
    }
}

#[test]
fn test_register_observer() {
    let mut observer_service = TxObservationService::default();
    let key = "Test Observing".to_string();
    let registered_attrs = BTreeSet::new();

    let tx_observer = TxObserver::new(registered_attrs, move |_obs_key, _batch| {});

    observer_service.register(key.clone(), tx_observer);
    assert!(observer_service.is_registered(&key));
}

#[test]
fn test_deregister_observer() {
    let mut observer_service = TxObservationService::default();
    let key = "Test Observing".to_string();
    let registered_attrs = BTreeSet::new();

    let tx_observer = TxObserver::new(registered_attrs, move |_obs_key, _batch| {});

    observer_service.register(key.clone(), tx_observer);
    assert!(observer_service.is_registered(&key));

    observer_service.deregister(&key);

    assert!(!observer_service.is_registered(&key));
}

#[test]
fn test_observer_notified_on_registered_change() {
    let mut observer_service = TxObservationService::default();
    let key = "Test Observing".to_string();
    let registered_attrs = get_registered_observer_attributes();

    let txids = Rc::new(RefCell::new(Vec::new()));
    let changes = Rc::new(RefCell::new(Vec::new()));
    let called_key: Rc<RefCell<Option<String>>> = Rc::new(RefCell::new(None));

    let mut_txids = Rc::clone(&txids);
    let mut_changes = Rc::clone(&changes);
    let mut_key = Rc::clone(&called_key);
    let tx_observer = TxObserver::new(registered_attrs, move |obs_key, batch| {
        let mut k = mut_key.borrow_mut();
        *k = Some(obs_key.clone());
        let mut t = mut_txids.borrow_mut();
        let mut c = mut_changes.borrow_mut();
        for report in batch.iter() {
            t.push(report.tx_id.clone());
            c.push(report.changeset.clone());
        }
        t.sort();
    });

    observer_service.register(key.clone(), tx_observer);
    assert!(observer_service.is_registered(&key));

    let mut tx_set_1 = BTreeSet::new();
    tx_set_1.insert(100);
    tx_set_1.insert(400);
    tx_set_1.insert(700);
    let mut tx_set_2 = BTreeSet::new();
    tx_set_2.insert(200);
    tx_set_2.insert(300);
    let mut tx_set_3 = BTreeSet::new();
    tx_set_3.insert(600);
    let mut batch = Vec::new();
    batch.push(tx_report(10, tx_set_1.clone()));
    batch.push(tx_report(11, tx_set_2.clone()));
    batch.push(tx_report(12, tx_set_3));
    observer_service.transaction_did_commit(&batch);

    let val = called_key.deref();
    assert_eq!(val, &RefCell::new(Some(key.clone())));
    let t = txids.deref();
    assert_eq!(t, &RefCell::new(vec![10, 11]));

    let c = changes.deref();
    assert_eq!(c, &RefCell::new(vec![tx_set_1, tx_set_2]));
}

#[test]
fn test_observer_not_notified_on_unregistered_change() {
    let mut observer_service = TxObservationService::default();
    let key = "Test Observing".to_string();
    let registered_attrs = get_registered_observer_attributes();

    let txids = Rc::new(RefCell::new(Vec::new()));
    let changes = Rc::new(RefCell::new(Vec::new()));
    let called_key: Rc<RefCell<Option<String>>> = Rc::new(RefCell::new(None));

    let mut_txids = Rc::clone(&txids);
    let mut_changes = Rc::clone(&changes);
    let mut_key = Rc::clone(&called_key);
    let tx_observer = TxObserver::new(registered_attrs, move |obs_key, batch| {
        let mut k = mut_key.borrow_mut();
        *k = Some(obs_key.clone());
        let mut t = mut_txids.borrow_mut();
        let mut c = mut_changes.borrow_mut();
        for report in batch.iter() {
            t.push(report.tx_id.clone());
            c.push(report.changeset.clone());
        }
        t.sort();
    });

    observer_service.register(key.clone(), tx_observer);
    assert!(observer_service.is_registered(&key));

    let mut tx_set_1 = BTreeSet::new();
    tx_set_1.insert(101);
    tx_set_1.insert(401);
    tx_set_1.insert(701);
    let mut tx_set_2 = BTreeSet::new();
    tx_set_2.insert(201);
    tx_set_2.insert(301);
    let mut tx_set_3 = BTreeSet::new();
    tx_set_3.insert(601);
    let mut batch = Vec::new();
    batch.push(tx_report(10, tx_set_1));
    batch.push(tx_report(11, tx_set_2));
    batch.push(tx_report(12, tx_set_3));
    observer_service.transaction_did_commit(&batch);

    let val = called_key.deref();
    assert_eq!(val, &RefCell::new(None));
    let t = txids.deref();
    assert_eq!(t, &RefCell::new(vec![]));
    let c = changes.deref();
    assert_eq!(c, &RefCell::new(vec![]));
}

#[test]
fn test_only_notifies_observers_registered_at_transact_start() {
    let mut observer_service = TxObservationService::default();
    let key_1 = "Test Observing 1".to_string();
    let registered_attrs = get_registered_observer_attributes();

    let txids_1 = Rc::new(RefCell::new(Vec::new()));
    let changes_1 = Rc::new(RefCell::new(Vec::new()));
    let called_key_1: Rc<RefCell<Option<String>>> = Rc::new(RefCell::new(None));

    let mut_txids_1 = Rc::clone(&txids_1);
    let mut_changes_1 = Rc::clone(&changes_1);
    let mut_key_1 = Rc::clone(&called_key_1);

    let tx_observer_1 = TxObserver::new(registered_attrs.clone(), move |obs_key, batch| {
        let mut k = mut_key_1.borrow_mut();
        *k = Some(obs_key.clone());
        let mut t = mut_txids_1.borrow_mut();
        let mut c = mut_changes_1.borrow_mut();
        for report in batch.iter() {
            t.push(report.tx_id.clone());
            c.push(report.changeset.clone());
        }
        t.sort();
    });

    observer_service.register(key_1.clone(), tx_observer_1);
    assert!(observer_service.is_registered(&key_1));

    let mut tx_set_1 = BTreeSet::new();
    tx_set_1.insert(100);
    tx_set_1.insert(400);
    tx_set_1.insert(700);

    let mut batch = Vec::new();
    batch.push(tx_report(10, tx_set_1.clone()));

    // register second observer after one transact has occured
    let key_2 = "Test Observing 2".to_string();
    let txids_2 = Rc::new(RefCell::new(Vec::new()));
    let changes_2 = Rc::new(RefCell::new(Vec::new()));
    let called_key_2: Rc<RefCell<Option<String>>> = Rc::new(RefCell::new(None));

    let mut_txids_2 = Rc::clone(&txids_2);
    let mut_changes_2 = Rc::clone(&changes_2);
    let mut_key_2 = Rc::clone(&called_key_2);

    let tx_observer_2 = TxObserver::new(registered_attrs, move |obs_key, batch| {
        let mut k = mut_key_2.borrow_mut();
        *k = Some(obs_key.clone());
        let mut t = mut_txids_2.borrow_mut();
        let mut c = mut_changes_2.borrow_mut();
        for report in batch.iter() {
            t.push(report.tx_id.clone());
            c.push(report.changeset.clone());
        }
        t.sort();
    });
    observer_service.register(key_2.clone(), tx_observer_2);
    assert!(observer_service.is_registered(&key_2));

    let mut tx_set_2 = BTreeSet::new();
    tx_set_2.insert(200);
    tx_set_2.insert(300);
    batch.push(tx_report(11, tx_set_2.clone()));
    observer_service.transaction_did_commit(&batch);

    let val = called_key_1.deref();
    assert_eq!(val, &RefCell::new(Some(key_1.clone())));
    let t = txids_1.deref();
    assert_eq!(t, &RefCell::new(vec![10, 11]));
    let c = changes_1.deref();
    assert_eq!(c, &RefCell::new(vec![tx_set_1.clone(), tx_set_2.clone()]));

    let val = called_key_2.deref();
    assert_eq!(val, &RefCell::new(None));
    let t = txids_2.deref();
    assert_eq!(t, &RefCell::new(vec![]));
    let c = changes_2.deref();
    assert_eq!(c, &RefCell::new(vec![]));
}
