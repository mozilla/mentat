// Copyright 2018 Mozilla
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

extern crate mentat;

use std::collections::{
    BTreeSet,
};
use std::os::raw::{
    c_char,
    c_int,
};
use std::slice;
use std::sync::{
    Arc,
};

pub use mentat::{
    Entid,
    HasSchema,
    NamespacedKeyword,
    Store,
    Syncable,
    TxObserver,
};

pub use mentat::errors::{
    Result,
};

pub mod android;
pub mod utils;

pub use utils::strings::{
    c_char_to_string,
    string_to_c_char,
    str_to_c_char,
};

use utils::log;

#[repr(C)]
#[derive(Debug, Clone)]
pub struct ExternTxReport {
    pub txid: Entid,
    pub changes: Box<[Entid]>,
    pub changes_len: usize,
}

#[repr(C)]
#[derive(Debug)]
pub struct ExternTxReportList {
    pub reports: Box<[ExternTxReport]>,
    pub len: usize,
}

#[repr(C)]
pub struct ExternResult {
    pub error: *const c_char,
}

impl From<Result<()>> for ExternResult {
    fn from(result: Result<()>) -> Self {
        match result {
            Ok(_) => {
                ExternResult {
                    error: std::ptr::null(),
                }
            },
            Err(e) => {
                ExternResult {
                    error: string_to_c_char(e.description().into())
                }
            }
        }
    }
}

// A store cannot be opened twice to the same location.
// Once created, the reference to the store is held by the caller and not Rust,
// therefore the caller is responsible for calling `store_destroy` to release the memory
// used by the Store in order to avoid a memory leak.
// TODO: Start returning `ExternResult`s rather than crashing on error.
#[no_mangle]
pub extern "C" fn store_open(uri: *const c_char) -> *mut Store {
    let uri = c_char_to_string(uri);
    let store = Store::open(&uri).expect("expected a store");
    Box::into_raw(Box::new(store))
}

// Reclaim the memory for the provided Store and drop, therefore releasing it.
#[no_mangle]
pub unsafe extern "C" fn store_destroy(store: *mut Store) {
    let _ = Box::from_raw(store);
}

#[no_mangle]
pub unsafe extern "C" fn store_register_observer(store: *mut Store,
                                                   key: *const c_char,
                                            attributes: *const Entid,
                                        attributes_len: usize,
                                              callback: extern fn(key: *const c_char, reports: &ExternTxReportList)) {
    let store = &mut*store;
    let mut attribute_set = BTreeSet::new();
    let slice = slice::from_raw_parts(attributes, attributes_len);
    attribute_set.extend(slice.iter());
    let key = c_char_to_string(key);
    let tx_observer = Arc::new(TxObserver::new(attribute_set, move |obs_key, batch| {
        log::d(&format!("Calling observer registered for {:?}, batch: {:?}", obs_key, batch));
        let extern_reports: Vec<ExternTxReport> = batch.iter().map(|report| {
            let changes: Vec<Entid> = report.changeset.iter().map(|i|i.clone()).collect();
            let len = changes.len();
            ExternTxReport {
                txid: report.tx_id.clone(),
                changes: changes.into_boxed_slice(),
                changes_len: len,
            }
        }).collect();
        let len = extern_reports.len();
        let reports = ExternTxReportList {
            reports: extern_reports.into_boxed_slice(),
            len: len,
        };
        callback(str_to_c_char(obs_key), &reports);
    }));
    store.register_observer(key, tx_observer);
}

#[no_mangle]
pub unsafe extern "C" fn store_unregister_observer(store: *mut Store, key: *const c_char) {
    let store = &mut*store;
    let key = c_char_to_string(key);
    log::d(&format!("Unregistering observer for key: {:?}", key));
    store.unregister_observer(&key);
}

#[no_mangle]
pub unsafe extern "C" fn store_entid_for_attribute(store: *mut Store, attr: *const c_char) -> Entid {
    let store = &mut*store;
    let mut keyword_string = c_char_to_string(attr);
    let attr_name = keyword_string.split_off(1);
    let parts: Vec<&str> = attr_name.split("/").collect();
    let kw = NamespacedKeyword::new(parts[0], parts[1]);
    let conn = store.conn();
    let current_schema = conn.current_schema();
    let got_entid = current_schema.get_entid(&kw);
    let entid = got_entid.unwrap();
    entid.into()
}

#[no_mangle]
pub unsafe extern "C" fn tx_report_list_entry_at(tx_report_list: *mut ExternTxReportList, index: c_int) -> *const ExternTxReport {
    let tx_report_list = &*tx_report_list;
    let index = index as usize;
    let report = Box::new(tx_report_list.reports[index].clone());
    Box::into_raw(report)
}

#[no_mangle]
pub unsafe extern "C" fn changelist_entry_at(tx_report: *mut ExternTxReport, index: c_int) -> Entid {
    let tx_report = &*tx_report;
    let index = index as usize;
    tx_report.changes[index].clone()
}

#[no_mangle]
pub unsafe extern "C" fn store_sync(store: *mut Store, user_uuid: *const c_char, server_uri: *const c_char) -> *mut ExternResult {
    let store = &mut*store;
    let user_uuid = c_char_to_string(user_uuid);
    let server_uri = c_char_to_string(server_uri);
    let res = store.sync(&server_uri, &user_uuid);
    Box::into_raw(Box::new(res.into()))
}
