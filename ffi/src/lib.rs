// Copyright 2018 Mozilla
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

extern crate libc;
extern crate mentat;

use std::collections::{
    BTreeSet,
};
use std::os::raw::{
    c_char,
    c_int,
    c_longlong,
    c_void,
};
use std::slice;
use std::sync::{
    Arc,
};
use std::vec;

pub use mentat::{
    Entid,
    FindSpec,
    HasSchema,
    KnownEntid,
    Keyword,
    Queryable,
    QueryBuilder,
    QueryInputs,
    QueryOutput,
    QueryResults,
    Store,
    Syncable,
    TypedValue,
    TxObserver,
    TxReport,
    Uuid,
    ValueType,
    Variable,
};

pub use mentat::entity_builder::{
    BuildTerms,
    EntityBuilder,
    InProgressBuilder,
    IntoThing,
};

pub mod android;
pub mod utils;

pub use utils::strings::{
    c_char_to_string,
    kw_from_string,
    string_to_c_char,
};

pub use utils::log;

pub type TypedValueIterator = vec::IntoIter<TypedValue>;
pub type TypedValueListIterator = vec::IntoIter<Vec<TypedValue>>;

#[repr(C)]
#[derive(Debug, Clone)]
pub struct TransactionChange {
    pub txid: Entid,
    pub changes_len: usize,
    pub changes: Box<[Entid]>,
}

#[repr(C)]
#[derive(Debug)]
pub struct TxChangeList {
    pub reports: Box<[TransactionChange]>,
    pub len: usize,
}

#[repr(C)]
#[derive(Debug)]
pub struct ExternOption {
    pub value: *mut c_void,
}

impl<T> From<Option<T>> for ExternOption {
    fn from(option: Option<T>) -> Self {
        ExternOption {
            value: option.map_or(std::ptr::null_mut(), |v| Box::into_raw(Box::new(v)) as *mut _ as *mut c_void)
        }
    }
}

#[repr(C)]
#[derive(Debug)]
pub struct ExternResult {
    pub ok: *const c_void,
    pub err: *const c_char,
}

impl<T, E> From<Result<T, E>> for ExternResult where E: std::error::Error {
    fn from(result: Result<T, E>) -> Self {
        match result {
            Ok(value) => {
                ExternResult {
                    err: std::ptr::null(),
                    ok: Box::into_raw(Box::new(value)) as *const _ as *const c_void,
                }
            },
            Err(e) => {
                ExternResult {
                    err: string_to_c_char(e.to_string()),
                    ok: std::ptr::null(),
                }
            }
        }
    }
}

// A store cannot be opened twice to the same location.
// Once created, the reference to the store is held by the caller and not Rust,
// therefore the caller is responsible for calling `destroy` to release the memory
// used by the Store in order to avoid a memory leak.
// TODO: Start returning `ExternResult`s rather than crashing on error.
#[no_mangle]
pub extern "C" fn store_open(uri: *const c_char) -> *mut Store {
    let uri = c_char_to_string(uri);
    let store = Store::open(&uri).expect("expected a store");
    Box::into_raw(Box::new(store))
}

// TODO: open empty

// TODO: dismantle

// TODO: conn

// TODO: begin_read

// TODO: begin_transaction

#[no_mangle]
pub unsafe extern "C" fn store_transact(store: *mut Store, transaction: *const c_char) -> *mut ExternResult {
    let store = &mut*store;
    let transaction = c_char_to_string(transaction);
    let result = store.begin_transaction().and_then(|mut in_progress| {
        in_progress.transact(&transaction).and_then(|tx_report| {
            in_progress.commit()
                       .map(|_| tx_report)
        })
    });
    Box::into_raw(Box::new(result.into()))
}

#[no_mangle]
pub unsafe extern "C" fn tx_report_get_entid(tx_report: *mut TxReport) -> c_longlong {
    let tx_report = &*tx_report;
    tx_report.tx_id as c_longlong
}

#[no_mangle]
pub unsafe extern "C" fn tx_report_get_tx_instant(tx_report: *mut TxReport) -> c_longlong {
    let tx_report = &*tx_report;
    tx_report.tx_instant.timestamp() as c_longlong
}

#[no_mangle]
pub unsafe extern "C" fn tx_report_entity_for_temp_id(tx_report: *mut TxReport, tempid: *const c_char) -> *mut c_longlong {
    let tx_report = &*tx_report;
    let key = c_char_to_string(tempid);
    if let Some(entid) = tx_report.tempids.get(&key) {
        Box::into_raw(Box::new(entid.clone() as c_longlong))
    } else {
        std::ptr::null_mut()
    }
}

// TODO: cache

// TODO: q_once
#[no_mangle]
pub unsafe extern "C" fn store_query<'a>(store: *mut Store, query: *const c_char) -> *mut QueryBuilder<'a> {
    let query = c_char_to_string(query);
    let store = &mut*store;
    let query_builder = QueryBuilder::new(store, query);
    Box::into_raw(Box::new(query_builder))
}

#[no_mangle]
pub unsafe extern "C" fn query_builder_bind_long(query_builder: *mut QueryBuilder, var: *const c_char, value: c_longlong) {
    let var = c_char_to_string(var);
    let query_builder = &mut*query_builder;
   query_builder.bind_long(&var, value);
}

#[no_mangle]
pub unsafe extern "C" fn query_builder_bind_ref(query_builder: *mut QueryBuilder, var: *const c_char, value: c_longlong) {
    let var = c_char_to_string(var);
    let query_builder = &mut*query_builder;
    query_builder.bind_ref(&var, value);
}

#[no_mangle]
pub unsafe extern "C" fn query_builder_bind_ref_kw(query_builder: *mut QueryBuilder, var: *const c_char, value: *const c_char) {
    let var = c_char_to_string(var);
    let kw = kw_from_string(c_char_to_string(value));
    let query_builder = &mut*query_builder;
    if let Some(err) = query_builder.bind_ref_from_kw(&var, kw).err() {
        panic!(err);
    }
}

#[no_mangle]
pub unsafe extern "C" fn query_builder_bind_kw(query_builder: *mut QueryBuilder, var: *const c_char, value: *const c_char) {
    let var = c_char_to_string(var);
    let query_builder = &mut*query_builder;
    let kw = kw_from_string(c_char_to_string(value));
    query_builder.bind_value(&var, kw);
}

// boolean
#[no_mangle]
pub unsafe extern "C" fn query_builder_bind_boolean(query_builder: *mut QueryBuilder, var: *const c_char, value: bool) {
    let var = c_char_to_string(var);
    let query_builder = &mut*query_builder;
    query_builder.bind_value(&var, value);
}

// double
#[no_mangle]
pub unsafe extern "C" fn query_builder_bind_double(query_builder: *mut QueryBuilder, var: *const c_char, value: f64) {
    let var = c_char_to_string(var);
    let query_builder = &mut*query_builder;
    query_builder.bind_value(&var, value);
}

// instant
#[no_mangle]
pub unsafe extern "C" fn query_builder_bind_timestamp(query_builder: *mut QueryBuilder, var: *const c_char, value: c_longlong) {
    let var = c_char_to_string(var);
    let query_builder = &mut*query_builder;
    query_builder.bind_instant(&var, value);
}

// string
#[no_mangle]
pub unsafe extern "C" fn query_builder_bind_string(query_builder: *mut QueryBuilder, var: *const c_char, value: *const c_char) {
    let var = c_char_to_string(var);
    let value = c_char_to_string(value);
    let query_builder = &mut*query_builder;
    query_builder.bind_value(&var, value);
}

// uuid
#[no_mangle]
pub unsafe extern "C" fn query_builder_bind_uuid(query_builder: *mut QueryBuilder, var: *const c_char, value: *mut [u8; 16]) {
    let var = c_char_to_string(var);
    let value = &*value;
    let value = Uuid::from_bytes(value).expect("valid uuid");
    let query_builder = &mut*query_builder;
    query_builder.bind_value(&var, value);
}

#[no_mangle]
pub unsafe extern "C" fn query_builder_execute_scalar(query_builder: *mut QueryBuilder) -> *mut ExternResult {
    let query_builder = &mut*query_builder;
    let results = query_builder.execute_scalar();
    let extern_result = match results {
        Ok(Some(v)) => ExternResult { err: std::ptr::null(), ok: Box::into_raw(Box::new(v)) as *const _ as *const c_void, },
        Ok(None) => ExternResult { err: std::ptr::null(), ok: std::ptr::null(), },
        Err(e) => ExternResult { err: string_to_c_char(e.to_string()), ok: std::ptr::null(), }
    };
    Box::into_raw(Box::new(extern_result))
}

#[no_mangle]
pub unsafe extern "C" fn query_builder_execute_coll(query_builder: *mut QueryBuilder) -> *mut ExternResult {
    let query_builder = &mut*query_builder;
    let results = query_builder.execute_coll();
    Box::into_raw(Box::new(results.into()))
}

#[no_mangle]
pub unsafe extern "C" fn query_builder_execute_tuple(query_builder: *mut QueryBuilder) -> *mut ExternResult {
    let query_builder = &mut*query_builder;
    let results = query_builder.execute_tuple();
    let extern_result = match results {
        Ok(Some(v)) => ExternResult { err: std::ptr::null(), ok: Box::into_raw(Box::new(v)) as *const _ as *const c_void, },
        Ok(None) => ExternResult { err: std::ptr::null(), ok: std::ptr::null(), },
        Err(e) => ExternResult { err: string_to_c_char(e.to_string()), ok: std::ptr::null(), }
    };
    Box::into_raw(Box::new(extern_result))
}

#[no_mangle]
pub unsafe extern "C" fn query_builder_execute(query_builder: *mut QueryBuilder) -> *mut ExternResult {
    let query_builder = &mut*query_builder;
    let results = query_builder.execute_rel();
    Box::into_raw(Box::new(results.into()))
}

fn unwrap_conversion<T>(value: Option<T>, expected_type: ValueType) -> T {
    match value {
        Some(v) => v,
        None => panic!("Typed value cannot be coerced into a {}", expected_type)
    }
}

// as_long
#[no_mangle]
pub unsafe extern "C" fn typed_value_as_long(typed_value: *mut TypedValue) ->  c_longlong {
    let typed_value = Box::from_raw(typed_value);
    unwrap_conversion(typed_value.into_long(), ValueType::Long)
}

// as_entid
#[no_mangle]
pub unsafe extern "C" fn typed_value_as_entid(typed_value: *mut TypedValue) ->  Entid {
    let typed_value = Box::from_raw(typed_value);
    unwrap_conversion(typed_value.into_entid(), ValueType::Ref)
}

// kw
#[no_mangle]
pub unsafe extern "C" fn typed_value_as_kw(typed_value: *mut TypedValue) ->  *const c_char {
    let typed_value = Box::from_raw(typed_value);
    string_to_c_char(unwrap_conversion(typed_value.into_kw(), ValueType::Keyword).to_string())
}

//as_boolean
#[no_mangle]
pub unsafe extern "C" fn typed_value_as_boolean(typed_value: *mut TypedValue) -> i32 {
    let typed_value = Box::from_raw(typed_value);
    if unwrap_conversion(typed_value.into_boolean(), ValueType::Boolean) { 1 } else { 0 }
}

//as_double
#[no_mangle]
pub unsafe extern "C" fn typed_value_as_double(typed_value: *mut TypedValue) ->  f64 {
    let typed_value = Box::from_raw(typed_value);
    unwrap_conversion(typed_value.into_double(), ValueType::Double)
}

//as_timestamp
#[no_mangle]
pub unsafe extern "C" fn typed_value_as_timestamp(typed_value: *mut TypedValue) ->  c_longlong {
    let typed_value = Box::from_raw(typed_value);
    unwrap_conversion(typed_value.into_timestamp(), ValueType::Instant)
}

//as_string
#[no_mangle]
pub unsafe extern "C" fn typed_value_as_string(typed_value: *mut TypedValue) ->  *const c_char {
    let typed_value = Box::from_raw(typed_value);
    c_char_from_rc(unwrap_conversion(typed_value.into_string(), ValueType::String))
}

//as_uuid
#[no_mangle]
pub unsafe extern "C" fn typed_value_as_uuid(typed_value: *mut TypedValue) ->  *mut [u8; 16] {
    let typed_value = Box::from_raw(typed_value);
    let value = unwrap_conversion(typed_value.into_uuid(), ValueType::Uuid);
    Box::into_raw(Box::new(*value.as_bytes()))
}

//value_type
#[no_mangle]
pub unsafe extern "C" fn typed_value_value_type(typed_value: *mut TypedValue) ->  ValueType {
    let typed_value = &*typed_value;
    typed_value.value_type()
}

#[no_mangle]
pub unsafe extern "C" fn row_at_index(rows: *mut Vec<Vec<TypedValue>>, index: c_int) ->  *mut Vec<TypedValue> {
    let result = &*rows;
    result.get(index as usize).map_or_else(std::ptr::null_mut, |v| Box::into_raw(Box::new(v.clone())))
}

#[no_mangle]
pub unsafe extern "C" fn rows_iter(rows: *mut Vec<Vec<TypedValue>>) ->  *mut TypedValueListIterator {
    let result = Box::from_raw(rows);
    Box::into_raw(Box::new(result.into_iter()))
}

#[no_mangle]
pub unsafe extern "C" fn rows_iter_next(iter: *mut TypedValueListIterator) ->  *mut Vec<TypedValue> {
    let iter = &mut *iter;
    iter.next().map_or(std::ptr::null_mut(), |v| Box::into_raw(Box::new(v)))
}

#[no_mangle]
pub unsafe extern "C" fn values_iter(values: *mut Vec<TypedValue>) ->  *mut TypedValueIterator {
    let result = Box::from_raw(values);
    Box::into_raw(Box::new(result.into_iter()))
}

#[no_mangle]
pub unsafe extern "C" fn values_iter_next(iter: *mut TypedValueIterator) ->  *mut TypedValue {
    let iter = &mut *iter;
    iter.next().map_or(std::ptr::null_mut(), |v| Box::into_raw(Box::new(v)))
}

#[no_mangle]
pub unsafe extern "C" fn value_at_index(values: *mut Vec<TypedValue>, index: c_int) ->  *const TypedValue {
    let result = &*values;
    result.get(index as usize).expect("No value at index") as *const TypedValue
}

//as_long
#[no_mangle]
pub unsafe extern "C" fn value_at_index_as_long(values: *mut Vec<TypedValue>, index: c_int) ->  c_longlong {
    let result = &*values;
    let value = result.get(index as usize).expect("No value at index");
    unwrap_conversion(value.clone().into_long(), ValueType::Long)
}
// as ref
#[no_mangle]
pub unsafe extern "C" fn value_at_index_as_entid(values: *mut Vec<TypedValue>, index: c_int) ->  Entid {
    let result = &*values;
    let value = result.get(index as usize).expect("No value at index");
    unwrap_conversion(value.clone().into_entid(), ValueType::Ref)
}

// as kw
#[no_mangle]
pub unsafe extern "C" fn value_at_index_as_kw(values: *mut Vec<TypedValue>, index: c_int) ->  *const c_char {
    let result = &*values;
    let value = result.get(index as usize).expect("No value at index");
    string_to_c_char(unwrap_conversion(value.clone().into_kw(), ValueType::Keyword).to_string())
}

//as_boolean
#[no_mangle]
pub unsafe extern "C" fn value_at_index_as_boolean(values: *mut Vec<TypedValue>, index: c_int) ->  i32 {
    let result = &*values;
    let value = result.get(index as usize).expect("No value at index");
    if unwrap_conversion(value.clone().into_boolean(), ValueType::Boolean) { 1 } else { 0 }
}

//as_double
#[no_mangle]
pub unsafe extern "C" fn value_at_index_as_double(values: *mut Vec<TypedValue>, index: c_int) ->  f64 {
    let result = &*values;
    let value = result.get(index as usize).expect("No value at index");
    unwrap_conversion(value.clone().into_double(), ValueType::Boolean)
}

//as_timestamp
#[no_mangle]
pub unsafe extern "C" fn value_at_index_as_timestamp(values: *mut Vec<TypedValue>, index: c_int) ->  c_longlong {
    let result = &*values;
    let value = result.get(index as usize).expect("No value at index");
    unwrap_conversion(value.clone().into_timestamp(), ValueType::Instant)
}

//as_string
#[no_mangle]
pub unsafe extern "C" fn value_at_index_as_string(values: *mut Vec<TypedValue>, index: c_int) ->  *mut c_char {
    let result = &*values;
    let value = result.get(index as usize).expect("No value at index");
    c_char_from_rc(unwrap_conversion(value.clone().into_string(), ValueType::String))
}

//as_uuid
#[no_mangle]
pub unsafe extern "C" fn value_at_index_as_uuid(values: *mut Vec<TypedValue>, index: c_int) ->  *mut [u8; 16] {
    let result = &*values;
    let value = result.get(index as usize).expect("No value at index");
    let uuid = unwrap_conversion(value.clone().into_uuid(), ValueType::Uuid);
    Box::into_raw(Box::new(*uuid.as_bytes()))
}

#[no_mangle]
pub unsafe extern "C" fn store_value_for_attribute(store: *mut Store, entid: c_longlong, attribute: *const c_char) ->  *mut ExternResult {
    let store = &*store;
    let kw = kw_from_string(c_char_to_string(attribute));
    let value = match store.lookup_value_for_attribute(entid, &kw) {
        Ok(Some(v)) => ExternResult { ok: Box::into_raw(Box::new(v)) as *const _ as *const c_void, err: std::ptr::null() },
        Ok(None) => ExternResult { ok: std::ptr::null(), err: std::ptr::null() },
        Err(e) => ExternResult { ok: std::ptr::null(), err: string_to_c_char(e.to_string()) },
    };
    Box::into_raw(Box::new(value))
}

#[no_mangle]
pub unsafe extern "C" fn store_register_observer(store: *mut Store,
                                                   key: *const c_char,
                                            attributes: *const Entid,
                                        attributes_len: usize,
                                              callback: extern fn(key: *const c_char, reports: &TxChangeList)) {
    let store = &mut*store;
    let mut attribute_set = BTreeSet::new();
    let slice = slice::from_raw_parts(attributes, attributes_len);
    attribute_set.extend(slice.iter());
    let key = c_char_to_string(key);
    let tx_observer = Arc::new(TxObserver::new(attribute_set, move |obs_key, batch| {
        let extern_reports: Vec<TransactionChange> = batch.into_iter().map(|(tx_id, changes)| {
            let changes: Vec<Entid> = changes.into_iter().map(|i|*i).collect();
            let len = changes.len();
            TransactionChange {
                txid: *tx_id,
                changes: changes.into_boxed_slice(),
                changes_len: len,
            }
        }).collect();
        let len = extern_reports.len();
        let reports = TxChangeList {
            reports: extern_reports.into_boxed_slice(),
            len: len,
        };
        callback(string_to_c_char(obs_key), &reports);
    }));
    store.register_observer(key, tx_observer);
}

#[no_mangle]
pub unsafe extern "C" fn store_unregister_observer(store: *mut Store, key: *const c_char) {
    let store = &mut*store;
    let key = c_char_to_string(key);
    store.unregister_observer(&key);
}

#[no_mangle]
pub unsafe extern "C" fn store_entid_for_attribute(store: *mut Store, attr: *const c_char) -> Entid {
    let store = &mut*store;
    let keyword_string = c_char_to_string(attr);
    let kw = kw_from_string(keyword_string);
    let conn = store.conn();
    let current_schema = conn.current_schema();
    current_schema.get_entid(&kw).expect("Unable to find entid for invalid attribute").into()
}

#[no_mangle]
pub unsafe extern "C" fn tx_change_list_entry_at(tx_report_list: *mut TxChangeList, index: c_int) -> *const TransactionChange {
    let tx_report_list = &*tx_report_list;
    let index = index as usize;
    let report = Box::new(tx_report_list.reports[index].clone());
    Box::into_raw(report)
}

#[no_mangle]
pub unsafe extern "C" fn changelist_entry_at(tx_report: *mut TransactionChange, index: c_int) -> Entid {
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

#[no_mangle]
pub unsafe extern "C" fn destroy(obj: *mut c_void) {
    if !obj.is_null() {
        let _ = Box::from_raw(obj);
    }
}

macro_rules! define_destructor (
    ($name:ident, $t:ty) => (
        #[no_mangle]
        pub unsafe extern "C" fn $name(obj: *mut $t) {
            if !obj.is_null() { let _ = Box::from_raw(obj); }
        }
    )
);
define_destructor!(query_builder_destroy, QueryBuilder);

define_destructor!(store_destroy, Store);

define_destructor!(tx_report_destroy, TxReport);

define_destructor!(typed_value_destroy, TypedValue);

define_destructor!(typed_value_list_destroy, Vec<TypedValue>);

define_destructor!(typed_value_list_iter_destroy, TypedValueIterator);

define_destructor!(typed_value_result_set_destroy, Vec<Vec<TypedValue>>);

define_destructor!(typed_value_result_set_iter_destroy, TypedValueListIterator);
