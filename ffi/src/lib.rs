// Copyright 2018 Mozilla
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

//! This module exposes an Foreign Function Interface (FFI) that allows Mentat to be
//! called from other languages.
//!
//! Functions that are available to other languages in this module are defined as
//! extern "C" functions which allow them to be layed out correctly for the
//! platform's C ABI. They all have a `#[no_mangle]` decorator to ensure
//! Rust's name mangling is turned off, so that it is easier to link to.
//!
//! Mentat's FFI contains unsafe code. As it is an interface between foreign code
//! and native Rust code, Rust cannot guarentee that the types and data that it is
//! handling that have been passed to it from another language are present and in
//! the format it is expecting.
//! This interface is designed to ensure that nothing unsafe ever leaves this module
//! and enters Mentat proper.
//!
//! Structs defined with `#[repr(C)]` are guaranteed to have a layout that is compatible
//! with the platform's representation in C.
//!
//! This API passes pointers in two ways, depending on the lifetime of the object and
//! what object owns it.
//! Pointers to values that can guarenteed to live beyond the lifetime of the function,
//! are passed over the FFI as a raw pointer.
//! ```
//! value as *const TypedValue
//! ```
//! Pointers to values that cannot be guarenteed to live beyond the lifetime of the function
//! are first Boxed so that they live on the heap, and the raw pointer passed this way.
//! ```
//! Box::into_raw(Box::new(value))
//! ```
//!
//! The memory for values that are moved onto the heap before being passed over the FFI
//! are no longer managed by Rust, but Rust still owns the value. Therefore the pointer
//! must be returned to Rust in order to be released. To this effect a number of `destructor`
//! function are provided for each Rust value type that is passed, and a catch all destructor
//! to destroy memory for `#[repr(C)]` values. The destructors reclaim the memory via `Box`
//! and then drop the reference, causing the memory to be released.
//!
//! A macro has been provided to make defining destructors easier.
//! ```
//! define_destructor!(query_builder_destroy, QueryBuilder);
//! ```
//!
//! Passing a pointer to released memory will cause Mentat to crash, so callers have to be
//! careful to ensure they manage their pointers properly. Failure to call a destructor for a
//! value on the heap will cause a memory leak.
//!
//! Most of the functions exposed in this FFI have a direct mapping to existing Mentat APIs.
//! Application logic has been kept to a minumum in order to provide the greatest flexibility
//! for callers using the interface, however there are a one exception where several steps
//! have been wrapped into a single call in order to make the interface easier to use.
//! `store_register_observer` takes a single native callback function is wrapped inside a
//! Rust closure and added to a `TxObserver` struct that this then used to register the
//! observer with the store.
//!
//! Result and Option Rust types have `repr(C)` structs that mirror them. This is to provide a more
//! native access pattern to callers and to enable easier passing of optional types and error
//! propogation. These types have implemented `From` such that conversion from the Rust type
//! to the C type is as painless as possible.
//!
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
    Binding,
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
    RelResult,
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

pub type BindingIterator = vec::IntoIter<Binding>;
pub type BindingListIterator = std::slice::Chunks<'static, mentat::Binding>;

/// A C representation of the a change provided by the transaction observers
/// from a single transact.
/// Holds a transaction identifier, the changes as a set of affected attributes
/// and the length of the list of changes.
///
/// #Safety
///
/// Callers are responsible for managing the memory for the return value.
/// A destructor `destroy` is provided for releasing the memory for this
/// pointer type.
#[repr(C)]
#[derive(Debug, Clone)]
pub struct TransactionChange {
    pub txid: Entid,
    pub changes_len: usize,
    pub changes: Box<[Entid]>,
}

 /// A C representation of the list of changes provided by the transaction observers.
 /// Provides the list of changes as the length of the list.
///
/// #Safety
///
/// Callers are responsible for managing the memory for the return value.
/// A destructor `destroy` is provided for releasing the memory for this
/// pointer type.
#[repr(C)]
#[derive(Debug)]
pub struct TxChangeList {
    pub reports: Box<[TransactionChange]>,
    pub len: usize,
}

/// A C representation Rust's Option.
/// A value of Some results in `value` containing a raw pointer as a `c_void`.
/// A value of None results in `value` containing a null pointer.
///
/// #Safety
///
/// Callers are responsible for managing the memory for the return value.
/// A destructor `destroy` is provided for releasing the memory for this
/// pointer type.
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

/// A C representation Rust's Result.
/// A value of Ok results in `ok` containing a raw pointer as a `c_void`
/// and `err` containing a null pointer.
/// A value of Err results in `value` containing a null pointer and `err` containing an error message.
///
/// #Safety
///
/// Callers are responsible for managing the memory for the return value.
/// A destructor `destroy` is provided for releasing the memory for this
/// pointer type.
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

/// A store cannot be opened twice to the same location.
/// Once created, the reference to the store is held by the caller and not Rust,
/// therefore the caller is responsible for calling `destroy` to release the memory
/// used by the Store in order to avoid a memory leak.
/// TODO: Start returning `ExternResult`s rather than crashing on error.
///
/// # Safety
///
/// Callers are responsible for managing the memory for the return value.
/// A destructor `store_destroy` is provided for releasing the memory for this
/// pointer type.
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

/// Performs a single transaction against the store.
///
/// # Safety
///
/// Callers must ensure that the pointer to the `Store` is not dangling and that
/// the C string provided to `transaction` is valid.
///
/// Callers are responsible for managing the memory for the return value.
/// A destructor `destroy` is provided for releasing the memory for this
/// pointer type.
///
/// TODO: Document the errors that can result from transact
#[no_mangle]
pub unsafe extern "C" fn store_transact(store: *mut Store, transaction: *const c_char) -> *mut ExternResult {
    let store = &mut*store;
    let transaction = c_char_to_string(transaction);
    let result = store.begin_transaction().and_then(|mut in_progress| {
        in_progress.transact(transaction).and_then(|tx_report| {
            in_progress.commit()
                       .map(|_| tx_report)
        })
    });
    Box::into_raw(Box::new(result.into()))
}

/// Fetches the `tx_id` for the given `TxReport`.
///
/// # Safety
///
/// Callers must ensure that the pointer to the `TxReport` is not dangling.
#[no_mangle]
pub unsafe extern "C" fn tx_report_get_entid(tx_report: *mut TxReport) -> c_longlong {
    let tx_report = &*tx_report;
    tx_report.tx_id as c_longlong
}

/// Fetches the `tx_instant` for the given `TxReport`.
///
/// # Safety
///
/// Callers must ensure that the pointer to the `TxReport` is not dangling.
#[no_mangle]
pub unsafe extern "C" fn tx_report_get_tx_instant(tx_report: *mut TxReport) -> c_longlong {
    let tx_report = &*tx_report;
    tx_report.tx_instant.timestamp() as c_longlong
}

/// Fetches the `Entid` assigned to the `tempid` during the transaction represented
/// by the given `TxReport`.
///
/// # Safety
///
/// Callers must ensure that the pointer to the `TxReport` is not dangling and that
/// the C string provided to `tempid` is valid.
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

/// Creates a `QueryBuilder` from the given store to execute the provided query.
///
/// # Safety
///
/// Callers must ensure that the pointer to the `Store` is not dangling and that
/// the C string provided to `query` is valid.
///
/// Callers are responsible for managing the memory for the return value.
/// A destructor `query_builder_destroy` is provided for releasing the memory for this
/// pointer type.
///
/// TODO: Update QueryBuilder so it only takes a `Store` pointer on execution
#[no_mangle]
pub unsafe extern "C" fn store_query<'a>(store: *mut Store, query: *const c_char) -> *mut QueryBuilder<'a> {
    let query = c_char_to_string(query);
    let store = &mut*store;
    let query_builder = QueryBuilder::new(store, query);
    Box::into_raw(Box::new(query_builder))
}

/// Binds a `TypedValue::Long` to a `Variable` with the given name.
///
/// # Safety
///
/// Callers must ensure that the pointer to the `QueryBuilder` is not dangling and that
/// the C string provided to `var` is valid.
#[no_mangle]
pub unsafe extern "C" fn query_builder_bind_long(query_builder: *mut QueryBuilder, var: *const c_char, value: c_longlong) {
    let var = c_char_to_string(var);
    let query_builder = &mut*query_builder;
   query_builder.bind_long(&var, value);
}

/// Binds a `TypedValue::Ref` to a `Variable` with the given name.
///
/// # Safety
///
/// Callers must ensure that the pointer to the `QueryBuilder` is not dangling and that
/// the C string provided to `var` is valid.
#[no_mangle]
pub unsafe extern "C" fn query_builder_bind_ref(query_builder: *mut QueryBuilder, var: *const c_char, value: c_longlong) {
    let var = c_char_to_string(var);
    let query_builder = &mut*query_builder;
    query_builder.bind_ref(&var, value);
}

/// Binds a `TypedValue::Ref` to a `Variable` with the given name. Takes a keyword as a c string in the format
/// `:namespace/name` and converts it into an `NamespacedKeyword`.
///
/// # Panics
///
/// If the provided keyword does not map to a valid keyword in the schema.
///
/// # Safety
///
/// Callers must ensure that the pointer to the `QueryBuilder` is not dangling and that
/// the C strings provided to `var` and `value` are valid.
#[no_mangle]
pub unsafe extern "C" fn query_builder_bind_ref_kw(query_builder: *mut QueryBuilder, var: *const c_char, value: *const c_char) {
    let var = c_char_to_string(var);
    let kw = kw_from_string(c_char_to_string(value));
    let query_builder = &mut*query_builder;
    if let Some(err) = query_builder.bind_ref_from_kw(&var, kw).err() {
        panic!(err);
    }
}

/// Binds a `TypedValue::Keyword` to a `Variable` with the given name. Takes a keyword as a c string in the format
/// `:namespace/name` and converts it into an `NamespacedKeyword`.
///
/// # Safety
///
/// Callers must ensure that the pointer to the `QueryBuilder` is not dangling and that
/// the C strings provided to `var` and `value` are valid.
#[no_mangle]
pub unsafe extern "C" fn query_builder_bind_kw(query_builder: *mut QueryBuilder, var: *const c_char, value: *const c_char) {
    let var = c_char_to_string(var);
    let query_builder = &mut*query_builder;
    let kw = kw_from_string(c_char_to_string(value));
    query_builder.bind_value(&var, kw);
}

/// Binds a `TypedValue::Boolean` to a `Variable` with the given name.
///
/// # Safety
///
/// Callers must ensure that the pointer to the `QueryBuilder` is not dangling and that
/// the C string provided to `var` is valid.
#[no_mangle]
pub unsafe extern "C" fn query_builder_bind_boolean(query_builder: *mut QueryBuilder, var: *const c_char, value: bool) {
    let var = c_char_to_string(var);
    let query_builder = &mut*query_builder;
    query_builder.bind_value(&var, value);
}

/// Binds a `TypedValue::Double` to a `Variable` with the given name.
///
/// # Safety
///
/// Callers must ensure that the pointer to the `QueryBuilder` is not dangling and that
/// the C string provided to `var` is valid.
#[no_mangle]
pub unsafe extern "C" fn query_builder_bind_double(query_builder: *mut QueryBuilder, var: *const c_char, value: f64) {
    let var = c_char_to_string(var);
    let query_builder = &mut*query_builder;
    query_builder.bind_value(&var, value);
}

/// Binds a `TypedValue::Instant` to a `Variable` with the given name.
/// Takes a timestamp in microseconds.
///
/// # Safety
///
/// Callers must ensure that the pointer to the `QueryBuilder` is not dangling and that
/// the C string provided to `var` is valid.
#[no_mangle]
pub unsafe extern "C" fn query_builder_bind_timestamp(query_builder: *mut QueryBuilder, var: *const c_char, value: c_longlong) {
    let var = c_char_to_string(var);
    let query_builder = &mut*query_builder;
    query_builder.bind_instant(&var, value);
}

/// Binds a `TypedValue::String` to a `Variable` with the given name.
///
/// # Safety
///
/// Callers must ensure that the pointer to the `QueryBuilder` is not dangling and that
/// the C strings provided to `var` and `value` are valid.
#[no_mangle]
pub unsafe extern "C" fn query_builder_bind_string(query_builder: *mut QueryBuilder, var: *const c_char, value: *const c_char) {
    let var = c_char_to_string(var);
    let value = c_char_to_string(value);
    let query_builder = &mut*query_builder;
    query_builder.bind_value(&var, value);
}

/// Binds a `TypedValue::Uuid` to a `Variable` with the given name.
/// Takes a UUID as a byte slice of length 16. This maps directly to the `uuid_t` C type.
///
/// # Safety
///
/// Callers must ensure that the pointer to the `QueryBuilder` is not dangling and that
/// the C string provided to `var` is valid.
#[no_mangle]
pub unsafe extern "C" fn query_builder_bind_uuid(query_builder: *mut QueryBuilder, var: *const c_char, value: *mut [u8; 16]) {
    let var = c_char_to_string(var);
    let value = &*value;
    let value = Uuid::from_bytes(value).expect("valid uuid");
    let query_builder = &mut*query_builder;
    query_builder.bind_value(&var, value);
}

/// Executes a query and returns the results as a Scalar.
///
/// # Panics
///
/// If the query executed is not structured as Scalar.
///
/// # Safety
///
/// Callers must ensure that the pointer to the `QueryBuilder` is not dangling.
///
/// Callers are responsible for managing the memory for the return value.
/// A destructor `destroy` is provided for releasing the memory for this
/// pointer type.
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

/// Executes a query and returns the results as a Coll.
///
/// # Panics
///
/// If the query executed is not structured as Coll.
///
/// # Safety
///
/// Callers must ensure that the pointer to the `QueryBuilder` is not dangling.
///
/// Callers are responsible for managing the memory for the return value.
/// A destructor `destroy` is provided for releasing the memory for this
/// pointer type.
#[no_mangle]
pub unsafe extern "C" fn query_builder_execute_coll(query_builder: *mut QueryBuilder) -> *mut ExternResult {
    let query_builder = &mut*query_builder;
    let results = query_builder.execute_coll();
    Box::into_raw(Box::new(results.into()))
}

/// Executes a query and returns the results as a Tuple.
///
/// # Panics
///
/// If the query executed is not structured as Tuple.
///
/// # Safety
///
/// Callers must ensure that the pointer to the `QueryBuilder` is not dangling.
///
/// Callers are responsible for managing the memory for the return value.
/// A destructor `destroy` is provided for releasing the memory for this
/// pointer type.
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

/// Executes a query and returns the results as a Rel.
///
/// # Panics
///
/// If the query executed is not structured as Rel.
///
/// # Safety
///
/// Callers must ensure that the pointer to the `QueryBuilder` is not dangling.
///
/// Callers are responsible for managing the memory for the return value.
/// A destructor `destroy` is provided for releasing the memory for this
/// pointer type.
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

/// Consumes a `TypedValue` and returns the value as a `long`.
///
/// # Panics
///
/// If the `ValueType` of the `TypedValue` is not `ValueType::Long`.
///
/// # Safety
///
/// Callers must ensure that the pointer to the `TypedValue` is not dangling .
#[no_mangle]
pub unsafe extern "C" fn typed_value_as_long(typed_value: *mut Binding) -> c_longlong {
    let typed_value = Box::from_raw(typed_value);
    unwrap_conversion(typed_value.into_long(), ValueType::Long)
}

/// Consumes a `TypedValue` and returns the value as an `Entid`.
///
/// # Panics
///
/// If the `ValueType` of the `TypedValue` is not `ValueType::Ref`.
///
/// # Safety
///
/// Callers must ensure that the pointer to the `TypedValue` is not dangling .
#[no_mangle]
pub unsafe extern "C" fn typed_value_as_entid(typed_value: *mut Binding) -> Entid {
    let typed_value = Box::from_raw(typed_value);
    unwrap_conversion(typed_value.into_entid(), ValueType::Ref)
}

/// Consumes a `TypedValue` and returns the value as an keyword `String`.
///
/// # Panics
///
/// If the `ValueType` of the `TypedValue` is not `ValueType::Ref`.
///
/// # Safety
///
/// Callers must ensure that the pointer to the `TypedValue` is not dangling .
#[no_mangle]
pub unsafe extern "C" fn typed_value_as_kw(typed_value: *mut Binding) -> *const c_char {
    let typed_value = Box::from_raw(typed_value);
    unwrap_conversion(typed_value.into_kw_c_string(), ValueType::Keyword) as *const c_char
}

/// Consumes a `TypedValue` and returns the value as a boolean represented as an `i32`.
/// If the value of the boolean is `true` the value returned is 1.
/// If the value of the boolean is `false` the value returned is 0.
///
/// # Panics
///
/// If the `ValueType` of the `TypedValue` is not `ValueType::Boolean`.
///
/// # Safety
///
/// Callers must ensure that the pointer to the `TypedValue` is not dangling .
#[no_mangle]
pub unsafe extern "C" fn typed_value_as_boolean(typed_value: *mut Binding) -> i32 {
    let typed_value = Box::from_raw(typed_value);
    if unwrap_conversion(typed_value.into_boolean(), ValueType::Boolean) { 1 } else { 0 }
}

/// Consumes a `TypedValue` and returns the value as a `f64`.
///
/// # Panics
///
/// If the `ValueType` of the `TypedValue` is not `ValueType::Double`.
///
/// # Safety
///
/// Callers must ensure that the pointer to the `TypedValue` is not dangling .
#[no_mangle]
pub unsafe extern "C" fn typed_value_as_double(typed_value: *mut Binding) -> f64 {
    let typed_value = Box::from_raw(typed_value);
    unwrap_conversion(typed_value.into_double(), ValueType::Double)
}

/// Consumes a `TypedValue` and returns the value as a microsecond timestamp.
///
/// # Panics
///
/// If the `ValueType` of the `TypedValue` is not `ValueType::Instant`.
///
/// # Safety
///
/// Callers must ensure that the pointer to the `TypedValue` is not dangling .
#[no_mangle]
pub unsafe extern "C" fn typed_value_as_timestamp(typed_value: *mut Binding) -> c_longlong {
    let typed_value = Box::from_raw(typed_value);
    unwrap_conversion(typed_value.into_timestamp(), ValueType::Instant)
}

/// Consumes a `TypedValue` and returns the value as a `String`.
///
/// # Panics
///
/// If the `ValueType` of the `TypedValue` is not `ValueType::String`.
///
/// # Safety
///
/// Callers must ensure that the pointer to the `TypedValue` is not dangling .
#[no_mangle]
pub unsafe extern "C" fn typed_value_as_string(typed_value: *mut Binding) -> *const c_char {
    let typed_value = Box::from_raw(typed_value);
    unwrap_conversion(typed_value.into_c_string(), ValueType::String) as *const c_char
}

/// Consumes a `TypedValue` and returns the value as a UUID byte slice of length 16.
///
/// # Panics
///
/// If the `ValueType` of the `TypedValue` is not `ValueType::Uuid`.
///
/// # Safety
///
/// Callers must ensure that the pointer to the `TypedValue` is not dangling .
#[no_mangle]
pub unsafe extern "C" fn typed_value_as_uuid(typed_value: *mut Binding) -> *mut [u8; 16] {
    let typed_value = Box::from_raw(typed_value);
    let value = unwrap_conversion(typed_value.into_uuid(), ValueType::Uuid);
    Box::into_raw(Box::new(*value.as_bytes()))
}

/// Returns the `ValueType` of this `TypedValue`.
///
/// # Safety
///
/// Callers must ensure that the pointer to the `TypedValue` is not dangling .
#[no_mangle]
pub unsafe extern "C" fn typed_value_value_type(typed_value: *mut Binding) -> ValueType {
    let typed_value = &*typed_value;
    typed_value.value_type().unwrap_or_else(|| panic!("Binding is not Scalar and has no ValueType"))
}

/// Returns the value at the provided `index` as a `Vec<ValueType>`.
/// If there is no value present at the `index`, a null pointer is returned.
///
/// # Safety
///
/// Callers must ensure that the pointer to the `Vec<Vec<TypedValue>>` is not dangling .
///
/// Callers are responsible for managing the memory for the return value.
/// A destructor `typed_value_result_set_destroy` is provided for releasing the memory for this
/// pointer type.
#[no_mangle]
pub unsafe extern "C" fn row_at_index(rows: *mut RelResult<Binding>, index: c_int) -> *mut Vec<Binding> {
    let result = &*rows;
    result.row(index as usize).map_or_else(std::ptr::null_mut, |v| Box::into_raw(Box::new(v.to_vec())))
}

/// Consumes the `Vec<Vec<TypedValue>>` and returns an iterator over the values.
///
/// # Safety
///
/// Callers must ensure that the pointer to the `Vec<Vec<TypedValue>>` is not dangling .
///
/// Callers are responsible for managing the memory for the return value.
/// A destructor `typed_value_result_set_iter_destroy` is provided for releasing the memory for this
/// pointer type.
#[no_mangle]
pub unsafe extern "C" fn rows_iter(rows: *mut RelResult<Binding>) -> *mut BindingListIterator {
    let result = &*rows;
    let rows = result.rows();
    Box::into_raw(Box::new(rows))
}

/// Returns the next value in the `iter` as a `Vec<ValueType>`.
/// If there is no value next value, a null pointer is returned.
///
/// # Safety
///
/// Callers must ensure that the pointer to the `Vec<Vec<TypedValue>>` is not dangling .
///
/// Callers are responsible for managing the memory for the return value.
/// A destructor `typed_value_list_destroy` is provided for releasing the memory for this
/// pointer type.
#[no_mangle]
pub unsafe extern "C" fn rows_iter_next(iter: *mut BindingListIterator) -> *mut Vec<Binding> {
    let iter = &mut *iter;
    iter.next().map_or(std::ptr::null_mut(), |v| Box::into_raw(Box::new(v.to_vec())))
}

/// Consumes the `Vec<TypedValue>` and returns an iterator over the values.
///
/// # Safety
///
/// Callers must ensure that the pointer to the `Vec<TypedValue>` is not dangling .
///
/// Callers are responsible for managing the memory for the return value.
/// A destructor `typed_value_list_iter_destroy` is provided for releasing the memory for this
/// pointer type.
#[no_mangle]
pub unsafe extern "C" fn values_iter(values: *mut Vec<Binding>) -> *mut BindingIterator {
    let result = Box::from_raw(values);
    Box::into_raw(Box::new(result.into_iter()))
}

/// Returns the next value in the `iter` as a `ValueType`.
/// If there is no value next value, a null pointer is returned.
///
/// # Safety
///
/// Callers must ensure that the pointer to the `Vec<TypedValue>` is not dangling .
///
/// Callers are responsible for managing the memory for the return value.
/// A destructor `typed_value_destroy` is provided for releasing the memory for this
/// pointer type.
#[no_mangle]
pub unsafe extern "C" fn values_iter_next(iter: *mut BindingIterator) -> *mut Binding {
    let iter = &mut *iter;
    iter.next().map_or(std::ptr::null_mut(), |v| Box::into_raw(Box::new(v)))
}

/// Returns the value at the provided `index` as a `ValueType`.
/// If there is no value present at the `index`, a null pointer is returned.
///
/// # Safety
///
/// Callers must ensure that the pointer to the `Vec<TypedValue>` is not dangling .
///
/// Callers are responsible for managing the memory for the return value.
/// A destructor `typed_value_destroy` is provided for releasing the memory for this
/// pointer type.
#[no_mangle]
pub unsafe extern "C" fn value_at_index(values: *mut Vec<Binding>, index: c_int) -> *const Binding {
    let result = &*values;
    result.get(index as usize).expect("No value at index") as *const Binding
}

/// Returns the value of the `TypedValue` at `index` as a `long`.
///
/// # Panics
///
/// If the `ValueType` of the `TypedValue` is not `ValueType::Long`.
/// If there is no value at `index`.
///
/// # Safety
///
/// Callers must ensure that the pointer to the `Vec<TypedValue>` is not dangling .
#[no_mangle]
pub unsafe extern "C" fn value_at_index_as_long(values: *mut Vec<Binding>, index: c_int) -> c_longlong {
    let result = &*values;
    let value = result.get(index as usize).expect("No value at index");
    unwrap_conversion(value.clone().into_long(), ValueType::Long)
}

/// Returns the value of the `TypedValue` at `index` as an `Entid`.
///
/// # Panics
///
/// If the `ValueType` of the `TypedValue` is not `ValueType::Ref`.
/// If there is no value at `index`.
///
/// # Safety
///
/// Callers must ensure that the pointer to the `Vec<TypedValue>` is not dangling .
#[no_mangle]
pub unsafe extern "C" fn value_at_index_as_entid(values: *mut Vec<Binding>, index: c_int) -> Entid {
    let result = &*values;
    let value = result.get(index as usize).expect("No value at index");
    unwrap_conversion(value.clone().into_entid(), ValueType::Ref)
}

/// Returns the value of the `TypedValue` at `index` as a keyword `String`.
///
/// # Panics
///
/// If the `ValueType` of the `TypedValue` is not `ValueType::Ref`.
/// If there is no value at `index`.
///
/// # Safety
///
/// Callers must ensure that the pointer to the `Vec<TypedValue>` is not dangling .
#[no_mangle]
pub unsafe extern "C" fn value_at_index_as_kw(values: *mut Vec<Binding>, index: c_int) -> *const c_char {
    let result = &*values;
    let value = result.get(index as usize).expect("No value at index");
    unwrap_conversion(value.clone().into_kw_c_string(), ValueType::Keyword) as *const c_char
}

/// Returns the value of the `TypedValue` at `index` as a boolean represented by a `i32`.
/// If the value of the `boolean` is `true` then the value returned is 1.
/// If the value of the `boolean` is `false` then the value returned is 0.
///
/// # Panics
///
/// If the `ValueType` of the `TypedValue` is not `ValueType::Long`.
/// If there is no value at `index`.
///
/// # Safety
///
/// Callers must ensure that the pointer to the `Vec<TypedValue>` is not dangling .
#[no_mangle]
pub unsafe extern "C" fn value_at_index_as_boolean(values: *mut Vec<Binding>, index: c_int) -> i32 {
    let result = &*values;
    let value = result.get(index as usize).expect("No value at index");
    if unwrap_conversion(value.clone().into_boolean(), ValueType::Boolean) { 1 } else { 0 }
}

/// Returns the value of the `TypedValue` at `index` as an `f64`.
///
/// # Panics
///
/// If the `ValueType` of the `TypedValue` is not `ValueType::Double`.
/// If there is no value at `index`.
///
/// # Safety
///
/// Callers must ensure that the pointer to the `Vec<TypedValue>` is not dangling .
#[no_mangle]
pub unsafe extern "C" fn value_at_index_as_double(values: *mut Vec<Binding>, index: c_int) -> f64 {
    let result = &*values;
    let value = result.get(index as usize).expect("No value at index");
    unwrap_conversion(value.clone().into_double(), ValueType::Double)
}

/// Returns the value of the `TypedValue` at `index` as a microsecond timestamp.
///
/// # Panics
///
/// If the `ValueType` of the `TypedValue` is not `ValueType::instant`.
/// If there is no value at `index`.
///
/// # Safety
///
/// Callers must ensure that the pointer to the `Vec<TypedValue>` is not dangling .
#[no_mangle]
pub unsafe extern "C" fn value_at_index_as_timestamp(values: *mut Vec<Binding>, index: c_int) -> c_longlong {
    let result = &*values;
    let value = result.get(index as usize).expect("No value at index");
    unwrap_conversion(value.clone().into_timestamp(), ValueType::Instant)
}

/// Returns the value of the `TypedValue` at `index` as an `String`.
///
/// # Panics
///
/// If the `ValueType` of the `TypedValue` is not `ValueType::String`.
/// If there is no value at `index`.
///
/// # Safety
///
/// Callers must ensure that the pointer to the `Vec<TypedValue>` is not dangling .
#[no_mangle]
pub unsafe extern "C" fn value_at_index_as_string(values: *mut Vec<Binding>, index: c_int) -> *const c_char {
    let result = &*values;
    let value = result.get(index as usize).expect("No value at index");
    unwrap_conversion(value.clone().into_c_string(), ValueType::String) as *const c_char
}

/// Returns the value of the `TypedValue` at `index` as a UUID byte slice of length 16.
///
/// # Panics
///
/// If the `ValueType` of the `TypedValue` is not `ValueType::Uuid`.
/// If there is no value at `index`.
///
/// # Safety
///
/// Callers must ensure that the pointer to the `Vec<TypedValue>` is not dangling .
#[no_mangle]
pub unsafe extern "C" fn value_at_index_as_uuid(values: *mut Vec<Binding>, index: c_int) -> *mut [u8; 16] {
    let result = &*values;
    let value = result.get(index as usize).expect("No value at index");
    let uuid = unwrap_conversion(value.clone().into_uuid(), ValueType::Uuid);
    Box::into_raw(Box::new(*uuid.as_bytes()))
}

/// Returns an `ExternResult` containing the `TypedValue` associated with the `attribute` as `:namespace/name`
/// for the given `entid`.
/// If there is a value for that `attribute` on the entity with id `entid` then the value is returned in `ok`.
/// If there no value for that `attribute` on the entity with id `entid` but the attribute is value,
/// then a null pointer is returned in `ok`.
/// If there is no `Attribute` in the `Schema` for the given `attribute` then an error is returned in `err`.
///
/// # Safety
///
/// Callers must ensure that the pointer to the `Store` is not dangling .
///
/// Callers are responsible for managing the memory for the return value.
/// A destructor `destroy` is provided for releasing the memory for this
/// pointer type.
///
/// TODO: list the types of error that can be caused by this function
#[no_mangle]
pub unsafe extern "C" fn store_value_for_attribute(store: *mut Store, entid: c_longlong, attribute: *const c_char) -> *mut ExternResult {
    let store = &*store;
    let kw = kw_from_string(c_char_to_string(attribute));
    let value = match store.lookup_value_for_attribute(entid, &kw) {
        Ok(Some(v)) => {
            let value: Binding = v.into();
            ExternResult { ok: Box::into_raw(Box::new(value)) as *const _ as *const c_void, err: std::ptr::null() }
        },
        Ok(None) => ExternResult { ok: std::ptr::null(), err: std::ptr::null() },
        Err(e) => ExternResult { ok: std::ptr::null(), err: string_to_c_char(e.to_string()) },
    };
    Box::into_raw(Box::new(value))
}

/// Registers a `TxObserver` with the `key` to observe changes to `attributes`
/// on this `store`.
/// Calls `callback` is a relevant transaction occurs.
///
/// # Panics
///
/// If there is no `Attribute` in the `Schema` for a given `attribute`.
///
/// # Safety
///
/// Callers must ensure that the pointer to the `Store` is not dangling, that the
/// C string in `key` is valid and that the function pointer is not dangling.
///
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

/// Unregisters a `TxObserver` with the `key` to observe changes on this `store`.
///
/// # Safety
///
/// Callers must ensure that the pointer to the `Store` is not dangling, that the
/// C string in `key` is valid.
#[no_mangle]
pub unsafe extern "C" fn store_unregister_observer(store: *mut Store, key: *const c_char) {
    let store = &mut*store;
    let key = c_char_to_string(key);
    store.unregister_observer(&key);
}

/// Returns the `Entid` associated with the `attr` as `:namespace/name`.
///
/// # Panics
///
/// If there is no `Attribute` in the `Schema` for `attr`.
///
/// # Safety
///
/// Callers must ensure that the pointer to the `Store` is not dangling, that the
/// C string in `attr` is valid.
#[no_mangle]
pub unsafe extern "C" fn store_entid_for_attribute(store: *mut Store, attr: *const c_char) -> Entid {
    let store = &mut*store;
    let keyword_string = c_char_to_string(attr);
    let kw = kw_from_string(keyword_string);
    let conn = store.conn();
    let current_schema = conn.current_schema();
    current_schema.get_entid(&kw).expect("Unable to find entid for invalid attribute").into()
}

/// Returns the value at the provided `index` as a `TransactionChange`.
///
/// # Panics
///
/// If there is no value present at the `index`.
///
/// # Safety
///
/// Callers must ensure that the pointer to the `TxChangeList` is not dangling .
///
/// Callers are responsible for managing the memory for the return value.
/// A destructor `typed_value_destroy` is provided for releasing the memory for this
/// pointer type.
#[no_mangle]
pub unsafe extern "C" fn tx_change_list_entry_at(tx_report_list: *mut TxChangeList, index: c_int) -> *const TransactionChange {
    let tx_report_list = &*tx_report_list;
    let index = index as usize;
    let report = Box::new(tx_report_list.reports[index].clone());
    Box::into_raw(report)
}

/// Returns the value at the provided `index` as a `Entid`.
///
/// # Panics
///
/// If there is no value present at the `index`.
///
/// # Safety
///
/// Callers must ensure that the pointer to the `TransactionChange` is not dangling .
#[no_mangle]
pub unsafe extern "C" fn changelist_entry_at(tx_report: *mut TransactionChange, index: c_int) -> Entid {
    let tx_report = &*tx_report;
    let index = index as usize;
    tx_report.changes[index].clone()
}

/// destroy function for releasing the memory for `repr(C)` structs.
#[no_mangle]
pub unsafe extern "C" fn destroy(obj: *mut c_void) {
    if !obj.is_null() {
        let _ = Box::from_raw(obj);
    }
}

/// Creates a function with a given `$name` that releases the memroy for a type `$t`.
macro_rules! define_destructor (
    ($name:ident, $t:ty) => (
        #[no_mangle]
        pub unsafe extern "C" fn $name(obj: *mut $t) {
            if !obj.is_null() { let _ = Box::from_raw(obj); }
        }
    )
);

/// Destructor for releasing the memory of `QueryBuilder`.
define_destructor!(query_builder_destroy, QueryBuilder);

/// Destructor for releasing the memory of `Store`.
define_destructor!(store_destroy, Store);

/// Destructor for releasing the memory of `TxReport`.
define_destructor!(tx_report_destroy, TxReport);

define_destructor!(typed_value_destroy, Binding);

define_destructor!(typed_value_list_destroy, Vec<Binding>);

define_destructor!(typed_value_list_iter_destroy, BindingIterator);

define_destructor!(typed_value_result_set_destroy, RelResult<Binding>);

define_destructor!(typed_value_result_set_iter_destroy, BindingListIterator);
