// Copyright 2016 Mozilla
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

// Oh, error_chain.
#![recursion_limit="128"]

#[macro_use]
extern crate error_chain;
extern crate itertools;

#[macro_use]
extern crate lazy_static;
extern crate rusqlite;
extern crate tabwriter;
extern crate time;

#[macro_use]
extern crate edn;
extern crate mentat_core;
extern crate mentat_tx;
extern crate mentat_tx_parser;

use std::iter::repeat;

use itertools::Itertools;

pub use errors::{Error, ErrorKind, ResultExt, Result};

pub mod db;
mod bootstrap;
pub mod debug;
mod add_retract_alter_set;
pub mod entids;
pub mod errors;
mod metadata;
mod schema;
pub mod types;
pub mod internal_types;    // pub because we need them for building entities programmatically.
mod upsert_resolution;
mod tx;

// Export these for reference from tests. cfg(test) should work, but doesn't.
// #[cfg(test)]
pub use bootstrap::{
    TX0,
    USER0,
};

pub use schema::{
    AttributeBuilder,
    AttributeValidation,
};

pub use bootstrap::{
    CORE_SCHEMA_VERSION,
};

pub use entids::{
    DB_SCHEMA_CORE,
};

pub use db::{
    TypedSQLValue,
    new_connection,
};

pub use tx::{
    transact,
    transact_terms,
};

pub use types::{
    DB,
    PartitionMap,
    TxReport,
};

use edn::symbols;

pub fn to_namespaced_keyword(s: &str) -> Result<symbols::NamespacedKeyword> {
    let splits = [':', '/'];
    let mut i = s.split(&splits[..]);
    let nsk = match (i.next(), i.next(), i.next(), i.next()) {
        (Some(""), Some(namespace), Some(name), None) => Some(symbols::NamespacedKeyword::new(namespace, name)),
        _ => None,
    };

    // TODO Use custom ErrorKind https://github.com/brson/error-chain/issues/117
    nsk.ok_or(ErrorKind::NotYetImplemented(format!("InvalidNamespacedKeyword: {}", s)).into())
}

/// Prepare an SQL `VALUES` block, like (?, ?, ?), (?, ?, ?).
///
/// The number of values per tuple determines  `(?, ?, ?)`.  The number of tuples determines `(...), (...)`.
///
/// # Examples
///
/// ```rust
/// # use mentat_db::{repeat_values};
/// assert_eq!(repeat_values(1, 3), "(?), (?), (?)".to_string());
/// assert_eq!(repeat_values(3, 1), "(?, ?, ?)".to_string());
/// assert_eq!(repeat_values(2, 2), "(?, ?), (?, ?)".to_string());
/// ```
pub fn repeat_values(values_per_tuple: usize, tuples: usize) -> String {
    assert!(values_per_tuple >= 1);
    assert!(tuples >= 1);
    // Like "(?, ?, ?)".
    let inner = format!("({})", repeat("?").take(values_per_tuple).join(", "));
    // Like "(?, ?, ?), (?, ?, ?)".
    let values: String = repeat(inner).take(tuples).join(", ");
    values
}
