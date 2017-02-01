// Copyright 2016 Mozilla
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

#[macro_use]
extern crate error_chain;
extern crate itertools;
#[macro_use]
extern crate lazy_static;
extern crate rusqlite;
extern crate time;

extern crate tabwriter;

extern crate edn;
extern crate mentat_tx;
extern crate mentat_tx_parser;

use itertools::Itertools;
use std::iter::repeat;

pub use errors::*;
pub use schema::*;
pub use types::*;

pub mod db;
mod bootstrap;
mod debug;
mod entids;
mod errors;
mod schema;
mod types;
mod values;

use edn::symbols;

pub const SQLITE_MAX_VARIABLE_NUMBER: usize = 999;

pub fn to_namespaced_keyword(s: &str) -> Option<symbols::NamespacedKeyword> {
    let splits = [':', '/'];
    let mut i = s.split(&splits[..]);
    match (i.next(), i.next(), i.next(), i.next()) {
        (Some(""), Some(namespace), Some(name), None) => Some(symbols::NamespacedKeyword::new(namespace, name)),
        _ => None
    }
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
    assert!(values_per_tuple * tuples < SQLITE_MAX_VARIABLE_NUMBER, "Too many values: {} * {} >= {}", values_per_tuple, tuples, SQLITE_MAX_VARIABLE_NUMBER);
    // Like "(?, ?, ?)".
    let inner = format!("({})", repeat("?").take(values_per_tuple).join(", "));
    // Like "(?, ?, ?), (?, ?, ?)".
    let values: String = repeat(inner).take(tuples).join(", ");
    values
}
