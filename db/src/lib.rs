// Copyright 2016 Mozilla
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

extern crate failure;
#[macro_use] extern crate failure_derive;
extern crate indexmap;
extern crate itertools;
#[macro_use] extern crate lazy_static;
#[macro_use] extern crate log;

#[cfg(feature = "syncable")]
#[macro_use] extern crate serde_derive;

extern crate petgraph;
extern crate rusqlite;
extern crate tabwriter;
extern crate time;

#[macro_use] extern crate edn;
#[macro_use] extern crate mentat_core;
extern crate mentat_sql;

use std::iter::repeat;

use itertools::Itertools;

pub use errors::{
    DbError,
    DbErrorKind,
    Result,
    SchemaConstraintViolation,
};
#[macro_use] pub mod errors;

#[macro_use] pub mod debug;

mod add_retract_alter_set;
pub mod cache;
pub mod db;
mod bootstrap;
pub mod entids;
pub mod internal_types;    // pub because we need them for building entities programmatically.
mod metadata;
mod schema;
pub mod tx_observer;
mod watcher;
pub mod timelines;
mod tx;
mod tx_checking;
pub mod types;
mod upsert_resolution;

// Export these for reference from tests. cfg(test) should work, but doesn't.
// #[cfg(test)]
pub use bootstrap::{
    TX0,
    USER0,
};

pub static TIMELINE_MAIN: i64 = 0;

pub use schema::{
    AttributeBuilder,
    AttributeValidation,
};

pub use bootstrap::{
    CORE_SCHEMA_VERSION,
};

use edn::symbols;

pub use entids::{
    DB_SCHEMA_CORE,
};

pub use db::{
    TypedSQLValue,
    new_connection,
};

#[cfg(feature = "sqlcipher")]
pub use db::{
    new_connection_with_key,
    change_encryption_key,
};

pub use watcher::{
    TransactWatcher,
};

pub use tx::{
    transact,
    transact_terms,
};

pub use tx_observer::{
    InProgressObserverTransactWatcher,
    TxObservationService,
    TxObserver,
};

pub use types::{
    AttributeSet,
    DB,
    PartitionMap,
    TransactableValue,
};

pub fn to_namespaced_keyword(s: &str) -> Result<symbols::Keyword> {
    let splits = [':', '/'];
    let mut i = s.split(&splits[..]);
    let nsk = match (i.next(), i.next(), i.next(), i.next()) {
        (Some(""), Some(namespace), Some(name), None) => Some(symbols::Keyword::namespaced(namespace, name)),
        _ => None,
    };

    nsk.ok_or(DbErrorKind::NotYetImplemented(format!("InvalidKeyword: {}", s)).into())
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
