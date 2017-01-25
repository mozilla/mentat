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
#[macro_use]
extern crate lazy_static;
extern crate num;
extern crate ordered_float;
extern crate rusqlite;

extern crate edn;
extern crate mentat_tx;
extern crate mentat_tx_parser;

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

pub fn to_namespaced_keyword(s: &str) -> Option<symbols::NamespacedKeyword> {
    let splits = [':', '/'];
    let mut i = s.split(&splits[..]);
    match (i.next(), i.next(), i.next(), i.next()) {
        (Some(""), Some(namespace), Some(name), None) => Some(symbols::NamespacedKeyword::new(namespace, name)),
        _ => None
    }
}
