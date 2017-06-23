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

extern crate rusqlite;

extern crate edn;
extern crate mentat_core;
extern crate mentat_db;
extern crate mentat_query;
extern crate mentat_query_algebrizer;
extern crate mentat_query_parser;
extern crate mentat_query_projector;
extern crate mentat_query_translator;
extern crate mentat_sql;
extern crate mentat_tx;
extern crate mentat_tx_parser;

use rusqlite::Connection;

pub mod errors;
pub mod ident;
pub mod conn;
pub mod query;

pub fn get_name() -> String {
    return String::from("mentat");
}

// Will ultimately not return the sqlite connection directly
pub fn get_connection() -> Connection {
    return Connection::open_in_memory().unwrap();
}

pub use mentat_core::{
    TypedValue,
    ValueType,
};

pub use mentat_db::{
    new_connection,
};

pub use query::{
    NamespacedKeyword,
    PlainSymbol,
    QueryInputs,
    QueryResults,
    Variable,
    q_once,
};

pub use conn::{
    Conn,
    Metadata,
};

#[cfg(test)]
mod tests {
    use edn::symbols::Keyword;

    #[test]
    fn can_import_edn() {
        assert_eq!("foo", Keyword::new("foo").0);
    }
}
