// Copyright 2016 Mozilla
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

#![recursion_limit="128"]

#[macro_use]
extern crate error_chain;

#[macro_use]
extern crate lazy_static;

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

pub use mentat_core::{
    Attribute,
    Entid,
    HasSchema,
    NamespacedKeyword,
    TypedValue,
    Uuid,
    ValueType,
};

pub use mentat_query::{
    FindSpec,
};

pub use mentat_db::{
    CORE_SCHEMA_VERSION,
    DB_SCHEMA_CORE,
    TxReport,
    new_connection,
};

/// Produce the appropriate `NamespacedKeyword` for the provided namespace and name.
/// This lives here because we can't re-export macros:
/// https://github.com/rust-lang/rust/issues/29638.
#[macro_export]
macro_rules! kw {
    ( : $ns:ident / $n:ident ) => {
        // We don't need to go through `new` -- `ident` is strict enough.
        $crate::NamespacedKeyword {
            namespace: stringify!($ns).into(),
            name: stringify!($n).into(),
        }
    };

    ( : $ns:ident$(. $nss:ident)+ / $n:ident ) => {
        // We don't need to go through `new` -- `ident` is strict enough.
        $crate::NamespacedKeyword {
            namespace: concat!(stringify!($ns) $(, ".", stringify!($nss))+).into(),
            name: stringify!($n).into(),
        }
    };
}

pub mod errors;
pub mod ident;
pub mod vocabulary;
pub mod conn;
pub mod query;
pub mod entity_builder;

pub fn get_name() -> String {
    return String::from("mentat");
}

pub use query::{
    IntoResult,
    PlainSymbol,
    QueryExplanation,
    QueryInputs,
    QueryOutput,
    QueryPlanStep,
    QueryResults,
    Variable,
    q_once,
};

pub use conn::{
    Conn,
    InProgress,
    Metadata,
    Queryable,
    Store,
};

#[cfg(test)]
mod tests {
    use edn::symbols::Keyword;
    use super::*;

    #[test]
    fn can_import_edn() {
        assert_eq!("foo", Keyword::new("foo").0);
    }

    #[test]
    fn test_kw() {
        assert_eq!(kw!(:foo/bar), NamespacedKeyword::new("foo", "bar"));
        assert_eq!(kw!(:org.mozilla.foo/bar_baz), NamespacedKeyword::new("org.mozilla.foo", "bar_baz"));
    }
}
