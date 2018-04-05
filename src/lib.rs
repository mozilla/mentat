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

extern crate uuid;

pub extern crate edn;
extern crate mentat_core;
extern crate mentat_db;
extern crate mentat_query;
extern crate mentat_query_algebrizer;
extern crate mentat_query_parser;
extern crate mentat_query_projector;
extern crate mentat_query_translator;
extern crate mentat_sql;
extern crate mentat_tolstoy;
extern crate mentat_tx;
extern crate mentat_tx_parser;

pub use mentat_core::{
    Attribute,
    Entid,
    DateTime,
    HasSchema,
    KnownEntid,
    NamespacedKeyword,
    Schema,
    TypedValue,
    Uuid,
    Utc,
    ValueType,
};

pub use mentat_query::{
    FindSpec,
};

pub use mentat_db::{
    CORE_SCHEMA_VERSION,
    DB_SCHEMA_CORE,
    TxObserver,
    TxReport,
    new_connection,
};

/// Produce the appropriate `Variable` for the provided valid ?-prefixed name.
/// This lives here because we can't re-export macros:
/// https://github.com/rust-lang/rust/issues/29638.
#[macro_export]
macro_rules! var {
    ( ? $var:ident ) => {
        $crate::Variable::from_valid_name(concat!("?", stringify!($var)))
    };
}

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
pub mod query_builder;

pub use query::{
    IntoResult,
    PlainSymbol,
    QueryExecutionResult,
    QueryExplanation,
    QueryInputs,
    QueryOutput,
    QueryPlanStep,
    QueryResults,
    Variable,
    q_once,
};

pub use query_builder::{
    QueryBuilder,
};

pub use conn::{
    CacheAction,
    CacheDirection,
    Conn,
    InProgress,
    Metadata,
    Queryable,
    Syncable,
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

    #[test]
    fn test_var() {
        let foo_baz = var!(?foo_baz);
        let vu = var!(?vü);
        assert_eq!(foo_baz, Variable::from_valid_name("?foo_baz"));
        assert_eq!(vu, Variable::from_valid_name("?vü"));
        assert_eq!(foo_baz.as_str(), "?foo_baz");
    }
}
