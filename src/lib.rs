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
extern crate failure_derive;
extern crate failure;

#[macro_use]
extern crate lazy_static;

extern crate rusqlite;

extern crate uuid;

pub extern crate edn;
extern crate mentat_core;
extern crate mentat_db;
extern crate mentat_query;
extern crate mentat_query_algebrizer;
extern crate mentat_query_projector;
extern crate mentat_query_pull;
extern crate mentat_query_translator;
extern crate mentat_sql;
extern crate mentat_tolstoy;

pub use mentat_core::{
    Attribute,
    Entid,
    DateTime,
    HasSchema,
    KnownEntid,
    Keyword,
    Schema,
    Binding,
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
    AttributeSet,
    TxObserver,
    TxReport,
    new_connection,
};

#[cfg(feature = "sqlcipher")]
pub use mentat_db::{
    new_connection_with_key,
    change_encryption_key,
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

/// Produce the appropriate `Keyword` for the provided namespace and name.
/// This lives here because we can't re-export macros:
/// https://github.com/rust-lang/rust/issues/29638.
#[macro_export]
macro_rules! kw {
    ( : $n:ident ) => {
        $crate::Keyword::plain(
            stringify!($n)
        )
    };

    ( : $ns:ident / $n:ident ) => {
        $crate::Keyword::namespaced(
            stringify!($ns),
            stringify!($n)
        )
    };

    ( : $ns:ident$(. $nss:ident)+ / $n:ident ) => {
        $crate::Keyword::namespaced(
            concat!(stringify!($ns) $(, ".", stringify!($nss))+),
            stringify!($n)
        )
    };
}

#[macro_use]
pub mod errors;
pub mod conn;
pub mod entity_builder;
pub mod ident;
pub mod query;
pub mod query_builder;
pub mod store;
pub mod vocabulary;

pub use query::{
    IntoResult,
    PlainSymbol,
    QueryExecutionResult,
    QueryExplanation,
    QueryInputs,
    QueryOutput,
    QueryPlanStep,
    QueryResults,
    RelResult,
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
    Pullable,
    Queryable,
    Syncable,
};

pub use store::{
    Store,
};

#[cfg(test)]
mod tests {
    use edn::symbols::Keyword;
    use super::*;

    #[test]
    fn can_import_edn() {
        assert_eq!(":foo", &Keyword::plain("foo").to_string());
    }

    #[test]
    fn test_kw() {
        assert_eq!(kw!(:foo/bar), Keyword::namespaced("foo", "bar"));
        assert_eq!(kw!(:org.mozilla.foo/bar_baz), Keyword::namespaced("org.mozilla.foo", "bar_baz"));
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
