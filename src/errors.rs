// Copyright 2016 Mozilla
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

#![allow(dead_code)]

use std; // To refer to std::result::Result.

use std::collections::BTreeSet;

use rusqlite;
use failure::{
    Backtrace,
    Context,
    Fail,
};
use std::fmt;
use edn;

use mentat_core::{
    Attribute,
    ValueType,
};

use mentat_db;
use mentat_query;
use mentat_query_algebrizer;
use mentat_query_projector;
use mentat_query_pull;
use mentat_sql;

#[cfg(feature = "syncable")]
use mentat_tolstoy;

pub type Result<T> = std::result::Result<T, MentatError>;

#[macro_export]
macro_rules! bail {
    ($e:expr) => (
        return Err($e.into());
    )
}

#[derive(Debug)]
pub struct MentatError(Box<Context<MentatErrorKind>>);

impl Fail for MentatError {
    #[inline]
    fn cause(&self) -> Option<&Fail> {
        self.0.cause()
    }

    #[inline]
    fn backtrace(&self) -> Option<&Backtrace> {
        self.0.backtrace()
    }
}

impl fmt::Display for MentatError {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        fmt::Display::fmt(&*self.0, f)
    }
}

impl MentatError {
    #[inline]
    pub fn kind(&self) -> &MentatErrorKind {
        &*self.0.get_context()
    }
}

impl From<MentatErrorKind> for MentatError {
    #[inline]
    fn from(kind: MentatErrorKind) -> MentatError {
        MentatError(Box::new(Context::new(kind)))
    }
}

impl From<Context<MentatErrorKind>> for MentatError {
    #[inline]
    fn from(inner: Context<MentatErrorKind>) -> MentatError {
        MentatError(Box::new(inner))
    }
}

#[derive(Debug, Fail)]
pub enum MentatErrorKind {
    #[fail(display = "bad uuid {}", _0)]
    BadUuid(String),

    #[fail(display = "path {} already exists", _0)]
    PathAlreadyExists(String),

    #[fail(display = "variables {:?} unbound at query execution time", _0)]
    UnboundVariables(BTreeSet<String>),

    #[fail(display = "invalid argument name: '{}'", _0)]
    InvalidArgumentName(String),

    #[fail(display = "unknown attribute: '{}'", _0)]
    UnknownAttribute(String),

    #[fail(display = "invalid vocabulary version")]
    InvalidVocabularyVersion,

    #[fail(display = "vocabulary {}/{} already has attribute {}, and the requested definition differs", _0, _1, _2)]
    ConflictingAttributeDefinitions(String, ::vocabulary::Version, String, Attribute, Attribute),

    #[fail(display = "existing vocabulary {} too new: wanted {}, got {}", _0, _1, _2)]
    ExistingVocabularyTooNew(String, ::vocabulary::Version, ::vocabulary::Version),

    #[fail(display = "core schema: wanted {}, got {:?}", _0, _1)]
    UnexpectedCoreSchema(::vocabulary::Version, Option<::vocabulary::Version>),

    #[fail(display = "Lost the transact() race!")]
    UnexpectedLostTransactRace,

    #[fail(display = "missing core attribute {}", _0)]
    MissingCoreVocabulary(mentat_query::Keyword),

    #[fail(display = "schema changed since query was prepared")]
    PreparedQuerySchemaMismatch,

    #[fail(display = "provided value of type {} doesn't match attribute value type {}", _0, _1)]
    ValueTypeMismatch(ValueType, ValueType),

    #[fail(display = "{}", _0)]
    IoError(#[cause] std::io::Error),

    // It would be better to capture the underlying `rusqlite::Error`, but that type doesn't
    // implement many useful traits, including `Clone`, `Eq`, and `PartialEq`.
    #[fail(display = "SQL error: {}", _0)]
    RusqliteError(String),

    #[fail(display = "{}", _0)]
    EdnParseError(#[cause] edn::ParseError),

    #[fail(display = "{}", _0)]
    DbError(#[cause] mentat_db::DbError),

    #[fail(display = "{}", _0)]
    AlgebrizerError(#[cause] mentat_query_algebrizer::AlgebrizerError),

    #[fail(display = "{}", _0)]
    ProjectorError(#[cause] mentat_query_projector::ProjectorError),

    #[fail(display = "{}", _0)]
    PullError(#[cause] mentat_query_pull::PullError),

    #[fail(display = "{}", _0)]
    SQLError(#[cause] mentat_sql::SQLError),

    #[cfg(feature = "syncable")]
    #[fail(display = "{}", _0)]
    TolstoyError(#[cause] mentat_tolstoy::TolstoyError),
}

impl From<std::io::Error> for MentatErrorKind {
    fn from(error: std::io::Error) -> MentatErrorKind {
        MentatErrorKind::IoError(error)
    }
}

impl From<rusqlite::Error> for MentatErrorKind {
    fn from(error: rusqlite::Error) -> MentatErrorKind {
        MentatErrorKind::RusqliteError(error.to_string())
    }
}

impl From<edn::ParseError> for MentatErrorKind {
    fn from(error: edn::ParseError) -> MentatErrorKind {
        MentatErrorKind::EdnParseError(error)
    }
}

impl From<mentat_db::DbError> for MentatErrorKind {
    fn from(error: mentat_db::DbError) -> MentatErrorKind {
        MentatErrorKind::DbError(error)
    }
}

impl From<mentat_query_algebrizer::AlgebrizerError> for MentatErrorKind {
    fn from(error: mentat_query_algebrizer::AlgebrizerError) -> MentatErrorKind {
        MentatErrorKind::AlgebrizerError(error)
    }
}

impl From<mentat_query_projector::ProjectorError> for MentatErrorKind {
    fn from(error: mentat_query_projector::ProjectorError) -> MentatErrorKind {
        MentatErrorKind::ProjectorError(error)
    }
}

impl From<mentat_query_pull::PullError> for MentatErrorKind {
    fn from(error: mentat_query_pull::PullError) -> MentatErrorKind {
        MentatErrorKind::PullError(error)
    }
}

impl From<mentat_sql::SQLError> for MentatErrorKind {
    fn from(error: mentat_sql::SQLError) -> MentatErrorKind {
        MentatErrorKind::SQLError(error)
    }
}

#[cfg(feature = "syncable")]
impl From<mentat_tolstoy::TolstoyError> for MentatErrorKind {
    fn from(error: mentat_tolstoy::TolstoyError) -> MentatErrorKind {
        MentatErrorKind::TolstoyError(error)
    }
}

// XXX reduce dupe if this isn't completely throwaway

impl From<std::io::Error> for MentatError {
    fn from(error: std::io::Error) -> MentatError {
        MentatErrorKind::from(error).into()
    }
}

impl From<rusqlite::Error> for MentatError {
    fn from(error: rusqlite::Error) -> MentatError {
        MentatErrorKind::from(error).into()
    }
}

impl From<edn::ParseError> for MentatError {
    fn from(error: edn::ParseError) -> MentatError {
        MentatErrorKind::from(error).into()
    }
}

impl From<mentat_db::DbError> for MentatError {
    fn from(error: mentat_db::DbError) -> MentatError {
        MentatErrorKind::from(error).into()
    }
}

impl From<mentat_query_algebrizer::AlgebrizerError> for MentatError {
    fn from(error: mentat_query_algebrizer::AlgebrizerError) -> MentatError {
        MentatErrorKind::from(error).into()
    }
}

impl From<mentat_query_projector::ProjectorError> for MentatError {
    fn from(error: mentat_query_projector::ProjectorError) -> MentatError {
        MentatErrorKind::from(error).into()
    }
}

impl From<mentat_query_pull::PullError> for MentatError {
    fn from(error: mentat_query_pull::PullError) -> MentatError {
        MentatErrorKind::from(error).into()
    }
}

impl From<mentat_sql::SQLError> for MentatError {
    fn from(error: mentat_sql::SQLError) -> MentatError {
        MentatErrorKind::from(error).into()
    }
}

#[cfg(feature = "syncable")]
impl From<mentat_tolstoy::TolstoyError> for MentatError {
    fn from(error: mentat_tolstoy::TolstoyError) -> MentatError {
        MentatErrorKind::from(error).into()
    }
}
