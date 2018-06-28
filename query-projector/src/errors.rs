// Copyright 2018 Mozilla
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

use std; // To refer to std::result::Result.

use rusqlite;

use mentat_core::{
    ValueTypeSet,
};
use mentat_db;
use mentat_query::{
    PlainSymbol,
};
use mentat_query_pull;

use aggregates::{
    SimpleAggregationOp,
};

#[macro_export]
macro_rules! bail {
    ($e:expr) => (
        return Err($e.into());
    )
}

pub type Result<T> = std::result::Result<T, ProjectorError>;

#[derive(Debug, Fail)]
pub enum ProjectorError {
    /// We're just not done yet.  Message that the feature is recognized but not yet
    /// implemented.
    #[fail(display = "not yet implemented: {}", _0)]
    NotYetImplemented(String),

    #[fail(display = "no possible types for value provided to {:?}", _0)]
    CannotProjectImpossibleBinding(SimpleAggregationOp),

    #[fail(display = "cannot apply projection operation {:?} to types {:?}", _0, _1)]
    CannotApplyAggregateOperationToTypes(SimpleAggregationOp, ValueTypeSet),

    #[fail(display = "invalid projection: {}", _0)]
    InvalidProjection(String),

    #[fail(display = "cannot project unbound variable {:?}", _0)]
    UnboundVariable(PlainSymbol),

    #[fail(display = "cannot find type for variable {:?}", _0)]
    NoTypeAvailableForVariable(PlainSymbol),

    #[fail(display = "expected {}, got {}", _0, _1)]
    UnexpectedResultsType(&'static str, &'static str),

    #[fail(display = "expected tuple of length {}, got tuple of length {}", _0, _1)]
    UnexpectedResultsTupleLength(usize, usize),

    #[fail(display = "min/max expressions: {} (max 1), corresponding: {}", _0, _1)]
    AmbiguousAggregates(usize, usize),

    // It would be better to capture the underlying `rusqlite::Error`, but that type doesn't
    // implement many useful traits, including `Clone`, `Eq`, and `PartialEq`.
    #[fail(display = "SQL error: {}", _0)]
    RusqliteError(String),

    #[fail(display = "{}", _0)]
    DbError(#[cause] mentat_db::DbError),

    #[fail(display = "{}", _0)]
    PullError(#[cause] mentat_query_pull::PullError),
}

impl From<rusqlite::Error> for ProjectorError {
    fn from(error: rusqlite::Error) -> ProjectorError {
        ProjectorError::RusqliteError(error.to_string())
    }
}

impl From<mentat_db::DbError> for ProjectorError {
    fn from(error: mentat_db::DbError) -> ProjectorError {
        ProjectorError::DbError(error)
    }
}

impl From<mentat_query_pull::PullError> for ProjectorError {
    fn from(error: mentat_query_pull::PullError) -> ProjectorError {
        ProjectorError::PullError(error)
    }
}
