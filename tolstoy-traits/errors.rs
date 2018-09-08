// Copyright 2018 Mozilla
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

use std;
use std::error::Error;
use rusqlite;
use uuid;
use hyper;
use serde_json;

use db_traits::errors::{
    DbError,
};

#[derive(Debug, Fail)]
pub enum TolstoyError {
    #[fail(display = "Received bad response from the remote: {}", _0)]
    BadRemoteResponse(String),

    // TODO expand this into concrete error types
    #[fail(display = "Received bad remote state: {}", _0)]
    BadRemoteState(String),

    #[fail(display = "encountered more than one metadata value for key: {}", _0)]
    DuplicateMetadata(String),

    #[fail(display = "transaction processor didn't say it was done")]
    TxProcessorUnfinished,

    #[fail(display = "expected one, found {} uuid mappings for tx", _0)]
    TxIncorrectlyMapped(usize),

    #[fail(display = "encountered unexpected state: {}", _0)]
    UnexpectedState(String),

    #[fail(display = "not yet implemented: {}", _0)]
    NotYetImplemented(String),

    #[fail(display = "{}", _0)]
    DbError(#[cause] DbError),

    #[fail(display = "{}", _0)]
    SerializationError(#[cause] serde_json::Error),

    // It would be better to capture the underlying `rusqlite::Error`, but that type doesn't
    // implement many useful traits, including `Clone`, `Eq`, and `PartialEq`.
    #[fail(display = "SQL error: {}, cause: {}", _0, _1)]
    RusqliteError(String, String),

    #[fail(display = "{}", _0)]
    IoError(#[cause] std::io::Error),

    #[fail(display = "{}", _0)]
    UuidError(#[cause] uuid::ParseError),

    #[fail(display = "{}", _0)]
    NetworkError(#[cause] hyper::Error),

    #[fail(display = "{}", _0)]
    UriError(#[cause] hyper::error::UriError),
}

impl From<DbError> for TolstoyError {
    fn from(error: DbError) -> TolstoyError {
        TolstoyError::DbError(error)
    }
}

impl From<serde_json::Error> for TolstoyError {
    fn from(error: serde_json::Error) -> TolstoyError {
        TolstoyError::SerializationError(error)
    }
}

impl From<rusqlite::Error> for TolstoyError {
    fn from(error: rusqlite::Error) -> TolstoyError {
        let cause = match error.cause() {
            Some(e) => e.to_string(),
            None => "".to_string()
        };
        TolstoyError::RusqliteError(error.to_string(), cause)
    }
}

impl From<std::io::Error> for TolstoyError {
    fn from(error: std::io::Error) -> TolstoyError {
        TolstoyError::IoError(error)
    }
}

impl From<uuid::ParseError> for TolstoyError {
    fn from(error: uuid::ParseError) -> TolstoyError {
        TolstoyError::UuidError(error)
    }
}

impl From<hyper::Error> for TolstoyError {
    fn from(error: hyper::Error) -> TolstoyError {
        TolstoyError::NetworkError(error)
    }
}

impl From<hyper::error::UriError> for TolstoyError {
    fn from(error: hyper::error::UriError) -> TolstoyError {
        TolstoyError::UriError(error)
    }
}
