// Copyright 2018 Mozilla
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

#![allow(dead_code)]

use failure::Error;

#[macro_export]
macro_rules! bail {
    ($e:expr) => (
        return Err($e.into());
    )
}

pub type Result<T> = ::std::result::Result<T, Error>;

#[derive(Debug, Fail)]
pub enum TolstoyError {
    #[fail(display = "Received bad response from the server: {}", _0)]
    BadServerResponse(String),

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
}
