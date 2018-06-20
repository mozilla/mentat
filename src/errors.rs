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

use failure::Error;

use mentat_core::{
    Attribute,
    ValueType,
};

use mentat_query;

pub type Result<T> = std::result::Result<T, Error>;

#[macro_export]
macro_rules! bail {
    ($e:expr) => (
        return Err($e.into());
    )
}

#[derive(Debug, Fail)]
pub enum MentatError {
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
}
