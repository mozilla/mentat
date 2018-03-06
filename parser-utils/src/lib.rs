// Copyright 2016 Mozilla
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

extern crate combine;
extern crate edn;
extern crate itertools;

/// A `ValueParseError` is a `combine::primitives::ParseError`-alike that implements the `Debug`,
/// `Display`, and `std::error::Error` traits.  In addition, it doesn't capture references, making
/// it possible to store `ValueParseError` instances in local links with the `error-chain` crate.
///
/// This is achieved by wrapping slices of type `&'a [edn::Value]` in an owning type that implements
/// `Display`; rather than introducing a newtype like `DisplayVec`, we re-use `edn::Value::Vector`.
#[derive(PartialEq)]
pub struct ValueParseError {
    pub position: edn::Span,
    // Think of this as `Vec<Error<edn::Value, DisplayVec<edn::Value>>>`; see above.
    pub errors: Vec<combine::primitives::Error<edn::ValueAndSpan, edn::ValueAndSpan>>,
}

#[macro_use]
pub mod macros;

pub use macros::{
    KeywordMapParser,
    ResultParser,
};

pub mod log;
pub mod value_and_span;
pub use value_and_span::{
    Stream,
};

impl std::fmt::Debug for ValueParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f,
               "ParseError {{ position: {:?}, errors: {:?} }}",
               self.position,
               self.errors)
    }
}

impl std::fmt::Display for ValueParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        try!(writeln!(f, "Parse error at {:?}", self.position));
        combine::primitives::Error::fmt_errors(&self.errors, f)
    }
}

impl std::error::Error for ValueParseError {
    fn description(&self) -> &str {
        "parse error parsing EDN values"
    }
}

impl<'a> From<combine::primitives::ParseError<Stream<'a>>> for ValueParseError {
    fn from(e: combine::primitives::ParseError<Stream<'a>>) -> ValueParseError {
        ValueParseError {
            position: e.position.0,
            errors: e.errors.into_iter()
                .map(|e| e.map_token(|t| t.clone()).map_range(|r| r.clone()))
                .collect(),
        }
    }
}
