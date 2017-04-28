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

#[macro_use]
pub mod macros;

pub mod log;
pub mod value_and_span;

pub use log::{
    LogParsing,
};

/// `assert_parses_to!` simplifies some of the boilerplate around running a
/// parser function against input and expecting a certain result.
#[macro_export]
macro_rules! assert_parses_to {
    ( $parser: expr, $input: expr, $expected: expr ) => {{
        let par = $parser();
        let result = par.skip(eof()).parse($input.with_spans().into_atom_stream()).map(|x| x.0);
        assert_eq!(result, Ok($expected));
    }}
}

/// `assert_edn_parses_to!` simplifies some of the boilerplate around running a parser function
/// against string input and expecting a certain result.
#[macro_export]
macro_rules! assert_edn_parses_to {
    ( $parser: expr, $input: expr, $expected: expr ) => {{
        let par = $parser();
        let input = edn::parse::value($input).expect("to be able to parse input as EDN");
        let result = par.skip(eof()).parse(input.into_atom_stream()).map(|x| x.0);
        assert_eq!(result, Ok($expected));
    }}
}

/// A `ValueParseError` is a `combine::primitives::ParseError`-alike that implements the `Debug`,
/// `Display`, and `std::error::Error` traits.  In addition, it doesn't capture references, making
/// it possible to store `ValueParseError` instances in local links with the `error-chain` crate.
///
/// This is achieved by wrapping slices of type `&'a [edn::Value]` in an owning type that implements
/// `Display`; rather than introducing a newtype like `DisplayVec`, we re-use `edn::Value::Vector`.
#[derive(PartialEq)]
pub struct ValueParseError {
    pub position: usize,
    // Think of this as `Vec<Error<edn::Value, DisplayVec<edn::Value>>>`; see above.
    pub errors: Vec<combine::primitives::Error<edn::Value, edn::Value>>,
}

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
        try!(writeln!(f, "Parse error at {}", self.position));
        combine::primitives::Error::fmt_errors(&self.errors, f)
    }
}

impl std::error::Error for ValueParseError {
    fn description(&self) -> &str {
        "parse error parsing EDN values"
    }
}

impl<'a> From<combine::primitives::ParseError<&'a [edn::Value]>> for ValueParseError {
    fn from(e: combine::primitives::ParseError<&'a [edn::Value]>) -> ValueParseError {
        ValueParseError {
            position: e.position,
            errors: e.errors.into_iter().map(|e| e.map_range(|r| {
                let mut v = Vec::new();
                v.extend_from_slice(r);
                edn::Value::Vector(v)
            })).collect(),
        }
    }
}

/// Allow to map the range types of combine::primitives::{Info, Error}.
trait MapRange<R, S> {
    type Output;
    fn map_range<F>(self, f: F) -> Self::Output where F: FnOnce(R) -> S;
}

impl<T, R, S> MapRange<R, S> for combine::primitives::Info<T, R> {
    type Output = combine::primitives::Info<T, S>;

    fn map_range<F>(self, f: F) -> combine::primitives::Info<T, S> where F: FnOnce(R) -> S {
        use combine::primitives::Info::*;
        match self {
            Token(t) => Token(t),
            Range(r) => Range(f(r)),
            Owned(s) => Owned(s),
            Borrowed(x) => Borrowed(x),
        }
    }
}

impl<T, R, S> MapRange<R, S> for combine::primitives::Error<T, R> {
    type Output = combine::primitives::Error<T, S>;

    fn map_range<F>(self, f: F) -> combine::primitives::Error<T, S> where F: FnOnce(R) -> S {
        use combine::primitives::Error::*;
        match self {
            Unexpected(x) => Unexpected(x.map_range(f)),
            Expected(x) => Expected(x.map_range(f)),
            Message(x) => Message(x.map_range(f)),
            Other(x) => Other(x),
        }
    }
}
