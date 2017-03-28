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

pub mod value_and_span;

use combine::{
    ParseResult,
    Stream,
};
use combine::combinator::{
    Expected,
    FnParser,
};

/// A type definition for a function parser that either parses an `O` from an input stream of type
/// `I`, or fails with an "expected" failure.
/// See <https://docs.rs/combine/2.2.1/combine/trait.Parser.html#method.expected> for more
/// illumination.
/// Nothing about this is specific to the result type of the parser.
pub type ResultParser<O, I> = Expected<FnParser<I, fn(I) -> ParseResult<O, I>>>;

/// `assert_parses_to!` simplifies some of the boilerplate around running a
/// parser function against input and expecting a certain result.
#[macro_export]
macro_rules! assert_parses_to {
    ( $parser: expr, $input: expr, $expected: expr ) => {{
        let mut par = $parser();
        let result = par.parse($input.with_spans().into_atom_stream()).map(|x| x.0); // TODO: check remainder of stream.
        assert_eq!(result, Ok($expected));
    }}
}

/// `satisfy_unwrap!` makes it a little easier to implement a `satisfy_map`
/// body that matches a particular `Value` enum case, otherwise returning `None`.
#[macro_export]
macro_rules! satisfy_unwrap {
    ( $cas: path, $var: ident, $body: block ) => {
        satisfy_map(|x: edn::Value| if let $cas($var) = x $body else { None })
    }
}

/// Generate a `satisfy_map` expression that matches a `PlainSymbol`
/// value with the given name.
///
/// We do this rather than using `combine::token` so that we don't
/// need to allocate a new `String` inside a `PlainSymbol` inside a `Value`
/// just to match input.
#[macro_export]
macro_rules! matches_plain_symbol {
    ($name: expr, $input: ident) => {
        satisfy_map(|x: edn::Value| {
            if let edn::Value::PlainSymbol(ref s) = x {
                if s.0.as_str() == $name {
                    return Some(());
                }
            }
            return None;
        }).parse_stream($input)
    }
}

/// Define an `impl` body for the `$parser` type. The body will contain a parser
/// function called `$name`, consuming a stream of `$item_type`s. The parser's
/// result type will be `$result_type`.
///
/// The provided `$body` will be evaluated with `$input` bound to the input stream.
///
/// `$body`, when run, should return a `ParseResult` of the appropriate result type.
#[macro_export]
macro_rules! def_parser_fn {
    ( $parser: ident, $name: ident, $item_type: ty, $result_type: ty, $input: ident, $body: block ) => {
        impl<I> $parser<I> where I: Stream<Item = $item_type> {
            fn $name() -> ResultParser<$result_type, I> {
                fn inner<I: Stream<Item = $item_type>>($input: I) -> ParseResult<$result_type, I> {
                    $body
                }
                parser(inner as fn(I) -> ParseResult<$result_type, I>).expected(stringify!($name))
            }
        }
    }
}

#[macro_export]
macro_rules! def_parser {
    ( $parser: ident, $name: ident, $result_type: ty, $body: block ) => {
        impl $parser {
            fn $name() -> ResultParser<$result_type, $crate::value_and_span::Stream> {
                fn inner(input: $crate::value_and_span::Stream) -> ParseResult<$result_type, $crate::value_and_span::Stream> {
                    $body.parse_lazy(input).into()
                }
                parser(inner as fn($crate::value_and_span::Stream) -> ParseResult<$result_type, $crate::value_and_span::Stream>).expected(stringify!($name))
            }
        }
    }
}

/// `def_value_parser_fn` is a short-cut to `def_parser_fn` with the input type
/// being `edn::Value`.
#[macro_export]
macro_rules! def_value_parser_fn {
    ( $parser: ident, $name: ident, $result_type: ty, $input: ident, $body: block ) => {
        def_parser_fn!($parser, $name, edn::Value, $result_type, $input, $body);
    }
}

/// `def_value_satisfy_parser_fn` is a short-cut to `def_parser_fn` with the input type
/// being `edn::Value` and the body being a call to `satisfy_map` with the given transformer.
///
/// In practice this allows you to simply pass a function that accepts an `&edn::Value` and
/// returns an `Option<$result_type>`: if a suitable value is at the front of the stream,
/// it will be converted and returned by the parser; otherwise, the parse will fail.
#[macro_export]
macro_rules! def_value_satisfy_parser_fn {
    ( $parser: ident, $name: ident, $result_type: ty, $transformer: path ) => {
        def_value_parser_fn!($parser, $name, $result_type, input, {
            satisfy_map(|x: edn::Value| $transformer(&x)).parse_stream(input)
        });
    }
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
