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

use combine::ParseResult;
use combine::combinator::{Expected, FnParser};

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
    ( $parser: path, $input: expr, $expected: expr ) => {{
        let mut par = $parser();
        let result = par.parse(&$input[..]);
        assert_eq!(result, Ok(($expected, &[][..])));
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
                parser(inner as fn(I) -> ParseResult<$result_type, I>).expected("$name")
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
