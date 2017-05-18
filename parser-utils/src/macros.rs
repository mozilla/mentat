// Copyright 2016 Mozilla
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

// use combine::{
//     ParseResult,
// };
// use combine::combinator::{
//     Expected,
//     FnParser,
// };

use combine::{
    ParseResult,
};
// use combine::primitives; // To not shadow Error.
// use combine::primitives::{
//     Consumed,
//     FastResult,
// };
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

pub struct KeywordMapParser<T>(pub T);

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

#[macro_export]
macro_rules! def_parser {
    ( $parser: ident, $name: ident, $result_type: ty, $body: block ) => {
        impl<'p> $parser<'p> {
            fn $name<'a>() -> ResultParser<$result_type, $crate::value_and_span::Stream<'a>> {
                fn inner<'a>(input: $crate::value_and_span::Stream<'a>) -> ParseResult<$result_type, $crate::value_and_span::Stream<'a>> {
                    $body.parse_lazy(input).into()
                }
                parser(inner as fn($crate::value_and_span::Stream<'a>) -> ParseResult<$result_type, $crate::value_and_span::Stream<'a>>).expected(stringify!($name))
            }
        }
    }
}

/// `assert_parses_to!` simplifies some of the boilerplate around running a
/// parser function against input and expecting a certain result.
#[macro_export]
macro_rules! assert_parses_to {
    ( $parser: expr, $input: expr, $expected: expr ) => {{
        let input = $input.with_spans();
        let par = $parser();
        let stream = input.atom_stream();
        let result = par.skip(eof()).parse(stream).map(|x| x.0);
        assert_eq!(result, Ok($expected));
    }}
}

/// `assert_edn_parses_to!` simplifies some of the boilerplate around running a parser function
/// against string input and expecting a certain result.
#[macro_export]
macro_rules! assert_edn_parses_to {
    ( $parser: expr, $input: expr, $expected: expr ) => {{
        let input = edn::parse::value($input).expect("to be able to parse input as EDN");
        let par = $parser();
        let stream = input.atom_stream();
        let result = par.skip(eof()).parse(stream).map(|x| x.0);
        assert_eq!(result, Ok($expected));
    }}
}

/// `assert_parse_failure_contains!` simplifies running a parser function against string input and
/// expecting a certain failure.  This is working around the complexity of pattern matching parse
/// errors that contain spans.
#[macro_export]
macro_rules! assert_parse_failure_contains {
    ( $parser: expr, $input: expr, $expected: expr ) => {{
        let input = edn::parse::value($input).expect("to be able to parse input as EDN");
        let par = $parser();
        let stream = input.atom_stream();
        let result = par.skip(eof()).parse(stream).map(|x| x.0).map_err(|e| -> ::ValueParseError { e.into() });
        assert!(format!("{:?}", result).contains($expected), "Expected {:?} to contain {:?}", result, $expected);
    }}
}

#[macro_export]
macro_rules! keyword_map_of {
    ($(($keyword:expr, $value:expr)),+) => {{
        let mut seen = std::collections::BTreeSet::default();

        $(
            if !seen.insert($keyword) {
                panic!("keyword map has repeated key: {}", stringify!($keyword));
            }
        )+

        KeywordMapParser(($(($keyword, $value)),+))
    }}
}
