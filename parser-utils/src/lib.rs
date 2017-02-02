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

use combine::ParseResult;
use combine::combinator::{Expected, FnParser};

// Nothing about this is specific to the type of parser.
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
