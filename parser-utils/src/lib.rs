// Copyright 2016 Mozilla
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

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