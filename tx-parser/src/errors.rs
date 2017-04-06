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

use combine;
use mentat_parser_utils::value_and_span::Stream;

error_chain! {
    types {
        Error, ErrorKind, ResultExt, Result;
    }

    errors {
        ParseError(parse_error: combine::ParseError<Stream>) {
            description("error parsing edn values")
            display("error parsing edn values:\n{}", parse_error)
        }

        DbIdError {
            description("bad :db/id in map notation")
            display("bad :db/id in map notation: must either be not present or be an entid, an ident, or a tempid")
        }
    }
}
