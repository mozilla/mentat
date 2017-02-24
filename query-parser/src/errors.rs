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

use edn;
use mentat_parser_utils::ValueParseError;

error_chain! {
    types {
        Error, ErrorKind, ResultExt, Result;
    }

    foreign_links {
        EdnParseError(edn::ParseError);
    }

    links {
    }

    errors {
        NotAVariableError(value: edn::Value) {}
        InvalidInput(value: edn::Value) {}
        FindParseError(value_parse_error: ValueParseError) {}
        WhereParseError(value_parse_error: ValueParseError) {}
        MissingField(field: edn::Keyword) {}
    }
}
