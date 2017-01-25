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
extern crate mentat_query;

use self::mentat_query::{FindSpec, FindQuery};

#[derive(Clone,Debug,Eq,PartialEq)]
pub enum FindParseError {
  Err,
}

#[derive(Clone,Debug,Eq,PartialEq)]
pub enum QueryParseError {
    InvalidInput(edn::Value),
    EdnParseError(edn::parse::ParseError),
    MissingField(edn::Keyword),
    FindParseError(FindParseError),
}

pub type FindParseResult = Result<FindSpec, FindParseError>;
pub type QueryParseResult = Result<FindQuery, QueryParseError>;

