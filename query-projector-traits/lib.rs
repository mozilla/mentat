// Copyright 2018 Mozilla
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

extern crate failure;
#[macro_use]
extern crate failure_derive;
extern crate rusqlite;

#[macro_use]
extern crate core_traits;
extern crate db_traits;
extern crate edn;
extern crate query_pull_traits;

// TODO we only want to import a *_traits here, this is a smell.
extern crate mentat_query_algebrizer;
extern crate mentat_query_sql;

pub mod errors;
pub mod aggregates;

