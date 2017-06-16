// Copyright 2016 Mozilla
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

#[macro_use]
extern crate error_chain;
extern crate mentat_core;
extern crate mentat_query;
extern crate mentat_query_algebrizer;
extern crate mentat_query_projector;
extern crate mentat_query_sql;
extern crate mentat_sql;

mod translate;

pub use mentat_query_sql::{
    Projection,
};

pub use translate::{
    cc_to_exists,
    query_to_select,
};

error_chain! {
    types {
        Error, ErrorKind, ResultExt, Result;
    }

    foreign_links {
    }

    links {
        ProjectorError(mentat_query_projector::Error, mentat_query_projector::ErrorKind);
    }
}
