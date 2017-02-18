// Copyright 2016 Mozilla
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

extern crate mentat_query;

mod cc;

use mentat_query::{
    FindQuery,
    FindSpec,
    SrcVar,
};

#[allow(dead_code)]
pub struct AlgebraicQuery {
    default_source: SrcVar,
    find_spec: FindSpec,
    has_aggregates: bool,
    limit: Option<i64>,
    cc: cc::ConjoiningClauses,
}

#[allow(dead_code)]
pub fn algebrize(parsed: FindQuery) -> AlgebraicQuery {
    AlgebraicQuery {
        default_source: parsed.default_source,
        find_spec: parsed.find_spec,
        has_aggregates: false,           // TODO: we don't parse them yet.
        limit: None,
        cc: cc::ConjoiningClauses::default(),
    }
}

pub use cc::{
    ConjoiningClauses,
};

pub use cc::{
    DatomsColumn,
    DatomsTable,
    QualifiedAlias,
    SourceAlias,
    TableAlias,
};

