// Copyright 2016 Mozilla
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

use std::error::Error;

pub type BuildQueryError = Box<Error+Send+Sync>;
pub type BuildQueryResult = Result<(), BuildQueryError>;

/// Gratefully based on Diesel's QueryBuilder trait:
/// https://github.com/diesel-rs/diesel/blob/4885f61b8205f7f3c2cfa03837ed6714831abe6b/diesel/src/query_builder/mod.rs#L56
pub trait QueryBuilder {
    fn push_sql(&mut self, sql: &str);
    fn push_identifier(&mut self, identifier: &str) -> BuildQueryResult;
    fn push_bind_param(&mut self);
    fn finish(self) -> String;
}

pub trait QueryFragment {
    fn to_sql(&self, out: &mut QueryBuilder) -> BuildQueryResult;
}

impl QueryFragment for Box<QueryFragment> {
    fn to_sql(&self, out: &mut QueryBuilder) -> BuildQueryResult {
        QueryFragment::to_sql(&**self, out)
    }
}

impl<'a> QueryFragment for &'a QueryFragment {
    fn to_sql(&self, out: &mut QueryBuilder) -> BuildQueryResult {
        QueryFragment::to_sql(&**self, out)
    }
}

impl QueryFragment for () {
    fn to_sql(&self, _out: &mut QueryBuilder) -> BuildQueryResult {
        Ok(())
    }
}

pub struct SQLiteQueryBuilder {
    pub sql: String,
}

impl SQLiteQueryBuilder {
    pub fn new() -> Self {
        SQLiteQueryBuilder {
            sql: String::new(),
        }
    }
}

impl QueryBuilder for SQLiteQueryBuilder {
    fn push_sql(&mut self, sql: &str) {
        self.sql.push_str(sql);
    }

    fn push_identifier(&mut self, identifier: &str) -> BuildQueryResult {
        self.push_sql("`");
        self.push_sql(&identifier.replace("`", "``"));
        self.push_sql("`");
        Ok(())
    }

    fn push_bind_param(&mut self) {
        self.push_sql("?");
    }

    fn finish(self) -> String {
        self.sql
    }
}
