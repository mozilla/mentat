// Copyright 2016 Mozilla
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

use std::collections::HashMap;

use mentat_core::{
    TypedValue,
};

use mentat_db::DB;

use mentat_query_parser::{
    parse_find_string,
    QueryParseError,
};

// TODO
pub type SQLiteConnection = ();

pub enum QueryResults {
    Scalar(Option<TypedValue>),
    Tuple(Vec<TypedValue>),
    Coll(Vec<TypedValue>),
    Rel(Vec<Vec<TypedValue>>),
}

pub enum QueryExecutionError {
    ParseError(QueryParseError),
    InvalidArgumentName(String),
}

impl From<QueryParseError> for QueryExecutionError {
    fn from(err: QueryParseError) -> QueryExecutionError {
        QueryExecutionError::ParseError(err)
    }
}

pub type QueryExecutionResult = Result<QueryResults, QueryExecutionError>;

/// Take an EDN query string, a reference to a open SQLite connection, a Mentat DB, and an optional
/// collection of input bindings (which should be keyed by `"?varname"`), and execute the query
/// immediately, blocking the current thread.
/// Returns a structure that corresponds to the kind of input query, populated with `TypedValue`
/// instances.
#[allow(unused_variables)]
pub fn q_once(sqlite: SQLiteConnection,
              db: DB,
              query: &str,
              inputs: Option<HashMap<String, TypedValue>>) -> QueryExecutionResult {
    // TODO: validate inputs.
    let parsed = parse_find_string(query)?;
    Ok(QueryResults::Scalar(Some(TypedValue::Boolean(true))))
}
