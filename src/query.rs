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
    Schema,
    TypedValue,
};

use mentat_query_algebrizer::algebrize;

use mentat_query_parser::{
    parse_find_string,
};

use mentat_sql::{
    SQLQuery,
};

use mentat_query_translator::{
    cc_to_select,
    Projection,
};

use errors::Result;

use rusqlite;

pub enum QueryResults {
    Scalar(Option<TypedValue>),
    Tuple(Vec<TypedValue>),
    Coll(Vec<TypedValue>),
    Rel(Vec<Vec<TypedValue>>),
}

pub type QueryExecutionResult = Result<QueryResults>;

/// Take an EDN query string, a reference to a open SQLite connection, a Mentat DB, and an optional
/// collection of input bindings (which should be keyed by `"?varname"`), and execute the query
/// immediately, blocking the current thread.
/// Returns a structure that corresponds to the kind of input query, populated with `TypedValue`
/// instances.
/// The caller is responsible for ensuring that the SQLite connection is in a transaction if
/// isolation is required.
#[allow(unused_variables)]
pub fn q_once<'sqlite, 'schema, 'query>
(sqlite: &'sqlite rusqlite::Connection,
 schema: &'schema Schema,
 query: &'query str,
 inputs: Option<HashMap<String, TypedValue>>) -> QueryExecutionResult {
    // TODO: validate inputs.
    let parsed = parse_find_string(query)?;
    let algebrized = algebrize(schema, parsed);
    let projection = Projection::Star;
    let select = cc_to_select(projection, algebrized.cc);
    let SQLQuery { sql, args } = select.to_sql_query()?;

    /*
    let mut statement = sqlite.prepare(sql.as_str())?;

    let mut rows = if args.is_empty() {
        statement.query(&[])?
    } else {
        statement.query_named(args.map(|(k, v)| (k.as_str(), &v)))?
    };
    */



    Ok(QueryResults::Scalar(Some(TypedValue::Boolean(true))))
}
