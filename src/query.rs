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

use rusqlite;
use rusqlite::types::ToSql;

use mentat_core::{
    Schema,
    TypedValue,
};

use mentat_query_algebrizer::algebrize;

pub use mentat_query::{
    NamespacedKeyword,
    PlainSymbol,
};

use mentat_query_parser::{
    parse_find_string,
};

use mentat_sql::{
    SQLQuery,
};

use mentat_query_translator::{
    query_to_select,
};

pub use mentat_query_projector::{
    QueryResults,
};

use errors::Result;

pub type QueryExecutionResult = Result<QueryResults>;

/// Take an EDN query string, a reference to a open SQLite connection, a Mentat DB, and an optional
/// collection of input bindings (which should be keyed by `"?varname"`), and execute the query
/// immediately, blocking the current thread.
/// Returns a structure that corresponds to the kind of input query, populated with `TypedValue`
/// instances.
/// The caller is responsible for ensuring that the SQLite connection is in a transaction if
/// isolation is required.
#[allow(unused_variables)]
pub fn q_once<'sqlite, 'schema, 'query, T, U>
(sqlite: &'sqlite rusqlite::Connection,
 schema: &'schema Schema,
 query: &'query str,
 inputs: T,
 limit: U) -> QueryExecutionResult
        where T: Into<Option<HashMap<String, TypedValue>>>,
              U: Into<Option<u64>>
{
    // TODO: validate inputs.

    let parsed = parse_find_string(query)?;
    let mut algebrized = algebrize(schema, parsed)?;

    if algebrized.is_known_empty() {
        // We don't need to do any SQL work at all.
        return Ok(QueryResults::empty(&algebrized.find_spec));
    }

    algebrized.apply_limit(limit.into());

    let select = query_to_select(algebrized);
    let SQLQuery { sql, args } = select.query.to_sql_query()?;

    let mut statement = sqlite.prepare(sql.as_str())?;

    let rows = if args.is_empty() {
        statement.query(&[])?
    } else {
        let refs: Vec<(&str, &ToSql)> =
            args.iter()
                .map(|&(ref k, ref v)| (k.as_str(), v as &ToSql))
                .collect();
        statement.query_named(refs.as_slice())?
    };

    select.projector
          .project(rows)
          .map_err(|e| e.into())
}
