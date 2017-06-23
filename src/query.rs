// Copyright 2016 Mozilla
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

use rusqlite;
use rusqlite::types::ToSql;

use mentat_core::{
    Schema,
};

use mentat_db::PartitionMap;

use mentat_query_algebrizer::{
    algebrize_with_inputs,
};

pub use mentat_query_algebrizer::{
    QueryInputs,
};

pub use mentat_query::{
    NamespacedKeyword,
    PlainSymbol,
    Variable,
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

use errors::{
    ErrorKind,
    Result,
};

pub type QueryExecutionResult = Result<QueryResults>;

/// Take an EDN query string, a reference to an open SQLite connection, a Mentat schema, and an
/// optional collection of input bindings (which should be keyed by `"?varname"`), and execute the
/// query immediately, blocking the current thread.
/// Returns a structure that corresponds to the kind of input query, populated with `TypedValue`
/// instances.
/// The caller is responsible for ensuring that the SQLite connection has an open transaction if
/// isolation is required.
pub fn q_once<'sqlite, 'schema, 'partition, 'query, T>
(sqlite: &'sqlite rusqlite::Connection,
 schema: &'schema Schema,
 partition_map: &'partition PartitionMap,
 query: &'query str,
 inputs: T) -> QueryExecutionResult
        where T: Into<Option<QueryInputs>>
{
    let parsed = parse_find_string(query)?;
    let algebrized = algebrize_with_inputs(schema, partition_map, parsed, 0, inputs.into().unwrap_or(QueryInputs::default()))?;

    if algebrized.is_known_empty() {
        // We don't need to do any SQL work at all.
        return Ok(QueryResults::empty(&algebrized.find_spec));
    }

    // Because this is q_once, we can check that all of our `:in` variables are bound at this point.
    // If they aren't, the user has made an error -- perhaps writing the wrong variable in `:in`, or
    // not binding in the `QueryInput`.
    let unbound = algebrized.unbound_variables();
    if !unbound.is_empty() {
        bail!(ErrorKind::UnboundVariables(unbound.into_iter().map(|v| v.to_string()).collect()));
    }
    let select = query_to_select(algebrized);
    let SQLQuery { sql, args } = select.query.to_sql_query()?;

    let mut statement = sqlite.prepare(sql.as_str())?;

    let rows = if args.is_empty() {
        statement.query(&[])?
    } else {
        let refs: Vec<(&str, &ToSql)> =
            args.iter()
                .map(|&(ref k, ref v)| (k.as_str(), v.as_ref() as &ToSql))
                .collect();
        statement.query_named(refs.as_slice())?
    };

    select.projector
          .project(rows)
          .map_err(|e| e.into())
}
