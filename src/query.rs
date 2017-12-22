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
    Entid,
    HasSchema,
    Schema,
    TypedValue,
};

use mentat_query_algebrizer::{
    AlgebraicQuery,
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

use mentat_query::{
    Element,
    FindQuery,
    FindSpec,
    Pattern,
    PatternNonValuePlace,
    PatternValuePlace,
    WhereClause,
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

pub trait IntoResult {
    fn into_scalar_result(self) -> Result<Option<TypedValue>>;
    fn into_coll_result(self) -> Result<Vec<TypedValue>>;
    fn into_tuple_result(self) -> Result<Option<Vec<TypedValue>>>;
    fn into_rel_result(self) -> Result<Vec<Vec<TypedValue>>>;
}

impl IntoResult for QueryExecutionResult {
    fn into_scalar_result(self) -> Result<Option<TypedValue>> {
        self?.into_scalar().map_err(|e| e.into())
    }

    fn into_coll_result(self) -> Result<Vec<TypedValue>> {
        self?.into_coll().map_err(|e| e.into())
    }

    fn into_tuple_result(self) -> Result<Option<Vec<TypedValue>>> {
        self?.into_tuple().map_err(|e| e.into())
    }

    fn into_rel_result(self) -> Result<Vec<Vec<TypedValue>>> {
        self?.into_rel().map_err(|e| e.into())
    }
}

fn fetch_values<'sqlite, 'schema>
(sqlite: &'sqlite rusqlite::Connection,
 schema: &'schema Schema,
 entity: Entid,
 attribute: Entid,
 only_one: bool) -> QueryExecutionResult {
    let v = Variable::from_valid_name("?v");

    // This should never fail.
    // TODO: it should be possible to algebrize with variable entity and attribute,
    // particularly with known type, allowing the use of prepared statements.
    let pattern = Pattern::simple(PatternNonValuePlace::Entid(entity),
                                  PatternNonValuePlace::Entid(attribute),
                                  PatternValuePlace::Variable(v.clone()))
                        .unwrap();

    let element = Element::Variable(v);
    let spec = if only_one { FindSpec::FindScalar(element) } else { FindSpec::FindColl(element) };
    let query = FindQuery::simple(spec,
                                  vec![WhereClause::Pattern(pattern)]);

    let algebrized = algebrize_with_inputs(schema, query, 0, QueryInputs::default())?;

    run_algebrized_query(sqlite, algebrized)
}

fn lookup_attribute(schema: &Schema, attribute: &NamespacedKeyword) -> Result<Entid> {
    schema.get_entid(attribute)
          .ok_or_else(|| ErrorKind::UnknownAttribute(attribute.clone()).into())
}

/// Return a single value for the provided entity and attribute.
/// If the attribute is multi-valued, an arbitrary value is returned.
/// If no value is present for that entity, `None` is returned.
/// If `attribute` isn't an attribute, `None` is returned.
pub fn lookup_value<'sqlite, 'schema>
(sqlite: &'sqlite rusqlite::Connection,
 schema: &'schema Schema,
 entity: Entid,
 attribute: Entid) -> Result<Option<TypedValue>> {
    fetch_values(sqlite, schema, entity, attribute, true).into_scalar_result()
}

pub fn lookup_values<'sqlite, 'schema>
(sqlite: &'sqlite rusqlite::Connection,
 schema: &'schema Schema,
 entity: Entid,
 attribute: Entid) -> Result<Vec<TypedValue>> {
    fetch_values(sqlite, schema, entity, attribute, false).into_coll_result()
}

/// Return a single value for the provided entity and attribute.
/// If the attribute is multi-valued, an arbitrary value is returned.
/// If no value is present for that entity, `None` is returned.
/// If `attribute` doesn't name an attribute, an error is returned.
pub fn lookup_value_for_attribute<'sqlite, 'schema, 'attribute>
(sqlite: &'sqlite rusqlite::Connection,
 schema: &'schema Schema,
 entity: Entid,
 attribute: &'attribute NamespacedKeyword) -> Result<Option<TypedValue>> {
    lookup_value(sqlite, schema, entity, lookup_attribute(schema, attribute)?)
}

pub fn lookup_values_for_attribute<'sqlite, 'schema, 'attribute>
(sqlite: &'sqlite rusqlite::Connection,
 schema: &'schema Schema,
 entity: Entid,
 attribute: &'attribute NamespacedKeyword) -> Result<Vec<TypedValue>> {
    lookup_values(sqlite, schema, entity, lookup_attribute(schema, attribute)?)
}

fn run_algebrized_query<'sqlite>(sqlite: &'sqlite rusqlite::Connection, algebrized: AlgebraicQuery) -> QueryExecutionResult {
    if algebrized.is_known_empty() {
        // We don't need to do any SQL work at all.
        return Ok(QueryResults::empty(&algebrized.find_spec));
    }

    // Because we are running once, we can check that all of our `:in` variables are bound at this point.
    // If they aren't, the user has made an error -- perhaps writing the wrong variable in `:in`, or
    // not binding in the `QueryInput`.
    let unbound = algebrized.unbound_variables();
    if !unbound.is_empty() {
        bail!(ErrorKind::UnboundVariables(unbound.into_iter().map(|v| v.to_string()).collect()));
    }

    let select = query_to_select(algebrized)?;
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

/// Take an EDN query string, a reference to an open SQLite connection, a Mentat schema, and an
/// optional collection of input bindings (which should be keyed by `"?varname"`), and execute the
/// query immediately, blocking the current thread.
/// Returns a structure that corresponds to the kind of input query, populated with `TypedValue`
/// instances.
/// The caller is responsible for ensuring that the SQLite connection has an open transaction if
/// isolation is required.
pub fn q_once<'sqlite, 'schema, 'query, T>
(sqlite: &'sqlite rusqlite::Connection,
 schema: &'schema Schema,
 query: &'query str,
 inputs: T) -> QueryExecutionResult
        where T: Into<Option<QueryInputs>>
{
    let parsed = parse_find_string(query)?;
    let algebrized = algebrize_with_inputs(schema, parsed, 0, inputs.into().unwrap_or(QueryInputs::default()))?;

    run_algebrized_query(sqlite, algebrized)
}
