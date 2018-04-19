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

use std::rc::Rc;

use mentat_core::{
    Entid,
    HasSchema,
    KnownEntid,
    Schema,
    Binding,
    TypedValue,
};

use mentat_query_algebrizer::{
    AlgebraicQuery,
    EmptyBecause,
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

use mentat_query_projector::{
    ConstantProjector,
    Projector,
};

use mentat_sql::{
    SQLQuery,
};

use mentat_query_translator::{
    ProjectedSelect,
    query_to_select,
};

pub use mentat_query_algebrizer::{
    Known,
};

pub use mentat_query_projector::{
    QueryOutput,        // Includes the columns/find spec.
    QueryResults,       // The results themselves.
    RelResult,
};

use errors::{
    ErrorKind,
    Result,
};

pub type QueryExecutionResult = Result<QueryOutput>;
pub type PreparedResult<'sqlite> = Result<PreparedQuery<'sqlite>>;

pub enum PreparedQuery<'sqlite> {
    Empty {
        find_spec: Rc<FindSpec>,
    },
    Constant {
        select: ConstantProjector,
    },
    Bound {
        statement: rusqlite::Statement<'sqlite>,
        schema: Schema,
        connection: &'sqlite rusqlite::Connection,
        args: Vec<(String, Rc<rusqlite::types::Value>)>,
        projector: Box<Projector>,
    },
}

impl<'sqlite> PreparedQuery<'sqlite> {
    pub fn run<T>(&mut self, _inputs: T) -> QueryExecutionResult where T: Into<Option<QueryInputs>> {
        match self {
            &mut PreparedQuery::Empty { ref find_spec } => {
                Ok(QueryOutput::empty(find_spec))
            },
            &mut PreparedQuery::Constant { ref select } => {
                select.project_without_rows().map_err(|e| e.into())
            },
            &mut PreparedQuery::Bound { ref mut statement, ref schema, ref connection, ref args, ref projector } => {
                let rows = run_statement(statement, args)?;
                projector.project(schema, connection, rows)
                         .map_err(|e| e.into())
            }
        }
    }
}

pub trait IntoResult {
    fn into_scalar_result(self) -> Result<Option<Binding>>;
    fn into_coll_result(self) -> Result<Vec<Binding>>;
    fn into_tuple_result(self) -> Result<Option<Vec<Binding>>>;
    fn into_rel_result(self) -> Result<RelResult<Binding>>;
}

impl IntoResult for QueryExecutionResult {
    fn into_scalar_result(self) -> Result<Option<Binding>> {
        self?.into_scalar().map_err(|e| e.into())
    }

    fn into_coll_result(self) -> Result<Vec<Binding>> {
        self?.into_coll().map_err(|e| e.into())
    }

    fn into_tuple_result(self) -> Result<Option<Vec<Binding>>> {
        self?.into_tuple().map_err(|e| e.into())
    }

    fn into_rel_result(self) -> Result<RelResult<Binding>> {
        self?.into_rel().map_err(|e| e.into())
    }
}

/// A struct describing information about how Mentat would execute a query.
pub enum QueryExplanation {
    /// A query known in advance to be empty, and why we believe that.
    KnownEmpty(EmptyBecause),

    /// A query known in advance to return a constant value.
    KnownConstant,

    /// A query that takes actual work to execute.
    ExecutionPlan {
        /// The translated query and any bindings.
        query: SQLQuery,
        /// The output of SQLite's `EXPLAIN QUERY PLAN`.
        steps: Vec<QueryPlanStep>,
    },
}

/// A single row in the output of SQLite's `EXPLAIN QUERY PLAN`.
/// See https://www.sqlite.org/eqp.html for an explanation of each field.
pub struct QueryPlanStep {
    pub select_id: i32,
    pub order: i32,
    pub from: i32,
    pub detail: String,
}

fn algebrize_query<T>
(known: Known,
 query: FindQuery,
 inputs: T) -> Result<AlgebraicQuery>
    where T: Into<Option<QueryInputs>>
{
    let algebrized = algebrize_with_inputs(known, query, 0, inputs.into().unwrap_or(QueryInputs::default()))?;
    let unbound = algebrized.unbound_variables();
    // Because we are running once, we can check that all of our `:in` variables are bound at this point.
    // If they aren't, the user has made an error -- perhaps writing the wrong variable in `:in`, or
    // not binding in the `QueryInput`.
    if !unbound.is_empty() {
        bail!(ErrorKind::UnboundVariables(unbound.into_iter().map(|v| v.to_string()).collect()));
    }
    Ok(algebrized)
}

fn fetch_values<'sqlite>
(sqlite: &'sqlite rusqlite::Connection,
 known: Known,
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

    let algebrized = algebrize_query(known, query, None)?;

    run_algebrized_query(known, sqlite, algebrized)
}

fn lookup_attribute(schema: &Schema, attribute: &NamespacedKeyword) -> Result<KnownEntid> {
    schema.get_entid(attribute)
          .ok_or_else(|| ErrorKind::UnknownAttribute(attribute.name().into()).into())
}

/// Return a single value for the provided entity and attribute.
/// If the attribute is multi-valued, an arbitrary value is returned.
/// If no value is present for that entity, `None` is returned.
/// If `attribute` isn't an attribute, `None` is returned.
pub fn lookup_value<'sqlite, 'schema, 'cache, E, A>
(sqlite: &'sqlite rusqlite::Connection,
 known: Known,
 entity: E,
 attribute: A) -> Result<Option<TypedValue>>
 where E: Into<Entid>,
       A: Into<Entid> {
    let entid = entity.into();
    let attrid = attribute.into();

    if known.is_attribute_cached_forward(attrid) {
        Ok(known.get_value_for_entid(known.schema, attrid, entid).cloned())
    } else {
        fetch_values(sqlite, known, entid, attrid, true)
            .into_scalar_result()
            // Safe to unwrap: we never retrieve structure.
            .map(|r| r.map(|v| v.val().unwrap()))
    }
}

pub fn lookup_values<'sqlite, E, A>
(sqlite: &'sqlite rusqlite::Connection,
 known: Known,
 entity: E,
 attribute: A) -> Result<Vec<TypedValue>>
 where E: Into<Entid>,
       A: Into<Entid> {
    let entid = entity.into();
    let attrid = attribute.into();

    if known.is_attribute_cached_forward(attrid) {
        Ok(known.get_values_for_entid(known.schema, attrid, entid)
                .cloned()
                .unwrap_or_else(|| vec![]))
    } else {
        fetch_values(sqlite, known, entid, attrid, false)
            .into_coll_result()
            // Safe to unwrap: we never retrieve structure.
            .map(|v| v.into_iter().map(|x| x.val().unwrap()).collect())
    }
}

/// Return a single value for the provided entity and attribute.
/// If the attribute is multi-valued, an arbitrary value is returned.
/// If no value is present for that entity, `None` is returned.
/// If `attribute` doesn't name an attribute, an error is returned.
pub fn lookup_value_for_attribute<'sqlite, 'attribute, E>
(sqlite: &'sqlite rusqlite::Connection,
 known: Known,
 entity: E,
 attribute: &'attribute NamespacedKeyword) -> Result<Option<TypedValue>>
 where E: Into<Entid> {
    let attribute = lookup_attribute(known.schema, attribute)?;
    lookup_value(sqlite, known, entity.into(), attribute)
}

pub fn lookup_values_for_attribute<'sqlite, 'attribute, E>
(sqlite: &'sqlite rusqlite::Connection,
 known: Known,
 entity: E,
 attribute: &'attribute NamespacedKeyword) -> Result<Vec<TypedValue>>
 where E: Into<Entid> {
    let attribute = lookup_attribute(known.schema, attribute)?;
    lookup_values(sqlite, known, entity.into(), attribute)
}

fn run_statement<'sqlite, 'stmt, 'bound>
(statement: &'stmt mut rusqlite::Statement<'sqlite>,
 bindings: &'bound [(String, Rc<rusqlite::types::Value>)]) -> Result<rusqlite::Rows<'stmt>> {

    let rows = if bindings.is_empty() {
        statement.query(&[])?
    } else {
        let refs: Vec<(&str, &ToSql)> =
            bindings.iter()
                    .map(|&(ref k, ref v)| (k.as_str(), v.as_ref() as &ToSql))
                    .collect();
        statement.query_named(&refs)?
    };
    Ok(rows)
}

fn run_sql_query<'sqlite, 'sql, 'bound, T, F>
(sqlite: &'sqlite rusqlite::Connection,
 sql: &'sql str,
 bindings: &'bound [(String, Rc<rusqlite::types::Value>)],
 mut mapper: F) -> Result<Vec<T>>
    where F: FnMut(&rusqlite::Row) -> T
{
    let mut statement = sqlite.prepare(sql)?;
    let mut rows = run_statement(&mut statement, &bindings)?;
    let mut result = vec![];
    while let Some(row_or_error) = rows.next() {
        result.push(mapper(&row_or_error?));
    }
    Ok(result)
}

fn algebrize_query_str<'query, T>
(known: Known,
 query: &'query str,
 inputs: T) -> Result<AlgebraicQuery>
    where T: Into<Option<QueryInputs>> {
    let parsed = parse_find_string(query)?;
    algebrize_query(known, parsed, inputs)
}

fn run_algebrized_query<'sqlite>
(known: Known,
 sqlite: &'sqlite rusqlite::Connection,
 algebrized: AlgebraicQuery) -> QueryExecutionResult {
    assert!(algebrized.unbound_variables().is_empty(),
            "Unbound variables should be checked by now");
    if algebrized.is_known_empty() {
        // We don't need to do any SQL work at all.
        return Ok(QueryOutput::empty(&algebrized.find_spec));
    }

    let select = query_to_select(known.schema, algebrized)?;
    match select {
        ProjectedSelect::Constant(constant) => {
            constant.project_without_rows()
                    .map_err(|e| e.into())
        },
        ProjectedSelect::Query { query, projector } => {
            let SQLQuery { sql, args } = query.to_sql_query()?;

            let mut statement = sqlite.prepare(sql.as_str())?;
            let rows = run_statement(&mut statement, &args)?;

            projector.project(known.schema, sqlite, rows).map_err(|e| e.into())
        },
    }
}

/// Take an EDN query string, a reference to an open SQLite connection, a Mentat schema, and an
/// optional collection of input bindings (which should be keyed by `"?varname"`), and execute the
/// query immediately, blocking the current thread.
/// Returns a structure that corresponds to the kind of input query, populated with `TypedValue`
/// instances.
/// The caller is responsible for ensuring that the SQLite connection has an open transaction if
/// isolation is required.
pub fn q_once<'sqlite, 'query, T>
(sqlite: &'sqlite rusqlite::Connection,
 known: Known,
 query: &'query str,
 inputs: T) -> QueryExecutionResult
        where T: Into<Option<QueryInputs>>
{
    let algebrized = algebrize_query_str(known, query, inputs)?;
    run_algebrized_query(known, sqlite, algebrized)
}

/// Just like `q_once`, but doesn't use any cached values.
pub fn q_uncached<'sqlite, 'schema, 'query, T>
(sqlite: &'sqlite rusqlite::Connection,
 schema: &'schema Schema,
 query: &'query str,
 inputs: T) -> QueryExecutionResult
        where T: Into<Option<QueryInputs>>
{
    let known = Known::for_schema(schema);
    let algebrized = algebrize_query_str(known, query, inputs)?;

    run_algebrized_query(known, sqlite, algebrized)
}

pub fn q_prepare<'sqlite, 'schema, 'cache, 'query, T>
(sqlite: &'sqlite rusqlite::Connection,
 known: Known<'schema, 'cache>,
 query: &'query str,
 inputs: T) -> PreparedResult<'sqlite>
        where T: Into<Option<QueryInputs>>
{
    let algebrized = algebrize_query_str(known, query, inputs)?;

    let unbound = algebrized.unbound_variables();
    if !unbound.is_empty() {
        // TODO: Allow binding variables at execution time, not just
        // preparation time.
        bail!(ErrorKind::UnboundVariables(unbound.into_iter().map(|v| v.to_string()).collect()));
    }

    if algebrized.is_known_empty() {
        // We don't need to do any SQL work at all.
        return Ok(PreparedQuery::Empty {
            find_spec: algebrized.find_spec,
        });
    }

    let select = query_to_select(known.schema, algebrized)?;
    match select {
        ProjectedSelect::Constant(constant) => {
            Ok(PreparedQuery::Constant {
                select: constant,
            })
        },
        ProjectedSelect::Query { query, projector } => {
            let SQLQuery { sql, args } = query.to_sql_query()?;
            let statement = sqlite.prepare(sql.as_str())?;

            Ok(PreparedQuery::Bound {
                statement,
                schema: known.schema.clone(),
                connection: sqlite,
                args,
                projector: projector
            })
        },
    }
}

pub fn q_explain<'sqlite, 'query, T>
(sqlite: &'sqlite rusqlite::Connection,
 known: Known,
 query: &'query str,
 inputs: T) -> Result<QueryExplanation>
        where T: Into<Option<QueryInputs>>
{
    let algebrized = algebrize_query_str(known, query, inputs)?;
    if algebrized.is_known_empty() {
        return Ok(QueryExplanation::KnownEmpty(algebrized.cc.empty_because.unwrap()));
    }
    match query_to_select(known.schema, algebrized)? {
        ProjectedSelect::Constant(_constant) => Ok(QueryExplanation::KnownConstant),
        ProjectedSelect::Query { query, projector: _projector } => {
            let query = query.to_sql_query()?;

            let plan_sql = format!("EXPLAIN QUERY PLAN {}", query.sql);

            let steps = run_sql_query(sqlite, &plan_sql, &query.args, |row| {
                QueryPlanStep {
                    select_id: row.get(0),
                    order: row.get(1),
                    from: row.get(2),
                    detail: row.get(3)
                }
            })?;

            Ok(QueryExplanation::ExecutionPlan { query, steps })
        },
    }
}
