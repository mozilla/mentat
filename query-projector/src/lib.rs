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
extern crate rusqlite;

extern crate mentat_core;
extern crate mentat_db;                 // For value conversion.
extern crate mentat_query;
extern crate mentat_query_algebrizer;
extern crate mentat_query_sql;
extern crate mentat_sql;

use std::collections::BTreeSet;
use std::iter;

use rusqlite::{
    Row,
    Rows,
};

use mentat_core::{
    SQLValueType,
    SQLValueTypeSet,
    TypedValue,
    ValueType,
    ValueTypeTag,
    ValueTypeSet,
};

use mentat_db::{
    TypedSQLValue,
};

use mentat_query::{
    Aggregate,
    Element,
    FindSpec,
    Limit,
    PlainSymbol,
    QueryFunction,
    Variable,
};

use mentat_query_algebrizer::{
    AlgebraicQuery,
    ColumnName,
    ConjoiningClauses,
    QualifiedAlias,
    VariableColumn,
};

use mentat_query_sql::{
    ColumnOrExpression,
    Expression,
    GroupBy,
    Name,
    Projection,
    ProjectedColumn,
};

error_chain! {
    types {
        Error, ErrorKind, ResultExt, Result;
    }

    errors {
        /// We're just not done yet.  Message that the feature is recognized but not yet
        /// implemented.
        NotYetImplemented(t: String) {
            description("not yet implemented")
            display("not yet implemented: {}", t)
        }
        CannotProjectImpossibleBinding(op: SimpleAggregationOp) {
            description("no possible types for variable in projection list")
            display("no possible types for value provided to {:?}", op)
        }
        CannotApplyAggregateOperationToTypes(op: SimpleAggregationOp, types: ValueTypeSet) {
            description("cannot apply projection operation to types")
            display("cannot apply projection operation {:?} to types {:?}", op, types)
        }
        UnboundVariable(var: PlainSymbol) {
            description("cannot project unbound variable")
            display("cannot project unbound variable {:?}", var)
        }
        NoTypeAvailableForVariable(var: PlainSymbol) {
            description("cannot find type for variable")
            display("cannot find type for variable {:?}", var)
        }
    }

    foreign_links {
        Rusqlite(rusqlite::Error);
    }

    links {
        DbError(mentat_db::Error, mentat_db::ErrorKind);
    }
}

#[derive(Debug)]
pub enum QueryResults {
    Scalar(Option<TypedValue>),
    Tuple(Option<Vec<TypedValue>>),
    Coll(Vec<TypedValue>),
    Rel(Vec<Vec<TypedValue>>),
}

impl QueryResults {
    pub fn len(&self) -> usize {
        use QueryResults::*;
        match self {
            &Scalar(ref o) => if o.is_some() { 1 } else { 0 },
            &Tuple(ref o)  => if o.is_some() { 1 } else { 0 },
            &Coll(ref v)   => v.len(),
            &Rel(ref v)    => v.len(),
        }
    }

    pub fn is_empty(&self) -> bool {
        use QueryResults::*;
        match self {
            &Scalar(ref o) => o.is_none(),
            &Tuple(ref o)  => o.is_none(),
            &Coll(ref v)   => v.is_empty(),
            &Rel(ref v)    => v.is_empty(),
        }
    }

    pub fn empty(spec: &FindSpec) -> QueryResults {
        use self::FindSpec::*;
        match spec {
            &FindScalar(_) => QueryResults::Scalar(None),
            &FindTuple(_)  => QueryResults::Tuple(None),
            &FindColl(_)   => QueryResults::Coll(vec![]),
            &FindRel(_)    => QueryResults::Rel(vec![]),
        }
    }

    pub fn empty_factory(spec: &FindSpec) -> Box<Fn() -> QueryResults> {
        use self::FindSpec::*;
        match spec {
            &FindScalar(_) => Box::new(|| QueryResults::Scalar(None)),
            &FindTuple(_)  => Box::new(|| QueryResults::Tuple(None)),
            &FindColl(_)   => Box::new(|| QueryResults::Coll(vec![])),
            &FindRel(_)    => Box::new(|| QueryResults::Rel(vec![])),
        }
    }
}

type Index = i32;            // See rusqlite::RowIndex.
enum TypedIndex {
    Known(Index, ValueTypeTag),
    Unknown(Index, Index),
}

impl TypedIndex {
    /// Look up this index and type(index) pair in the provided row.
    /// This function will panic if:
    ///
    /// - This is an `Unknown` and the retrieved type tag isn't an i32.
    /// - If the retrieved value can't be coerced to a rusqlite `Value`.
    /// - Either index is out of bounds.
    ///
    /// Because we construct our SQL projection list, the tag that stored the data, and this
    /// consumer, a panic here implies that we have a bad bug — we put data of a very wrong type in
    /// a row, and thus can't coerce to Value, we're retrieving from the wrong place, or our
    /// generated SQL is junk.
    ///
    /// This function will return a runtime error if the type tag is unknown, or the value is
    /// otherwise not convertible by the DB layer.
    fn lookup<'a, 'stmt>(&self, row: &Row<'a, 'stmt>) -> Result<TypedValue> {
        use TypedIndex::*;

        match self {
            &Known(value_index, value_type) => {
                let v: rusqlite::types::Value = row.get(value_index);
                TypedValue::from_sql_value_pair(v, value_type).map_err(|e| e.into())
            },
            &Unknown(value_index, type_index) => {
                let v: rusqlite::types::Value = row.get(value_index);
                let value_type_tag: i32 = row.get(type_index);
                TypedValue::from_sql_value_pair(v, value_type_tag).map_err(|e| e.into())
            },
        }
    }
}

fn cc_column(cc: &ConjoiningClauses, var: &Variable) -> Result<QualifiedAlias> {
    cc.column_bindings
      .get(var)
      .and_then(|cols| cols.get(0).cloned())
      .ok_or_else(|| ErrorKind::UnboundVariable(var.name()).into())
}

fn candidate_column(cc: &ConjoiningClauses, var: &Variable) -> Result<(ColumnOrExpression, Name)> {
    // Every variable should be bound by the top-level CC to at least
    // one column in the query. If that constraint is violated it's a
    // bug in our code, so it's appropriate to panic here.
    cc_column(cc, var)
        .map(|qa| {
            let name = VariableColumn::Variable(var.clone()).column_name();
            (ColumnOrExpression::Column(qa), name)
        })
}

fn candidate_type_column(cc: &ConjoiningClauses, var: &Variable) -> (ColumnOrExpression, Name) {
    let extracted_alias = cc.extracted_types
                            .get(var)
                            .expect("Every variable has a known type or an extracted type");
    let type_name = VariableColumn::VariableTypeTag(var.clone()).column_name();
    (ColumnOrExpression::Column(extracted_alias.clone()), type_name)
}

/// Return the projected column -- that is, a value or SQL column and an associated name -- for a
/// given variable. Also return the type.
/// Callers are expected to determine whether to project a type tag as an additional SQL column.
pub fn projected_column_for_var(var: &Variable, cc: &ConjoiningClauses) -> Result<(ProjectedColumn, ValueTypeSet)> {
    if let Some(value) = cc.bound_value(&var) {
        // If we already know the value, then our lives are easy.
        let tag = value.value_type();
        let name = VariableColumn::Variable(var.clone()).column_name();
        Ok((ProjectedColumn(ColumnOrExpression::Value(value.clone()), name), ValueTypeSet::of_one(tag)))
    } else {
        // If we don't, then the CC *must* have bound the variable.
        let (column, name) = candidate_column(cc, var)?;
        Ok((ProjectedColumn(column, name), cc.known_type_set(var)))
    }
}

fn projected_column_for_simple_aggregate(simple: &SimpleAggregate, cc: &ConjoiningClauses) -> Result<(ProjectedColumn, ValueType)> {
    let known_types = cc.known_type_set(&simple.var);
    let return_type = simple.op.is_applicable_to_types(known_types)?;
    let projected_column_or_expression =
        if let Some(value) = cc.bound_value(&simple.var) {
            // Oh, we already know the value!
            if simple.use_static_value() {
                // We can statically compute the aggregate result for some operators -- not count or
                // sum, but avg/max/min are OK.
                ColumnOrExpression::Value(value)
            } else {
                let expression = Expression::Unary {
                    sql_op: simple.op.to_sql(),
                    arg: ColumnOrExpression::Value(value),
                };
                ColumnOrExpression::Expression(Box::new(expression), return_type)
            }
        } else {
            // The common case: the values are bound during execution.
            let column = cc_column(cc, &simple.var)?;
            let expression = Expression::Unary {
                sql_op: simple.op.to_sql(),
                arg: ColumnOrExpression::Column(column),
            };
            ColumnOrExpression::Expression(Box::new(expression), return_type)
        };
    Ok((ProjectedColumn(projected_column_or_expression, simple.column_name()), return_type))
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SimpleAggregationOp {
    Avg,
    Count,
    Max,
    Min,
    Sum,
}

impl SimpleAggregationOp {
    fn to_sql(&self) -> &'static str {
        use SimpleAggregationOp::*;
        match self {
            &Avg => "avg",
            &Count => "count",
            &Max => "max",
            &Min => "min",
            &Sum => "sum",
        }
    }

    fn for_function(function: &QueryFunction) -> Option<SimpleAggregationOp> {
        match function.0.plain_name() {
            "avg" => Some(SimpleAggregationOp::Avg),
            "count" => Some(SimpleAggregationOp::Count),
            "max" => Some(SimpleAggregationOp::Max),
            "min" => Some(SimpleAggregationOp::Min),
            "sum" => Some(SimpleAggregationOp::Sum),
            _ => None,
        }
    }

    /// With knowledge of the types to which a variable might be bound,
    /// return a `Result` to determine whether this aggregation is suitable.
    /// For example, it's valid to take the `Avg` of `{Double, Long}`, invalid
    /// to take `Sum` of `{Instant}`, valid to take (lexicographic) `Max` of `{String}`,
    /// but invalid to take `Max` of `{Uuid, String}`.
    ///
    /// The returned type is the type of the result of the aggregation.
    fn is_applicable_to_types(&self, possibilities: ValueTypeSet) -> Result<ValueType> {
        use SimpleAggregationOp::*;
        if possibilities.is_empty() {
            bail!(ErrorKind::CannotProjectImpossibleBinding(*self))
        }

        match self {
            // One can always count results.
            &Count => Ok(ValueType::Long),

            // Only numeric types can be averaged or summed.
            &Avg => {
                if possibilities.is_only_numeric() {
                    // The mean of a set of numeric values will always, for our purposes, be a double.
                    Ok(ValueType::Double)
                } else {
                    bail!(ErrorKind::CannotApplyAggregateOperationToTypes(*self, possibilities))
                }
            },
            &Sum => {
                if possibilities.is_only_numeric() {
                    if possibilities.contains(ValueType::Double) {
                        Ok(ValueType::Double)
                    } else {
                        // TODO: BigInt.
                        Ok(ValueType::Long)
                    }
                } else {
                    bail!(ErrorKind::CannotApplyAggregateOperationToTypes(*self, possibilities))
                }
            },

            &Max | &Min => {
                if possibilities.is_unit() {
                    use ValueType::*;
                    let the_type = possibilities.exemplar().expect("a type");
                    match the_type {
                        // These types are numerically ordered.
                        Double | Long | Instant => Ok(the_type),

                        // Boolean: false < true.
                        Boolean => Ok(the_type),

                        // String: lexicographic order.
                        String => Ok(the_type),

                        // These types are unordered.
                        Keyword | Ref | Uuid => {
                            bail!(ErrorKind::CannotApplyAggregateOperationToTypes(*self, possibilities))
                        },
                    }
                } else {
                    // It cannot be empty -- we checked.
                    // The only types that are valid to compare cross-type are numbers.
                    if possibilities.is_only_numeric() {
                        // Note that if the max/min is a Long, it will be returned as a Double!
                        if possibilities.contains(ValueType::Double) {
                            Ok(ValueType::Double)
                        } else {
                            // TODO: BigInt.
                            Ok(ValueType::Long)
                        }
                    } else {
                        bail!(ErrorKind::CannotApplyAggregateOperationToTypes(*self, possibilities))
                    }
                }
            },
        }
    }
}

struct SimpleAggregate {
    op: SimpleAggregationOp,
    var: Variable,
}

impl SimpleAggregate {
    fn column_name(&self) -> Name {
        format!("({} {})", self.op.to_sql(), self.var.name())
    }

    fn use_static_value(&self) -> bool {
        use SimpleAggregationOp::*;
        match self.op {
            Avg | Max | Min => true,
            Count | Sum => false,
        }
    }
}

trait SimpleAggregation {
    fn to_simple(&self) -> Option<SimpleAggregate>;
}

impl SimpleAggregation for Aggregate {
    fn to_simple(&self) -> Option<SimpleAggregate> {
        if self.args.len() != 1 {
            return None;
        }
        self.args[0]
            .as_variable()
            .and_then(|v| SimpleAggregationOp::for_function(&self.func)
                              .map(|op| SimpleAggregate { op, var: v.clone(), }))
    }
}

/// An internal temporary struct to pass between the projection 'walk' and the
/// resultant projector.
/// Projection accumulates three things:
/// - A SQL projection list.
/// - A collection of templates for the projector to use to extract values.
/// - A list of columns to use for grouping. Grouping is a property of the projection!
struct ProjectedElements {
    sql_projection: Projection,
    templates: Vec<TypedIndex>,
    group_by: Vec<GroupBy>,
}

/// Walk an iterator of `Element`s, collecting projector templates and columns.
///
/// Returns a pair: the SQL projection (which should always be a `Projection::Columns`)
/// and a `Vec` of `TypedIndex` 'keys' to use when looking up values.
///
/// Callers must ensure that every `Element` is distinct -- a query like
///
/// ```edn
/// [:find ?x ?x :where [?x _ _]]
/// ```
///
/// should fail to parse. See #358.
fn project_elements<'a, I: IntoIterator<Item = &'a Element>>(
    count: usize,
    elements: I,
    query: &AlgebraicQuery) -> Result<ProjectedElements> {

    let mut cols = Vec::with_capacity(count);
    let mut i: i32 = 0;
    let mut templates = vec![];

    // "Query variables not in aggregate expressions will group the results and appear intact
    // in the result."
    // Compute the set of variables projected by the query, then subtract
    // those used in aggregate expressions. This will be our GROUP BY clause.
    // The GROUP BY clause should begin with any non-projected :with variables, in order,
    // then the non-aggregated projected variables, in order.

    // Predetermined:
    // extras: variables needed for ORDER BY. query.named_projection.
    // with: variables to be used for grouping. query.with.
    //
    // Accumulated:
    // variables: variables in the projection list.
    // aggregated: variables used in aggregates.

    // Results:
    // group_by: (with + variables) - aggregated
    // extra_projection: (with + extras) - variables

    let mut aggregated = BTreeSet::new();
    let mut variables = BTreeSet::new();
    for e in elements {
        match e {
            // Each time we come across a variable, we push a SQL column
            // into the SQL projection, aliased to the name of the variable,
            // and we push an annotated index into the projector.
            &Element::Variable(ref var) => {
                variables.insert(var.clone());

                let (projected_column, type_set) = projected_column_for_var(&var, &query.cc)?;
                cols.push(projected_column);
                if let Some(tag) = type_set.unique_type_tag() {
                    templates.push(TypedIndex::Known(i, tag));
                    i += 1;     // We used one SQL column.
                } else {
                    templates.push(TypedIndex::Unknown(i, i + 1));
                    i += 2;     // We used two SQL columns.

                    // Also project the type from the SQL query.
                    let (type_column, type_name) = candidate_type_column(&query.cc, &var);
                    cols.push(ProjectedColumn(type_column, type_name));
                }
            },
            &Element::Aggregate(ref a) => {
                if let Some(simple) = a.to_simple() {
                    aggregated.insert(simple.var.clone());

                    // When we encounter a simple aggregate -- one in which the aggregation can be
                    // implemented in SQL, on a single variable -- we just push the SQL aggregation op.
                    // We must ensure the following:
                    // - There's a column for the var.
                    // - The type of the var is known to be restricted to a sensible input set
                    //   (not necessarily a single type, but e.g., all vals must be Double or Long).
                    // - The type set must be appropriate for the operation. E.g., `Sum` is not a
                    //   meaningful operation on instants.

                    let (projected_column, return_type) = projected_column_for_simple_aggregate(&simple, &query.cc)?;
                    cols.push(projected_column);

                    // We might regret using the type tag here instead of the `ValueType`.
                    templates.push(TypedIndex::Known(i, return_type.value_type_tag()));
                    i += 1;
                } else {
                    // TODO: complex aggregates.
                    bail!(ErrorKind::NotYetImplemented("complex aggregates".into()));
                }
            },
        }
    }

    // Anything we're projecting, or that's part of an aggregate, doesn't need to be in GROUP BY.
    //
    // Anything used in ORDER BY (which we're given in `named_projection`)
    // needs to be in the SQL column list so we can refer to it by name.
    //
    // They don't affect projection.
    //
    // If a variable is of a non-fixed type, also project the type tag column, so we don't
    // accidentally unify across types when considering uniqueness!
    // Similarly, the type tag needs to be grouped.
    // extra_projection: extras - variables
    for var in query.named_projection.iter() {
        if variables.contains(var) {
            continue;
        }

        // If it's a fixed value, we need do nothing further.
        if query.cc.is_value_bound(&var) {
            continue;
        }

        let (column, name) = candidate_column(&query.cc, &var)?;
        cols.push(ProjectedColumn(column, name));

        // We don't care if a column has a single _type_, we care if it has a single type _tag_,
        // because that's what we'll use if we're projecting. E.g., Long and Double.
        // Single type implies single type tag, and is cheaper, so we check that first.
        let types = query.cc.known_type_set(&var);
        if !types.has_unique_type_tag() {
            let (type_column, type_name) = candidate_type_column(&query.cc, &var);
            cols.push(ProjectedColumn(type_column, type_name));
        }
    }

    if aggregated.is_empty() {
        // We're done -- we never need to group unless we're aggregating.
        return Ok(ProjectedElements {
                      sql_projection: Projection::Columns(cols),
                      templates,
                      group_by: vec![],
                  });
    }

    // group_by: (with + variables) - aggregated
    let mut group_by_vars: BTreeSet<Variable> = query.with.union(&variables).cloned().collect();
    for var in aggregated.iter() {
        group_by_vars.remove(var);
    }

    // We never need to group by a constant.
    for var in query.cc.value_bound_variables() {
        group_by_vars.remove(&var);
    }

    // Turn this collection of vars into a collection of columns from the query.
    // We don't allow grouping on anything but a variable bound in the query.
    // We group by tag if necessary.
    let mut group_by = Vec::with_capacity(2 * group_by_vars.len());

    for var in group_by_vars {
        let types = query.cc.known_type_set(&var);
        if !types.has_unique_type_tag() {
            // Group by type then SQL value.
            let type_col = query.cc
                                .extracted_types
                                .get(&var)
                                .cloned()
                                .map(GroupBy::QueryColumn)
                                .ok_or_else(|| ErrorKind::NoTypeAvailableForVariable(var.name().clone()))?;
            group_by.push(type_col);
        }
        let val_col = cc_column(&query.cc, &var).map(GroupBy::QueryColumn)?;
        group_by.push(val_col);
    }

    Ok(ProjectedElements {
        sql_projection: Projection::Columns(cols),
        templates,
        group_by,
    })
}

pub trait Projector {
    fn project<'stmt>(&self, rows: Rows<'stmt>) -> Result<QueryResults>;
}

/// A projector that produces a `QueryResult` containing fixed data.
/// Takes a boxed function that should return an empty result set of the desired type.
struct ConstantProjector {
    results_factory: Box<Fn() -> QueryResults>,
}

impl ConstantProjector {
    fn new(results_factory: Box<Fn() -> QueryResults>) -> ConstantProjector {
        ConstantProjector { results_factory: results_factory }
    }
}

impl Projector for ConstantProjector {
    fn project<'stmt>(&self, _: Rows<'stmt>) -> Result<QueryResults> {
        Ok((self.results_factory)())
    }
}

struct ScalarProjector {
    template: TypedIndex,
}

impl ScalarProjector {
    fn with_template(template: TypedIndex) -> ScalarProjector {
        ScalarProjector {
            template: template,
        }
    }

    fn combine(mut elements: ProjectedElements) -> Result<CombinedProjection> {
        let template = elements.templates.pop().expect("Expected a single template");
        Ok(CombinedProjection {
            sql_projection: elements.sql_projection,
            datalog_projector: Box::new(ScalarProjector::with_template(template)),
            distinct: false,
            group_by_cols: elements.group_by,
        })
    }
}

impl Projector for ScalarProjector {
    fn project<'stmt>(&self, mut rows: Rows<'stmt>) -> Result<QueryResults> {
        if let Some(r) = rows.next() {
            let row = r?;
            let binding = self.template.lookup(&row)?;
            Ok(QueryResults::Scalar(Some(binding)))
        } else {
            Ok(QueryResults::Scalar(None))
        }
    }
}

/// A tuple projector produces a single vector. It's the single-result version of rel.
struct TupleProjector {
    len: usize,
    templates: Vec<TypedIndex>,
}

impl TupleProjector {
    fn with_templates(len: usize, templates: Vec<TypedIndex>) -> TupleProjector {
        TupleProjector {
            len: len,
            templates: templates,
        }
    }

    // This is exactly the same as for rel.
    fn collect_bindings<'a, 'stmt>(&self, row: Row<'a, 'stmt>) -> Result<Vec<TypedValue>> {
        // gte 'cos we might be querying extra columns for ordering.
        // The templates will take care of ignoring columns.
        assert!(row.column_count() >= self.len as i32);
        self.templates
            .iter()
            .map(|ti| ti.lookup(&row))
            .collect::<Result<Vec<TypedValue>>>()
    }

    fn combine(column_count: usize, elements: ProjectedElements) -> Result<CombinedProjection> {
        let p = TupleProjector::with_templates(column_count, elements.templates);
        Ok(CombinedProjection {
            sql_projection: elements.sql_projection,
            datalog_projector: Box::new(p),
            distinct: false,
            group_by_cols: elements.group_by,
        })
    }
}

impl Projector for TupleProjector {
    fn project<'stmt>(&self, mut rows: Rows<'stmt>) -> Result<QueryResults> {
        if let Some(r) = rows.next() {
            let row = r?;
            let bindings = self.collect_bindings(row)?;
            Ok(QueryResults::Tuple(Some(bindings)))
        } else {
            Ok(QueryResults::Tuple(None))
        }
    }
}

/// A rel projector produces a vector of vectors.
/// Each inner vector is the same size, and sourced from the same columns.
/// One inner vector is produced per `Row`.
/// Each column in the inner vector is the result of taking one or two columns from
/// the `Row`: one for the value and optionally one for the type tag.
struct RelProjector {
    len: usize,
    templates: Vec<TypedIndex>,
}

impl RelProjector {
    fn with_templates(len: usize, templates: Vec<TypedIndex>) -> RelProjector {
        RelProjector {
            len: len,
            templates: templates,
        }
    }

    fn collect_bindings<'a, 'stmt>(&self, row: Row<'a, 'stmt>) -> Result<Vec<TypedValue>> {
        // gte 'cos we might be querying extra columns for ordering.
        // The templates will take care of ignoring columns.
        assert!(row.column_count() >= self.len as i32);
        self.templates
            .iter()
            .map(|ti| ti.lookup(&row))
            .collect::<Result<Vec<TypedValue>>>()
    }

    fn combine(column_count: usize, elements: ProjectedElements) -> Result<CombinedProjection> {
        let p = RelProjector::with_templates(column_count, elements.templates);
        Ok(CombinedProjection {
            sql_projection: elements.sql_projection,
            datalog_projector: Box::new(p),
            distinct: true,
            group_by_cols: elements.group_by,
        })
    }
}

impl Projector for RelProjector {
    fn project<'stmt>(&self, mut rows: Rows<'stmt>) -> Result<QueryResults> {
        let mut out: Vec<Vec<TypedValue>> = vec![];
        while let Some(r) = rows.next() {
            let row = r?;
            let bindings = self.collect_bindings(row)?;
            out.push(bindings);
        }
        Ok(QueryResults::Rel(out))
    }
}

/// A coll projector produces a vector of values.
/// Each value is sourced from the same column.
struct CollProjector {
    template: TypedIndex,
}

impl CollProjector {
    fn with_template(template: TypedIndex) -> CollProjector {
        CollProjector {
            template: template,
        }
    }

    fn combine(mut elements: ProjectedElements) -> Result<CombinedProjection> {
        let template = elements.templates.pop().expect("Expected a single template");
        Ok(CombinedProjection {
            sql_projection: elements.sql_projection,
            datalog_projector: Box::new(CollProjector::with_template(template)),
            distinct: true,
            group_by_cols: elements.group_by,
        })
    }
}

impl Projector for CollProjector {
    fn project<'stmt>(&self, mut rows: Rows<'stmt>) -> Result<QueryResults> {
        let mut out: Vec<TypedValue> = vec![];
        while let Some(r) = rows.next() {
            let row = r?;
            let binding = self.template.lookup(&row)?;
            out.push(binding);
        }
        Ok(QueryResults::Coll(out))
    }
}

/// Combines the two things you need to turn a query into SQL and turn its results into
/// `QueryResults`.
pub struct CombinedProjection {
    /// A SQL projection, mapping columns mentioned in the body of the query to columns in the
    /// output.
    pub sql_projection: Projection,

    /// A Datalog projection. This consumes rows of the appropriate shape (as defined by
    /// the SQL projection) to yield one of the four kinds of Datalog query result.
    pub datalog_projector: Box<Projector>,

    /// True if this query requires the SQL query to include DISTINCT.
    pub distinct: bool,

    // A list of column names to use as a GROUP BY clause.
    pub group_by_cols: Vec<GroupBy>,
}

impl CombinedProjection {
    fn flip_distinct_for_limit(mut self, limit: &Limit) -> Self {
        if *limit == Limit::Fixed(1) {
            self.distinct = false;
        }
        self
    }
}

/// Compute a suitable SQL projection for an algebrized query.
/// This takes into account a number of things:
/// - The variable list in the find spec.
/// - The presence of any aggregate operations in the find spec. TODO: for now we only handle
///   simple variables
/// - The bindings established by the topmost CC.
/// - The types known at algebrizing time.
/// - The types extracted from the store for unknown attributes.
pub fn query_projection(query: &AlgebraicQuery) -> Result<CombinedProjection> {
    use self::FindSpec::*;

    if query.is_known_empty() {
        // Do a few gyrations to produce empty results of the right kind for the query.
        let empty = QueryResults::empty_factory(&query.find_spec);
        let constant_projector = ConstantProjector::new(empty);
        Ok(CombinedProjection {
            sql_projection: Projection::One,
            datalog_projector: Box::new(constant_projector),
            distinct: false,
            group_by_cols: vec![],
        })
    } else {
        match query.find_spec {
            FindColl(ref element) => {
                let e = project_elements(1, iter::once(element), query)?;
                CollProjector::combine(e).map(|p| p.flip_distinct_for_limit(&query.limit))
            },

            FindScalar(ref element) => {
                let e = project_elements(1, iter::once(element), query)?;
                ScalarProjector::combine(e)
            },

            FindRel(ref elements) => {
                let column_count = query.find_spec.expected_column_count();
                let e = project_elements(column_count, elements, query)?;
                RelProjector::combine(column_count, e).map(|p| p.flip_distinct_for_limit(&query.limit))
            },

            FindTuple(ref elements) => {
                let column_count = query.find_spec.expected_column_count();
                let e = project_elements(column_count, elements, query)?;
                TupleProjector::combine(column_count, e)
            },
        }
    }
}
