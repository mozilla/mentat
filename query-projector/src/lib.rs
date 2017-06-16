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
extern crate indexmap;
extern crate rusqlite;

extern crate mentat_core;
extern crate mentat_db;                 // For value conversion.
extern crate mentat_query;
extern crate mentat_query_algebrizer;
extern crate mentat_query_sql;
extern crate mentat_sql;

use std::collections::{
    BTreeSet,
};

use std::iter;

use std::rc::Rc;

use indexmap::{
    IndexSet,
};

use rusqlite::{
    Row,
    Rows,
};

use mentat_core::{
    SQLValueType,
    SQLValueTypeSet,
    TypedValue,
    ValueType,
    ValueTypeSet,
    ValueTypeTag,
};

use mentat_core::util::{
    Either,
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
    VariableBindings,
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
        UnexpectedResultsType(actual: &'static str, expected: &'static str) {
            description("unexpected query results type")
            display("expected {}, got {}", expected, actual)
        }
    }

    foreign_links {
        Rusqlite(rusqlite::Error);
    }

    links {
        DbError(mentat_db::Error, mentat_db::ErrorKind);
    }
}

#[derive(Debug, PartialEq, Eq)]
pub struct QueryOutput {
    pub spec: Rc<FindSpec>,
    pub results: QueryResults,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum QueryResults {
    Scalar(Option<TypedValue>),
    Tuple(Option<Vec<TypedValue>>),
    Coll(Vec<TypedValue>),
    Rel(Vec<Vec<TypedValue>>),
}

impl From<QueryOutput> for QueryResults {
    fn from(o: QueryOutput) -> QueryResults {
        o.results
    }
}

impl QueryOutput {
    pub fn empty_factory(spec: &FindSpec) -> Box<Fn() -> QueryResults> {
        use self::FindSpec::*;
        match spec {
            &FindScalar(_) => Box::new(|| QueryResults::Scalar(None)),
            &FindTuple(_)  => Box::new(|| QueryResults::Tuple(None)),
            &FindColl(_)   => Box::new(|| QueryResults::Coll(vec![])),
            &FindRel(_)    => Box::new(|| QueryResults::Rel(vec![])),
        }
    }

    pub fn len(&self) -> usize {
        self.results.len()
    }

    pub fn is_empty(&self) -> bool {
        self.results.is_empty()
    }

    pub fn empty(spec: &Rc<FindSpec>) -> QueryOutput {
        use self::FindSpec::*;
        let results =
            match &**spec {
                &FindScalar(_) => QueryResults::Scalar(None),
                &FindTuple(_)  => QueryResults::Tuple(None),
                &FindColl(_)   => QueryResults::Coll(vec![]),
                &FindRel(_)    => QueryResults::Rel(vec![]),
            };
        QueryOutput {
            spec: spec.clone(),
            results: results,
        }
    }

    pub fn from_constants(spec: &Rc<FindSpec>, bindings: VariableBindings) -> QueryResults {
        use self::FindSpec::*;
        match &**spec {
            &FindScalar(Element::Variable(ref var)) => {
                let val = bindings.get(var).cloned();
                QueryResults::Scalar(val)
            },
            &FindScalar(Element::Aggregate(ref _agg)) => {
                // TODO
                unimplemented!();
            },
            &FindTuple(ref elements) => {
                let values = elements.iter()
                                     .map(|e| match e {
                                         &Element::Variable(ref var) => {
                                             bindings.get(var).cloned().expect("every var to have a binding")
                                         },
                                         &Element::Aggregate(ref _agg) => {
                                            // TODO: static computation of aggregates, then
                                            // implement the condition in `is_fully_bound`.
                                            unreachable!();
                                         },
                                     })
                                     .collect();
                QueryResults::Tuple(Some(values))
            },
            &FindColl(Element::Variable(ref var)) => {
                let val = bindings.get(var).cloned().expect("every var to have a binding");
                QueryResults::Coll(vec![val])
            },
            &FindColl(Element::Aggregate(ref _agg)) => {
                // Does it even make sense to write
                // [:find [(max ?x) ...] :where [_ :foo/bar ?x]]
                // ?
                // TODO
                unimplemented!();
            },
            &FindRel(ref elements) => {
                let values = elements.iter().map(|e| match e {
                    &Element::Variable(ref var) => {
                        bindings.get(var).cloned().expect("every var to have a binding")
                    },
                    &Element::Aggregate(ref _agg) => {
                        // TODO: static computation of aggregates, then
                        // implement the condition in `is_fully_bound`.
                        unreachable!();
                    },
                }).collect();
                QueryResults::Rel(vec![values])
            },
        }
    }

    pub fn into_scalar(self) -> Result<Option<TypedValue>> {
        self.results.into_scalar()
    }

    pub fn into_coll(self) -> Result<Vec<TypedValue>> {
        self.results.into_coll()
    }

    pub fn into_tuple(self) -> Result<Option<Vec<TypedValue>>> {
        self.results.into_tuple()
    }

    pub fn into_rel(self) -> Result<Vec<Vec<TypedValue>>> {
        self.results.into_rel()
    }
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

    pub fn into_scalar(self) -> Result<Option<TypedValue>> {
        match self {
            QueryResults::Scalar(o) => Ok(o),
            QueryResults::Coll(_) => bail!(ErrorKind::UnexpectedResultsType("coll", "scalar")),
            QueryResults::Tuple(_) => bail!(ErrorKind::UnexpectedResultsType("tuple", "scalar")),
            QueryResults::Rel(_) => bail!(ErrorKind::UnexpectedResultsType("rel", "scalar")),
        }
    }

    pub fn into_coll(self) -> Result<Vec<TypedValue>> {
        match self {
            QueryResults::Scalar(_) => bail!(ErrorKind::UnexpectedResultsType("scalar", "coll")),
            QueryResults::Coll(c) => Ok(c),
            QueryResults::Tuple(_) => bail!(ErrorKind::UnexpectedResultsType("tuple", "coll")),
            QueryResults::Rel(_) => bail!(ErrorKind::UnexpectedResultsType("rel", "coll")),
        }
    }

    pub fn into_tuple(self) -> Result<Option<Vec<TypedValue>>> {
        match self {
            QueryResults::Scalar(_) => bail!(ErrorKind::UnexpectedResultsType("scalar", "tuple")),
            QueryResults::Coll(_) => bail!(ErrorKind::UnexpectedResultsType("coll", "tuple")),
            QueryResults::Tuple(t) => Ok(t),
            QueryResults::Rel(_) => bail!(ErrorKind::UnexpectedResultsType("rel", "tuple")),
        }
    }

    pub fn into_rel(self) -> Result<Vec<Vec<TypedValue>>> {
        match self {
            QueryResults::Scalar(_) => bail!(ErrorKind::UnexpectedResultsType("scalar", "rel")),
            QueryResults::Coll(_) => bail!(ErrorKind::UnexpectedResultsType("coll", "rel")),
            QueryResults::Tuple(_) => bail!(ErrorKind::UnexpectedResultsType("tuple", "rel")),
            QueryResults::Rel(r) => Ok(r),
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

/// Returns two values:
/// - The `ColumnOrExpression` to use in the query. This will always refer to other
///   variables by name; never to a datoms column.
/// - The known type of that value.
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
            let name = VariableColumn::Variable(simple.var.clone()).column_name();
            let expression = Expression::Unary {
                sql_op: simple.op.to_sql(),
                arg: ColumnOrExpression::ExistingColumn(name),
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
/// Projection accumulates four things:
/// - Two SQL projection lists. We need two because aggregate queries are nested
///   in order to apply DISTINCT to values prior to aggregation.
/// - A collection of templates for the projector to use to extract values.
/// - A list of columns to use for grouping. Grouping is a property of the projection!
struct ProjectedElements {
    sql_projection: Projection,
    pre_aggregate_projection: Option<Projection>,
    templates: Vec<TypedIndex>,
    group_by: Vec<GroupBy>,
}

/// Walk an iterator of `Element`s, collecting projector templates and columns.
///
/// Returns a `ProjectedElements`, which combines SQL projections
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

    // Give a little padding for type tags.
    let mut inner_projection = Vec::with_capacity(count + 2);

    // Everything in the outer query will _either_ be an aggregate operation
    // _or_ a reference to a name projected from the inner.
    // We'll expand them later.
    let mut outer_projection: Vec<Either<Name, ProjectedColumn>> = Vec::with_capacity(count + 2);

    let mut i: i32 = 0;
    let mut templates = vec![];

    let mut aggregates = false;

    // Any variable that appears intact in the :find clause, not inside an aggregate expression.
    // "Query variables not in aggregate expressions will group the results and appear intact
    // in the result."
    // We use an ordered set here so that we group in the correct order.
    let mut outer_variables = IndexSet::new();

    // Any variable that we are projecting from the inner query.
    let mut inner_variables = BTreeSet::new();

    for e in elements {
        match e {
            // Each time we come across a variable, we push a SQL column
            // into the SQL projection, aliased to the name of the variable,
            // and we push an annotated index into the projector.
            &Element::Variable(ref var) => {
                if outer_variables.contains(var) {
                    eprintln!("Warning: duplicate variable {} in query.", var);
                }

                outer_variables.insert(var.clone());
                inner_variables.insert(var.clone());

                let (projected_column, type_set) = projected_column_for_var(&var, &query.cc)?;
                outer_projection.push(Either::Left(projected_column.1.clone()));
                inner_projection.push(projected_column);

                if let Some(tag) = type_set.unique_type_tag() {
                    templates.push(TypedIndex::Known(i, tag));
                    i += 1;     // We used one SQL column.
                } else {
                    templates.push(TypedIndex::Unknown(i, i + 1));
                    i += 2;     // We used two SQL columns.

                    // Also project the type from the SQL query.
                    let (type_column, type_name) = candidate_type_column(&query.cc, &var);
                    inner_projection.push(ProjectedColumn(type_column, type_name.clone()));
                    outer_projection.push(Either::Left(type_name));
                }
            },
            &Element::Aggregate(ref a) => {
                if let Some(simple) = a.to_simple() {
                    aggregates = true;

                    // When we encounter a simple aggregate -- one in which the aggregation can be
                    // implemented in SQL, on a single variable -- we just push the SQL aggregation op.
                    // We must ensure the following:
                    // - There's a column for the var.
                    // - The type of the var is known to be restricted to a sensible input set
                    //   (not necessarily a single type, but e.g., all vals must be Double or Long).
                    // - The type set must be appropriate for the operation. E.g., `Sum` is not a
                    //   meaningful operation on instants.

                    let (projected_column, return_type) = projected_column_for_simple_aggregate(&simple, &query.cc)?;
                    outer_projection.push(Either::Right(projected_column));

                    if !inner_variables.contains(&simple.var) {
                        inner_variables.insert(simple.var.clone());
                        let (projected_column, _type_set) = projected_column_for_var(&simple.var, &query.cc)?;
                        inner_projection.push(projected_column);
                        if query.cc.known_type_set(&simple.var).unique_type_tag().is_none() {
                            // Also project the type from the SQL query.
                            let (type_column, type_name) = candidate_type_column(&query.cc, &simple.var);
                            inner_projection.push(ProjectedColumn(type_column, type_name.clone()));
                        }
                    }

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

    // Anything used in ORDER BY (which we're given in `named_projection`)
    // needs to be in the SQL column list so we can refer to it by name.
    //
    // They don't affect projection.
    //
    // If a variable is of a non-fixed type, also project the type tag column, so we don't
    // accidentally unify across types when considering uniqueness!
    for var in query.named_projection.iter() {
        if outer_variables.contains(var) {
            continue;
        }

        // If it's a fixed value, we need do nothing further.
        if query.cc.is_value_bound(&var) {
            continue;
        }

        let already_inner = inner_variables.contains(&var);
        let (column, name) = candidate_column(&query.cc, &var)?;
        if !already_inner {
            inner_projection.push(ProjectedColumn(column, name.clone()));
            inner_variables.insert(var.clone());
        }

        outer_projection.push(Either::Left(name));
        outer_variables.insert(var.clone());

        // We don't care if a column has a single _type_, we care if it has a single type _tag_,
        // because that's what we'll use if we're projecting. E.g., Long and Double.
        // Single type implies single type tag, and is cheaper, so we check that first.
        let types = query.cc.known_type_set(&var);
        if !types.has_unique_type_tag() {
            let (type_column, type_name) = candidate_type_column(&query.cc, &var);
            if !already_inner {
                inner_projection.push(ProjectedColumn(type_column, type_name.clone()));
            }

            outer_projection.push(Either::Left(type_name));
        }
    }

    if !aggregates {
        // We're done -- we never need to group unless we're aggregating.
        return Ok(ProjectedElements {
                      sql_projection: Projection::Columns(inner_projection),
                      pre_aggregate_projection: None,
                      templates,
                      group_by: vec![],
                  });
    }

    // OK, on to aggregates.
    // We need to produce two SQL projection lists: one for an inner query and one for the outer.
    //
    // The inner serves these purposes:
    // - Projecting variables to avoid duplicates being elided. (:with)
    // - Making bindings available to the outermost query for projection, ordering, and grouping.
    //
    // The outer is consumed by the projector.
    //
    // We will also be producing:
    // - A GROUP BY list to group the output of the inner query by non-aggregate variables
    //   so that it can be correctly aggregated.

    // Turn this collection of vars into a collection of columns from the query.
    // We don't allow grouping on anything but a variable bound in the query.
    // We group by tag if necessary.
    let mut group_by = Vec::with_capacity(outer_variables.len() + 2);
    for var in outer_variables.into_iter() {
        if query.cc.is_value_bound(&var) {
            continue;
        }

        // The GROUP BY goes outside, but it needs every variable and type tag to be
        // projected from inside. Collect in both directions here.
        let name = VariableColumn::Variable(var.clone()).column_name();
        group_by.push(GroupBy::ProjectedColumn(name));

        let needs_type_projection = !query.cc.known_type_set(&var).has_unique_type_tag();

        let already_inner = inner_variables.contains(&var);
        if !already_inner {
            let (column, name) = candidate_column(&query.cc, &var)?;
            inner_projection.push(ProjectedColumn(column, name.clone()));
        }

        if needs_type_projection {
            let type_name = VariableColumn::VariableTypeTag(var.clone()).column_name();
            if !already_inner {
                let type_col = query.cc
                                    .extracted_types
                                    .get(&var)
                                    .cloned()
                                    .ok_or_else(|| ErrorKind::NoTypeAvailableForVariable(var.name().clone()))?;
                inner_projection.push(ProjectedColumn(ColumnOrExpression::Column(type_col), type_name.clone()));
            }
            group_by.push(GroupBy::ProjectedColumn(type_name));
        };
    }

    for var in query.with.iter() {
        // We never need to project a constant.
        if query.cc.is_value_bound(&var) {
            continue;
        }

        // We don't need to add inner projections for :with if they are already there.
        if !inner_variables.contains(&var) {
            let (projected_column, type_set) = projected_column_for_var(&var, &query.cc)?;
            inner_projection.push(projected_column);

            if type_set.unique_type_tag().is_none() {
                // Also project the type from the SQL query.
                let (type_column, type_name) = candidate_type_column(&query.cc, &var);
                inner_projection.push(ProjectedColumn(type_column, type_name.clone()));
            }
        }
    }

    // At this point we know we have a double-layer projection. Collect the outer.
    //
    // If we have an inner and outer layer, the inner layer will name its
    // variables, and the outer will re-project them.
    // If we only have one layer, then the outer will do the naming.
    // (We could try to not use names in the inner query, but then what would we do for
    // `ground` and known values?)
    // Walk the projection, switching the outer columns to use the inner names.

    let outer_projection = outer_projection.into_iter().map(|c| {
        match c {
            Either::Left(name) => {
                ProjectedColumn(ColumnOrExpression::ExistingColumn(name.clone()),
                                name)
            },
            Either::Right(pc) => pc,
        }
    }).collect();

    Ok(ProjectedElements {
        sql_projection: Projection::Columns(outer_projection),
        pre_aggregate_projection: Some(Projection::Columns(inner_projection)),
        templates,
        group_by,
    })
}

pub trait Projector {
    fn project<'stmt>(&self, rows: Rows<'stmt>) -> Result<QueryOutput>;
    fn columns<'s>(&'s self) -> Box<Iterator<Item=&Element> + 's>;
}

/// A projector that produces a `QueryResult` containing fixed data.
/// Takes a boxed function that should return an empty result set of the desired type.
pub struct ConstantProjector {
    spec: Rc<FindSpec>,
    results_factory: Box<Fn() -> QueryResults>,
}

impl ConstantProjector {
    fn new(spec: Rc<FindSpec>, results_factory: Box<Fn() -> QueryResults>) -> ConstantProjector {
        ConstantProjector {
            spec: spec,
            results_factory: results_factory,
        }
    }

    pub fn project_without_rows<'stmt>(&self) -> Result<QueryOutput> {
        let results = (self.results_factory)();
        let spec = self.spec.clone();
        Ok(QueryOutput {
            spec: spec,
            results: results,
        })
    }
}

impl Projector for ConstantProjector {
    fn project<'stmt>(&self, _: Rows<'stmt>) -> Result<QueryOutput> {
        self.project_without_rows()
    }

    fn columns<'s>(&'s self) -> Box<Iterator<Item=&Element> + 's> {
        self.spec.columns()
    }
}

struct ScalarProjector {
    spec: Rc<FindSpec>,
    template: TypedIndex,
}

impl ScalarProjector {
    fn with_template(spec: Rc<FindSpec>, template: TypedIndex) -> ScalarProjector {
        ScalarProjector {
            spec: spec,
            template: template,
        }
    }

    fn combine(spec: Rc<FindSpec>, mut elements: ProjectedElements) -> Result<CombinedProjection> {
        let template = elements.templates.pop().expect("Expected a single template");
        Ok(CombinedProjection {
            sql_projection: elements.sql_projection,
            pre_aggregate_projection: elements.pre_aggregate_projection,
            datalog_projector: Box::new(ScalarProjector::with_template(spec, template)),
            distinct: false,
            group_by_cols: elements.group_by,
        })
    }
}

impl Projector for ScalarProjector {
    fn project<'stmt>(&self, mut rows: Rows<'stmt>) -> Result<QueryOutput> {
        let results =
            if let Some(r) = rows.next() {
                let row = r?;
                let binding = self.template.lookup(&row)?;
                QueryResults::Scalar(Some(binding))
            } else {
                QueryResults::Scalar(None)
            };
        Ok(QueryOutput {
            spec: self.spec.clone(),
            results: results,
        })
    }

    fn columns<'s>(&'s self) -> Box<Iterator<Item=&Element> + 's> {
        self.spec.columns()
    }
}

/// A tuple projector produces a single vector. It's the single-result version of rel.
struct TupleProjector {
    spec: Rc<FindSpec>,
    len: usize,
    templates: Vec<TypedIndex>,
}

impl TupleProjector {
    fn with_templates(spec: Rc<FindSpec>, len: usize, templates: Vec<TypedIndex>) -> TupleProjector {
        TupleProjector {
            spec: spec,
            len: len,
            templates: templates,
        }
    }

    // This is exactly the same as for rel.
    fn collect_bindings<'a, 'stmt>(&self, row: Row<'a, 'stmt>) -> Result<Vec<TypedValue>> {
        // There will be at least as many SQL columns as Datalog columns.
        // gte 'cos we might be querying extra columns for ordering.
        // The templates will take care of ignoring columns.
        assert!(row.column_count() >= self.len as i32);
        self.templates
            .iter()
            .map(|ti| ti.lookup(&row))
            .collect::<Result<Vec<TypedValue>>>()
    }

    fn combine(spec: Rc<FindSpec>, column_count: usize, elements: ProjectedElements) -> Result<CombinedProjection> {
        let p = TupleProjector::with_templates(spec, column_count, elements.templates);
        Ok(CombinedProjection {
            sql_projection: elements.sql_projection,
            pre_aggregate_projection: elements.pre_aggregate_projection,
            datalog_projector: Box::new(p),
            distinct: false,
            group_by_cols: elements.group_by,
        })
    }
}

impl Projector for TupleProjector {
    fn project<'stmt>(&self, mut rows: Rows<'stmt>) -> Result<QueryOutput> {
        let results =
            if let Some(r) = rows.next() {
                let row = r?;
                let bindings = self.collect_bindings(row)?;
                QueryResults::Tuple(Some(bindings))
            } else {
                QueryResults::Tuple(None)
            };
        Ok(QueryOutput {
            spec: self.spec.clone(),
            results: results,
        })
    }

    fn columns<'s>(&'s self) -> Box<Iterator<Item=&Element> + 's> {
        self.spec.columns()
    }
}

/// A rel projector produces a vector of vectors.
/// Each inner vector is the same size, and sourced from the same columns.
/// One inner vector is produced per `Row`.
/// Each column in the inner vector is the result of taking one or two columns from
/// the `Row`: one for the value and optionally one for the type tag.
struct RelProjector {
    spec: Rc<FindSpec>,
    len: usize,
    templates: Vec<TypedIndex>,
}

impl RelProjector {
    fn with_templates(spec: Rc<FindSpec>, len: usize, templates: Vec<TypedIndex>) -> RelProjector {
        RelProjector {
            spec: spec,
            len: len,
            templates: templates,
        }
    }

    fn collect_bindings<'a, 'stmt>(&self, row: Row<'a, 'stmt>) -> Result<Vec<TypedValue>> {
        // There will be at least as many SQL columns as Datalog columns.
        // gte 'cos we might be querying extra columns for ordering.
        // The templates will take care of ignoring columns.
        assert!(row.column_count() >= self.len as i32);
        self.templates
            .iter()
            .map(|ti| ti.lookup(&row))
            .collect::<Result<Vec<TypedValue>>>()
    }

    fn combine(spec: Rc<FindSpec>, column_count: usize, elements: ProjectedElements) -> Result<CombinedProjection> {
        let p = RelProjector::with_templates(spec, column_count, elements.templates);

        // If every column yields only one value, or if this is an aggregate query
        // (because by definition every column in an aggregate query is either
        // aggregated or is a variable _upon which we group_), then don't bother
        // with DISTINCT.
        let already_distinct = elements.pre_aggregate_projection.is_some() ||
                               p.columns().all(|e| e.is_unit());
        Ok(CombinedProjection {
            sql_projection: elements.sql_projection,
            pre_aggregate_projection: elements.pre_aggregate_projection,
            datalog_projector: Box::new(p),
            distinct: !already_distinct,
            group_by_cols: elements.group_by,
        })
    }
}

impl Projector for RelProjector {
    fn project<'stmt>(&self, mut rows: Rows<'stmt>) -> Result<QueryOutput> {
        let mut out: Vec<Vec<TypedValue>> = vec![];
        while let Some(r) = rows.next() {
            let row = r?;
            let bindings = self.collect_bindings(row)?;
            out.push(bindings);
        }
        Ok(QueryOutput {
            spec: self.spec.clone(),
            results: QueryResults::Rel(out),
        })
    }

    fn columns<'s>(&'s self) -> Box<Iterator<Item=&Element> + 's> {
        self.spec.columns()
    }
}

/// A coll projector produces a vector of values.
/// Each value is sourced from the same column.
struct CollProjector {
    spec: Rc<FindSpec>,
    template: TypedIndex,
}

impl CollProjector {
    fn with_template(spec: Rc<FindSpec>, template: TypedIndex) -> CollProjector {
        CollProjector {
            spec: spec,
            template: template,
        }
    }

    fn combine(spec: Rc<FindSpec>, mut elements: ProjectedElements) -> Result<CombinedProjection> {
        let template = elements.templates.pop().expect("Expected a single template");
        let p = CollProjector::with_template(spec, template);

        // If every column yields only one value, or if this is an aggregate query
        // (because by definition every column in an aggregate query is either
        // aggregated or is a variable _upon which we group_), then don't bother
        // with DISTINCT.
        let already_distinct = elements.pre_aggregate_projection.is_some() ||
                               p.columns().all(|e| e.is_unit());
        Ok(CombinedProjection {
            sql_projection: elements.sql_projection,
            pre_aggregate_projection: elements.pre_aggregate_projection,
            datalog_projector: Box::new(p),
            distinct: !already_distinct,
            group_by_cols: elements.group_by,
        })
    }
}

impl Projector for CollProjector {
    fn project<'stmt>(&self, mut rows: Rows<'stmt>) -> Result<QueryOutput> {
        let mut out: Vec<TypedValue> = vec![];
        while let Some(r) = rows.next() {
            let row = r?;
            let binding = self.template.lookup(&row)?;
            out.push(binding);
        }
        Ok(QueryOutput {
            spec: self.spec.clone(),
            results: QueryResults::Coll(out),
        })
    }

    fn columns<'s>(&'s self) -> Box<Iterator<Item=&Element> + 's> {
        self.spec.columns()
    }
}

/// Combines the things you need to turn a query into SQL and turn its results into
/// `QueryResults`: SQL-related projection information (`DISTINCT`, columns, etc.) and
/// a Datalog projector that turns SQL into structures.
pub struct CombinedProjection {
    /// A SQL projection, mapping columns mentioned in the body of the query to columns in the
    /// output.
    pub sql_projection: Projection,

    /// If a query contains aggregates, we need to generate a nested subquery: an inner query
    /// that returns our distinct variable bindings (and any `:with` vars), and an outer query
    /// that applies aggregation. That's so we can put `DISTINCT` in the inner query and apply
    /// aggregation afterwards -- `SELECT DISTINCT count(foo)` counts _then_ uniques, and we need
    /// the opposite to implement Datalog distinct semantics.
    /// If this is the case, `sql_projection` will be the outer query's projection list, and
    /// `pre_aggregate_projection` will be the inner.
    /// If the query doesn't use aggregation, this field will be `None`.
    pub pre_aggregate_projection: Option<Projection>,

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
pub fn query_projection(query: &AlgebraicQuery) -> Result<Either<ConstantProjector, CombinedProjection>> {
    use self::FindSpec::*;

    let spec = query.find_spec.clone();
    if query.is_fully_unit_bound() {
        // Do a few gyrations to produce empty results of the right kind for the query.

        let variables: BTreeSet<Variable> = spec.columns()
                                                .map(|e| match e {
                                                    &Element::Variable(ref var) => var.clone(),
                                                    &Element::Aggregate(ref _agg) => {
                                                        // TODO: static computation of aggregates, then
                                                        // implement the condition in `is_fully_bound`.
                                                        unreachable!();
                                                    },
                                                })
                                                .collect();

        // TODO: error handling
        let results = QueryOutput::from_constants(&spec, query.cc.value_bindings(&variables));
        let f = Box::new(move || {results.clone()});

        Ok(Either::Left(ConstantProjector::new(spec, f)))
    } else if query.is_known_empty() {
        // Do a few gyrations to produce empty results of the right kind for the query.
        let empty = QueryOutput::empty_factory(&spec);
        Ok(Either::Left(ConstantProjector::new(spec, empty)))
    } else {
        match *query.find_spec {
            FindColl(ref element) => {
                let elements = project_elements(1, iter::once(element), query)?;
                CollProjector::combine(spec, elements).map(|p| p.flip_distinct_for_limit(&query.limit))
            },

            FindScalar(ref element) => {
                let elements = project_elements(1, iter::once(element), query)?;
                ScalarProjector::combine(spec, elements)
            },

            FindRel(ref elements) => {
                let column_count = query.find_spec.expected_column_count();
                let elements = project_elements(column_count, elements, query)?;
                RelProjector::combine(spec, column_count, elements).map(|p| p.flip_distinct_for_limit(&query.limit))
            },

            FindTuple(ref elements) => {
                let column_count = query.find_spec.expected_column_count();
                let elements = project_elements(column_count, elements, query)?;
                TupleProjector::combine(spec, column_count, elements)
            },
        }.map(Either::Right)
    }
}
