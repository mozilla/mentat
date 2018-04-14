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

use rusqlite::{
    Row,
    Rows,
};

use mentat_core::{
    TypedValue,
    ValueType,
    ValueTypeTag,
};

use mentat_core::util::{
    Either,
};

use mentat_db::{
    TypedSQLValue,
};

use mentat_query::{
    Element,
    FindSpec,
    Limit,
    Variable,
};

use mentat_query_algebrizer::{
    AlgebraicQuery,
    VariableBindings,
};

use mentat_query_sql::{
    GroupBy,
    Projection,
};

mod aggregates;
mod project;
mod relresult;
pub mod errors;

pub use aggregates::{
    SimpleAggregationOp,
};

use project::{
    ProjectedElements,
    project_elements,
};

pub use project::{
    projected_column_for_var,
};

pub use relresult::{
    RelResult,
};

use errors::{
    ErrorKind,
    Result,
};

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
    Rel(RelResult),
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
            &FindScalar(_)   => Box::new(|| QueryResults::Scalar(None)),
            &FindTuple(_)    => Box::new(|| QueryResults::Tuple(None)),
            &FindColl(_)     => Box::new(|| QueryResults::Coll(vec![])),
            &FindRel(ref es) => {
                let width = es.len();
                Box::new(move || QueryResults::Rel(RelResult::empty(width)))
            },
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
                &FindScalar(_)   => QueryResults::Scalar(None),
                &FindTuple(_)    => QueryResults::Tuple(None),
                &FindColl(_)     => QueryResults::Coll(vec![]),
                &FindRel(ref es) => QueryResults::Rel(RelResult::empty(es.len())),
            };
        QueryOutput {
            spec: spec.clone(),
            results: results,
        }
    }

    pub fn from_constants(spec: &Rc<FindSpec>, bindings: VariableBindings) -> QueryResults {
        use self::FindSpec::*;
        match &**spec {
            &FindScalar(Element::Variable(ref var)) |
            &FindScalar(Element::Corresponding(ref var)) => {
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
                                         &Element::Variable(ref var) |
                                         &Element::Corresponding(ref var) => {
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
            &FindColl(Element::Variable(ref var)) |
            &FindColl(Element::Corresponding(ref var)) => {
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
                let width = elements.len();
                let values = elements.iter().map(|e| match e {
                    &Element::Variable(ref var) |
                    &Element::Corresponding(ref var) => {
                        bindings.get(var).cloned().expect("every var to have a binding")
                    },
                    &Element::Aggregate(ref _agg) => {
                        // TODO: static computation of aggregates, then
                        // implement the condition in `is_fully_bound`.
                        unreachable!();
                    },
                }).collect();
                QueryResults::Rel(RelResult { width, values })
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

    pub fn into_rel(self) -> Result<RelResult> {
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
            &Rel(ref r)    => r.row_count(),
        }
    }

    pub fn is_empty(&self) -> bool {
        use QueryResults::*;
        match self {
            &Scalar(ref o) => o.is_none(),
            &Tuple(ref o)  => o.is_none(),
            &Coll(ref v)   => v.is_empty(),
            &Rel(ref r)    => r.is_empty(),
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

    pub fn into_rel(self) -> Result<RelResult> {
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

/// A rel projector produces a RelResult, which is a striding abstraction over a vector.
/// Each stride across the vector is the same size, and sourced from the same columns.
/// Each column in each stride is the result of taking one or two columns from
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

    fn collect_bindings_into<'a, 'stmt, 'out>(&self, row: Row<'a, 'stmt>, out: &mut Vec<TypedValue>) -> Result<()> {
        // There will be at least as many SQL columns as Datalog columns.
        // gte 'cos we might be querying extra columns for ordering.
        // The templates will take care of ignoring columns.
        assert!(row.column_count() >= self.len as i32);
        let mut count = 0;
        for binding in self.templates
                           .iter()
                           .map(|ti| ti.lookup(&row)) {
            out.push(binding?);
            count += 1;
        }
        assert_eq!(self.len, count);
        Ok(())
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
        // Allocate space for five rows to start.
        // This is better than starting off by doubling the buffer a couple of times, and will
        // rapidly grow to support larger query results.
        let width = self.len;
        let mut values: Vec<_> = Vec::with_capacity(5 * width);

        while let Some(r) = rows.next() {
            let row = r?;
            self.collect_bindings_into(row, &mut values)?;
        }
        Ok(QueryOutput {
            spec: self.spec.clone(),
            results: QueryResults::Rel(RelResult { width, values }),
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
                                                    &Element::Variable(ref var) |
                                                    &Element::Corresponding(ref var) => var.clone(),
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
