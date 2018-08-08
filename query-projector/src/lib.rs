// Copyright 2016 Mozilla
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

extern crate failure;

extern crate indexmap;
extern crate rusqlite;

extern crate edn;
extern crate mentat_core;
extern crate db_traits;
extern crate core_traits;
extern crate mentat_db;                 // For value conversion.
extern crate mentat_query_algebrizer;
extern crate mentat_query_pull;
extern crate query_pull_traits;
#[macro_use]
extern crate query_projector_traits;
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

use core_traits::{
    Binding,
    TypedValue,
};

use mentat_core::{
    Schema,
    ValueTypeTag,
};

use mentat_core::util::{
    Either,
};

use mentat_db::{
    TypedSQLValue,
};

use edn::query::{
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

pub mod translate;

mod binding_tuple;
pub use binding_tuple::{
    BindingTuple,
};
mod project;
mod projectors;
mod pull;
mod relresult;

use project::{
    ProjectedElements,
    project_elements,
};

pub use project::{
    projected_column_for_var,
};

pub use projectors::{
    ConstantProjector,
    Projector,
};

use projectors::{
    CollProjector,
    CollTwoStagePullProjector,
    RelProjector,
    RelTwoStagePullProjector,
    ScalarProjector,
    ScalarTwoStagePullProjector,
    TupleProjector,
    TupleTwoStagePullProjector,
};

pub use relresult::{
    RelResult,
    StructuredRelResult,
};

use query_projector_traits::errors::{
    ProjectorError,
    Result,
};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct QueryOutput {
    pub spec: Rc<FindSpec>,
    pub results: QueryResults,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum QueryResults {
    Scalar(Option<Binding>),
    Tuple(Option<Vec<Binding>>),
    Coll(Vec<Binding>),
    Rel(RelResult<Binding>),
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
                let val = bindings.get(var)
                                  .cloned()
                                  .map(|v| v.into());
                QueryResults::Scalar(val)
            },
            &FindScalar(Element::Aggregate(ref _agg)) => {
                // TODO: static aggregates.
                unimplemented!();
            },
            &FindScalar(Element::Pull(ref _pull)) => {
                // TODO: static pull.
                unimplemented!();
            },
            &FindTuple(ref elements) => {
                let values = elements.iter()
                                     .map(|e| match e {
                                         &Element::Variable(ref var) |
                                         &Element::Corresponding(ref var) => {
                                             bindings.get(var)
                                                     .cloned()
                                                     .expect("every var to have a binding")
                                                     .into()
                                         },
                                         &Element::Pull(ref _pull) => {
                                            // TODO: static pull.
                                            unreachable!();
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
                let val = bindings.get(var)
                                  .cloned()
                                  .expect("every var to have a binding")
                                  .into();
                QueryResults::Coll(vec![val])
            },
            &FindColl(Element::Pull(ref _pull)) => {
                // TODO: static pull.
                unimplemented!();
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
                        bindings.get(var)
                                .cloned()
                                .expect("every var to have a binding")
                                .into()
                    },
                    &Element::Pull(ref _pull) => {
                        // TODO: static pull.
                        unreachable!();
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

    pub fn into_scalar(self) -> Result<Option<Binding>> {
        self.results.into_scalar()
    }

    pub fn into_coll(self) -> Result<Vec<Binding>> {
        self.results.into_coll()
    }

    /// Mentat tuple results can be expressed as multiple different data structures.  Some
    /// structures are generic (vectors) and some are easier for pattern matching (fixed length
    /// tuples).
    ///
    /// This is the moral equivalent of `collect` (and `BindingTuple` of `FromIterator`), but
    /// specialized to tuples of expected length.
    pub fn into_tuple<B>(self) -> Result<Option<B>> where B: BindingTuple {
        let expected = self.spec.expected_column_count();
        self.results.into_tuple().and_then(|vec| B::from_binding_vec(expected, vec))
    }

    pub fn into_rel(self) -> Result<RelResult<Binding>> {
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

    pub fn into_scalar(self) -> Result<Option<Binding>> {
        match self {
            QueryResults::Scalar(o) => Ok(o),
            QueryResults::Coll(_) => bail!(ProjectorError::UnexpectedResultsType("coll", "scalar")),
            QueryResults::Tuple(_) => bail!(ProjectorError::UnexpectedResultsType("tuple", "scalar")),
            QueryResults::Rel(_) => bail!(ProjectorError::UnexpectedResultsType("rel", "scalar")),
        }
    }

    pub fn into_coll(self) -> Result<Vec<Binding>> {
        match self {
            QueryResults::Scalar(_) => bail!(ProjectorError::UnexpectedResultsType("scalar", "coll")),
            QueryResults::Coll(c) => Ok(c),
            QueryResults::Tuple(_) => bail!(ProjectorError::UnexpectedResultsType("tuple", "coll")),
            QueryResults::Rel(_) => bail!(ProjectorError::UnexpectedResultsType("rel", "coll")),
        }
    }

    pub fn into_tuple(self) -> Result<Option<Vec<Binding>>> {
        match self {
            QueryResults::Scalar(_) => bail!(ProjectorError::UnexpectedResultsType("scalar", "tuple")),
            QueryResults::Coll(_) => bail!(ProjectorError::UnexpectedResultsType("coll", "tuple")),
            QueryResults::Tuple(t) => Ok(t),
            QueryResults::Rel(_) => bail!(ProjectorError::UnexpectedResultsType("rel", "tuple")),
        }
    }

    pub fn into_rel(self) -> Result<RelResult<Binding>> {
        match self {
            QueryResults::Scalar(_) => bail!(ProjectorError::UnexpectedResultsType("scalar", "rel")),
            QueryResults::Coll(_) => bail!(ProjectorError::UnexpectedResultsType("coll", "rel")),
            QueryResults::Tuple(_) => bail!(ProjectorError::UnexpectedResultsType("tuple", "rel")),
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
    fn lookup<'a, 'stmt>(&self, row: &Row<'a, 'stmt>) -> Result<Binding> {
        use TypedIndex::*;

        match self {
            &Known(value_index, value_type) => {
                let v: rusqlite::types::Value = row.get(value_index);
                TypedValue::from_sql_value_pair(v, value_type)
                    .map(|v| v.into())
                    .map_err(|e| e.into())
            },
            &Unknown(value_index, type_index) => {
                let v: rusqlite::types::Value = row.get(value_index);
                let value_type_tag: i32 = row.get(type_index);
                TypedValue::from_sql_value_pair(v, value_type_tag)
                    .map(|v| v.into())
                    .map_err(|e| e.into())
            },
        }
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

trait IsPull {
    fn is_pull(&self) -> bool;
}

impl IsPull for Element {
    fn is_pull(&self) -> bool {
        match self {
            &Element::Pull(_) => true,
            _ => false,
        }
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
pub fn query_projection(schema: &Schema, query: &AlgebraicQuery) -> Result<Either<ConstantProjector, CombinedProjection>> {
    use self::FindSpec::*;

    let spec = query.find_spec.clone();
    if query.is_fully_unit_bound() {
        // Do a few gyrations to produce empty results of the right kind for the query.

        let variables: BTreeSet<Variable> = spec.columns()
                                                .map(|e| match e {
                                                    &Element::Variable(ref var) |
                                                    &Element::Corresponding(ref var) => var.clone(),

                                                    // Pull expressions can never be fully bound.
                                                    // TODO: but the interior can be, in which case we
                                                    // can handle this and simply project.
                                                    &Element::Pull(_) => {
                                                        unreachable!();
                                                    },
                                                    &Element::Aggregate(ref _agg) => {
                                                        // TODO: static computation of aggregates, then
                                                        // implement the condition in `is_fully_bound`.
                                                        unreachable!();
                                                    },
                                                })
                                                .collect();

        // TODO: error handling
        let results = QueryOutput::from_constants(&spec, query.cc.value_bindings(&variables));
        let f = Box::new(move || { results.clone() });

        Ok(Either::Left(ConstantProjector::new(spec, f)))
    } else if query.is_known_empty() {
        // Do a few gyrations to produce empty results of the right kind for the query.
        let empty = QueryOutput::empty_factory(&spec);
        Ok(Either::Left(ConstantProjector::new(spec, empty)))
    } else {
        match *query.find_spec {
            FindColl(ref element) => {
                let elements = project_elements(1, iter::once(element), query)?;
                if element.is_pull() {
                    CollTwoStagePullProjector::combine(spec, elements)
                } else {
                    CollProjector::combine(spec, elements)
                }.map(|p| p.flip_distinct_for_limit(&query.limit))
            },

            FindScalar(ref element) => {
                let elements = project_elements(1, iter::once(element), query)?;
                if element.is_pull() {
                    ScalarTwoStagePullProjector::combine(schema, spec, elements)
                } else {
                    ScalarProjector::combine(spec, elements)
                }
            },

            FindRel(ref elements) => {
                let is_pull = elements.iter().any(|e| e.is_pull());
                let column_count = query.find_spec.expected_column_count();
                let elements = project_elements(column_count, elements, query)?;
                if is_pull {
                    RelTwoStagePullProjector::combine(spec, column_count, elements)
                } else {
                    RelProjector::combine(spec, column_count, elements)
                }.map(|p| p.flip_distinct_for_limit(&query.limit))
            },

            FindTuple(ref elements) => {
                let is_pull = elements.iter().any(|e| e.is_pull());
                let column_count = query.find_spec.expected_column_count();
                let elements = project_elements(column_count, elements, query)?;
                if is_pull {
                    TupleTwoStagePullProjector::combine(spec, column_count, elements)
                } else {
                    TupleProjector::combine(spec, column_count, elements)
                }
            },
        }.map(Either::Right)
    }
}

#[test]
fn test_into_tuple() {
    let query_output = QueryOutput {
        spec: Rc::new(FindSpec::FindTuple(vec![Element::Variable(Variable::from_valid_name("?x")),
                                               Element::Variable(Variable::from_valid_name("?y"))])),
        results: QueryResults::Tuple(Some(vec![Binding::Scalar(TypedValue::Long(0)),
                                               Binding::Scalar(TypedValue::Long(2))])),
    };

    assert_eq!(query_output.clone().into_tuple().expect("into_tuple"),
               Some((Binding::Scalar(TypedValue::Long(0)),
                     Binding::Scalar(TypedValue::Long(2)))));

    match query_output.clone().into_tuple() {
        Err(ProjectorError::UnexpectedResultsTupleLength(expected, got)) => {
            assert_eq!((expected, got), (3, 2));
        },
        // This forces the result type.
        Ok(Some((_, _, _))) | _ => panic!("expected error"),
    }

    let query_output = QueryOutput {
        spec: Rc::new(FindSpec::FindTuple(vec![Element::Variable(Variable::from_valid_name("?x")),
                                               Element::Variable(Variable::from_valid_name("?y"))])),
        results: QueryResults::Tuple(None),
    };


    match query_output.clone().into_tuple() {
        Ok(None) => {},
        // This forces the result type.
        Ok(Some((_, _))) | _ => panic!("expected error"),
    }

    match query_output.clone().into_tuple() {
        Err(ProjectorError::UnexpectedResultsTupleLength(expected, got)) => {
            assert_eq!((expected, got), (3, 2));
        },
        // This forces the result type.
        Ok(Some((_, _, _))) | _ => panic!("expected error"),
    }
}
