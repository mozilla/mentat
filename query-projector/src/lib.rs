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
    SQLValueType,
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
    ColumnName,
    ConjoiningClauses,
    VariableBindings,
    VariableColumn,
};

use mentat_query_sql::{
    ColumnOrExpression,
    Name,
    Projection,
    ProjectedColumn,
};

error_chain! {
    types {
        Error, ErrorKind, ResultExt, Result;
    }

    foreign_links {
        Rusqlite(rusqlite::Error);
    }

    links {
        DbError(mentat_db::Error, mentat_db::ErrorKind);
    }

    errors {
        UnexpectedResultsType(actual: &'static str, expected: &'static str) {
            description("unexpected query results type")
            display("expected {}, got {}", expected, actual)
        }
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
            &FindTuple(ref elements) => {
                let values = elements.iter().map(|e| match e {
                    &Element::Variable(ref var) => bindings.get(var).cloned().expect("every var to have a binding"),
                }).collect();
                QueryResults::Tuple(Some(values))
            },
            &FindColl(Element::Variable(ref var)) => {
                let val = bindings.get(var).cloned().expect("every var to have a binding");
                QueryResults::Coll(vec![val])
            },
            &FindRel(ref elements) => {
                let values = elements.iter().map(|e| match e {
                    &Element::Variable(ref var) => bindings.get(var).cloned().expect("every var to have a binding"),
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
    /// - This is an `Unknown` and the retrieved type code isn't an i32.
    /// - If the retrieved value can't be coerced to a rusqlite `Value`.
    /// - Either index is out of bounds.
    ///
    /// Because we construct our SQL projection list, the code that stored the data, and this
    /// consumer, a panic here implies that we have a bad bug — we put data of a very wrong type in
    /// a row, and thus can't coerce to Value, we're retrieving from the wrong place, or our
    /// generated SQL is junk.
    ///
    /// This function will return a runtime error if the type code is unknown, or the value is
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

fn candidate_column(cc: &ConjoiningClauses, var: &Variable) -> (ColumnOrExpression, Name) {
    // Every variable should be bound by the top-level CC to at least
    // one column in the query. If that constraint is violated it's a
    // bug in our code, so it's appropriate to panic here.
    let columns = cc.column_bindings
                    .get(var)
                    .expect(format!("Every variable should have a binding, but {:?} does not", var).as_str());

    let qa = columns[0].clone();
    let name = VariableColumn::Variable(var.clone()).column_name();
    (ColumnOrExpression::Column(qa), name)
}

fn candidate_type_column(cc: &ConjoiningClauses, var: &Variable) -> (ColumnOrExpression, Name) {
    let extracted_alias = cc.extracted_types
                            .get(var)
                            .expect("Every variable has a known type or an extracted type");
    let type_name = VariableColumn::VariableTypeTag(var.clone()).column_name();
    (ColumnOrExpression::Column(extracted_alias.clone()), type_name)
}

/// Return the projected column -- that is, a value or SQL column and an associated name -- for a
/// given variable. Also return the type, if known.
/// Callers are expected to determine whether to project a type tag as an additional SQL column.
pub fn projected_column_for_var(var: &Variable, cc: &ConjoiningClauses) -> (ProjectedColumn, Option<ValueType>) {
    if let Some(value) = cc.bound_value(&var) {
        // If we already know the value, then our lives are easy.
        let tag = value.value_type();
        let name = VariableColumn::Variable(var.clone()).column_name();
        (ProjectedColumn(ColumnOrExpression::Value(value.clone()), name), Some(tag))
    } else {
        // If we don't, then the CC *must* have bound the variable.
        let (column, name) = candidate_column(cc, var);
        (ProjectedColumn(column, name), cc.known_type(var))
    }
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
    query: &AlgebraicQuery) -> Result<(Projection, Vec<TypedIndex>)> {

    let mut cols = Vec::with_capacity(count);
    let mut i: i32 = 0;
    let mut templates = vec![];
    let mut with = query.with.clone();

    for e in elements {
        match e {
            // Each time we come across a variable, we push a SQL column
            // into the SQL projection, aliased to the name of the variable,
            // and we push an annotated index into the projector.
            &Element::Variable(ref var) => {
                // If we're projecting this, we don't need it in :with.
                with.remove(var);

                let (projected_column, maybe_type) = projected_column_for_var(&var, &query.cc);
                cols.push(projected_column);
                if let Some(ty) = maybe_type {
                    let tag = ty.value_type_tag();
                    templates.push(TypedIndex::Known(i, tag));
                    i += 1;     // We used one SQL column.
                } else {
                    templates.push(TypedIndex::Unknown(i, i + 1));
                    i += 2;     // We used two SQL columns.

                    // Also project the type from the SQL query.
                    let (type_column, type_name) = candidate_type_column(&query.cc, &var);
                    cols.push(ProjectedColumn(type_column, type_name));
                }
            }
        }
    }

    for var in with {
        // We need to collect these into the SQL column list, but they don't affect projection.
        // If a variable is of a non-fixed type, also project the type tag column, so we don't
        // accidentally unify across types when considering uniqueness!
        let (column, name) = candidate_column(&query.cc, &var);
        cols.push(ProjectedColumn(column, name));
        if query.cc.known_type(&var).is_none() {
            let (type_column, type_name) = candidate_type_column(&query.cc, &var);
            cols.push(ProjectedColumn(type_column, type_name));
        }
    }

    Ok((Projection::Columns(cols), templates))
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

    fn combine(spec: Rc<FindSpec>, sql: Projection, mut templates: Vec<TypedIndex>) -> Result<CombinedProjection> {
        let template = templates.pop().expect("Expected a single template");
        Ok(CombinedProjection {
            sql_projection: sql,
            datalog_projector: Box::new(ScalarProjector::with_template(spec, template)),
            distinct: false,
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
        assert!(row.column_count() >= self.len as i32);
        self.templates
            .iter()
            .map(|ti| ti.lookup(&row))
            .collect::<Result<Vec<TypedValue>>>()
    }

    fn combine(spec: Rc<FindSpec>, column_count: usize, sql: Projection, templates: Vec<TypedIndex>) -> Result<CombinedProjection> {
        let p = TupleProjector::with_templates(spec, column_count, templates);
        Ok(CombinedProjection {
            sql_projection: sql,
            datalog_projector: Box::new(p),
            distinct: false,
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
        assert!(row.column_count() >= self.len as i32);
        self.templates
            .iter()
            .map(|ti| ti.lookup(&row))
            .collect::<Result<Vec<TypedValue>>>()
    }

    fn combine(spec: Rc<FindSpec>, column_count: usize, sql: Projection, templates: Vec<TypedIndex>) -> Result<CombinedProjection> {
        let p = RelProjector::with_templates(spec, column_count, templates);
        Ok(CombinedProjection {
            sql_projection: sql,
            datalog_projector: Box::new(p),
            distinct: true,
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

    fn combine(spec: Rc<FindSpec>, sql: Projection, mut templates: Vec<TypedIndex>) -> Result<CombinedProjection> {
        let template = templates.pop().expect("Expected a single template");
        Ok(CombinedProjection {
            sql_projection: sql,
            datalog_projector: Box::new(CollProjector::with_template(spec, template)),
            distinct: true,
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

        let variables: BTreeSet<Variable> = spec.columns().map(|e| match e { &Element::Variable(ref var) => var.clone() }).collect();

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
                let (cols, templates) = project_elements(1, iter::once(element), query)?;
                CollProjector::combine(spec, cols, templates).map(|p| p.flip_distinct_for_limit(&query.limit))
            },

            FindScalar(ref element) => {
                let (cols, templates) = project_elements(1, iter::once(element), query)?;
                ScalarProjector::combine(spec, cols, templates)
            },

            FindRel(ref elements) => {
                let column_count = query.find_spec.expected_column_count();
                let (cols, templates) = project_elements(column_count, elements, query)?;
                RelProjector::combine(spec, column_count, cols, templates).map(|p| p.flip_distinct_for_limit(&query.limit))
            },

            FindTuple(ref elements) => {
                let column_count = query.find_spec.expected_column_count();
                let (cols, templates) = project_elements(column_count, elements, query)?;
                TupleProjector::combine(spec, column_count, cols, templates)
            },
        }.map(Either::Right)
    }
}
