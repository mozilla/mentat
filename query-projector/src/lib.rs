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

use std::iter;

use rusqlite::{
    Row,
    Rows,
};

use mentat_core::{
    TypedValue,
};

use mentat_db::{
    SQLValueType,
    TypedSQLValue,
};

use mentat_query::{
    Element,
    FindSpec,
    PlainSymbol,
    Variable,
};

use mentat_query_algebrizer::{
    AlgebraicQuery,
    DatomsColumn,
    QualifiedAlias,
    /*
    ConjoiningClauses,
    DatomsTable,
    SourceAlias,
    */
};

use mentat_query_sql::{
    ColumnOrExpression,
    /*
    Constraint,
    FromClause,
    */
    Name,
    Projection,
    ProjectedColumn,
    /*
    SelectQuery,
    TableList,
    */
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
}

type Index = i32;            // See rusqlite::RowIndex.
type ValueTypeTag = i32;
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
    /// This function will return an error if the type code is unknown, or the value is
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

// TODO: this sucks. Can we take ownership and avoid the clone?
fn column_name(var: &Variable) -> Name {
    let &Variable(PlainSymbol(ref s)) = var;
    s.clone()
}

fn value_type_tag_name(var: &Variable) -> Name {
    let &Variable(PlainSymbol(ref s)) = var;
    format!("{}_value_type_tag", s)
}

/// Walk an iterator of `Element`s, collecting projector templates and
/// pushing columns into the provided `Vec`.
fn project_elements<'a, I: IntoIterator<Item = &'a Element>>(
    elements: I,
    starting_at: i32,
    query: &AlgebraicQuery,
    cols: &mut Vec<ProjectedColumn>) -> Vec<TypedIndex> {

    let mut i: i32 = starting_at;
    let mut templates = vec![];

    for e in elements {
        match e {
            // Each time we come across a variable, we push a SQL column
            // into the SQL projection, aliased to the name of the variable,
            // and we push an annotated index into the projector.
            &Element::Variable(ref var) => {
                let qa = query.cc.bindings.get(var).unwrap()[0].clone();   // TODO
                let name = column_name(var);

                if let Some(t) = query.cc.known_types.get(var) {
                    cols.push(ProjectedColumn(ColumnOrExpression::Column(qa), name));
                    let tag = t.value_type_tag();
                    templates.push(TypedIndex::Known(i, tag));
                    i += 1;     // We used one SQL column.
                } else {
                    let table = qa.0.clone();
                    cols.push(ProjectedColumn(ColumnOrExpression::Column(qa), name));
                    templates.push(TypedIndex::Unknown(i, i + 1));
                    i += 2;     // We used two SQL columns.

                    // Also project the type from the SQL query.
                    let type_name = value_type_tag_name(var);
                    let type_qa = QualifiedAlias(table, DatomsColumn::ValueTypeTag);
                    cols.push(ProjectedColumn(ColumnOrExpression::Column(type_qa), type_name));
                }
            }
        }
    }

    templates
}

pub trait Projector {
    fn project<'stmt>(&self, rows: Rows<'stmt>) -> Result<QueryResults>;
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
        assert_eq!(row.column_count(), self.len as i32);
        self.templates
            .iter()
            .map(|ti| ti.lookup(&row))
            .collect::<Result<Vec<TypedValue>>>()
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
        assert_eq!(row.column_count(), self.len as i32);
        self.templates
            .iter()
            .map(|ti| ti.lookup(&row))
            .collect::<Result<Vec<TypedValue>>>()
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

/// Convenience function for grabbing a single-element projection.
fn one_typed_index(element: &Element, query: &AlgebraicQuery, mut cols: &mut Vec<ProjectedColumn>) -> TypedIndex {
    let mut templates = project_elements(iter::once(element), 0, query, &mut cols);
    assert_eq!(1, templates.len());
    templates.pop().expect("Couldn't pop a vec of len 1!")
}

/// Combines the two things you need to turn a query into SQL and turn its results into
/// `QueryResults`.
pub struct CombinedProjection {
    pub sql_projection: Projection,
    pub projector: Box<Projector>,
}

/// Compute a suitable SQL projection for an algebrized query.
/// This takes into account a number of things:
/// - The variable list in the find spec.
/// - The presence of any aggregate operations in the find spec. TODO: for now we only handle
///   simple variables
/// - The bindings established by the topmost CC.
/// - The types known at algebrizing time.
/// - The types extracted from the store for unknown attributes.
pub fn query_projection(query: &AlgebraicQuery) -> CombinedProjection {
    use self::FindSpec::*;

    let column_count = query.find_spec.expected_column_count();
    let mut cols = Vec::with_capacity(column_count);
    let proj: Box<Projector> = 
        match query.find_spec {
            FindColl(ref element) => {
                let template = one_typed_index(element, query, &mut cols);
                Box::new(CollProjector::with_template(template))
            },

            FindScalar(ref element) => {
                let template = one_typed_index(element, query, &mut cols);
                Box::new(ScalarProjector::with_template(template))
            },

            FindRel(ref elements) => {
                let templates = project_elements(elements, 0, query, &mut cols);
                Box::new(RelProjector::with_templates(column_count, templates))
            },

            FindTuple(ref elements) => {
                let templates = project_elements(elements, 0, query, &mut cols);
                Box::new(TupleProjector::with_templates(column_count, templates))
            },
        };

    CombinedProjection {
        sql_projection: Projection::Columns(cols),
        projector: proj,
    }
}

