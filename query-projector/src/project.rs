// Copyright 2018 Mozilla
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

use std::collections::{
    BTreeSet,
};

use indexmap::{
    IndexSet,
};

use mentat_core::{
    SQLValueType,
    SQLValueTypeSet,
    ValueTypeSet,
};

use mentat_core::util::{
    Either,
};

use mentat_query::{
    Element,
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
    GroupBy,
    Name,
    Projection,
    ProjectedColumn,
};

use aggregates::{
    SimpleAggregation,
    projected_column_for_simple_aggregate,
};

use errors::{
    ErrorKind,
    Result,
};

use super::{
    TypedIndex,
};

/// An internal temporary struct to pass between the projection 'walk' and the
/// resultant projector.
/// Projection accumulates four things:
/// - Two SQL projection lists. We need two because aggregate queries are nested
///   in order to apply DISTINCT to values prior to aggregation.
/// - A collection of templates for the projector to use to extract values.
/// - A list of columns to use for grouping. Grouping is a property of the projection!
pub(crate) struct ProjectedElements {
    pub sql_projection: Projection,
    pub pre_aggregate_projection: Option<Projection>,
    pub templates: Vec<TypedIndex>,
    pub group_by: Vec<GroupBy>,
}

fn candidate_type_column(cc: &ConjoiningClauses, var: &Variable) -> Result<(ColumnOrExpression, Name)> {
    cc.extracted_types
      .get(var)
      .cloned()
      .map(|alias| {
          let type_name = VariableColumn::VariableTypeTag(var.clone()).column_name();
          (ColumnOrExpression::Column(alias), type_name)
      })
      .ok_or_else(|| ErrorKind::UnboundVariable(var.name()).into())
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
pub(crate) fn project_elements<'a, I: IntoIterator<Item = &'a Element>>(
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
    let mut min_max_count: usize = 0;
    let mut corresponding_count: usize = 0;
    let mut templates = vec![];

    let mut aggregates = false;

    // Any variable that appears intact in the :find clause, not inside an aggregate expression.
    // "Query variables not in aggregate expressions will group the results and appear intact
    // in the result."
    // We use an ordered set here so that we group in the correct order.
    let mut outer_variables = IndexSet::new();
    let mut corresponded_variables = IndexSet::new();

    // Any variable that we are projecting from the inner query.
    let mut inner_variables = BTreeSet::new();

    for e in elements {
        // Check for and reject duplicates.
        match e {
            &Element::Variable(ref var) => {
                if outer_variables.contains(var) {
                    bail!(ErrorKind::InvalidProjection(format!("Duplicate variable {} in query.", var)));
                }
                if corresponded_variables.contains(var) {
                    bail!(ErrorKind::InvalidProjection(format!("Can't project both {} and (the {}) from a query.", var, var)));
                }
            },
            &Element::Corresponding(ref var) => {
                if outer_variables.contains(var) {
                    bail!(ErrorKind::InvalidProjection(format!("Can't project both {} and (the {}) from a query.", var, var)));
                }
                if corresponded_variables.contains(var) {
                    bail!(ErrorKind::InvalidProjection(format!("(the {}) appears twice in query.", var)));
                }
            },
            &Element::Aggregate(_) => {
            },
        };

        // Record variables -- (the ?x) and ?x are different in this regard, because we don't want
        // to group on variables that are corresponding-projected.
        match e {
            &Element::Variable(ref var) => {
                outer_variables.insert(var.clone());
            },
            &Element::Corresponding(ref var) => {
                // We will project these later; don't put them in `outer_variables`
                // so we know not to group them.
                corresponding_count += 1;
                corresponded_variables.insert(var.clone());
            },
            &Element::Aggregate(_) => {
            },
        };

        // Now do the main processing of each element.
        match e {
            // Each time we come across a variable, we push a SQL column
            // into the SQL projection, aliased to the name of the variable,
            // and we push an annotated index into the projector.
            &Element::Variable(ref var) |
            &Element::Corresponding(ref var) => {
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
                    let (type_column, type_name) = candidate_type_column(&query.cc, &var)?;
                    inner_projection.push(ProjectedColumn(type_column, type_name.clone()));
                    outer_projection.push(Either::Left(type_name));
                }
            },
            &Element::Aggregate(ref a) => {
                if let Some(simple) = a.to_simple() {
                    aggregates = true;

                    use aggregates::SimpleAggregationOp::*;
                    match simple.op {
                        Max | Min => {
                            min_max_count += 1;
                        },
                        Avg | Count | Sum => (),
                    }

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
                            let (type_column, type_name) = candidate_type_column(&query.cc, &simple.var)?;
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

    match (min_max_count, corresponding_count) {
        (0, 0) | (_, 0) => {},
        (0, _) => {
            bail!(ErrorKind::InvalidProjection("Warning: used `the` without `min` or `max`.".to_string()));
        },
        (1, _) => {
            // This is the success case!
        },
        (n, c) => {
            bail!(ErrorKind::AmbiguousAggregates(n, c));
        },
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
            let (type_column, type_name) = candidate_type_column(&query.cc, &var)?;
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

    let vars = outer_variables.into_iter().zip(::std::iter::repeat(true));
    let corresponds = corresponded_variables.into_iter().zip(::std::iter::repeat(false));

    for (var, group) in vars.chain(corresponds) {
        if query.cc.is_value_bound(&var) {
            continue;
        }

        if group {
            // The GROUP BY goes outside, but it needs every variable and type tag to be
            // projected from inside. Collect in both directions here.
            let name = VariableColumn::Variable(var.clone()).column_name();
            group_by.push(GroupBy::ProjectedColumn(name));
        }

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
            if group {
                group_by.push(GroupBy::ProjectedColumn(type_name));
            }
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
                let (type_column, type_name) = candidate_type_column(&query.cc, &var)?;
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
