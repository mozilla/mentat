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

extern crate mentat_core;
extern crate mentat_query;

use std::collections::BTreeSet;
use std::ops::Sub;

mod errors;
mod types;
mod validate;
mod clauses;

use mentat_core::{
    Schema,
};

use mentat_core::counter::RcCounter;

use mentat_query::{
    FindQuery,
    FindSpec,
    Order,
    SrcVar,
    Variable,
};

pub use errors::{
    Error,
    ErrorKind,
    Result,
};

pub use clauses::{
    QueryInputs,
};

#[allow(dead_code)]
pub struct AlgebraicQuery {
    default_source: SrcVar,
    pub find_spec: FindSpec,
    has_aggregates: bool,
    pub with: BTreeSet<Variable>,
    pub order: Option<Vec<OrderBy>>,
    pub limit: Option<u64>,
    pub cc: clauses::ConjoiningClauses,
}

impl AlgebraicQuery {
    /**
     * Apply a new limit to this query, if one is provided and any existing limit is larger.
     */
    pub fn apply_limit(&mut self, limit: Option<u64>) {
        match self.limit {
            None => self.limit = limit,
            Some(existing) =>
                match limit {
                    None => (),
                    Some(new) =>
                        if new < existing {
                            self.limit = limit;
                        },
                },
        };
    }

    #[inline]
    pub fn is_known_empty(&self) -> bool {
        self.cc.is_known_empty()
    }

    /// Return a set of the input variables mentioned in the `:in` clause that have not yet been
    /// bound. We do this by looking at the CC.
    pub fn unbound_variables(&self) -> BTreeSet<Variable> {
        self.cc.input_variables.sub(&self.cc.value_bound_variables())
    }
}

pub fn algebrize_with_counter(schema: &Schema, parsed: FindQuery, counter: usize) -> Result<AlgebraicQuery> {
    algebrize_with_inputs(schema, parsed, counter, QueryInputs::default())
}

pub fn algebrize(schema: &Schema, parsed: FindQuery) -> Result<AlgebraicQuery> {
    algebrize_with_inputs(schema, parsed, 0, QueryInputs::default())
}

/// Take an ordering list. Any variables that aren't fixed by the query are used to produce
/// a vector of `OrderBy` instances, including type comparisons if necessary. This function also
/// returns a set of variables that should be added to the `with` clause to make the ordering
/// clauses possible.
fn validate_and_simplify_order(cc: &ConjoiningClauses, order: Option<Vec<Order>>)
    -> Result<(Option<Vec<OrderBy>>, BTreeSet<Variable>)> {
    match order {
        None => Ok((None, BTreeSet::default())),
        Some(order) => {
            let mut order_bys: Vec<OrderBy> = Vec::with_capacity(order.len() * 2);   // Space for tags.
            let mut vars: BTreeSet<Variable> = BTreeSet::default();

            for Order(direction, var) in order.into_iter() {
                // Eliminate any ordering clauses that are bound to fixed values.
                if cc.bound_value(&var).is_some() {
                    continue;
                }

                // Fail if the var isn't bound by the query.
                if !cc.column_bindings.contains_key(&var) {
                    bail!(ErrorKind::UnboundVariable(var.name()));
                }

                // Otherwise, determine if we also need to order by type…
                if cc.known_type(&var).is_none() {
                    order_bys.push(OrderBy(direction.clone(), VariableColumn::VariableTypeTag(var.clone())));
                }
                order_bys.push(OrderBy(direction, VariableColumn::Variable(var.clone())));
                vars.insert(var.clone());
            }

            Ok((if order_bys.is_empty() { None } else { Some(order_bys) }, vars))
        }
    }
}

pub fn algebrize_with_inputs(schema: &Schema,
                             parsed: FindQuery,
                             counter: usize,
                             inputs: QueryInputs) -> Result<AlgebraicQuery> {
    let alias_counter = RcCounter::with_initial(counter);
    let mut cc = ConjoiningClauses::with_inputs_and_alias_counter(parsed.in_vars, inputs, alias_counter);

    // TODO: integrate default source into pattern processing.
    // TODO: flesh out the rest of find-into-context.
    let where_clauses = parsed.where_clauses;
    for where_clause in where_clauses {
        cc.apply_clause(schema, where_clause)?;
    }
    cc.expand_column_bindings();
    cc.prune_extracted_types();

    let (order, extra_vars) = validate_and_simplify_order(&cc, parsed.order)?;
    let with: BTreeSet<Variable> = parsed.with.into_iter().chain(extra_vars.into_iter()).collect();
    let limit = if parsed.find_spec.is_unit_limited() { Some(1) } else { None };
    Ok(AlgebraicQuery {
        default_source: parsed.default_source,
        find_spec: parsed.find_spec,
        has_aggregates: false,           // TODO: we don't parse them yet.
        with: with,
        order: order,
        limit: limit,
        cc: cc,
    })
}

pub use clauses::{
    ConjoiningClauses,
};

pub use types::{
    Column,
    ColumnAlternation,
    ColumnConstraint,
    ColumnConstraintOrAlternation,
    ColumnIntersection,
    ColumnName,
    ComputedTable,
    DatomsColumn,
    DatomsTable,
    OrderBy,
    QualifiedAlias,
    QueryValue,
    SourceAlias,
    TableAlias,
    VariableColumn,
};

