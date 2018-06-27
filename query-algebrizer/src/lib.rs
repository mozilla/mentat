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

#[macro_use] extern crate failure_derive;

extern crate mentat_core;
extern crate mentat_query;

use std::collections::BTreeSet;
use std::ops::Sub;
use std::rc::Rc;

#[macro_use]
mod errors;
mod types;
mod validate;
mod clauses;

use mentat_core::{
    CachedAttributes,
    Entid,
    Schema,
    TypedValue,
    ValueType,
    parse_query,
};

use mentat_core::counter::RcCounter;

use mentat_query::{
    Element,
    FindSpec,
    Limit,
    Order,
    ParsedQuery,
    SrcVar,
    Variable,
    WhereClause,
};

pub use errors::{
    AlgebrizerError,
    BindingError,
    Result,
};

pub use clauses::{
    QueryInputs,
    VariableBindings,
};

pub use types::{
    EmptyBecause,
    FindQuery,
};

/// A convenience wrapper around things known in memory: the schema and caches.
/// We use a trait object here to avoid making dozens of functions generic over the type
/// of the cache. If performance becomes a concern, we should hard-code specific kinds of
/// cache right here, and/or eliminate the Option.
#[derive(Clone, Copy)]
pub struct Known<'s, 'c> {
    pub schema: &'s Schema,
    pub cache: Option<&'c CachedAttributes>,
}

impl<'s, 'c> Known<'s, 'c> {
    pub fn for_schema(s: &'s Schema) -> Known<'s, 'static> {
        Known {
            schema: s,
            cache: None,
        }
    }

    pub fn new(s: &'s Schema, c: Option<&'c CachedAttributes>) -> Known<'s, 'c> {
        Known {
            schema: s,
            cache: c,
        }
    }
}

/// This is `CachedAttributes`, but with handy generic parameters.
/// Why not make the trait generic? Because then we can't use it as a trait object in `Known`.
impl<'s, 'c> Known<'s, 'c> {
    pub fn is_attribute_cached_reverse<U>(&self, entid: U) -> bool where U: Into<Entid> {
        self.cache
            .map(|cache| cache.is_attribute_cached_reverse(entid.into()))
            .unwrap_or(false)
    }

    pub fn is_attribute_cached_forward<U>(&self, entid: U) -> bool where U: Into<Entid> {
        self.cache
            .map(|cache| cache.is_attribute_cached_forward(entid.into()))
            .unwrap_or(false)
    }

    pub fn get_values_for_entid<U, V>(&self, schema: &Schema, attribute: U, entid: V) -> Option<&Vec<TypedValue>>
    where U: Into<Entid>, V: Into<Entid> {
        self.cache.and_then(|cache| cache.get_values_for_entid(schema, attribute.into(), entid.into()))
    }

    pub fn get_value_for_entid<U, V>(&self, schema: &Schema, attribute: U, entid: V) -> Option<&TypedValue>
    where U: Into<Entid>, V: Into<Entid> {
        self.cache.and_then(|cache| cache.get_value_for_entid(schema, attribute.into(), entid.into()))
    }

    pub fn get_entid_for_value<U>(&self, attribute: U, value: &TypedValue) -> Option<Entid>
    where U: Into<Entid> {
        self.cache.and_then(|cache| cache.get_entid_for_value(attribute.into(), value))
    }

    pub fn get_entids_for_value<U>(&self, attribute: U, value: &TypedValue) -> Option<&BTreeSet<Entid>>
    where U: Into<Entid> {
        self.cache.and_then(|cache| cache.get_entids_for_value(attribute.into(), value))
    }
}

#[derive(Debug)]
pub struct AlgebraicQuery {
    default_source: SrcVar,
    pub find_spec: Rc<FindSpec>,
    has_aggregates: bool,

    /// The set of variables that the caller wishes to be used for grouping when aggregating.
    /// These are specified in the query input, as `:with`, and are then chewed up during projection.
    /// If no variables are supplied, then no additional grouping is necessary beyond the
    /// non-aggregated projection list.
    pub with: BTreeSet<Variable>,

    /// Some query features, such as ordering, are implemented by implicit reference to SQL columns.
    /// In order for these references to be 'live', those columns must be projected.
    /// This is the set of variables that must be so projected.
    /// This is not necessarily every variable that will be so required -- some variables
    /// will already be in the projection list.
    pub named_projection: BTreeSet<Variable>,
    pub order: Option<Vec<OrderBy>>,
    pub limit: Limit,
    pub cc: clauses::ConjoiningClauses,
}

impl AlgebraicQuery {
    #[inline]
    pub fn is_known_empty(&self) -> bool {
        self.cc.is_known_empty()
    }

    /// Return true if every variable in the find spec is fully bound to a single value.
    pub fn is_fully_bound(&self) -> bool {
        self.find_spec
            .columns()
            .all(|e| match e {
                // Pull expressions are never fully bound.
                // TODO: but the 'inside' of a pull expression certainly can be.
                &Element::Pull(_) => false,

                &Element::Variable(ref var) |
                &Element::Corresponding(ref var) => self.cc.is_value_bound(var),

                // For now, we pretend that aggregate functions are never fully bound:
                // we don't statically compute them, even if we know the value of the var.
                &Element::Aggregate(ref _fn) => false,
            })
    }

    /// Return true if every variable in the find spec is fully bound to a single value,
    /// and evaluating the query doesn't require running SQL.
    pub fn is_fully_unit_bound(&self) -> bool {
        self.cc.wheres.is_empty() &&
        self.is_fully_bound()
    }


    /// Return a set of the input variables mentioned in the `:in` clause that have not yet been
    /// bound. We do this by looking at the CC.
    pub fn unbound_variables(&self) -> BTreeSet<Variable> {
        self.cc.input_variables.sub(&self.cc.value_bound_variable_set())
    }
}

pub fn algebrize_with_counter(known: Known, parsed: FindQuery, counter: usize) -> Result<AlgebraicQuery> {
    algebrize_with_inputs(known, parsed, counter, QueryInputs::default())
}

pub fn algebrize(known: Known, parsed: FindQuery) -> Result<AlgebraicQuery> {
    algebrize_with_inputs(known, parsed, 0, QueryInputs::default())
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
                    bail!(AlgebrizerError::UnboundVariable(var.name()))
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


fn simplify_limit(mut query: AlgebraicQuery) -> Result<AlgebraicQuery> {
    // Unpack any limit variables in place.
    let refined_limit =
        match query.limit {
            Limit::Variable(ref v) => {
                match query.cc.bound_value(v) {
                    Some(TypedValue::Long(n)) => {
                        if n <= 0 {
                            // User-specified limits should always be natural numbers (> 0).
                            bail!(AlgebrizerError::InvalidLimit(n.to_string(), ValueType::Long))
                        } else {
                            Some(Limit::Fixed(n as u64))
                        }
                    },
                    Some(val) => {
                        // Same.
                        bail!(AlgebrizerError::InvalidLimit(format!("{:?}", val), val.value_type()))
                    },
                    None => {
                        // We know that the limit variable is mentioned in `:in`.
                        // That it's not bound here implies that we haven't got all the variables
                        // we'll need to run the query yet.
                        // (We should never hit this in `q_once`.)
                        // Simply pass the `Limit` through to `SelectQuery` untouched.
                        None
                    },
                }
            },
            Limit::None => None,
            Limit::Fixed(_) => None,
        };

    if let Some(lim) = refined_limit {
        query.limit = lim;
    }
    Ok(query)
}

pub fn algebrize_with_inputs(known: Known,
                             parsed: FindQuery,
                             counter: usize,
                             inputs: QueryInputs) -> Result<AlgebraicQuery> {
    let alias_counter = RcCounter::with_initial(counter);
    let mut cc = ConjoiningClauses::with_inputs_and_alias_counter(parsed.in_vars, inputs, alias_counter);

    // This is so the rest of the query knows that `?x` is a ref if `(pull ?x …)` appears in `:find`.
    cc.derive_types_from_find_spec(&parsed.find_spec);

    // Do we have a variable limit? If so, tell the CC that the var must be numeric.
    if let &Limit::Variable(ref var) = &parsed.limit {
        cc.constrain_var_to_long(var.clone());
    }

    // TODO: integrate default source into pattern processing.
    // TODO: flesh out the rest of find-into-context.
    cc.apply_clauses(known, parsed.where_clauses)?;

    cc.expand_column_bindings();
    cc.prune_extracted_types();
    cc.process_required_types()?;

    let (order, extra_vars) = validate_and_simplify_order(&cc, parsed.order)?;

    // This might leave us with an unused `:in` variable.
    let limit = if parsed.find_spec.is_unit_limited() { Limit::Fixed(1) } else { parsed.limit };
    let q = AlgebraicQuery {
        default_source: parsed.default_source,
        find_spec: Rc::new(parsed.find_spec),
        has_aggregates: false,           // TODO: we don't parse them yet.
        with: parsed.with,
        named_projection: extra_vars,
        order: order,
        limit: limit,
        cc: cc,
    };

    // Substitute in any fixed values and fail if they're out of range.
    simplify_limit(q)
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
    FulltextColumn,
    OrderBy,
    QualifiedAlias,
    QueryValue,
    SourceAlias,
    TableAlias,
    VariableColumn,
};


impl FindQuery {
    pub fn simple(spec: FindSpec, where_clauses: Vec<WhereClause>) -> FindQuery {
        FindQuery {
            find_spec: spec,
            default_source: SrcVar::DefaultSrc,
            with: BTreeSet::default(),
            in_vars: BTreeSet::default(),
            in_sources: BTreeSet::default(),
            limit: Limit::None,
            where_clauses: where_clauses,
            order: None,
        }
    }

    pub fn from_parsed_query(parsed: ParsedQuery) -> Result<FindQuery> {
        let in_vars = {
            let mut set: BTreeSet<Variable> = BTreeSet::default();

            for var in parsed.in_vars.into_iter() {
                if !set.insert(var.clone()) {
                    bail!(AlgebrizerError::DuplicateVariableError(var.name(), ":in"));
                }
            }

            set
        };

        let with = {
            let mut set: BTreeSet<Variable> = BTreeSet::default();

            for var in parsed.with.into_iter() {
                if !set.insert(var.clone()) {
                    bail!(AlgebrizerError::DuplicateVariableError(var.name(), ":with"));
                }
            }

            set
        };

        // Make sure that if we have `:limit ?x`, `?x` appears in `:in`.
        if let Limit::Variable(ref v) = parsed.limit {
            if !in_vars.contains(v) {
                bail!(AlgebrizerError::UnknownLimitVar(v.name()));
            }
        }

        Ok(FindQuery {
            find_spec: parsed.find_spec,
            default_source: parsed.default_source,
            with,
            in_vars,
            in_sources: parsed.in_sources,
            limit: parsed.limit,
            where_clauses: parsed.where_clauses,
            order: parsed.order,
        })
    }
}

pub fn parse_find_string(string: &str) -> Result<FindQuery> {
    parse_query(string)
        .map_err(|e| e.into())
        .and_then(|parsed| FindQuery::from_parsed_query(parsed))
}
