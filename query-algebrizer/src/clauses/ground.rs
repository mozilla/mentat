// Copyright 2016 Mozilla
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

use core_traits::{
    ValueType,
    TypedValue,
};

use mentat_core::{
    Schema,
    ValueTypeSet,
};

use mentat_query::{
    Binding,
    FnArg,
    Variable,
    VariableOrPlaceholder,
    WhereFn,
};

use clauses::{
    ConjoiningClauses,
    PushComputed,
};

use clauses::convert::ValueConversion;

use errors::{
    AlgebrizerError,
    BindingError,
    Result,
};

use types::{
    ComputedTable,
    EmptyBecause,
    SourceAlias,
    VariableColumn,
};

use Known;

impl ConjoiningClauses {
    /// Take a relation: a matrix of values which will successively bind to named variables of
    /// the provided types.
    /// Construct a computed table to yield this relation.
    /// This function will panic if some invariants are not met.
    fn collect_named_bindings<'s>(&mut self, schema: &'s Schema, names: Vec<Variable>, types: Vec<ValueType>, values: Vec<TypedValue>) {
        if values.is_empty() {
            return;
        }

        assert!(!names.is_empty());
        assert_eq!(names.len(), types.len());
        assert!(values.len() >= names.len());
        assert_eq!(values.len() % names.len(), 0);      // It's an exact multiple.

        let named_values = ComputedTable::NamedValues {
            names: names.clone(),
            values: values,
        };

        let table = self.computed_tables.push_computed(named_values);
        let alias = self.next_alias_for_table(table);

        // Stitch the computed table into column_bindings, so we get cross-linking.
        for (name, ty) in names.iter().zip(types.into_iter()) {
            self.constrain_var_to_type(name.clone(), ty);
            self.bind_column_to_var(schema, alias.clone(), VariableColumn::Variable(name.clone()), name.clone());
        }

        self.from.push(SourceAlias(table, alias));
    }

    fn apply_ground_place<'s>(&mut self, schema: &'s Schema, var: VariableOrPlaceholder, arg: FnArg) -> Result<()> {
        match var {
            VariableOrPlaceholder::Placeholder => Ok(()),
            VariableOrPlaceholder::Variable(var) => self.apply_ground_var(schema, var, arg),
        }
    }

    /// Constrain the CC to associate the given var with the given ground argument.
    /// Marks known-empty on failure.
    fn apply_ground_var<'s>(&mut self, schema: &'s Schema, var: Variable, arg: FnArg) -> Result<()> {
        let known_types = self.known_type_set(&var);
        match self.typed_value_from_arg(schema, &var, arg, known_types)? {
            ValueConversion::Val(value) => self.apply_ground_value(var, value),
            ValueConversion::Impossible(because) => {
                self.mark_known_empty(because);
                Ok(())
            },
        }
    }

    /// Marks known-empty on failure.
    fn apply_ground_value(&mut self, var: Variable, value: TypedValue) -> Result<()> {
        if let Some(existing) = self.bound_value(&var) {
            if existing != value {
                self.mark_known_empty(EmptyBecause::ConflictingBindings {
                    var: var.clone(),
                    existing: existing.clone(),
                    desired: value,
                });
                return Ok(())
            }
        } else {
            self.bind_value(&var, value.clone());
        }

        Ok(())
    }

    pub(crate) fn apply_ground(&mut self, known: Known, where_fn: WhereFn) -> Result<()> {
        if where_fn.args.len() != 1 {
            bail!(AlgebrizerError::InvalidNumberOfArguments(where_fn.operator.clone(), where_fn.args.len(), 1));
        }

        let mut args = where_fn.args.into_iter();

        if where_fn.binding.is_empty() {
            // The binding must introduce at least one bound variable.
            bail!(AlgebrizerError::InvalidBinding(where_fn.operator.clone(), BindingError::NoBoundVariable));
        }

        if !where_fn.binding.is_valid() {
            // The binding must not duplicate bound variables.
            bail!(AlgebrizerError::InvalidBinding(where_fn.operator.clone(), BindingError::RepeatedBoundVariable));
        }

        let schema = known.schema;

        // Scalar and tuple bindings are a little special: because there's only one value,
        // we can immediately substitute the value as a known value in the CC, additionally
        // generating a WHERE clause if columns have already been bound.
        match (where_fn.binding, args.next().unwrap()) {
            (Binding::BindScalar(var), constant) =>
                self.apply_ground_var(schema, var, constant),

            (Binding::BindTuple(places), FnArg::Vector(children)) => {
                // Just the same, but we bind more than one column at a time.
                if children.len() != places.len() {
                    // Number of arguments don't match the number of values. TODO: better error message.
                    bail!(AlgebrizerError::GroundBindingsMismatch)
                }
                for (place, arg) in places.into_iter().zip(children.into_iter()) {
                    self.apply_ground_place(schema, place, arg)?  // TODO: short-circuit on impossible.
                }
                Ok(())
            },

            // Collection bindings and rel bindings are similar in that they are both
            // implemented as a subquery with a projection list and a set of values.
            // The difference is that BindColl has only a single variable, and its values
            // are all in a single structure. That makes it substantially simpler!
            (Binding::BindColl(var), FnArg::Vector(children)) => {
                if children.is_empty() {
                    bail!(AlgebrizerError::InvalidGroundConstant)
                }

                // Turn a collection of arguments into a Vec of `TypedValue`s of the same type.
                let known_types = self.known_type_set(&var);
                // Check that every value has the same type.
                let mut accumulated_types = ValueTypeSet::none();
                let mut skip: Option<EmptyBecause> = None;
                let values = children.into_iter()
                                     .filter_map(|arg| -> Option<Result<TypedValue>> {
                                         // We need to get conversion errors out.
                                         // We also want to mark known-empty on impossibilty, but
                                         // still detect serious errors.
                                         match self.typed_value_from_arg(schema, &var, arg, known_types) {
                                             Ok(ValueConversion::Val(tv)) => {
                                                 if accumulated_types.insert(tv.value_type()) &&
                                                    !accumulated_types.is_unit() {
                                                     // Values not all of the same type.
                                                     Some(Err(AlgebrizerError::InvalidGroundConstant.into()))
                                                 } else {
                                                     Some(Ok(tv))
                                                 }
                                             },
                                             Ok(ValueConversion::Impossible(because)) => {
                                                 // Skip this value.
                                                 skip = Some(because);
                                                 None
                                             },
                                             Err(e) => Some(Err(e.into())),
                                         }
                                     })
                                     .collect::<Result<Vec<TypedValue>>>()?;

                if values.is_empty() {
                    let because = skip.expect("we skipped all rows for a reason");
                    self.mark_known_empty(because);
                    return Ok(());
                }

                // Otherwise, we now have the values and the type.
                let types = vec![accumulated_types.exemplar().unwrap()];
                let names = vec![var.clone()];

                self.collect_named_bindings(schema, names, types, values);
                Ok(())
            },

            (Binding::BindRel(places), FnArg::Vector(rows)) => {
                if rows.is_empty() {
                    bail!(AlgebrizerError::InvalidGroundConstant)
                }

                // Grab the known types to which these args must conform, and track
                // the places that won't be bound in the output.
                let template: Vec<Option<(Variable, ValueTypeSet)>> =
                    places.iter()
                          .map(|x| match x {
                              &VariableOrPlaceholder::Placeholder     => None,
                              &VariableOrPlaceholder::Variable(ref v) => Some((v.clone(), self.known_type_set(v))),
                          })
                          .collect();

                // The expected 'width' of the matrix is the number of named variables.
                let full_width = places.len();
                let names: Vec<Variable> = places.into_iter().filter_map(|x| x.into_var()).collect();
                let expected_width = names.len();
                let expected_rows = rows.len();

                if expected_width == 0 {
                    // They can't all be placeholders.
                    bail!(AlgebrizerError::InvalidGroundConstant)
                }

                // Accumulate values into `matrix` and types into `a_t_f_c`.
                // This representation of a rectangular matrix is more efficient than one composed
                // of N separate vectors.
                let mut matrix = Vec::with_capacity(expected_width * expected_rows);
                let mut accumulated_types_for_columns = vec![ValueTypeSet::none(); expected_width];

                // Loop so we can bail out.
                let mut skipped_all: Option<EmptyBecause> = None;
                for row in rows.into_iter() {
                    match row {
                        FnArg::Vector(cols) => {
                            // Make sure that every row is the same length.
                            if cols.len() != full_width {
                                bail!(AlgebrizerError::InvalidGroundConstant)
                            }

                            // TODO: don't accumulate twice.
                            let mut vals = Vec::with_capacity(expected_width);
                            let mut skip: Option<EmptyBecause> = None;
                            for (col, pair) in cols.into_iter().zip(template.iter()) {
                                // Now we have (val, Option<(name, known_types)>). Silly,
                                // but this is how we iter!
                                // Convert each item in the row.
                                // If any value in the row is impossible, then skip the row.
                                // If all rows are impossible, fail the entire CC.
                                if let &Some(ref pair) = pair {
                                    match self.typed_value_from_arg(schema, &pair.0, col, pair.1)? {
                                        ValueConversion::Val(tv) => vals.push(tv),
                                        ValueConversion::Impossible(because) => {
                                            // Skip this row. It cannot produce bindings.
                                            skip = Some(because);
                                            break;
                                        },
                                    }
                                }
                            }

                            if skip.is_some() {
                                // Skip this row and record why, in case we skip all.
                                skipped_all = skip;
                                continue;
                            }

                            // Accumulate the values into the matrix and the types into the type set.
                            for (val, acc) in vals.into_iter().zip(accumulated_types_for_columns.iter_mut()) {
                                let inserted = acc.insert(val.value_type());
                                if inserted && !acc.is_unit() {
                                    // Heterogeneous types.
                                    bail!(AlgebrizerError::InvalidGroundConstant)
                                }
                                matrix.push(val);
                            }

                        },
                        _ => bail!(AlgebrizerError::InvalidGroundConstant),
                    }
                }

                // Do we have rows? If not, the CC cannot succeed.
                if matrix.is_empty() {
                    // We will either have bailed or will have accumulated *something* into the matrix,
                    // so we can safely unwrap here.
                    self.mark_known_empty(skipped_all.expect("we skipped for a reason"));
                    return Ok(());
                }

                // Take the single type from each set. We know there's only one: we got at least one
                // type, 'cos we bailed out for zero rows, and we also bailed out each time we
                // inserted a second type.
                // By restricting to homogeneous columns, we greatly simplify projection. In the
                // future, we could loosen this restriction, at the cost of projecting (some) value
                // type tags. If and when we want to algebrize in two phases and allow for
                // late-binding input variables, we'll probably be able to loosen this restriction
                // with little penalty.
                let types = accumulated_types_for_columns.into_iter()
                                                         .map(|x| x.exemplar().unwrap())
                                                         .collect();
                self.collect_named_bindings(schema, names, types, matrix);
                Ok(())
            },
            (_, _) => bail!(AlgebrizerError::InvalidGroundConstant),
        }
    }
}

#[cfg(test)]
mod testing {
    use super::*;

    use core_traits::{
        ValueType,
    };

    use mentat_core::{
        Attribute,
    };

    use mentat_query::{
        Binding,
        FnArg,
        Keyword,
        PlainSymbol,
        Variable,
    };

    use clauses::{
        add_attribute,
        associate_ident,
    };

    #[test]
    fn test_apply_ground() {
        let vz = Variable::from_valid_name("?z");

        let mut cc = ConjoiningClauses::default();
        let mut schema = Schema::default();

        associate_ident(&mut schema, Keyword::namespaced("foo", "fts"), 100);
        add_attribute(&mut schema, 100, Attribute {
            value_type: ValueType::String,
            index: true,
            fulltext: true,
            ..Default::default()
        });

        let known = Known::for_schema(&schema);

        // It's awkward enough to write these expansions that we give the details for the simplest
        // case only.  See the tests of the translator for more extensive (albeit looser) coverage.
        let op = PlainSymbol::plain("ground");
        cc.apply_ground(known, WhereFn {
            operator: op,
            args: vec![
                FnArg::EntidOrInteger(10),
            ],
            binding: Binding::BindScalar(vz.clone()),
        }).expect("to be able to apply_ground");

        assert!(!cc.is_known_empty());

        // Finally, expand column bindings.
        cc.expand_column_bindings();
        assert!(!cc.is_known_empty());

        let clauses = cc.wheres;
        assert_eq!(clauses.len(), 0);

        let column_bindings = cc.column_bindings;
        assert_eq!(column_bindings.len(), 0);           // Scalar doesn't need this.

        let known_types = cc.known_types;
        assert_eq!(known_types.len(), 1);
        assert_eq!(known_types.get(&vz).expect("to know the type of ?z"),
                   &ValueTypeSet::of_one(ValueType::Long));

        let value_bindings = cc.value_bindings;
        assert_eq!(value_bindings.len(), 1);
        assert_eq!(value_bindings.get(&vz).expect("to have a value for ?z"),
                   &TypedValue::Long(10));        // We default to Long instead of entid.
    }
}
