// Copyright 2016 Mozilla
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

use std::rc::Rc;

use mentat_core::{
    Schema,
    SQLValueType,
    TypedValue,
    ValueType,
};

use mentat_query::{
    Binding,
    FnArg,
    NonIntegerConstant,
    Variable,
    VariableOrPlaceholder,
    WhereFn,
};

use clauses::{
    ConjoiningClauses,
    PushComputed,
};

use errors::{
    BindingError,
    Error,
    ErrorKind,
    Result,
};

use super::QualifiedAlias;

use types::{
    ComputedTable,
    SourceAlias,
    ValueTypeSet,
    VariableColumn,
};

macro_rules! check_type_set {
    ($types: expr, $type: path) => { {
        if !$types.contains($type) {
            bail!(ErrorKind::InvalidGroundConstant);
        }
    } }
}

/// Conversion of FnArgs to TypedValues.
impl ConjoiningClauses {
    /// Convert the provided `FnArg` to a `TypedValue`.
    /// The conversion depends on, and can fail because of:
    /// - Existing known types of a variable to which this arg will be bound.
    /// - Existing bindings of a variable `FnArg`.
    fn typed_value_from_arg<'s>(&self, schema: &'s Schema, arg: FnArg, known_types: ValueTypeSet) -> Result<TypedValue> {
        if known_types.is_empty() {
            bail!(ErrorKind::InvalidGroundConstant);
        }

        match arg {
            // Longs are potentially ambiguous: they might be longs or entids.
            FnArg::EntidOrInteger(x) => {
                match (ValueType::Ref.accommodates_integer(x),
                       known_types.contains(ValueType::Ref),
                       known_types.contains(ValueType::Long)) {
                    (true, true, true) => {
                        // Ambiguous: this arg could be an entid or a long.
                        // We default to long.
                        Ok(TypedValue::Long(x))
                    },
                    (true, true, false) => {
                        // This can only be a ref.
                        Ok(TypedValue::Ref(x))
                    },
                    (_, false, true) => {
                        // This can only be a long.
                        Ok(TypedValue::Long(x))
                    },
                    (false, true, _) => {
                        // This isn't a valid ref, but that's the type to which this must conform!
                        bail!(ErrorKind::InvalidGroundConstant)
                    },
                    (_, false, false) => {
                        // Non-overlapping type sets.
                        bail!(ErrorKind::InvalidGroundConstant)
                    },
                }
            },

            // If you definitely want to look up an ident, do it before running the query.
            FnArg::IdentOrKeyword(x) => {
                match (known_types.contains(ValueType::Ref),
                       known_types.contains(ValueType::Keyword)) {
                    (true, true) => {
                        // Ambiguous: this could be a keyword or an ident.
                        // Default to keyword.
                        Ok(TypedValue::Keyword(Rc::new(x)))
                    },
                    (true, false) => {
                        // This can only be an ident. Look it up!
                        schema.get_entid(&x)
                              .map(TypedValue::Ref)
                              .ok_or_else(|| Error::from_kind(ErrorKind::InvalidGroundConstant))
                    },
                    (false, true) => {
                        Ok(TypedValue::Keyword(Rc::new(x)))
                    },
                    (false, false) => {
                        bail!(ErrorKind::InvalidGroundConstant)
                    },
                }
            },

            FnArg::Variable(in_var) => {
                if !self.input_variables.contains(&in_var) {
                    bail!(ErrorKind::UnboundVariable((*in_var.0).clone()));
                }
                match self.bound_value(&in_var) {
                    // The type is already known if it's a bound variable….
                    Some(ref in_value) => Ok(in_value.clone()),
                    None => bail!(ErrorKind::UnboundVariable((*in_var.0).clone())),
                }
            },

            // This isn't implemented yet.
            FnArg::Constant(NonIntegerConstant::BigInteger(_)) => unimplemented!(),

            // These don't make sense here.
            FnArg::Vector(_) |
            FnArg::SrcVar(_) => bail!(ErrorKind::InvalidGroundConstant),

            // These are all straightforward.
            FnArg::Constant(NonIntegerConstant::Boolean(x)) => {
                check_type_set!(known_types, ValueType::Boolean);
                Ok(TypedValue::Boolean(x).into())
            },
            FnArg::Constant(NonIntegerConstant::Instant(x)) => {
                check_type_set!(known_types, ValueType::Instant);
                Ok(TypedValue::Instant(x).into())
            },
            FnArg::Constant(NonIntegerConstant::Uuid(x)) => {
                check_type_set!(known_types, ValueType::Uuid);
                Ok(TypedValue::Uuid(x).into())
            },
            FnArg::Constant(NonIntegerConstant::Float(x)) => {
                check_type_set!(known_types, ValueType::Double);
                Ok(TypedValue::Double(x).into())
            },
            FnArg::Constant(NonIntegerConstant::Text(x)) => {
                check_type_set!(known_types, ValueType::String);
                Ok(TypedValue::String(x).into())
            },
        }
    }
}

/// Application of `where` functions.
impl ConjoiningClauses {
    /// There are several kinds of functions binding variables in our Datalog:
    /// - A set of functions like `ground`, fulltext` and `get-else` that are translated into SQL
    ///   `VALUES`, `MATCH`, or `JOIN`, yielding bindings.
    /// - In the future, some functions that are implemented via function calls in SQLite.
    ///
    /// At present we have implemented only a limited selection of functions.
    pub fn apply_where_fn<'s>(&mut self, schema: &'s Schema, where_fn: WhereFn) -> Result<()> {
        // Because we'll be growing the set of built-in functions, handling each differently, and
        // ultimately allowing user-specified functions, we match on the function name first.
        match where_fn.operator.0.as_str() {
            "ground" => self.apply_ground(schema, where_fn),
            _ => bail!(ErrorKind::UnknownFunction(where_fn.operator.clone())),
        }
    }

    fn apply_ground_place<'s>(&mut self, schema: &'s Schema, var: VariableOrPlaceholder, arg: FnArg) -> Result<()> {
        match var {
            VariableOrPlaceholder::Placeholder => Ok(()),
            VariableOrPlaceholder::Variable(var) => self.apply_ground_var(schema, var, arg),
        }
    }

    /// Constrain the CC to associate the given var with the given ground argument.
    fn apply_ground_var<'s>(&mut self, schema: &'s Schema, var: Variable, arg: FnArg) -> Result<()> {
        let value = self.typed_value_from_arg(schema, arg, self.known_type_set(&var))?;

        if let Some(existing) = self.bound_value(&var) {
            if existing != value {
                // TODO: better error.
                bail!(ErrorKind::InvalidGroundConstant);
            }
        } else {
            self.bind_value(&var, value.clone());
        }

        // Check to see whether this variable is already associated to a column.
        // If so, we want to add an equality filter (or, in the future, redo the existing patterns).
        if let Some(QualifiedAlias(table, column)) = self.column_bindings
                                                         .get(&var)
                                                         .and_then(|vec| vec.get(0).cloned()) {
            self.constrain_column_to_constant(table, column, value);
        }

        Ok(())
    }

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

    pub fn apply_ground<'s>(&mut self, schema: &'s Schema, where_fn: WhereFn) -> Result<()> {
        if where_fn.args.len() != 1 {
            bail!(ErrorKind::InvalidNumberOfArguments(where_fn.operator.clone(), where_fn.args.len(), 1));
        }

        let mut args = where_fn.args.into_iter();

        if where_fn.binding.is_empty() {
            // The binding must introduce at least one bound variable.
            bail!(ErrorKind::InvalidBinding(where_fn.operator.clone(), BindingError::NoBoundVariable));
        }

        if !where_fn.binding.is_valid() {
            // The binding must not duplicate bound variables.
            bail!(ErrorKind::InvalidBinding(where_fn.operator.clone(), BindingError::RepeatedBoundVariable));
        }

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
                    bail!(ErrorKind::GroundBindingsMismatch);
                }
                for (place, arg) in places.into_iter().zip(children.into_iter()) {
                    self.apply_ground_place(schema, place, arg)?;
                }
                Ok(())
            },

            // Collection bindings and rel bindings are similar in that they are both
            // implemented as a subquery with a projection list and a set of values.
            // The difference is that BindColl has only a single variable, and its values
            // are all in a single structure. That makes it substantially simpler!
            (Binding::BindColl(var), FnArg::Vector(children)) => {
                // Turn a collection of arguments into a Vec of `TypedValue`s of the same type.
                let known_types = self.known_type_set(&var);
                // Check that every value has the same type.
                let mut accumulated_types = ValueTypeSet::default();
                let values = children.into_iter()
                                        .map(|arg| {
                                            // We need to get conversion errors out.
                                            let r = self.typed_value_from_arg(schema, arg, known_types);
                                            if let Ok(ref tv) = r {
                                                if accumulated_types.insert(tv.value_type()) &&
                                                   !accumulated_types.is_unit() {
                                                    // Values not all of the same type.
                                                    bail!(ErrorKind::InvalidGroundConstant);
                                                }
                                            }
                                            r
                                        })
                                        .collect::<Result<Vec<_>>>()?;

                // Otherwise, we now have the values and the type.
                let types = vec![accumulated_types.exemplar().unwrap()];
                let names = vec![var.clone()];

                self.collect_named_bindings(schema, names, types, values);
                Ok(())
            },

            (Binding::BindRel(places), FnArg::Vector(rows)) => {
                if rows.is_empty() {
                    bail!(ErrorKind::InvalidGroundConstant);
                }

                // Grab the known types to which these args must conform, and track
                // the places that won't be bound in the output.
                let template: Vec<(bool, ValueTypeSet)> =
                    places.iter()
                          .map(|x| match x {
                              &VariableOrPlaceholder::Placeholder     => (false, ValueTypeSet::any()),
                              &VariableOrPlaceholder::Variable(ref v) => (true, self.known_type_set(v)),
                          })
                          .collect();

                // The expected 'width' of the matrix is the number of named variables.
                let full_width = places.len();
                let names: Vec<Variable> = places.into_iter().filter_map(|x| x.into_var()).collect();
                let expected_width = names.len();
                let expected_rows = rows.len();

                if expected_width == 0 {
                    // They can't all be placeholders.
                    bail!(ErrorKind::InvalidGroundConstant);
                }

                // Accumulate values into `matrix` and types into `a_t_f_c`.
                // This representation of a rectangular matrix is more efficient than one composed
                // of N separate vectors.
                let mut matrix = Vec::with_capacity(expected_width * expected_rows);
                let mut accumulated_types_for_columns = vec![ValueTypeSet::default(); expected_width];

                // Loop so we can bail out.
                for row in rows.into_iter() {
                    match row {
                        FnArg::Vector(cols) => {
                            // Make sure that every row is the same length.
                            if cols.len() != full_width {
                                bail!(ErrorKind::InvalidGroundConstant);
                            }

                            // Convert each item in the row.
                            let r = cols.into_iter()
                                        .zip(template.iter())
                                        .filter_map(|(col, pair)| {
                                            if pair.0 {
                                                Some(self.typed_value_from_arg(schema, col, pair.1))
                                            } else {
                                                None
                                            }
                                        })
                                        .collect::<Result<Vec<_>>>()?;

                            // Accumulate the value into the matrix and the type into the type set.
                            for (val, acc) in r.into_iter().zip(accumulated_types_for_columns.iter_mut()) {
                                let inserted = acc.insert(val.value_type());
                                if inserted && !acc.is_unit() {
                                    // Heterogeneous types.
                                    bail!(ErrorKind::InvalidGroundConstant);
                                }
                                matrix.push(val);
                            }
                        },
                        _ => bail!(ErrorKind::InvalidGroundConstant),
                    }
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
            (_, _) => bail!(ErrorKind::InvalidGroundConstant),
        }
    }
}

#[cfg(test)]
mod testing {
    use super::*;

    use mentat_core::{
        Attribute,
        ValueType,
    };

    use mentat_query::{
        Binding,
        FnArg,
        NamespacedKeyword,
        PlainSymbol,
        SrcVar,
        Variable,
    };

    use clauses::{
        add_attribute,
        associate_ident,
    };

    use types::{
        ValueTypeSet,
    };

    #[test]
    fn test_apply_ground() {
        let vz = Variable::from_valid_name("?z");

        let mut cc = ConjoiningClauses::default();
        let mut schema = Schema::default();

        associate_ident(&mut schema, NamespacedKeyword::new("foo", "fts"), 100);
        add_attribute(&mut schema, 100, Attribute {
            value_type: ValueType::String,
            index: true,
            fulltext: true,
            ..Default::default()
        });

        // It's awkward enough to write these expansions that we give the details for the simplest
        // case only.  See the tests of the translator for more extensive (albeit looser) coverage.
        let op = PlainSymbol::new("ground");
        cc.apply_ground(&schema, WhereFn {
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
