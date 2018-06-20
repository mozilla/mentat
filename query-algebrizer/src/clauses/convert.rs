// Copyright 2016 Mozilla
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

use mentat_core::{
    HasSchema,
    Schema,
    SQLValueType,
    TypedValue,
    ValueType,
    ValueTypeSet,
};

use mentat_query::{
    FnArg,
    NonIntegerConstant,
    Variable,
};

use clauses::{
    ConjoiningClauses,
};

use errors::{
    AlgebrizerError,
    Result,
};

use types::{
    EmptyBecause,
};

macro_rules! coerce_to_typed_value {
    ($var: ident, $val: ident, $types: expr, $type: path, $constructor: path) => { {
        Ok(if !$types.contains($type) {
               Impossible(EmptyBecause::TypeMismatch {
                   var: $var.clone(),
                   existing: $types,
                   desired: ValueTypeSet::of_one($type),
               })
           } else {
               Val($constructor($val).into())
           })
    } }
}

pub(crate) trait ValueTypes {
    fn potential_types(&self, schema: &Schema) -> Result<ValueTypeSet>;
}

impl ValueTypes for FnArg {
    fn potential_types(&self, schema: &Schema) -> Result<ValueTypeSet> {
        Ok(match self {
                &FnArg::EntidOrInteger(x) => {
                    if ValueType::Ref.accommodates_integer(x) {
                        // TODO: also see if it's a valid entid?
                        ValueTypeSet::of_longs()
                    } else {
                        ValueTypeSet::of_one(ValueType::Long)
                    }
                },

                &FnArg::IdentOrKeyword(ref x) => {
                    if schema.get_entid(x).is_some() {
                        ValueTypeSet::of_keywords()
                    } else {
                        ValueTypeSet::of_one(ValueType::Keyword)
                    }
                },

                &FnArg::Variable(_) => {
                    ValueTypeSet::any()
                },

                &FnArg::Constant(NonIntegerConstant::BigInteger(_)) => {
                    // Not yet implemented.
                    bail!(AlgebrizerError::UnsupportedArgument)
                },

                // These don't make sense here. TODO: split FnArg into scalar and non-scalar…
                &FnArg::Vector(_) |
                &FnArg::SrcVar(_) => bail!(AlgebrizerError::UnsupportedArgument),

                // These are all straightforward.
                &FnArg::Constant(NonIntegerConstant::Boolean(_)) => ValueTypeSet::of_one(ValueType::Boolean),
                &FnArg::Constant(NonIntegerConstant::Instant(_)) => ValueTypeSet::of_one(ValueType::Instant),
                &FnArg::Constant(NonIntegerConstant::Uuid(_)) => ValueTypeSet::of_one(ValueType::Uuid),
                &FnArg::Constant(NonIntegerConstant::Float(_)) => ValueTypeSet::of_one(ValueType::Double),
                &FnArg::Constant(NonIntegerConstant::Text(_)) => ValueTypeSet::of_one(ValueType::String),
            })
    }
}

pub(crate) enum ValueConversion {
    Val(TypedValue),
    Impossible(EmptyBecause),
}

/// Conversion of FnArgs to TypedValues.
impl ConjoiningClauses {
    /// Convert the provided `FnArg` to a `TypedValue`.
    /// The conversion depends on, and can fail because of:
    /// - Existing known types of a variable to which this arg will be bound.
    /// - Existing bindings of a variable `FnArg`.
    pub(crate) fn typed_value_from_arg<'s>(&self, schema: &'s Schema, var: &Variable, arg: FnArg, known_types: ValueTypeSet) -> Result<ValueConversion> {
        use self::ValueConversion::*;
        if known_types.is_empty() {
            // If this happens, it likely means the pattern has already failed!
            return Ok(Impossible(EmptyBecause::TypeMismatch {
                var: var.clone(),
                existing: known_types,
                desired: ValueTypeSet::any(),
            }));
        }

        let constrained_types;
        if let Some(required) = self.required_types.get(var) {
            constrained_types = known_types.intersection(required);
        } else {
            constrained_types = known_types;
        }

        match arg {
            // Longs are potentially ambiguous: they might be longs or entids.
            FnArg::EntidOrInteger(x) => {
                match (ValueType::Ref.accommodates_integer(x),
                       constrained_types.contains(ValueType::Ref),
                       constrained_types.contains(ValueType::Long)) {
                    (true, true, true) => {
                        // Ambiguous: this arg could be an entid or a long.
                        // We default to long.
                        Ok(Val(TypedValue::Long(x)))
                    },
                    (true, true, false) => {
                        // This can only be a ref.
                        Ok(Val(TypedValue::Ref(x)))
                    },
                    (_, false, true) => {
                        // This can only be a long.
                        Ok(Val(TypedValue::Long(x)))
                    },
                    (false, true, _) => {
                        // This isn't a valid ref, but that's the type to which this must conform!
                        Ok(Impossible(EmptyBecause::TypeMismatch {
                            var: var.clone(),
                            existing: known_types,
                            desired: ValueTypeSet::of_longs(),
                        }))
                    },
                    (_, false, false) => {
                        // Non-overlapping type sets.
                        Ok(Impossible(EmptyBecause::TypeMismatch {
                            var: var.clone(),
                            existing: known_types,
                            desired: ValueTypeSet::of_longs(),
                        }))
                    },
                }
            },

            // If you definitely want to look up an ident, do it before running the query.
            FnArg::IdentOrKeyword(x) => {
                match (constrained_types.contains(ValueType::Ref),
                       constrained_types.contains(ValueType::Keyword)) {
                    (true, true) => {
                        // Ambiguous: this could be a keyword or an ident.
                        // Default to keyword.
                        Ok(Val(x.into()))
                    },
                    (true, false) => {
                        // This can only be an ident. Look it up!
                        match schema.get_entid(&x).map(|k| k.into()) {
                            Some(e) => Ok(Val(e)),
                            None => Ok(Impossible(EmptyBecause::UnresolvedIdent(x.clone()))),
                        }
                    },
                    (false, true) => {
                        Ok(Val(TypedValue::Keyword(x.into())))
                    },
                    (false, false) => {
                        Ok(Impossible(EmptyBecause::TypeMismatch {
                            var: var.clone(),
                            existing: known_types,
                            desired: ValueTypeSet::of_keywords(),
                        }))
                    },
                }
            },

            FnArg::Variable(in_var) => {
                // TODO: technically you could ground an existing variable inside the query….
                if !self.input_variables.contains(&in_var) {
                    bail!(AlgebrizerError::UnboundVariable((*in_var.0).clone()))
                }
                match self.bound_value(&in_var) {
                    // The type is already known if it's a bound variable….
                    Some(ref in_value) => Ok(Val(in_value.clone())),
                    None => {
                        // The variable is present in `:in`, but it hasn't yet been provided.
                        // This is a restriction we will eventually relax: we don't yet have a way
                        // to collect variables as part of a computed table or substitution.
                        bail!(AlgebrizerError::UnboundVariable((*in_var.0).clone()))
                    },
                }
            },

            // This isn't implemented yet.
            FnArg::Constant(NonIntegerConstant::BigInteger(_)) => unimplemented!(),

            // These don't make sense here.
            FnArg::Vector(_) |
            FnArg::SrcVar(_) => bail!(AlgebrizerError::InvalidGroundConstant),

            // These are all straightforward.
            FnArg::Constant(NonIntegerConstant::Boolean(x)) => {
                coerce_to_typed_value!(var, x, known_types, ValueType::Boolean, TypedValue::Boolean)
            },
            FnArg::Constant(NonIntegerConstant::Instant(x)) => {
                coerce_to_typed_value!(var, x, known_types, ValueType::Instant, TypedValue::Instant)
            },
            FnArg::Constant(NonIntegerConstant::Uuid(x)) => {
                coerce_to_typed_value!(var, x, known_types, ValueType::Uuid, TypedValue::Uuid)
            },
            FnArg::Constant(NonIntegerConstant::Float(x)) => {
                coerce_to_typed_value!(var, x, known_types, ValueType::Double, TypedValue::Double)
            },
            FnArg::Constant(NonIntegerConstant::Text(x)) => {
                coerce_to_typed_value!(var, x, known_types, ValueType::String, TypedValue::String)
            },
        }
    }
}
