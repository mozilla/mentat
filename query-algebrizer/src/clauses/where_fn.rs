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
    TypedValue,
    ValueType,
};

use mentat_query::{
    Binding,
    FnArg,
    NonIntegerConstant,
    SrcVar,
    Variable,
    WhereFn,
};

use clauses::{
    ConjoiningClauses,
    PushComputed,
};

use errors::{
    BindingError,
    ErrorKind,
    Result,
};

use types::{
    ComputedTable,
    SourceAlias,
    VariableColumn,
};

trait FnArgInto<F> {
    fn into_typed_value(self, lookup: F) -> Result<TypedValue>;
    fn into_column_vector(self, lookup: F) -> Result<Vec<TypedValue>>;

    fn into_unit_matrix(self, lookup: F) -> Result<Vec<Vec<TypedValue>>>;
    fn into_row_matrix(self, lookup: F) -> Result<Vec<Vec<TypedValue>>>;
    fn into_column_matrix(self, lookup: F) -> Result<Vec<Vec<TypedValue>>>;
    fn into_block_matrix(self, lookup: F) -> Result<Vec<Vec<TypedValue>>>;
}

impl<F> FnArgInto<F> for FnArg where F: Fn(Variable) -> Result<TypedValue> {
    fn into_typed_value(self, lookup: F) -> Result<TypedValue> {
        match self {
            FnArg::EntidOrInteger(x) => Ok(TypedValue::Long(x)),
            FnArg::Ident(x) => Ok(TypedValue::Keyword(Rc::new(x))),
            FnArg::Constant(NonIntegerConstant::Boolean(x)) => Ok(TypedValue::Boolean(x)),
            FnArg::Constant(NonIntegerConstant::BigInteger(_)) => unimplemented!(),
            FnArg::Constant(NonIntegerConstant::Float(x)) => Ok(TypedValue::Double(x)),
            FnArg::Constant(NonIntegerConstant::Text(x)) => Ok(TypedValue::String(x)),
            FnArg::Variable(var) => lookup(var),
            FnArg::Vector(_) |
            FnArg::SrcVar(_) => bail!(ErrorKind::InvalidGroundConstant),
        }
    }

    fn into_column_vector(self, lookup: F) -> Result<Vec<TypedValue>> {
        if let FnArg::Vector(children) = self {
            children.into_iter().map(|x| x.into_typed_value(&lookup)).collect()
        } else {
            bail!(ErrorKind::InvalidGroundConstant)
        }
    }

    /// Convert an atom into a 1x1 matrix.
    fn into_unit_matrix(self, lookup: F) -> Result<Vec<Vec<TypedValue>>> {
        self.into_typed_value(lookup).map(|x| vec![vec![x]])
    }

    /// Convert a vector of atoms into a 1xN matrix.
    fn into_row_matrix(self, lookup: F) -> Result<Vec<Vec<TypedValue>>> {
        self.into_column_vector(lookup).map(|x| vec![x])
    }

    /// Convert a vector of atoms into an Mx1 matrix.
    fn into_column_matrix(self, lookup: F) -> Result<Vec<Vec<TypedValue>>> {
        self.into_column_vector(lookup).map(|x| x.into_iter().map(|y| vec![y]).collect())
    }

    /// Convert a vector of vectors (each of atoms) into an MxN matrix.
    fn into_block_matrix(self, lookup: F) -> Result<Vec<Vec<TypedValue>>> {
        if let FnArg::Vector(children) = self {
            children.into_iter().map(|x| x.into_column_vector(&lookup)).collect()
        } else {
            bail!(ErrorKind::InvalidGroundConstant)
        }
    }
}

/// This function takes a vector of rows of possibly different lengths and a sequence of columns to
/// ignore.  It ensures that each row has the same length, and drops the columns that have `false`
/// in the `column_is_bound` sequence.  Finally, it ensures that each column is homogeneously typed,
/// and returns a sequence of the derived type for each column.
fn reduced_rows_and_column_types(rows: Vec<Vec<TypedValue>>, column_is_bound: &Vec<bool>) -> Result<(Vec<Vec<TypedValue>>, Vec<ValueType>)> {
    let mut matrix = Vec::with_capacity(rows.len());

    for row in rows {
        if column_is_bound.len() != row.len() {
            bail!(ErrorKind::InvalidGroundConstant)
        }

        let mut retained_row = Vec::with_capacity(column_is_bound.len());
        for (is_bound, value) in column_is_bound.into_iter().zip(row.into_iter()) {
            if *is_bound {
                retained_row.push(value);
            }
        }

        matrix.push(retained_row);
    }

    let column_types: Vec<ValueType> = match matrix.get(0) {
        None => {
            bail!(ErrorKind::InvalidGroundConstant)
        },
        Some(first_row) => {
            first_row.iter().map(|x| x.value_type()).collect()
        },
    };

    for row in matrix.iter() {
        for (column_type, value) in column_types.iter().zip(row.into_iter()) {
            if *column_type != value.value_type() {
                bail!(ErrorKind::InvalidGroundConstant)
            }
        }
    }

    return Ok((matrix, column_types))
}

/// Application of where functions.
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

    #[allow(unused_variables)]
    pub fn apply_ground<'s>(&mut self, schema: &'s Schema, where_fn: WhereFn) -> Result<()> {
        if where_fn.args.len() != 2 {
            bail!(ErrorKind::InvalidNumberOfArguments(where_fn.operator.clone(), where_fn.args.len(), 2));
        }

        let mut args = where_fn.args.into_iter();

        // TODO: process source variables.
        match args.next().unwrap() {
            FnArg::SrcVar(SrcVar::DefaultSrc) => {},
            _ => bail!(ErrorKind::InvalidArgument(where_fn.operator.clone(), "source variable".into(), 0)),
        }

        if where_fn.binding.is_empty() {
            // The binding must introduce at least one bound variable.
            bail!(ErrorKind::InvalidBinding(where_fn.operator.clone(), BindingError::NoBoundVariable));
        }

        if !where_fn.binding.is_valid() {
            // The binding must not duplicate bound variables.
            bail!(ErrorKind::InvalidBinding(where_fn.operator.clone(), BindingError::RepeatedBoundVariable));
        }

        for var in &where_fn.binding.variables() {
            if let &Some(ref var) = var {
                if self.input_variables.contains(var) {
                    bail!(ErrorKind::InvalidBinding(where_fn.operator.clone(), BindingError::BoundInputVariable));
                }
            }
        }

        // Turn input constant into a matrix, ready to feed into a VALUES clause.
        let constant = args.next().unwrap();
        let rows = { // Scope borrow of self.
            let operator = where_fn.operator;
            let lookup = |in_var| {
                if !self.input_variables.contains(&in_var) {
                    bail!(ErrorKind::UnboundVariable((*in_var.0).clone()));
                }
                match self.bound_value(&in_var) {
                    Some(ref in_value) => Ok(in_value.clone()),
                    None => bail!(ErrorKind::UnboundVariable((*in_var.0).clone())),
                }
            };

            match where_fn.binding {
                Binding::BindScalar(_) => constant.into_unit_matrix(&lookup)?,
                Binding::BindTuple(_) => constant.into_row_matrix(&lookup)?,
                Binding::BindColl(_) => constant.into_column_matrix(&lookup)?,
                Binding::BindRel(_) => constant.into_block_matrix(&lookup)?,
            }
        };

        // Typecheck and drop columns that are not actually bound.  By restricting to homogeneous
        // columns, we greatly simplify projection.  In the future, we could loosen this
        // restriction, at the cost of projecting (some) value type tags.  If and when we want to
        // algebrize in two phases and allow for late-binding input variables, we'll probably be
        // able to loosen this restriction with little penalty.
        let variables: Vec<Option<Variable>> = where_fn.binding.variables();
        let (values, column_types) = reduced_rows_and_column_types(rows, &variables.iter().map(|x| x.is_some()).collect())?;
        let names: Vec<Variable> = variables.into_iter().filter_map(|x| x).collect();

        let named_values = ComputedTable::NamedValues {
            names: names.clone(),
            values: values,
        };
        let table = self.computed_tables.push_computed(named_values);
        let alias = self.next_alias_for_table(table);

        // Constrain the types of the bound variables we saw.
        for (name, column_type) in names.iter().zip(column_types.into_iter()) {
            self.constrain_var_to_type(name.clone(), column_type);
        }

        // Stitch the computed table into column_bindings, so we get cross-linking.
        for name in names {
            self.bind_column_to_var(schema, alias.clone(), VariableColumn::Variable(name.clone()), name.clone());
        }

        self.from.push(SourceAlias(table, alias));

        Ok(())
    }
}

#[cfg(test)]
mod testing {
    use super::*;

    use mentat_core::{
        Attribute,
        TypedValue,
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
        Column,
        QualifiedAlias,
        ValueTypeSet,
    };

    fn lookup(_v: Variable) -> Result<TypedValue> {
        bail!("expected to not be called".to_string())
    }

    #[test]
    fn test_fn_arg_into() {
        let x11 = FnArg::EntidOrInteger(10);
        let x1n = FnArg::Vector(vec![FnArg::EntidOrInteger(10), FnArg::Constant(NonIntegerConstant::Boolean(true))]);
        let xmn = FnArg::Vector(vec![FnArg::Vector(vec![FnArg::EntidOrInteger(10), FnArg::Constant(NonIntegerConstant::Boolean(true))]),
                                     FnArg::Vector(vec![FnArg::EntidOrInteger(11), FnArg::Constant(NonIntegerConstant::Boolean(false))])]);

        assert_eq!(x11.clone().into_unit_matrix(&lookup).expect("to produce a unit matrix"),
                   vec![vec![TypedValue::Long(10)]]);
        assert!(x1n.clone().into_unit_matrix(&lookup).is_err());
        assert!(xmn.clone().into_unit_matrix(&lookup).is_err());

        assert!(x11.clone().into_row_matrix(&lookup).is_err());
        assert_eq!(x1n.clone().into_row_matrix(&lookup).expect("to produce a row matrix"),
                   vec![vec![TypedValue::Long(10), TypedValue::Boolean(true)]]);
        assert!(xmn.clone().into_row_matrix(&lookup).is_err());

        assert!(x11.clone().into_column_matrix(&lookup).is_err());
        assert_eq!(x1n.clone().into_column_matrix(&lookup).expect("to produce a column matrix"),
                   vec![vec![TypedValue::Long(10)], vec![TypedValue::Boolean(true)]]);
        assert!(xmn.clone().into_column_matrix(&lookup).is_err());

        assert!(x11.clone().into_block_matrix(&lookup).is_err());
        assert!(x1n.clone().into_block_matrix(&lookup).is_err());
        assert_eq!(xmn.clone().into_block_matrix(&lookup).expect("to produce a block matrix"),
                   vec![vec![TypedValue::Long(10), TypedValue::Boolean(true)],
                        vec![TypedValue::Long(11), TypedValue::Boolean(false)]]);
    }

    #[test]
    fn test_reduced_rows_and_column_types() {
        let good = vec![vec![TypedValue::Long(10), TypedValue::Boolean(true)],
                        vec![TypedValue::Long(11), TypedValue::Boolean(false)]];
        assert_eq!(reduced_rows_and_column_types(good.clone(), &vec![true, true]).expect("to reduce rows"),
                   (good.clone(),
                    vec![ValueType::Long, ValueType::Boolean]));

        assert_eq!(reduced_rows_and_column_types(good.clone(), &vec![true, false]).expect("to reduce rows"),
                   (vec![vec![TypedValue::Long(10)],
                         vec![TypedValue::Long(11)]],
                    vec![ValueType::Long]));

        assert_eq!(reduced_rows_and_column_types(good.clone(), &vec![false, true]).expect("to reduce rows"),
                   (vec![vec![TypedValue::Boolean(true)],
                         vec![TypedValue::Boolean(false)]],
                    vec![ValueType::Boolean]));

        let bad_shape = vec![vec![TypedValue::Long(10), TypedValue::Boolean(true)],
                             vec![TypedValue::Long(11)]];
        assert!(reduced_rows_and_column_types(bad_shape.clone(), &vec![true, true]).is_err());

        let bad_shape = vec![vec![TypedValue::Long(10), TypedValue::Boolean(true)],
                             vec![TypedValue::Long(11), TypedValue::Boolean(true), TypedValue::String(Rc::new("test".into()))]];
        assert!(reduced_rows_and_column_types(bad_shape.clone(), &vec![true, true]).is_err());

        let bad_type = vec![vec![TypedValue::Long(10), TypedValue::Boolean(true)],
                            vec![TypedValue::String(Rc::new("test".into())), TypedValue::Boolean(false)]];
        assert!(reduced_rows_and_column_types(bad_type.clone(), &vec![true, true]).is_err());

        let bad_type = vec![vec![TypedValue::Long(10), TypedValue::Boolean(true)],
                            vec![TypedValue::Long(11), TypedValue::String(Rc::new("test".into()))]];
        assert!(reduced_rows_and_column_types(bad_type.clone(), &vec![true, true]).is_err());
    }

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
                FnArg::SrcVar(SrcVar::DefaultSrc),
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
        assert_eq!(column_bindings.len(), 1);
        assert_eq!(column_bindings.get(&vz).expect("to have bound ?z"),
                   &vec![QualifiedAlias("c00".to_string(), Column::Variable(VariableColumn::Variable(vz.clone())))]);

        let known_types = cc.known_types;
        assert_eq!(known_types.len(), 1);
        assert_eq!(known_types.get(&vz).expect("to know the type of ?z"),
                   &ValueTypeSet::of_one(ValueType::Long));
    }
}
