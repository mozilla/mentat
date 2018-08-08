// Copyright 2018 Mozilla
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
};

use mentat_query::{
    Binding,
    FnArg,
    SrcVar,
    VariableOrPlaceholder,
    WhereFn,
};

use clauses::{
    ConjoiningClauses,
};

use errors::{
    AlgebrizerError,
    BindingError,
    Result,
};

use types::{
    Column,
    ColumnConstraint,
    DatomsTable,
    Inequality,
    QualifiedAlias,
    QueryValue,
    SourceAlias,
    TransactionsColumn,
};

use Known;

impl ConjoiningClauses {
    // Log in Query: tx-ids and tx-data
    //
    // The log API includes two convenience functions that are available within query. The tx-ids
    // function takes arguments similar to txRange above, but returns a collection of transaction
    // entity ids. You will typically use the collection binding form [?tx …] to capture the
    // results.
    //
    // [(tx-ids ?log ?tx1 ?tx2) [?tx ...]]
    //
    // TODO: handle tx1 == 0 (more generally, tx1 < bootstrap::TX0) specially (no after constraint).
    // TODO: allow tx2 to be missing (no before constraint).
    // TODO: allow txK arguments to be instants.
    // TODO: allow arbitrary additional attribute arguments that restrict the tx-ids to those
    // transactions that impact one of the given attributes.
    pub(crate) fn apply_tx_ids(&mut self, known: Known, where_fn: WhereFn) -> Result<()> {
        if where_fn.args.len() != 3 {
            bail!(AlgebrizerError::InvalidNumberOfArguments(where_fn.operator.clone(), where_fn.args.len(), 3));
        }

        if where_fn.binding.is_empty() {
            // The binding must introduce at least one bound variable.
            bail!(AlgebrizerError::InvalidBinding(where_fn.operator.clone(), BindingError::NoBoundVariable));
        }

        if !where_fn.binding.is_valid() {
            // The binding must not duplicate bound variables.
            bail!(AlgebrizerError::InvalidBinding(where_fn.operator.clone(), BindingError::RepeatedBoundVariable));
        }

        // We should have exactly one binding. Destructure it now.
        let tx_var = match where_fn.binding {
            Binding::BindRel(bindings) => {
                let bindings_count = bindings.len();
                if bindings_count != 1 {
                    bail!(AlgebrizerError::InvalidBinding(where_fn.operator.clone(),
                                                    BindingError::InvalidNumberOfBindings {
                                                        number: bindings_count,
                                                        expected: 1,
                                                    }));
                }
                match bindings.into_iter().next().unwrap() {
                    VariableOrPlaceholder::Placeholder => unreachable!("binding.is_empty()!"),
                    VariableOrPlaceholder::Variable(v) => v,
                }
            },
            Binding::BindColl(v) => v,
            Binding::BindScalar(_) |
            Binding::BindTuple(_) => {
                bail!(AlgebrizerError::InvalidBinding(where_fn.operator.clone(), BindingError::ExpectedBindRelOrBindColl))
            },
        };

        let mut args = where_fn.args.into_iter();

        // TODO: process source variables.
        match args.next().unwrap() {
            FnArg::SrcVar(SrcVar::DefaultSrc) => {},
            _ => bail!(AlgebrizerError::InvalidArgument(where_fn.operator.clone(), "source variable", 0)),
        }

        let tx1 = self.resolve_tx_argument(&known.schema, &where_fn.operator, 1, args.next().unwrap())?;
        let tx2 = self.resolve_tx_argument(&known.schema, &where_fn.operator, 2, args.next().unwrap())?;

        let transactions = self.next_alias_for_table(DatomsTable::Transactions);

        self.from.push(SourceAlias(DatomsTable::Transactions, transactions.clone()));

        // Bound variable must be a ref.
        self.constrain_var_to_type(tx_var.clone(), ValueType::Ref);
        if self.is_known_empty() {
            return Ok(());
        }

        self.bind_column_to_var(known.schema, transactions.clone(), TransactionsColumn::Tx, tx_var.clone());

        let after_constraint = ColumnConstraint::Inequality {
            operator: Inequality::LessThanOrEquals,
            left: tx1,
            right: QueryValue::Column(QualifiedAlias(transactions.clone(), Column::Transactions(TransactionsColumn::Tx))),
        };
        self.wheres.add_intersection(after_constraint);

        let before_constraint = ColumnConstraint::Inequality {
            operator: Inequality::LessThan,
            left: QueryValue::Column(QualifiedAlias(transactions.clone(), Column::Transactions(TransactionsColumn::Tx))),
            right: tx2,
        };
        self.wheres.add_intersection(before_constraint);

        Ok(())
    }

    pub(crate) fn apply_tx_data(&mut self, known: Known, where_fn: WhereFn) -> Result<()> {
        if where_fn.args.len() != 2 {
            bail!(AlgebrizerError::InvalidNumberOfArguments(where_fn.operator.clone(), where_fn.args.len(), 2));
        }

        if where_fn.binding.is_empty() {
            // The binding must introduce at least one bound variable.
            bail!(AlgebrizerError::InvalidBinding(where_fn.operator.clone(), BindingError::NoBoundVariable));
        }

        if !where_fn.binding.is_valid() {
            // The binding must not duplicate bound variables.
            bail!(AlgebrizerError::InvalidBinding(where_fn.operator.clone(), BindingError::RepeatedBoundVariable));
        }

        // We should have at most five bindings. Destructure them now.
        let bindings = match where_fn.binding {
            Binding::BindRel(bindings) => {
                let bindings_count = bindings.len();
                if bindings_count < 1 || bindings_count > 5 {
                    bail!(AlgebrizerError::InvalidBinding(where_fn.operator.clone(),
                                                    BindingError::InvalidNumberOfBindings {
                                                        number: bindings.len(),
                                                        expected: 5,
                                                    }));
                }
                bindings
            },
            Binding::BindScalar(_) |
            Binding::BindTuple(_) |
            Binding::BindColl(_) => bail!(AlgebrizerError::InvalidBinding(where_fn.operator.clone(), BindingError::ExpectedBindRel)),
        };
        let mut bindings = bindings.into_iter();
        let b_e = bindings.next().unwrap_or(VariableOrPlaceholder::Placeholder);
        let b_a = bindings.next().unwrap_or(VariableOrPlaceholder::Placeholder);
        let b_v = bindings.next().unwrap_or(VariableOrPlaceholder::Placeholder);
        let b_tx = bindings.next().unwrap_or(VariableOrPlaceholder::Placeholder);
        let b_op = bindings.next().unwrap_or(VariableOrPlaceholder::Placeholder);

        let mut args = where_fn.args.into_iter();

        // TODO: process source variables.
        match args.next().unwrap() {
            FnArg::SrcVar(SrcVar::DefaultSrc) => {},
            _ => bail!(AlgebrizerError::InvalidArgument(where_fn.operator.clone(), "source variable", 0)),
        }

        let tx = self.resolve_tx_argument(&known.schema, &where_fn.operator, 1, args.next().unwrap())?;

        let transactions = self.next_alias_for_table(DatomsTable::Transactions);

        self.from.push(SourceAlias(DatomsTable::Transactions, transactions.clone()));

        let tx_constraint = ColumnConstraint::Equals(
            QualifiedAlias(transactions.clone(), Column::Transactions(TransactionsColumn::Tx)),
            tx);
        self.wheres.add_intersection(tx_constraint);

        if let VariableOrPlaceholder::Variable(ref var) = b_e {
            // It must be a ref.
            self.constrain_var_to_type(var.clone(), ValueType::Ref);
            if self.is_known_empty() {
                return Ok(());
            }

            self.bind_column_to_var(known.schema, transactions.clone(), TransactionsColumn::Entity, var.clone());
        }

        if let VariableOrPlaceholder::Variable(ref var) = b_a {
            // It must be a ref.
            self.constrain_var_to_type(var.clone(), ValueType::Ref);
            if self.is_known_empty() {
                return Ok(());
            }

            self.bind_column_to_var(known.schema, transactions.clone(), TransactionsColumn::Attribute, var.clone());
        }

        if let VariableOrPlaceholder::Variable(ref var) = b_v {
            self.bind_column_to_var(known.schema, transactions.clone(), TransactionsColumn::Value, var.clone());
        }

        if let VariableOrPlaceholder::Variable(ref var) = b_tx {
            // It must be a ref.
            self.constrain_var_to_type(var.clone(), ValueType::Ref);
            if self.is_known_empty() {
                return Ok(());
            }

            // TODO: this might be a programming error if var is our tx argument.  Perhaps we can be
            // helpful in that case.
            self.bind_column_to_var(known.schema, transactions.clone(), TransactionsColumn::Tx, var.clone());
        }

        if let VariableOrPlaceholder::Variable(ref var) = b_op {
            // It must be a boolean.
            self.constrain_var_to_type(var.clone(), ValueType::Boolean);
            if self.is_known_empty() {
                return Ok(());
            }

            self.bind_column_to_var(known.schema, transactions.clone(), TransactionsColumn::Added, var.clone());
        }

        Ok(())
    }
}

#[cfg(test)]
mod testing {
    use super::*;

    use core_traits::{
        TypedValue,
        ValueType,
    };

    use mentat_core::{
        Schema,
    };

    use mentat_query::{
        Binding,
        FnArg,
        PlainSymbol,
        Variable,
    };

    #[test]
    fn test_apply_tx_ids() {
        let mut cc = ConjoiningClauses::default();
        let schema = Schema::default();

        let known = Known::for_schema(&schema);

        let op = PlainSymbol::plain("tx-ids");
        cc.apply_tx_ids(known, WhereFn {
            operator: op,
            args: vec![
                FnArg::SrcVar(SrcVar::DefaultSrc),
                FnArg::EntidOrInteger(1000),
                FnArg::EntidOrInteger(2000),
            ],
            binding: Binding::BindRel(vec![VariableOrPlaceholder::Variable(Variable::from_valid_name("?tx")),
            ]),
        }).expect("to be able to apply_tx_ids");

        assert!(!cc.is_known_empty());

        // Finally, expand column bindings.
        cc.expand_column_bindings();
        assert!(!cc.is_known_empty());

        let clauses = cc.wheres;
        assert_eq!(clauses.len(), 2);

        assert_eq!(clauses.0[0],
                   ColumnConstraint::Inequality {
                       operator: Inequality::LessThanOrEquals,
                       left: QueryValue::TypedValue(TypedValue::Ref(1000)),
                       right: QueryValue::Column(QualifiedAlias("transactions00".to_string(), Column::Transactions(TransactionsColumn::Tx))),
                   }.into());

        assert_eq!(clauses.0[1],
                   ColumnConstraint::Inequality {
                       operator: Inequality::LessThan,
                       left: QueryValue::Column(QualifiedAlias("transactions00".to_string(), Column::Transactions(TransactionsColumn::Tx))),
                       right: QueryValue::TypedValue(TypedValue::Ref(2000)),
                   }.into());

        let bindings = cc.column_bindings;
        assert_eq!(bindings.len(), 1);

        assert_eq!(bindings.get(&Variable::from_valid_name("?tx")).expect("column binding for ?tx").clone(),
                   vec![QualifiedAlias("transactions00".to_string(), Column::Transactions(TransactionsColumn::Tx))]);

        let known_types = cc.known_types;
        assert_eq!(known_types.len(), 1);

        assert_eq!(known_types.get(&Variable::from_valid_name("?tx")).expect("known types for ?tx").clone(),
                   vec![ValueType::Ref].into_iter().collect());
    }

    #[test]
    fn test_apply_tx_data() {
        let mut cc = ConjoiningClauses::default();
        let schema = Schema::default();

        let known = Known::for_schema(&schema);

        let op = PlainSymbol::plain("tx-data");
        cc.apply_tx_data(known, WhereFn {
            operator: op,
            args: vec![
                FnArg::SrcVar(SrcVar::DefaultSrc),
                FnArg::EntidOrInteger(1000),
            ],
            binding: Binding::BindRel(vec![
                VariableOrPlaceholder::Variable(Variable::from_valid_name("?e")),
                VariableOrPlaceholder::Variable(Variable::from_valid_name("?a")),
                VariableOrPlaceholder::Variable(Variable::from_valid_name("?v")),
                VariableOrPlaceholder::Variable(Variable::from_valid_name("?tx")),
                VariableOrPlaceholder::Variable(Variable::from_valid_name("?added")),
            ]),
        }).expect("to be able to apply_tx_data");

        assert!(!cc.is_known_empty());

        // Finally, expand column bindings.
        cc.expand_column_bindings();
        assert!(!cc.is_known_empty());

        let clauses = cc.wheres;
        assert_eq!(clauses.len(), 1);

        assert_eq!(clauses.0[0],
                   ColumnConstraint::Equals(QualifiedAlias("transactions00".to_string(), Column::Transactions(TransactionsColumn::Tx)),
                                            QueryValue::TypedValue(TypedValue::Ref(1000))).into());

        let bindings = cc.column_bindings;
        assert_eq!(bindings.len(), 5);

        assert_eq!(bindings.get(&Variable::from_valid_name("?e")).expect("column binding for ?e").clone(),
                   vec![QualifiedAlias("transactions00".to_string(), Column::Transactions(TransactionsColumn::Entity))]);

        assert_eq!(bindings.get(&Variable::from_valid_name("?a")).expect("column binding for ?a").clone(),
                   vec![QualifiedAlias("transactions00".to_string(), Column::Transactions(TransactionsColumn::Attribute))]);

        assert_eq!(bindings.get(&Variable::from_valid_name("?v")).expect("column binding for ?v").clone(),
                   vec![QualifiedAlias("transactions00".to_string(), Column::Transactions(TransactionsColumn::Value))]);

        assert_eq!(bindings.get(&Variable::from_valid_name("?tx")).expect("column binding for ?tx").clone(),
                   vec![QualifiedAlias("transactions00".to_string(), Column::Transactions(TransactionsColumn::Tx))]);

        assert_eq!(bindings.get(&Variable::from_valid_name("?added")).expect("column binding for ?added").clone(),
                   vec![QualifiedAlias("transactions00".to_string(), Column::Transactions(TransactionsColumn::Added))]);

        let known_types = cc.known_types;
        assert_eq!(known_types.len(), 4);

        assert_eq!(known_types.get(&Variable::from_valid_name("?e")).expect("known types for ?e").clone(),
                   vec![ValueType::Ref].into_iter().collect());

        assert_eq!(known_types.get(&Variable::from_valid_name("?a")).expect("known types for ?a").clone(),
                   vec![ValueType::Ref].into_iter().collect());

        assert_eq!(known_types.get(&Variable::from_valid_name("?tx")).expect("known types for ?tx").clone(),
                   vec![ValueType::Ref].into_iter().collect());

        assert_eq!(known_types.get(&Variable::from_valid_name("?added")).expect("known types for ?added").clone(),
                   vec![ValueType::Boolean].into_iter().collect());

        let extracted_types = cc.extracted_types;
        assert_eq!(extracted_types.len(), 1);

        assert_eq!(extracted_types.get(&Variable::from_valid_name("?v")).expect("extracted types for ?v").clone(),
                   QualifiedAlias("transactions00".to_string(), Column::Transactions(TransactionsColumn::ValueTypeTag)));
    }
}
