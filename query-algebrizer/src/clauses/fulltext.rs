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
    VariableOrPlaceholder,
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
    Column,
    ColumnConstraint,
    ComputedTable,
    DatomsColumn,
    DatomsTable,
    FulltextColumn,
    QualifiedAlias,
    QueryValue,
    SourceAlias,
    VariableColumn,
};

impl ConjoiningClauses {
    #[allow(unused_variables)]
    pub fn apply_fulltext<'s>(&mut self, schema: &'s Schema, where_fn: WhereFn) -> Result<()> {
        if where_fn.args.len() != 3 {
            bail!(ErrorKind::InvalidNumberOfArguments(where_fn.operator.clone(), where_fn.args.len(), 3));
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
                    // It's OK for this to collide: the two must unify.
                    unimplemented!();
                }
            }
        }

        let bindings = match where_fn.binding {
            Binding::BindRel(bindings) => {
                if bindings.len() != 4 {
                    bail!(ErrorKind::InvalidBinding(where_fn.operator.clone(),
                                                    BindingError::InvalidNumberOfBindings {
                                                        number: bindings.len(),
                                                        expected: 4,
                                                    }));
                }
                bindings
            },
            Binding::BindScalar(_) |
            Binding::BindTuple(_) |
            Binding::BindColl(_) => bail!(ErrorKind::InvalidBinding(where_fn.operator.clone(), BindingError::ExpectedBindRel)),
        };

        let mut args = where_fn.args.into_iter();

        // TODO: process source variables.
        match args.next().unwrap() {
            FnArg::SrcVar(SrcVar::DefaultSrc) => {},
            _ => bail!(ErrorKind::InvalidArgument(where_fn.operator.clone(), "source variable".into(), 0)),
        }

        // TODO: accept placeholder and set of attributes.  Alternately, consider putting the search
        // term before the attribute arguments and collect the (variadic) attributes into a set.
        // let a: Entid  = self.resolve_attribute_argument(&where_fn.operator, 1, args.next().unwrap())?;
        //
        // TODO: allow non-constant attributes.
        // TODO: improve the expression of this matching, possibly by using attribute_for_* uniformly.
        let a = match args.next().unwrap() {
            FnArg::IdentOrKeyword(i) => schema.get_entid(&i),
            // Must be an entid.
            FnArg::EntidOrInteger(e) => Some(e),
            _ => None,
        };

        let a = a.ok_or(ErrorKind::InvalidArgument(where_fn.operator.clone(), "attribute".into(), 1))?;
        let attribute = schema.attribute_for_entid(a).cloned().ok_or(ErrorKind::InvalidArgument(where_fn.operator.clone(), "attribute".into(), 1))?;

        let fulltext_values = DatomsTable::FulltextValues;
        let datoms_table = DatomsTable::Datoms;

        let fulltext_values_alias = self.next_alias_for_table(fulltext_values);
        let datoms_table_alias = self.next_alias_for_table(datoms_table);

        self.from.push(SourceAlias(DatomsTable::FulltextValues, fulltext_values_alias.clone()));
        self.from.push(SourceAlias(DatomsTable::Datoms, datoms_table_alias.clone()));

        // TODO: constrain the type in the more general cases.
        self.constrain_attribute(datoms_table_alias.clone(), a);

        self.wheres.add_intersection(ColumnConstraint::Equals(
            QualifiedAlias(datoms_table_alias.clone(), Column::Fixed(DatomsColumn::Value)),
            QueryValue::Column(QualifiedAlias(fulltext_values_alias.clone(), Column::Fulltext(FulltextColumn::Rowid)))));

        // `search` is either text or a variable.
        let search: TypedValue = match args.next().unwrap() {
            FnArg::Variable(in_var) => {
                if !self.input_variables.contains(&in_var) {
                    bail!(ErrorKind::UnboundVariable((*in_var.0).clone()));
                }
                match self.bound_value(&in_var) {
                    Some(ref in_value) => in_value.clone(),
                    None => bail!(ErrorKind::UnboundVariable((*in_var.0).clone())),
                }
            },
            FnArg::Constant(NonIntegerConstant::Text(s)) => {
                TypedValue::typed_string(s.as_str())
            },
            _ => bail!(ErrorKind::InvalidArgument(where_fn.operator.clone(), "string".into(), 2)),
        };

        // TODO: should we build the FQA in ::Matches, preventing nonsense like matching on ::Rowid?
        let constraint = ColumnConstraint::Matches(QualifiedAlias(fulltext_values_alias.clone(), Column::Fulltext(FulltextColumn::Text)),
                                                   QueryValue::TypedValue(search));
        self.wheres.add_intersection(constraint);

        if let VariableOrPlaceholder::Variable(ref var) = bindings[0] {
            if self.bound_value(&var).is_some() || self.input_variables.contains(&var) {
                // It's OK for this to collide: the two must unify.
                unimplemented!();
            }
            self.constrain_var_to_type(var.clone(), ValueType::Ref);

            let entity_alias = QualifiedAlias(datoms_table_alias.clone(), Column::Fixed(DatomsColumn::Entity));
            self.column_bindings.entry(var.clone()).or_insert(vec![]).push(entity_alias);
        }

        if let VariableOrPlaceholder::Variable(ref var) = bindings[1] {
            if self.bound_value(&var).is_some() || self.input_variables.contains(&var) {
                // It's OK for this to collide: the two must unify.
                unimplemented!();
            }
            self.constrain_var_to_type(var.clone(), ValueType::String);

            let value_alias = QualifiedAlias(fulltext_values_alias.clone(), Column::Fulltext(FulltextColumn::Text));
            self.column_bindings.entry(var.clone()).or_insert(vec![]).push(value_alias);
        }

        if let VariableOrPlaceholder::Variable(ref var) = bindings[2] {
            if self.bound_value(&var).is_some() || self.input_variables.contains(&var) {
                // It's OK for this to collide: the two must unify.
                unimplemented!();
            }
            self.constrain_var_to_type(var.clone(), ValueType::Ref);

            let tx_alias = QualifiedAlias(datoms_table_alias.clone(), Column::Fixed(DatomsColumn::Tx));
            self.column_bindings.entry(var.clone()).or_insert(vec![]).push(tx_alias);
        }

        if let VariableOrPlaceholder::Variable(ref var) = bindings[3] {
            if self.bound_value(&var).is_some() || self.input_variables.contains(&var) {
                // It's OK for this to collide: the two must unify.
                unimplemented!();
            }
            self.constrain_var_to_type(var.clone(), ValueType::Double);

            // Right now we don't support binding a column to a constant value.  Therefore, we
            // introduce a computed table with exactly the constant value we require.  See the
            // translate tests for some edge cases in this space.
            // TODO: optimize this by binding the value, like:
            // self.value_bindings.insert(var.clone(), TypedValue::Double(0.0.into()));
            // TODO: produce real scores using SQLite's matchinfo.
            let names: Vec<Variable> = vec![var.clone()];
            let named_values = ComputedTable::NamedValues {
                names: names.clone(),
                values: vec![TypedValue::Double(0.0.into())],
            };
            let table = self.computed_tables.push_computed(named_values);
            let alias = self.next_alias_for_table(table);

            // Stitch the computed table into column_bindings, so we get cross-linking.
            self.bind_column_to_var(schema, alias.clone(), VariableColumn::Variable(var.clone()), var.clone());

            self.from.push(SourceAlias(table, alias.clone()));
        }

        Ok(())
    }
}

#[cfg(test)]
mod testing {
    use super::*;

    use std::rc::Rc;

    use mentat_core::{
        Attribute,
        ValueType,
    };

    use mentat_query::{
        Binding,
        FnArg,
        NamespacedKeyword,
        PlainSymbol,
        Variable,
    };

    use clauses::{
        add_attribute,
        associate_ident,
    };

    #[test]
    fn test_apply_fulltext() {
        let mut cc = ConjoiningClauses::default();
        let mut schema = Schema::default();

        associate_ident(&mut schema, NamespacedKeyword::new("foo", "fts"), 100);
        add_attribute(&mut schema, 100, Attribute {
            value_type: ValueType::String,
            index: true,
            fulltext: true,
            ..Default::default()
        });

        let op = PlainSymbol::new("fulltext");
        cc.apply_fulltext(&schema, WhereFn {
            operator: op,
            args: vec![
                FnArg::SrcVar(SrcVar::DefaultSrc),
                FnArg::IdentOrKeyword(NamespacedKeyword::new("foo", "fts")),
                FnArg::Constant(NonIntegerConstant::Text(Rc::new("needle".into()))),
            ],
            binding: Binding::BindRel(vec![VariableOrPlaceholder::Variable(Variable::from_valid_name("?entity")),
                                           VariableOrPlaceholder::Variable(Variable::from_valid_name("?value")),
                                           VariableOrPlaceholder::Variable(Variable::from_valid_name("?tx")),
                                           VariableOrPlaceholder::Variable(Variable::from_valid_name("?score"))]),
        }).expect("to be able to apply_fulltext");

        assert!(!cc.is_known_empty());

        // Finally, expand column bindings.
        cc.expand_column_bindings();
        assert!(!cc.is_known_empty());

        let clauses = cc.wheres;
        assert_eq!(clauses.len(), 3);

        assert_eq!(clauses.0[0], ColumnConstraint::Equals(QualifiedAlias("datoms01".to_string(), Column::Fixed(DatomsColumn::Attribute)),
                                                          QueryValue::Entid(100)).into());
        assert_eq!(clauses.0[1], ColumnConstraint::Equals(QualifiedAlias("datoms01".to_string(), Column::Fixed(DatomsColumn::Value)),
                                                          QueryValue::Column(QualifiedAlias("fulltext_values00".to_string(), Column::Fulltext(FulltextColumn::Rowid)))).into());
        assert_eq!(clauses.0[2], ColumnConstraint::Matches(QualifiedAlias("fulltext_values00".to_string(), Column::Fulltext(FulltextColumn::Text)),
                                                           QueryValue::TypedValue(TypedValue::String(Rc::new("needle".into())))).into());

        let bindings = cc.column_bindings;
        assert_eq!(bindings.len(), 4);

        assert_eq!(bindings.get(&Variable::from_valid_name("?entity")).expect("column binding for ?entity").clone(),
                   vec![QualifiedAlias("datoms01".to_string(), Column::Fixed(DatomsColumn::Entity))]);
        assert_eq!(bindings.get(&Variable::from_valid_name("?value")).expect("column binding for ?value").clone(),
                   vec![QualifiedAlias("fulltext_values00".to_string(), Column::Fulltext(FulltextColumn::Text))]);
        assert_eq!(bindings.get(&Variable::from_valid_name("?tx")).expect("column binding for ?tx").clone(),
                   vec![QualifiedAlias("datoms01".to_string(), Column::Fixed(DatomsColumn::Tx))]);
        assert_eq!(bindings.get(&Variable::from_valid_name("?score")).expect("column binding for ?score").clone(),
                   vec![QualifiedAlias("c00".to_string(), Column::Variable(VariableColumn::Variable(Variable::from_valid_name("?score"))))]);

        let known_types = cc.known_types;
        assert_eq!(known_types.len(), 4);

        assert_eq!(known_types.get(&Variable::from_valid_name("?entity")).expect("known types for ?entity").clone(),
                   vec![ValueType::Ref].into_iter().collect());
        assert_eq!(known_types.get(&Variable::from_valid_name("?value")).expect("known types for ?value").clone(),
                   vec![ValueType::String].into_iter().collect());
        assert_eq!(known_types.get(&Variable::from_valid_name("?tx")).expect("known types for ?tx").clone(),
                   vec![ValueType::Ref].into_iter().collect());
        assert_eq!(known_types.get(&Variable::from_valid_name("?score")).expect("known types for ?score").clone(),
                   vec![ValueType::Double].into_iter().collect());
    }
}
