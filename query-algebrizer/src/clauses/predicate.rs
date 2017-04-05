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
    Predicate,
    SrcVar,
    VariableOrPlaceholder,
    WhereFn,
};

use clauses::ConjoiningClauses;

use errors::{
    Error,
    ErrorKind,
    Result,
};

use types::{
    ColumnConstraint,
    DatomsColumn,
    DatomsTable,
    FulltextColumn,
    FulltextQualifiedAlias,
    NumericComparison,
    QualifiedAlias,
    QueryValue,
};

/// Application of predicates.
impl ConjoiningClauses {
    /// There are several kinds of predicates in our Datalog:
    /// - A limited set of binary comparison operators: < > <= >= !=.
    ///   These are converted into SQLite binary comparisons and some type constraints.
    /// - In the future, some predicates that are implemented via function calls in SQLite.
    ///
    /// At present we have implemented only the five built-in comparison binary operators.
    pub fn apply_predicate<'s>(&mut self, schema: &'s Schema, predicate: Predicate) -> Result<()> {
        // Because we'll be growing the set of built-in predicates, handling each differently,
        // and ultimately allowing user-specified predicates, we match on the predicate name first.
        if let Some(op) = NumericComparison::from_datalog_operator(predicate.operator.0.as_str()) {
            self.apply_numeric_predicate(schema, op, predicate)
        } else {
            bail!(ErrorKind::UnknownFunction(predicate.operator.clone()))
        }
    }

    /// This function:
    /// - Resolves variables and converts types to those more amenable to SQL.
    /// - Ensures that the predicate functions name a known operator.
    /// - Accumulates a `NumericInequality` constraint into the `wheres` list.
    #[allow(unused_variables)]
    pub fn apply_numeric_predicate<'s>(&mut self, schema: &'s Schema, comparison: NumericComparison, predicate: Predicate) -> Result<()> {
        if predicate.args.len() != 2 {
            bail!(ErrorKind::InvalidNumberOfArguments(predicate.operator.clone(), predicate.args.len(), 2));
        }

        // Go from arguments -- parser output -- to columns or values.
        // Any variables that aren't bound by this point in the linear processing of clauses will
        // cause the application of the predicate to fail.
        let mut args = predicate.args.into_iter();
        let left = self.resolve_numeric_argument(&predicate.operator, 0, args.next().unwrap())?;
        let right = self.resolve_numeric_argument(&predicate.operator, 1, args.next().unwrap())?;

        // These arguments must be variables or numeric constants.
        // TODO: generalize argument resolution and validation for different kinds of predicates:
        // as we process `(< ?x 5)` we are able to check or deduce that `?x` is numeric, and either
        // simplify the pattern or optimize the rest of the query.
        // To do so needs a slightly more sophisticated representation of type constraints — a set,
        // not a single `Option`.

        // TODO: static evaluation. #383.
        let constraint = ColumnConstraint::NumericInequality {
            operator: comparison,
            left: left,
            right: right,
        };
        self.wheres.add_intersection(constraint);
        Ok(())
    }


    /// There are several kinds of functions binding variables in our Datalog:
    /// - A set of functions like `fulltext` and `get-else` that are translated into
    ///   SQL `MATCH`es or joins, yielding bindings.
    /// - In the future, some functions that are implemented via function calls in SQLite.
    ///
    /// At present we have implemented only the `fulltext` operator.
    pub fn apply_where_fn<'s>(&mut self, schema: &'s Schema, where_fn: WhereFn) -> Result<()> {
        // Because we'll be growing the set of built-in functions, handling each differently, and
        // ultimately allowing user-specified functions, we match on the function name first.
        match where_fn.operator.0.as_str() {
            "fulltext" => self.apply_fulltext(schema, where_fn),
            _ => bail!(ErrorKind::UnknownFunction(where_fn.operator.clone())),
        }
    }

    /// This function:
    /// - Resolves variables and converts types to those more amenable to SQL.
    /// - Ensures that the predicate functions name a known operator.
    /// - Accumulates a `NumericInequality` constraint into the `wheres` list.
    #[allow(unused_variables)]
    pub fn apply_fulltext<'s>(&mut self, schema: &'s Schema, where_fn: WhereFn) -> Result<()> {
        if where_fn.args.len() != 3 {
            bail!(ErrorKind::InvalidNumberOfArguments(where_fn.operator.clone(), where_fn.args.len(), 3));
        }

        // TODO: binding-specific error messages.
        let mut bindings = match where_fn.binding {
            Binding::BindRel(bindings) => {
                if bindings.len() > 4 {
                    bail!(ErrorKind::InvalidNumberOfArguments(where_fn.operator.clone(), bindings.len(), 4));
                }
                bindings.into_iter()
            },
            _ => bail!(ErrorKind::InvalidArgument(where_fn.operator.clone(), "bindings".into(), 999)),
        };

        // Go from arguments -- parser output -- to columns or values.
        // Any variables that aren't bound by this point in the linear processing of clauses will
        // cause the application of the predicate to fail.
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
            FnArg::Ident(i) => schema.get_entid(&i),
            // Must be an entid.
            FnArg::EntidOrInteger(e) => Some(e),
            _ => None,
        };

        let a = a.ok_or(ErrorKind::InvalidArgument(where_fn.operator.clone(), "attribute".into(), 1))?;
        let attribute = schema.attribute_for_entid(a).cloned().ok_or(ErrorKind::InvalidArgument(where_fn.operator.clone(), "attribute".into(), 1))?;

        let fulltext_values = DatomsTable::FulltextValues;
        let datoms_table = DatomsTable::Datoms;

        let fulltext_values_alias = (self.aliaser)(fulltext_values);
        let datoms_table_alias = (self.aliaser)(datoms_table);

        // TODO: constrain types in more general cases?
        self.constrain_attribute(datoms_table_alias.clone(), a);

        self.wheres.add_intersection(ColumnConstraint::Equals(
            QualifiedAlias(datoms_table_alias.clone(), DatomsColumn::Value),
            QueryValue::FulltextColumn(FulltextQualifiedAlias(fulltext_values_alias.clone(), FulltextColumn::Rowid))));

        // search is either text or a variable.
        // TODO: should this just use `resolve_argument`?  Should it add a new `resolve_*` function?
        let search = match args.next().unwrap() {
            FnArg::Variable(var) => {
                self.column_bindings
                    .get(&var)
                    .and_then(|cols| cols.first().map(|col| QueryValue::Column(col.clone())))
                    .ok_or_else(|| Error::from_kind(ErrorKind::UnboundVariable(var.name())))?
            },
            FnArg::Constant(NonIntegerConstant::Text(s)) => {
                QueryValue::TypedValue(TypedValue::typed_string(s.as_str()))
            },
            _ => bail!(ErrorKind::InvalidArgument(where_fn.operator.clone(), "string".into(), 2)),
        };

        // TODO: should we build the FQA in ::Matches, preventing nonsense like matching on ::Rowid?
        let constraint = ColumnConstraint::Matches(FulltextQualifiedAlias(fulltext_values_alias.clone(), FulltextColumn::Text), search);
        self.wheres.add_intersection(constraint);

        if let Some(VariableOrPlaceholder::Variable(var)) = bindings.next() {
            // TODO: can we just check for late binding here?
            // Do we have, or will we have, an external binding for this variable?
            if self.bound_value(&var).is_some() || self.input_variables.contains(&var) {
                // That's a paddlin'!
                bail!(ErrorKind::InvalidArgument(where_fn.operator.clone(), "illegal bound variable".into(), 999))
            }
            self.constrain_var_to_type(var.clone(), ValueType::Ref);

            let entity_alias = QualifiedAlias(datoms_table_alias.clone(), DatomsColumn::Entity);
            self.column_bindings.entry(var).or_insert(vec![]).push(entity_alias);
        }

        if let Some(VariableOrPlaceholder::Variable(var)) = bindings.next() {
            // TODO: can we just check for late binding here?
            // Do we have, or will we have, an external binding for this variable?
            if self.bound_value(&var).is_some() || self.input_variables.contains(&var) {
                // That's a paddlin'!
                bail!(ErrorKind::InvalidArgument(where_fn.operator.clone(), "illegal bound variable".into(), 999))
            }
            self.constrain_var_to_type(var.clone(), ValueType::String);

            // TODO: figure out how to represent a FulltextQualifiedAlias.
            // let value_alias = FulltextQualifiedAlias(fulltext_values_alias.clone(), FulltextColumn::Text);
            // self.column_bindings.entry(var).or_insert(vec![]).push(value_alias);
        }

        if let Some(VariableOrPlaceholder::Variable(var)) = bindings.next() {
            // TODO: can we just check for late binding here?
            // Do we have, or will we have, an external binding for this variable?
            if self.bound_value(&var).is_some() || self.input_variables.contains(&var) {
                // That's a paddlin'!
                bail!(ErrorKind::InvalidArgument(where_fn.operator.clone(), "illegal bound variable".into(), 999))
            }
            self.constrain_var_to_type(var.clone(), ValueType::Ref);

            let tx_alias = QualifiedAlias(datoms_table_alias.clone(), DatomsColumn::Tx);
            self.column_bindings.entry(var).or_insert(vec![]).push(tx_alias);
        }

        if let Some(VariableOrPlaceholder::Variable(var)) = bindings.next() {
            // TODO: can we just check for late binding here?
            // Do we have, or will we have, an external binding for this variable?
            if self.bound_value(&var).is_some() || self.input_variables.contains(&var) {
                // That's a paddlin'!
                bail!(ErrorKind::InvalidArgument(where_fn.operator.clone(), "illegal bound variable".into(), 999))
            }
            self.constrain_var_to_type(var.clone(), ValueType::Double);

            // TODO: produce this using SQLite's matchinfo.
            self.value_bindings.insert(var.clone(), TypedValue::Double(0.0.into()));

            // TODO: figure out how to represent a constant binding.
            // self.column_bindings.entry(var).or_insert(vec![]).push(score_alias);
        }

        Ok(())
    }
}

#[cfg(test)]
mod testing {
    use super::*;

    use std::collections::HashSet;
    use std::rc::Rc;

    use mentat_core::attribute::Unique;
    use mentat_core::{
        Attribute,
        TypedValue,
        ValueType,
    };

    use mentat_query::{
        Binding,
        FnArg,
        NamespacedKeyword,
        Pattern,
        PatternNonValuePlace,
        PatternValuePlace,
        PlainSymbol,
        SrcVar,
        Variable,
        VariableOrPlaceholder,
    };

    use clauses::{
        add_attribute,
        associate_ident,
        ident,
    };

    use types::{
        ColumnConstraint,
        EmptyBecause,
        QueryValue,
    };

    #[test]
    /// Apply two patterns: a pattern and a numeric predicate.
    /// Verify that after application of the predicate we know that the value
    /// must be numeric.
    fn test_apply_numeric_predicate() {
        let mut cc = ConjoiningClauses::default();
        let mut schema = Schema::default();

        associate_ident(&mut schema, NamespacedKeyword::new("foo", "bar"), 99);
        add_attribute(&mut schema, 99, Attribute {
            value_type: ValueType::Long,
            ..Default::default()
        });

        let x = Variable::from_valid_name("?x");
        let y = Variable::from_valid_name("?y");
        cc.apply_pattern(&schema, Pattern {
            source: None,
            entity: PatternNonValuePlace::Variable(x.clone()),
            attribute: PatternNonValuePlace::Placeholder,
            value: PatternValuePlace::Variable(y.clone()),
            tx: PatternNonValuePlace::Placeholder,
        });
        assert!(!cc.is_known_empty);

        let op = PlainSymbol::new("<");
        let comp = NumericComparison::from_datalog_operator(op.plain_name()).unwrap();
        assert!(cc.apply_numeric_predicate(&schema, comp, Predicate {
             operator: op,
             args: vec![
                FnArg::Variable(Variable::from_valid_name("?y")), FnArg::EntidOrInteger(10),
            ]}).is_ok());

        assert!(!cc.is_known_empty);

        // Finally, expand column bindings to get the overlaps for ?x.
        cc.expand_column_bindings();
        assert!(!cc.is_known_empty);

        // After processing those two clauses, we know that ?y must be numeric, but not exactly
        // which type it must be.
        assert_eq!(None, cc.known_type(&y));      // Not just one.
        let expected: HashSet<ValueType> = vec![ValueType::Double, ValueType::Long].into_iter().collect();
        assert_eq!(Some(&expected), cc.known_types.get(&y));

        let clauses = cc.wheres;
        assert_eq!(clauses.len(), 1);
        assert_eq!(clauses.0[0], ColumnConstraint::NumericInequality {
            operator: NumericComparison::LessThan,
            left: QueryValue::Column(cc.column_bindings.get(&y).unwrap()[0].clone()),
            right: QueryValue::TypedValue(TypedValue::Long(10)),
        }.into());
    }

    #[test]
    /// Apply three patterns: an unbound pattern to establish a value var,
    /// a predicate to constrain the val to numeric types, and a third pattern to conflict with the
    /// numeric types and cause the pattern to fail.
    fn test_apply_conflict_with_numeric_range() {
        let mut cc = ConjoiningClauses::default();
        let mut schema = Schema::default();

        associate_ident(&mut schema, NamespacedKeyword::new("foo", "bar"), 99);
        associate_ident(&mut schema, NamespacedKeyword::new("foo", "roz"), 98);
        add_attribute(&mut schema, 99, Attribute {
            value_type: ValueType::Long,
            ..Default::default()
        });
        add_attribute(&mut schema, 98, Attribute {
            value_type: ValueType::String,
            unique: Some(Unique::Identity),
            ..Default::default()
        });

        let x = Variable::from_valid_name("?x");
        let y = Variable::from_valid_name("?y");
        cc.apply_pattern(&schema, Pattern {
            source: None,
            entity: PatternNonValuePlace::Variable(x.clone()),
            attribute: PatternNonValuePlace::Placeholder,
            value: PatternValuePlace::Variable(y.clone()),
            tx: PatternNonValuePlace::Placeholder,
        });
        assert!(!cc.is_known_empty);

        let op = PlainSymbol::new(">=");
        let comp = NumericComparison::from_datalog_operator(op.plain_name()).unwrap();
        assert!(cc.apply_numeric_predicate(&schema, comp, Predicate {
             operator: op,
             args: vec![
                FnArg::Variable(Variable::from_valid_name("?y")), FnArg::EntidOrInteger(10),
            ]}).is_ok());

        assert!(!cc.is_known_empty);
        cc.apply_pattern(&schema, Pattern {
            source: None,
            entity: PatternNonValuePlace::Variable(x.clone()),
            attribute: ident("foo", "roz"),
            value: PatternValuePlace::Variable(y.clone()),
            tx: PatternNonValuePlace::Placeholder,
        });

        // Finally, expand column bindings to get the overlaps for ?x.
        cc.expand_column_bindings();

        assert!(cc.is_known_empty);
        assert_eq!(cc.empty_because.unwrap(),
                   EmptyBecause::TypeMismatch(y.clone(),
                                              vec![ValueType::Double, ValueType::Long].into_iter()
                                                                                      .collect(),
                                              ValueType::String));
    }

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
                FnArg::Ident(NamespacedKeyword::new("foo", "fts")),
                FnArg::Constant(NonIntegerConstant::Text(Rc::new("needle".into()))),
            ],
            binding: Binding::BindRel(vec![VariableOrPlaceholder::Variable(Variable::from_valid_name("?entity")),
                                           VariableOrPlaceholder::Variable(Variable::from_valid_name("?value")),
                                           VariableOrPlaceholder::Variable(Variable::from_valid_name("?tx")),
                                           VariableOrPlaceholder::Variable(Variable::from_valid_name("?score"))]),
        }).expect("to be able to apply_fulltext");

        assert!(!cc.is_known_empty);

        // Finally, expand column bindings.
        cc.expand_column_bindings();
        assert!(!cc.is_known_empty);

        let clauses = cc.wheres;
        assert_eq!(clauses.len(), 3);

        assert_eq!(clauses.0[0], ColumnConstraint::Equals(QualifiedAlias("datoms01".to_string(), DatomsColumn::Attribute),
                                                          QueryValue::Entid(100)).into());
        assert_eq!(clauses.0[1], ColumnConstraint::Equals(QualifiedAlias("datoms01".to_string(), DatomsColumn::Value),
                                                          QueryValue::FulltextColumn(FulltextQualifiedAlias("fulltext_values00".to_string(), FulltextColumn::Rowid))).into());
        assert_eq!(clauses.0[2], ColumnConstraint::Matches(FulltextQualifiedAlias("fulltext_values00".to_string(), FulltextColumn::Text),
                                                           QueryValue::TypedValue(TypedValue::String(Rc::new("needle".into())))).into());

        let bindings = cc.column_bindings;
        assert_eq!(bindings.len(), 2);

        assert_eq!(bindings.get(&Variable::from_valid_name("?entity")).expect("column binding for ?entity").clone(),
                   vec![QualifiedAlias("datoms01".to_string(), DatomsColumn::Entity)]);
        assert_eq!(bindings.get(&Variable::from_valid_name("?tx")).expect("column binding for ?tx").clone(),
                   vec![QualifiedAlias("datoms01".to_string(), DatomsColumn::Tx)]);

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
