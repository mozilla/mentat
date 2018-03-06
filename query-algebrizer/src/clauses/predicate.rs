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
    ValueType,
    ValueTypeSet,
};

use mentat_query::{
    FnArg,
    Predicate,
    TypeAnnotation,
};

use clauses::ConjoiningClauses;

use clauses::convert::ValueTypes;

use errors::{
    Result,
    ErrorKind,
};

use types::{
    ColumnConstraint,
    EmptyBecause,
    Inequality,
};

use Known;

/// Application of predicates.
impl ConjoiningClauses {
    /// There are several kinds of predicates in our Datalog:
    /// - A limited set of binary comparison operators: < > <= >= !=.
    ///   These are converted into SQLite binary comparisons and some type constraints.
    /// - In the future, some predicates that are implemented via function calls in SQLite.
    ///
    /// At present we have implemented only the five built-in comparison binary operators.
    pub(crate) fn apply_predicate(&mut self, known: Known, predicate: Predicate) -> Result<()> {
        // Because we'll be growing the set of built-in predicates, handling each differently,
        // and ultimately allowing user-specified predicates, we match on the predicate name first.
        if let Some(op) = Inequality::from_datalog_operator(predicate.operator.0.as_str()) {
            self.apply_inequality(known, op, predicate)
        } else {
            bail!(ErrorKind::UnknownFunction(predicate.operator.clone()))
        }
    }

    fn potential_types(&self, schema: &Schema, fn_arg: &FnArg) -> Result<ValueTypeSet> {
        match fn_arg {
            &FnArg::Variable(ref v) => Ok(self.known_type_set(v)),
            _ => fn_arg.potential_types(schema),
        }
    }

    /// Apply a type annotation, which is a construct like a predicate that constrains the argument
    /// to be a specific ValueType.
    pub(crate) fn apply_type_anno(&mut self, anno: &TypeAnnotation) -> Result<()> {
        self.add_type_requirement(anno.variable.clone(), ValueTypeSet::of_one(anno.value_type));
        Ok(())
    }

    /// This function:
    /// - Resolves variables and converts types to those more amenable to SQL.
    /// - Ensures that the predicate functions name a known operator.
    /// - Accumulates an `Inequality` constraint into the `wheres` list.
    pub(crate) fn apply_inequality(&mut self, known: Known, comparison: Inequality, predicate: Predicate) -> Result<()> {
        if predicate.args.len() != 2 {
            bail!(ErrorKind::InvalidNumberOfArguments(predicate.operator.clone(), predicate.args.len(), 2));
        }

        // Go from arguments -- parser output -- to columns or values.
        // Any variables that aren't bound by this point in the linear processing of clauses will
        // cause the application of the predicate to fail.
        let mut args = predicate.args.into_iter();
        let left = args.next().expect("two args");
        let right = args.next().expect("two args");


        // The types we're handling here must be the intersection of the possible types of the arguments,
        // the known types of any variables, and the types supported by our inequality operators.
        let supported_types = comparison.supported_types();
        let mut left_types = self.potential_types(known.schema, &left)?
                                 .intersection(&supported_types);
        if left_types.is_empty() {
            bail!(ErrorKind::InvalidArgument(predicate.operator.clone(), "numeric or instant", 0));
        }

        let mut right_types = self.potential_types(known.schema, &right)?
                                  .intersection(&supported_types);
        if right_types.is_empty() {
            bail!(ErrorKind::InvalidArgument(predicate.operator.clone(), "numeric or instant", 1));
        }

        // We would like to allow longs to compare to doubles.
        // Do this by expanding the type sets. `resolve_numeric_argument` will
        // use `Long` by preference.
        if right_types.contains(ValueType::Long) {
            right_types.insert(ValueType::Double);
        }
        if left_types.contains(ValueType::Long) {
            left_types.insert(ValueType::Double);
        }

        let shared_types = left_types.intersection(&right_types);
        if shared_types.is_empty() {
            // In isolation these are both valid inputs to the operator, but the query cannot
            // succeed because the types don't match.
            self.mark_known_empty(
                if let Some(var) = left.as_variable().or_else(|| right.as_variable()) {
                    EmptyBecause::TypeMismatch {
                        var: var.clone(),
                        existing: left_types,
                        desired: right_types,
                    }
                } else {
                    EmptyBecause::KnownTypeMismatch {
                        left: left_types,
                        right: right_types,
                    }
                });
            return Ok(());
        }

        // We expect the intersection to be Long, Long+Double, Double, or Instant.
        let left_v;
        let right_v;
        if shared_types == ValueTypeSet::of_one(ValueType::Instant) {
            left_v = self.resolve_instant_argument(&predicate.operator, 0, left)?;
            right_v = self.resolve_instant_argument(&predicate.operator, 1, right)?;
        } else if !shared_types.is_empty() && shared_types.is_subset(&ValueTypeSet::of_numeric_types()) {
            left_v = self.resolve_numeric_argument(&predicate.operator, 0, left)?;
            right_v = self.resolve_numeric_argument(&predicate.operator, 1, right)?;
        } else {
            bail!(ErrorKind::InvalidArgument(predicate.operator.clone(), "numeric or instant", 0));
        }

        // These arguments must be variables or instant/numeric constants.
        // TODO: static evaluation. #383.
        let constraint = ColumnConstraint::Inequality {
            operator: comparison,
            left: left_v,
            right: right_v,
        };
        self.wheres.add_intersection(constraint);
        Ok(())
    }
}

#[cfg(test)]
mod testing {
    use super::*;

    use mentat_core::attribute::Unique;
    use mentat_core::{
        Attribute,
        TypedValue,
        ValueType,
    };

    use mentat_query::{
        FnArg,
        NamespacedKeyword,
        Pattern,
        PatternNonValuePlace,
        PatternValuePlace,
        PlainSymbol,
        Variable,
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
    fn test_apply_inequality() {
        let mut cc = ConjoiningClauses::default();
        let mut schema = Schema::default();

        associate_ident(&mut schema, NamespacedKeyword::new("foo", "bar"), 99);
        add_attribute(&mut schema, 99, Attribute {
            value_type: ValueType::Long,
            ..Default::default()
        });

        let x = Variable::from_valid_name("?x");
        let y = Variable::from_valid_name("?y");
        let known = Known::for_schema(&schema);
        cc.apply_parsed_pattern(known, Pattern {
            source: None,
            entity: PatternNonValuePlace::Variable(x.clone()),
            attribute: PatternNonValuePlace::Placeholder,
            value: PatternValuePlace::Variable(y.clone()),
            tx: PatternNonValuePlace::Placeholder,
        });
        assert!(!cc.is_known_empty());

        let op = PlainSymbol::new("<");
        let comp = Inequality::from_datalog_operator(op.plain_name()).unwrap();
        assert!(cc.apply_inequality(known, comp, Predicate {
             operator: op,
             args: vec![
                FnArg::Variable(Variable::from_valid_name("?y")), FnArg::EntidOrInteger(10),
            ]}).is_ok());

        assert!(!cc.is_known_empty());

        // Finally, expand column bindings to get the overlaps for ?x.
        cc.expand_column_bindings();
        assert!(!cc.is_known_empty());

        // After processing those two clauses, we know that ?y must be numeric, but not exactly
        // which type it must be.
        assert_eq!(None, cc.known_type(&y));      // Not just one.
        let expected = ValueTypeSet::of_numeric_types();
        assert_eq!(Some(&expected), cc.known_types.get(&y));

        let clauses = cc.wheres;
        assert_eq!(clauses.len(), 1);
        assert_eq!(clauses.0[0], ColumnConstraint::Inequality {
            operator: Inequality::LessThan,
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
        let known = Known::for_schema(&schema);
        cc.apply_parsed_pattern(known, Pattern {
            source: None,
            entity: PatternNonValuePlace::Variable(x.clone()),
            attribute: PatternNonValuePlace::Placeholder,
            value: PatternValuePlace::Variable(y.clone()),
            tx: PatternNonValuePlace::Placeholder,
        });
        assert!(!cc.is_known_empty());

        let op = PlainSymbol::new(">=");
        let comp = Inequality::from_datalog_operator(op.plain_name()).unwrap();
        assert!(cc.apply_inequality(known, comp, Predicate {
             operator: op,
             args: vec![
                FnArg::Variable(Variable::from_valid_name("?y")), FnArg::EntidOrInteger(10),
            ]}).is_ok());

        assert!(!cc.is_known_empty());
        cc.apply_parsed_pattern(known, Pattern {
            source: None,
            entity: PatternNonValuePlace::Variable(x.clone()),
            attribute: ident("foo", "roz"),
            value: PatternValuePlace::Variable(y.clone()),
            tx: PatternNonValuePlace::Placeholder,
        });

        // Finally, expand column bindings to get the overlaps for ?x.
        cc.expand_column_bindings();

        assert!(cc.is_known_empty());
        assert_eq!(cc.empty_because.unwrap(),
                   EmptyBecause::TypeMismatch {
                       var: y.clone(),
                       existing: ValueTypeSet::of_numeric_types(),
                       desired: ValueTypeSet::of_one(ValueType::String),
                   });
    }
}
