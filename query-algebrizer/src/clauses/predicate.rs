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
};

use mentat_query::{
    Predicate,
};

use clauses::ConjoiningClauses;

use errors::{
    Result,
    ErrorKind,
};

use types::{
    ColumnConstraint,
    NumericComparison,
};

/// Application of predicates.
impl ConjoiningClauses {
    /// There are several kinds of predicates/functions in our Datalog:
    /// - A limited set of binary comparison operators: < > <= >= !=.
    ///   These are converted into SQLite binary comparisons and some type constraints.
    /// - A set of predicates like `fulltext` and `get-else` that are translated into
    ///   SQL `MATCH`es or joins, yielding bindings.
    /// - In the future, some predicates that are implemented via function calls in SQLite.
    ///
    /// At present we have implemented only the five built-in comparison binary operators.
    pub fn apply_predicate<'s, 'p>(&mut self, schema: &'s Schema, predicate: Predicate) -> Result<()> {
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
    pub fn apply_numeric_predicate<'s, 'p>(&mut self, schema: &'s Schema, comparison: NumericComparison, predicate: Predicate) -> Result<()> {
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
        ValueTypeSet,
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
        assert!(!cc.is_known_empty());

        let op = PlainSymbol::new("<");
        let comp = NumericComparison::from_datalog_operator(op.plain_name()).unwrap();
        assert!(cc.apply_numeric_predicate(&schema, comp, Predicate {
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
        assert!(!cc.is_known_empty());

        let op = PlainSymbol::new(">=");
        let comp = NumericComparison::from_datalog_operator(op.plain_name()).unwrap();
        assert!(cc.apply_numeric_predicate(&schema, comp, Predicate {
             operator: op,
             args: vec![
                FnArg::Variable(Variable::from_valid_name("?y")), FnArg::EntidOrInteger(10),
            ]}).is_ok());

        assert!(!cc.is_known_empty());
        cc.apply_pattern(&schema, Pattern {
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
                   EmptyBecause::TypeMismatch(y.clone(),
                                              ValueTypeSet::of_numeric_types(),
                                              ValueType::String));
    }
}