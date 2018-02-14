// Copyright 2016 Mozilla
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

use mentat_query::{
    ContainsVariables,
    NotJoin,
    UnifyVars,
};

use clauses::ConjoiningClauses;

use errors::{
    ErrorKind,
    Result,
};

use types::{
    ColumnConstraint,
    ComputedTable,
};

use Known;

impl ConjoiningClauses {
    pub fn apply_not_join(&mut self, known: Known, not_join: NotJoin) -> Result<()> {
        let unified = match not_join.unify_vars {
            UnifyVars::Implicit => not_join.collect_mentioned_variables(),
            UnifyVars::Explicit(vs) => vs,
        };

        let mut template = self.use_as_template(&unified);

        for v in unified.iter() {
            if self.value_bindings.contains_key(&v) {
                let val = self.value_bindings.get(&v).unwrap().clone();
                template.value_bindings.insert(v.clone(), val);
            } else if self.column_bindings.contains_key(&v) {
                let col = self.column_bindings.get(&v).unwrap()[0].clone();
                template.column_bindings.insert(v.clone(), vec![col]);
            } else {
                bail!(ErrorKind::UnboundVariable(v.name()));
            }
        }

        template.apply_clauses(known, not_join.clauses)?;

        if template.is_known_empty() {
            return Ok(());
        }

        template.expand_column_bindings();
        if template.is_known_empty() {
            return Ok(());
        }

        template.prune_extracted_types();
        if template.is_known_empty() {
            return Ok(());
        }

        template.process_required_types()?;
        if template.is_known_empty() {
            return Ok(());
        }

        // If we don't impose any constraints on the output, we might as well
        // not exist.
        if template.wheres.is_empty() {
            return Ok(());
        }

        let subquery = ComputedTable::Subquery(template);

        self.wheres.add_intersection(ColumnConstraint::NotExists(subquery));

        Ok(())
    }
}

#[cfg(test)]
mod testing {
    extern crate mentat_query_parser;

    use std::rc::Rc;
    use std::collections::BTreeSet;

    use super::*;

    use mentat_core::{
        Attribute,
        Schema,
        TypedValue,
        ValueType,
        ValueTypeSet,
    };

    use mentat_query::{
        NamespacedKeyword,
        PlainSymbol,
        Variable
    };

    use self::mentat_query_parser::parse_find_string;

    use clauses::{
        QueryInputs,
        add_attribute,
        associate_ident,
    };

    use errors::{
        Error,
        ErrorKind,
    };

    use types::{
        ColumnAlternation,
        ColumnConstraint,
        ColumnConstraintOrAlternation,
        ColumnIntersection,
        DatomsColumn,
        DatomsTable,
        Inequality,
        QualifiedAlias,
        QueryValue,
        SourceAlias,
    };

    use {
        algebrize,
        algebrize_with_inputs,
    };

    fn alg(schema: &Schema, input: &str) -> ConjoiningClauses {
        let known = Known::for_schema(schema);
        let parsed = parse_find_string(input).expect("parse failed");
        algebrize(known, parsed).expect("algebrize failed").cc
    }

    fn alg_with_inputs(schema: &Schema, input: &str, inputs: QueryInputs) -> ConjoiningClauses {
        let known = Known::for_schema(schema);
        let parsed = parse_find_string(input).expect("parse failed");
        algebrize_with_inputs(known, parsed, 0, inputs).expect("algebrize failed").cc
    }

    fn prepopulated_schema() -> Schema {
        let mut schema = Schema::default();
        associate_ident(&mut schema, NamespacedKeyword::new("foo", "name"), 65);
        associate_ident(&mut schema, NamespacedKeyword::new("foo", "knows"), 66);
        associate_ident(&mut schema, NamespacedKeyword::new("foo", "parent"), 67);
        associate_ident(&mut schema, NamespacedKeyword::new("foo", "age"), 68);
        associate_ident(&mut schema, NamespacedKeyword::new("foo", "height"), 69);
        add_attribute(&mut schema,
                      65,
                      Attribute {
                          value_type: ValueType::String,
                          multival: false,
                          ..Default::default()
                      });
        add_attribute(&mut schema,
                      66,
                      Attribute {
                          value_type: ValueType::String,
                          multival: true,
                          ..Default::default()
                      });
        add_attribute(&mut schema,
                      67,
                      Attribute {
                          value_type: ValueType::String,
                          multival: true,
                          ..Default::default()
                      });
        add_attribute(&mut schema,
                      68,
                      Attribute {
                          value_type: ValueType::Long,
                          multival: false,
                          ..Default::default()
                      });
        add_attribute(&mut schema,
                      69,
                      Attribute {
                          value_type: ValueType::Long,
                          multival: false,
                          ..Default::default()
                      });
        schema
    }

    fn compare_ccs(left: ConjoiningClauses, right: ConjoiningClauses) {
        assert_eq!(left.wheres, right.wheres);
        assert_eq!(left.from, right.from);
    }

    // not.
    #[test]
    fn test_successful_not() {
        let schema = prepopulated_schema();
        let query = r#"
            [:find ?x
             :where [?x :foo/knows "John"]
                    (not [?x :foo/parent "Ámbar"]
                         [?x :foo/knows "Daphne"])]"#;
        let cc = alg(&schema, query);

        let vx = Variable::from_valid_name("?x");

        let d0 = "datoms00".to_string();
        let d0e = QualifiedAlias::new(d0.clone(), DatomsColumn::Entity);
        let d0a = QualifiedAlias::new(d0.clone(), DatomsColumn::Attribute);
        let d0v = QualifiedAlias::new(d0.clone(), DatomsColumn::Value);

        let d1 = "datoms01".to_string();
        let d1e = QualifiedAlias::new(d1.clone(), DatomsColumn::Entity);
        let d1a = QualifiedAlias::new(d1.clone(), DatomsColumn::Attribute);
        let d1v = QualifiedAlias::new(d1.clone(), DatomsColumn::Value);

        let d2 = "datoms02".to_string();
        let d2e = QualifiedAlias::new(d2.clone(), DatomsColumn::Entity);
        let d2a = QualifiedAlias::new(d2.clone(), DatomsColumn::Attribute);
        let d2v = QualifiedAlias::new(d2.clone(), DatomsColumn::Value);

        let knows = QueryValue::Entid(66);
        let parent = QueryValue::Entid(67);

        let john = QueryValue::TypedValue(TypedValue::typed_string("John"));
        let ambar = QueryValue::TypedValue(TypedValue::typed_string("Ámbar"));
        let daphne = QueryValue::TypedValue(TypedValue::typed_string("Daphne"));

        let mut subquery = ConjoiningClauses::default();
        subquery.from = vec![SourceAlias(DatomsTable::Datoms, d1),
                             SourceAlias(DatomsTable::Datoms, d2)];
        subquery.column_bindings.insert(vx.clone(), vec![d0e.clone(), d1e.clone(), d2e.clone()]);
        subquery.wheres = ColumnIntersection(vec![ColumnConstraintOrAlternation::Constraint(ColumnConstraint::Equals(d1a.clone(), parent)),
                               ColumnConstraintOrAlternation::Constraint(ColumnConstraint::Equals(d1v.clone(), ambar)),
                               ColumnConstraintOrAlternation::Constraint(ColumnConstraint::Equals(d2a.clone(), knows.clone())),
                               ColumnConstraintOrAlternation::Constraint(ColumnConstraint::Equals(d2v.clone(), daphne)),
                               ColumnConstraintOrAlternation::Constraint(ColumnConstraint::Equals(d0e.clone(), QueryValue::Column(d1e.clone()))),
                               ColumnConstraintOrAlternation::Constraint(ColumnConstraint::Equals(d0e.clone(), QueryValue::Column(d2e.clone())))]);

        subquery.known_types.insert(vx.clone(), ValueTypeSet::of_one(ValueType::Ref));

        assert!(!cc.is_known_empty());
        assert_eq!(cc.wheres, ColumnIntersection(vec![
                ColumnConstraintOrAlternation::Constraint(ColumnConstraint::Equals(d0a.clone(), knows.clone())),
                ColumnConstraintOrAlternation::Constraint(ColumnConstraint::Equals(d0v.clone(), john)),
                ColumnConstraintOrAlternation::Constraint(ColumnConstraint::NotExists(ComputedTable::Subquery(subquery))),
            ]));
        assert_eq!(cc.column_bindings.get(&vx), Some(&vec![d0e]));
        assert_eq!(cc.from, vec![SourceAlias(DatomsTable::Datoms, d0)]);
    }

    // not-join.
    #[test]
    fn test_successful_not_join() {
        let schema = prepopulated_schema();
        let query = r#"
            [:find ?x
             :where [?x :foo/knows ?y]
                    [?x :foo/age 11]
                    [?x :foo/name "John"]
                    (not-join [?x ?y]
                              [?x :foo/parent ?y])]"#;
        let cc = alg(&schema, query);

        let vx = Variable::from_valid_name("?x");
        let vy = Variable::from_valid_name("?y");

        let d0 = "datoms00".to_string();
        let d0e = QualifiedAlias::new(d0.clone(), DatomsColumn::Entity);
        let d0a = QualifiedAlias::new(d0.clone(), DatomsColumn::Attribute);
        let d0v = QualifiedAlias::new(d0.clone(), DatomsColumn::Value);

        let d1 = "datoms01".to_string();
        let d1e = QualifiedAlias::new(d1.clone(), DatomsColumn::Entity);
        let d1a = QualifiedAlias::new(d1.clone(), DatomsColumn::Attribute);
        let d1v = QualifiedAlias::new(d1.clone(), DatomsColumn::Value);

        let d2 = "datoms02".to_string();
        let d2e = QualifiedAlias::new(d2.clone(), DatomsColumn::Entity);
        let d2a = QualifiedAlias::new(d2.clone(), DatomsColumn::Attribute);
        let d2v = QualifiedAlias::new(d2.clone(), DatomsColumn::Value);

        let d3 = "datoms03".to_string();
        let d3e = QualifiedAlias::new(d3.clone(), DatomsColumn::Entity);
        let d3a = QualifiedAlias::new(d3.clone(), DatomsColumn::Attribute);
        let d3v = QualifiedAlias::new(d3.clone(), DatomsColumn::Value);

        let name = QueryValue::Entid(65);
        let knows = QueryValue::Entid(66);
        let parent = QueryValue::Entid(67);
        let age = QueryValue::Entid(68);

        let john = QueryValue::TypedValue(TypedValue::typed_string("John"));
        let eleven = QueryValue::TypedValue(TypedValue::Long(11));

        let mut subquery = ConjoiningClauses::default();
        subquery.from = vec![SourceAlias(DatomsTable::Datoms, d3)];
        subquery.column_bindings.insert(vx.clone(), vec![d0e.clone(), d3e.clone()]);
        subquery.column_bindings.insert(vy.clone(), vec![d0v.clone(), d3v.clone()]);
        subquery.wheres = ColumnIntersection(vec![ColumnConstraintOrAlternation::Constraint(ColumnConstraint::Equals(d3a.clone(), parent)),
                               ColumnConstraintOrAlternation::Constraint(ColumnConstraint::Equals(d0e.clone(), QueryValue::Column(d3e.clone()))),
                               ColumnConstraintOrAlternation::Constraint(ColumnConstraint::Equals(d0v.clone(), QueryValue::Column(d3v.clone())))]);

        subquery.known_types.insert(vx.clone(), ValueTypeSet::of_one(ValueType::Ref));
        subquery.known_types.insert(vy.clone(), ValueTypeSet::of_one(ValueType::String));

        assert!(!cc.is_known_empty());
        let expected_wheres = ColumnIntersection(vec![
                ColumnConstraintOrAlternation::Constraint(ColumnConstraint::Equals(d0a.clone(), knows)),
                ColumnConstraintOrAlternation::Constraint(ColumnConstraint::Equals(d1a.clone(), age.clone())),
                ColumnConstraintOrAlternation::Constraint(ColumnConstraint::Equals(d1v.clone(), eleven)),
                ColumnConstraintOrAlternation::Constraint(ColumnConstraint::Equals(d2a.clone(), name.clone())),
                ColumnConstraintOrAlternation::Constraint(ColumnConstraint::Equals(d2v.clone(), john)),
                ColumnConstraintOrAlternation::Constraint(ColumnConstraint::NotExists(ComputedTable::Subquery(subquery))),
                ColumnConstraintOrAlternation::Constraint(ColumnConstraint::Equals(d0e.clone(), QueryValue::Column(d1e.clone()))),
                ColumnConstraintOrAlternation::Constraint(ColumnConstraint::Equals(d0e.clone(), QueryValue::Column(d2e.clone()))),
            ]);
        assert_eq!(cc.wheres, expected_wheres);
        assert_eq!(cc.column_bindings.get(&vx), Some(&vec![d0e, d1e, d2e]));
        assert_eq!(cc.from, vec![SourceAlias(DatomsTable::Datoms, d0),
                                 SourceAlias(DatomsTable::Datoms, d1),
                                 SourceAlias(DatomsTable::Datoms, d2)]);
    }

    // Not with a pattern and a predicate.
    #[test]
    fn test_not_with_pattern_and_predicate() {
        let schema = prepopulated_schema();
        let query = r#"
            [:find ?x ?age
             :where
             [?x :foo/age ?age]
             [[< ?age 30]]
             (not [?x :foo/knows "John"]
                  [?x :foo/knows "Daphne"])]"#;
        let cc = alg(&schema, query);

        let vx = Variable::from_valid_name("?x");

        let d0 = "datoms00".to_string();
        let d0e = QualifiedAlias::new(d0.clone(), DatomsColumn::Entity);
        let d0a = QualifiedAlias::new(d0.clone(), DatomsColumn::Attribute);
        let d0v = QualifiedAlias::new(d0.clone(), DatomsColumn::Value);

        let d1 = "datoms01".to_string();
        let d1e = QualifiedAlias::new(d1.clone(), DatomsColumn::Entity);
        let d1a = QualifiedAlias::new(d1.clone(), DatomsColumn::Attribute);
        let d1v = QualifiedAlias::new(d1.clone(), DatomsColumn::Value);

        let d2 = "datoms02".to_string();
        let d2e = QualifiedAlias::new(d2.clone(), DatomsColumn::Entity);
        let d2a = QualifiedAlias::new(d2.clone(), DatomsColumn::Attribute);
        let d2v = QualifiedAlias::new(d2.clone(), DatomsColumn::Value);

        let knows = QueryValue::Entid(66);
        let age = QueryValue::Entid(68);

        let john = QueryValue::TypedValue(TypedValue::typed_string("John"));
        let daphne = QueryValue::TypedValue(TypedValue::typed_string("Daphne"));

        let mut subquery = ConjoiningClauses::default();
        subquery.from = vec![SourceAlias(DatomsTable::Datoms, d1),
                             SourceAlias(DatomsTable::Datoms, d2)];
        subquery.column_bindings.insert(vx.clone(), vec![d0e.clone(), d1e.clone(), d2e.clone()]);
        subquery.wheres = ColumnIntersection(vec![ColumnConstraintOrAlternation::Constraint(ColumnConstraint::Equals(d1a.clone(), knows.clone())),
                                                  ColumnConstraintOrAlternation::Constraint(ColumnConstraint::Equals(d1v.clone(), john.clone())),
                                                  ColumnConstraintOrAlternation::Constraint(ColumnConstraint::Equals(d2a.clone(), knows.clone())),
                                                  ColumnConstraintOrAlternation::Constraint(ColumnConstraint::Equals(d2v.clone(), daphne.clone())),
                                                  ColumnConstraintOrAlternation::Constraint(ColumnConstraint::Equals(d0e.clone(), QueryValue::Column(d1e.clone()))),
                                                  ColumnConstraintOrAlternation::Constraint(ColumnConstraint::Equals(d0e.clone(), QueryValue::Column(d2e.clone())))]);

        subquery.known_types.insert(vx.clone(), ValueTypeSet::of_one(ValueType::Ref));

        assert!(!cc.is_known_empty());
        assert_eq!(cc.wheres, ColumnIntersection(vec![
                ColumnConstraintOrAlternation::Constraint(ColumnConstraint::Equals(d0a.clone(), age.clone())),
                ColumnConstraintOrAlternation::Constraint(ColumnConstraint::Inequality {
                    operator: Inequality::LessThan,
                    left: QueryValue::Column(d0v.clone()),
                    right: QueryValue::TypedValue(TypedValue::Long(30)),
                }),
                ColumnConstraintOrAlternation::Constraint(ColumnConstraint::NotExists(ComputedTable::Subquery(subquery))),
            ]));
        assert_eq!(cc.column_bindings.get(&vx), Some(&vec![d0e]));
        assert_eq!(cc.from, vec![SourceAlias(DatomsTable::Datoms, d0)]);
    }

    // not with an or
    #[test]
    fn test_not_with_or() {
        let schema = prepopulated_schema();
        let query = r#"
            [:find ?x
             :where [?x :foo/knows "Bill"]
                    (not (or [?x :foo/knows "John"]
                             [?x :foo/knows "Ámbar"])
                        [?x :foo/parent "Daphne"])]"#;
        let cc = alg(&schema, query);

        let d0 = "datoms00".to_string();
        let d0e = QualifiedAlias::new(d0.clone(), DatomsColumn::Entity);
        let d0a = QualifiedAlias::new(d0.clone(), DatomsColumn::Attribute);
        let d0v = QualifiedAlias::new(d0.clone(), DatomsColumn::Value);

        let d1 = "datoms01".to_string();
        let d1e = QualifiedAlias::new(d1.clone(), DatomsColumn::Entity);
        let d1a = QualifiedAlias::new(d1.clone(), DatomsColumn::Attribute);
        let d1v = QualifiedAlias::new(d1.clone(), DatomsColumn::Value);

        let d2 = "datoms02".to_string();
        let d2e = QualifiedAlias::new(d2.clone(), DatomsColumn::Entity);
        let d2a = QualifiedAlias::new(d2.clone(), DatomsColumn::Attribute);
        let d2v = QualifiedAlias::new(d2.clone(), DatomsColumn::Value);

        let vx = Variable::from_valid_name("?x");

        let knows = QueryValue::Entid(66);
        let parent = QueryValue::Entid(67);

        let bill = QueryValue::TypedValue(TypedValue::typed_string("Bill"));
        let john = QueryValue::TypedValue(TypedValue::typed_string("John"));
        let ambar = QueryValue::TypedValue(TypedValue::typed_string("Ámbar"));
        let daphne = QueryValue::TypedValue(TypedValue::typed_string("Daphne"));


        let mut subquery = ConjoiningClauses::default();
        subquery.from = vec![SourceAlias(DatomsTable::Datoms, d1),
                             SourceAlias(DatomsTable::Datoms, d2)];
        subquery.column_bindings.insert(vx.clone(), vec![d0e.clone(), d1e.clone(), d2e.clone()]);
        subquery.wheres = ColumnIntersection(vec![ColumnConstraintOrAlternation::Alternation(ColumnAlternation(vec![
                                                    ColumnIntersection(vec![
                                                        ColumnConstraintOrAlternation::Constraint(ColumnConstraint::Equals(d1a.clone(), knows.clone())),
                                                        ColumnConstraintOrAlternation::Constraint(ColumnConstraint::Equals(d1v.clone(), john))]),
                                                    ColumnIntersection(vec![
                                                        ColumnConstraintOrAlternation::Constraint(ColumnConstraint::Equals(d1a.clone(), knows.clone())),
                                                        ColumnConstraintOrAlternation::Constraint(ColumnConstraint::Equals(d1v.clone(), ambar))]),
                                                    ])),
                                                    ColumnConstraintOrAlternation::Constraint(ColumnConstraint::Equals(d2a.clone(), parent)),
                                                    ColumnConstraintOrAlternation::Constraint(ColumnConstraint::Equals(d2v.clone(), daphne)),
                                                    ColumnConstraintOrAlternation::Constraint(ColumnConstraint::Equals(d0e.clone(), QueryValue::Column(d1e.clone()))),
                                                    ColumnConstraintOrAlternation::Constraint(ColumnConstraint::Equals(d0e.clone(), QueryValue::Column(d2e.clone())))]);

        subquery.known_types.insert(vx.clone(), ValueTypeSet::of_one(ValueType::Ref));

        assert!(!cc.is_known_empty());
        assert_eq!(cc.wheres, ColumnIntersection(vec![
                ColumnConstraintOrAlternation::Constraint(ColumnConstraint::Equals(d0a.clone(), knows)),
                ColumnConstraintOrAlternation::Constraint(ColumnConstraint::Equals(d0v.clone(), bill)),
                ColumnConstraintOrAlternation::Constraint(ColumnConstraint::NotExists(ComputedTable::Subquery(subquery))),
            ]));
    }

    // not-join with an input variable
    #[test]
    fn test_not_with_in() {
        let schema = prepopulated_schema();
        let query = r#"
            [:find ?x
             :in ?y
             :where [?x :foo/knows "Bill"]
                    (not [?x :foo/knows ?y])]"#;

        let inputs = QueryInputs::with_value_sequence(vec![(Variable::from_valid_name("?y"),TypedValue::String(Rc::new("John".to_string())))]);
        let cc = alg_with_inputs(&schema, query, inputs);

        let vx = Variable::from_valid_name("?x");
        let vy = Variable::from_valid_name("?y");

        let knows = QueryValue::Entid(66);

        let bill = QueryValue::TypedValue(TypedValue::typed_string("Bill"));
        let john = QueryValue::TypedValue(TypedValue::typed_string("John"));

        let d0 = "datoms00".to_string();
        let d0e = QualifiedAlias::new(d0.clone(), DatomsColumn::Entity);
        let d0a = QualifiedAlias::new(d0.clone(), DatomsColumn::Attribute);
        let d0v = QualifiedAlias::new(d0.clone(), DatomsColumn::Value);

        let d1 = "datoms01".to_string();
        let d1e = QualifiedAlias::new(d1.clone(), DatomsColumn::Entity);
        let d1a = QualifiedAlias::new(d1.clone(), DatomsColumn::Attribute);
        let d1v = QualifiedAlias::new(d1.clone(), DatomsColumn::Value);

        let mut subquery = ConjoiningClauses::default();
        subquery.from = vec![SourceAlias(DatomsTable::Datoms, d1)];
        subquery.column_bindings.insert(vx.clone(), vec![d0e.clone(), d1e.clone()]);
        subquery.wheres = ColumnIntersection(vec![ColumnConstraintOrAlternation::Constraint(ColumnConstraint::Equals(d1a.clone(), knows.clone())),
                                                  ColumnConstraintOrAlternation::Constraint(ColumnConstraint::Equals(d1v.clone(), john)),
                                                  ColumnConstraintOrAlternation::Constraint(ColumnConstraint::Equals(d0e.clone(), QueryValue::Column(d1e.clone())))]);

        subquery.known_types.insert(vx.clone(), ValueTypeSet::of_one(ValueType::Ref));
        subquery.known_types.insert(vy.clone(), ValueTypeSet::of_one(ValueType::String));

        let mut input_vars: BTreeSet<Variable> = BTreeSet::default();
        input_vars.insert(vy.clone());
        subquery.input_variables = input_vars;
        subquery.value_bindings.insert(vy.clone(), TypedValue::typed_string("John"));

        assert!(!cc.is_known_empty());
        assert_eq!(cc.wheres, ColumnIntersection(vec![
                ColumnConstraintOrAlternation::Constraint(ColumnConstraint::Equals(d0a.clone(), knows)),
                ColumnConstraintOrAlternation::Constraint(ColumnConstraint::Equals(d0v.clone(), bill)),
                ColumnConstraintOrAlternation::Constraint(ColumnConstraint::NotExists(ComputedTable::Subquery(subquery))),
            ]));
    }

    // Test that if any single clause in the `not` fails to resolve the whole clause is considered empty
    #[test]
    fn test_fails_if_any_clause_invalid() {
        let schema = prepopulated_schema();
        let query = r#"
            [:find ?x
             :where [?x :foo/knows "Bill"]
                    (not [?x :foo/nope "John"]
                         [?x :foo/parent "Ámbar"]
                         [?x :foo/nope "Daphne"])]"#;
        let cc = alg(&schema, query);
        assert!(!cc.is_known_empty());
        compare_ccs(cc,
                    alg(&schema,
                        r#"[:find ?x :where [?x :foo/knows "Bill"]]"#));
    }

    /// Test that if all the attributes in an `not` fail to resolve, the `cc` isn't considered empty.
    #[test]
    fn test_no_clauses_succeed() {
        let schema = prepopulated_schema();
        let query = r#"
            [:find ?x
             :where [?x :foo/knows "John"]
                    (not [?x :foo/nope "Ámbar"]
                         [?x :foo/nope "Daphne"])]"#;
        let cc = alg(&schema, query);
        assert!(!cc.is_known_empty());
        compare_ccs(cc,
                    alg(&schema, r#"[:find ?x :where [?x :foo/knows "John"]]"#));

    }

    #[test]
    fn test_unbound_var_fails() {
        let schema = prepopulated_schema();
        let known = Known::for_schema(&schema);
        let query = r#"
        [:find ?x
         :in ?y
         :where (not [?x :foo/knows ?y])]"#;
        let parsed = parse_find_string(query).expect("parse failed");
        let err = algebrize(known, parsed).err();
        assert!(err.is_some());
         match err.unwrap() {
            Error(ErrorKind::UnboundVariable(var), _) => { assert_eq!(var, PlainSymbol("?x".to_string())); },
            x => panic!("expected Unbound Variable error, got {:?}", x),
        }
    }
}
