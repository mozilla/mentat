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
    Pattern,
    PatternValuePlace,
    PatternNonValuePlace,
    SrcVar,
};

use super::RcCloned;

use clauses::ConjoiningClauses;

use types::{
    ColumnConstraint,
    DatomsColumn,
    EmptyBecause,
    SourceAlias,
};

/// Application of patterns.
impl ConjoiningClauses {

    /// Apply the constraints in the provided pattern to this CC.
    ///
    /// This is a single-pass process, which means it is naturally incomplete, failing to take into
    /// account all information spread across two patterns.
    ///
    /// If the constraints cannot be satisfied -- for example, if this pattern includes a numeric
    /// attribute and a string value -- then the `is_known_empty` field on the CC is flipped and
    /// the function returns.
    ///
    /// A pattern being impossible to satisfy isn't necessarily a bad thing -- this query might
    /// have branched clauses that apply to different knowledge bases, and might refer to
    /// vocabulary that isn't (yet) used in this one.
    ///
    /// Most of the work done by this function depends on the schema and ident maps in the DB. If
    /// these change, then any work done is invalid.
    ///
    /// There's a lot more we can do here and later by examining the
    /// attribute:
    ///
    /// - If it's unique, and we have patterns like
    ///
    ///     [?x :foo/unique 5] [?x :foo/unique ?y]
    ///
    ///   then we can either prove impossibility (the two objects must
    ///   be equal) or deduce identity and simplify the query.
    ///
    /// - The same, if it's cardinality-one and the entity is known.
    ///
    /// - If there's a value index on this attribute, we might want to
    ///   run this pattern early in the query.
    ///
    /// - A unique-valued attribute can sometimes be rewritten into an
    ///   existence subquery instead of a join.
    fn apply_pattern_clause_for_alias<'s>(&mut self, schema: &'s Schema, pattern: &Pattern, alias: &SourceAlias) {
        if self.is_known_empty {
            return;
        }

        // Process each place in turn, applying constraints.
        // Both `e` and `a` must be entities, which is equivalent here
        // to being typed as Ref.
        // Sorry for the duplication; Rust makes it a pain to abstract this.

        // The transaction part of a pattern must be an entid, variable, or placeholder.
        self.constrain_to_tx(&pattern.tx);
        self.constrain_to_ref(&pattern.entity);
        self.constrain_to_ref(&pattern.attribute);

        let ref col = alias.1;

        match pattern.entity {
            PatternNonValuePlace::Placeholder =>
                // Placeholders don't contribute any column bindings, nor do
                // they constrain the query -- there's no need to produce
                // IS NOT NULL, because we don't store nulls in our schema.
                (),
            PatternNonValuePlace::Variable(ref v) =>
                self.bind_column_to_var(schema, col.clone(), DatomsColumn::Entity, v.clone()),
            PatternNonValuePlace::Entid(entid) =>
                self.constrain_column_to_entity(col.clone(), DatomsColumn::Entity, entid),
            PatternNonValuePlace::Ident(ref ident) => {
                if let Some(entid) = self.entid_for_ident(schema, ident.as_ref()) {
                    self.constrain_column_to_entity(col.clone(), DatomsColumn::Entity, entid)
                } else {
                    // A resolution failure means we're done here.
                    self.mark_known_empty(EmptyBecause::UnresolvedIdent(ident.cloned()));
                    return;
                }
            }
        }

        match pattern.attribute {
            PatternNonValuePlace::Placeholder =>
                (),
            PatternNonValuePlace::Variable(ref v) =>
                self.bind_column_to_var(schema, col.clone(), DatomsColumn::Attribute, v.clone()),
            PatternNonValuePlace::Entid(entid) => {
                if !schema.is_attribute(entid) {
                    // Furthermore, that entid must resolve to an attribute. If it doesn't, this
                    // query is meaningless.
                    self.mark_known_empty(EmptyBecause::InvalidAttributeEntid(entid));
                    return;
                }
                self.constrain_attribute(col.clone(), entid)
            },
            PatternNonValuePlace::Ident(ref ident) => {
                if let Some(entid) = self.entid_for_ident(schema, ident) {
                    self.constrain_attribute(col.clone(), entid);

                    if !schema.is_attribute(entid) {
                        self.mark_known_empty(EmptyBecause::InvalidAttributeIdent(ident.cloned()));
                        return;
                    }
                } else {
                    // A resolution failure means we're done here.
                    self.mark_known_empty(EmptyBecause::UnresolvedIdent(ident.cloned()));
                    return;
                }
            }
        }

        // Determine if the pattern's value type is known.
        // We do so by examining the value place and the attribute.
        // At this point it's possible that the type of the value is
        // inconsistent with the attribute; in that case this pattern
        // cannot return results, and we short-circuit.
        let value_type = self.get_value_type(schema, pattern);

        match pattern.value {
            PatternValuePlace::Placeholder =>
                (),

            PatternValuePlace::Variable(ref v) => {
                if let Some(this_type) = value_type {
                    // Wouldn't it be nice if we didn't need to clone in the found case?
                    // It doesn't matter too much: collisons won't be too frequent.
                    self.constrain_var_to_type(v.clone(), this_type);
                    if self.is_known_empty {
                        return;
                    }
                }

                self.bind_column_to_var(schema, col.clone(), DatomsColumn::Value, v.clone());
            },
            PatternValuePlace::EntidOrInteger(i) =>
                // If we know the valueType, then we can determine whether this is an entid or an
                // integer. If we don't, then we must generate a more general query with a
                // value_type_tag.
                if let Some(ValueType::Ref) = value_type {
                    self.constrain_column_to_entity(col.clone(), DatomsColumn::Value, i);
                } else {
                    // If we have a pattern like:
                    //
                    //   `[123 ?a 1]`
                    //
                    // then `1` could be an entid (ref), a long, a boolean, or an instant.
                    //
                    // We represent these constraints during execution:
                    //
                    // - Constraining the value column to the plain numeric value '1'.
                    // - Constraining its type column to one of a set of types.
                    //
                    self.constrain_value_to_numeric(col.clone(), i);
                },
            PatternValuePlace::IdentOrKeyword(ref kw) => {
                // If we know the valueType, then we can determine whether this is an ident or a
                // keyword. If we don't, then we must generate a more general query with a
                // value_type_tag.
                // We can also speculatively try to resolve it as an ident; if we fail, then we
                // know it can only return results if treated as a keyword, and we can treat it as
                // such.
                if let Some(ValueType::Ref) = value_type {
                    if let Some(entid) = self.entid_for_ident(schema, kw) {
                        self.constrain_column_to_entity(col.clone(), DatomsColumn::Value, entid)
                    } else {
                        // A resolution failure means we're done here: this attribute must have an
                        // entity value.
                        self.mark_known_empty(EmptyBecause::UnresolvedIdent(kw.cloned()));
                        return;
                    }
                } else {
                    // It must be a keyword.
                    self.constrain_column_to_constant(col.clone(), DatomsColumn::Value, TypedValue::Keyword(kw.clone()));
                    self.wheres.add_intersection(ColumnConstraint::HasType(col.clone(), ValueType::Keyword));
                };
            },
            PatternValuePlace::Constant(ref c) => {
                // TODO: don't allocate.
                let typed_value = c.clone().into_typed_value();
                if !typed_value.is_congruent_with(value_type) {
                    // If the attribute and its value don't match, the pattern must fail.
                    // We can never have a congruence failure if `value_type` is `None`, so we
                    // forcibly unwrap here.
                    let value_type = value_type.expect("Congruence failure but couldn't unwrap");
                    let why = EmptyBecause::ValueTypeMismatch(value_type, typed_value);
                    self.mark_known_empty(why);
                    return;
                }

                // TODO: if we don't know the type of the attribute because we don't know the
                // attribute, we can actually work backwards to the set of appropriate attributes
                // from the type of the value itself! #292.
                let typed_value_type = typed_value.value_type();
                self.constrain_column_to_constant(col.clone(), DatomsColumn::Value, typed_value);

                // If we can't already determine the range of values in the DB from the attribute,
                // then we must also constrain the type tag.
                //
                // Input values might be:
                //
                // - A long. This is handled by EntidOrInteger.
                // - A boolean. This is unambiguous.
                // - A double. This is currently unambiguous, though note that SQLite will equate 5.0 with 5.
                // - A string. This is unambiguous.
                // - A keyword. This is unambiguous.
                //
                // Because everything we handle here is unambiguous, we generate a single type
                // restriction from the value type of the typed value.
                if value_type.is_none() {
                    self.wheres.add_intersection(ColumnConstraint::HasType(col.clone(), typed_value_type));
                }

            },
        }
    }

    pub fn apply_pattern<'s, 'p>(&mut self, schema: &'s Schema, pattern: Pattern) {
        // For now we only support the default source.
        match pattern.source {
            Some(SrcVar::DefaultSrc) | None => (),
            _ => unimplemented!(),
        };

        if let Some(alias) = self.alias_table(schema, &pattern) {
            self.apply_pattern_clause_for_alias(schema, &pattern, &alias);
            self.from.push(alias);
        } else {
            // We didn't determine a table, likely because there was a mismatch
            // between an attribute and a value.
            // We know we cannot return a result, so we short-circuit here.
            self.mark_known_empty(EmptyBecause::AttributeLookupFailed);
            return;
        }
    }
}

#[cfg(test)]
mod testing {
    use super::*;

    use std::collections::BTreeMap;
    use std::rc::Rc;

    use mentat_core::attribute::Unique;
    use mentat_core::{
        Attribute,
    };

    use mentat_query::{
        NamespacedKeyword,
        NonIntegerConstant,
        Variable,
    };

    use clauses::{
        add_attribute,
        associate_ident,
        ident,
        unit_type_set,
    };

    use types::{
        ColumnConstraint,
        DatomsTable,
        QualifiedAlias,
        QueryValue,
        SourceAlias,
    };

    #[test]
    fn test_unknown_ident() {
        let mut cc = ConjoiningClauses::default();
        let schema = Schema::default();

        cc.apply_pattern(&schema, Pattern {
            source: None,
            entity: PatternNonValuePlace::Variable(Variable::from_valid_name("?x")),
            attribute: ident("foo", "bar"),
            value: PatternValuePlace::Constant(NonIntegerConstant::Boolean(true)),
            tx: PatternNonValuePlace::Placeholder,
        });

        assert!(cc.is_known_empty);
    }

    #[test]
    fn test_unknown_attribute() {
        let mut cc = ConjoiningClauses::default();
        let mut schema = Schema::default();

        associate_ident(&mut schema, NamespacedKeyword::new("foo", "bar"), 99);

        cc.apply_pattern(&schema, Pattern {
            source: None,
            entity: PatternNonValuePlace::Variable(Variable::from_valid_name("?x")),
            attribute: ident("foo", "bar"),
            value: PatternValuePlace::Constant(NonIntegerConstant::Boolean(true)),
            tx: PatternNonValuePlace::Placeholder,
        });

        assert!(cc.is_known_empty);
    }

    #[test]
    fn test_apply_simple_pattern() {
        let mut cc = ConjoiningClauses::default();
        let mut schema = Schema::default();

        associate_ident(&mut schema, NamespacedKeyword::new("foo", "bar"), 99);
        add_attribute(&mut schema, 99, Attribute {
            value_type: ValueType::Boolean,
            ..Default::default()
        });

        let x = Variable::from_valid_name("?x");
        cc.apply_pattern(&schema, Pattern {
            source: None,
            entity: PatternNonValuePlace::Variable(x.clone()),
            attribute: ident("foo", "bar"),
            value: PatternValuePlace::Constant(NonIntegerConstant::Boolean(true)),
            tx: PatternNonValuePlace::Placeholder,
        });

        // println!("{:#?}", cc);

        let d0_e = QualifiedAlias("datoms00".to_string(), DatomsColumn::Entity);
        let d0_a = QualifiedAlias("datoms00".to_string(), DatomsColumn::Attribute);
        let d0_v = QualifiedAlias("datoms00".to_string(), DatomsColumn::Value);

        // After this, we know a lot of things:
        assert!(!cc.is_known_empty);
        assert_eq!(cc.from, vec![SourceAlias(DatomsTable::Datoms, "datoms00".to_string())]);

        // ?x must be a ref.
        assert_eq!(cc.known_type(&x).unwrap(), ValueType::Ref);

        // ?x is bound to datoms0.e.
        assert_eq!(cc.column_bindings.get(&x).unwrap(), &vec![d0_e.clone()]);

        // Our 'where' clauses are two:
        // - datoms0.a = 99
        // - datoms0.v = true
        // No need for a type tag constraint, because the attribute is known.
        assert_eq!(cc.wheres, vec![
                   ColumnConstraint::Equals(d0_a, QueryValue::Entid(99)),
                   ColumnConstraint::Equals(d0_v, QueryValue::TypedValue(TypedValue::Boolean(true))),
        ].into());
    }

    #[test]
    fn test_apply_unattributed_pattern() {
        let mut cc = ConjoiningClauses::default();
        let schema = Schema::default();

        let x = Variable::from_valid_name("?x");
        cc.apply_pattern(&schema, Pattern {
            source: None,
            entity: PatternNonValuePlace::Variable(x.clone()),
            attribute: PatternNonValuePlace::Placeholder,
            value: PatternValuePlace::Constant(NonIntegerConstant::Boolean(true)),
            tx: PatternNonValuePlace::Placeholder,
        });

        // println!("{:#?}", cc);

        let d0_e = QualifiedAlias("datoms00".to_string(), DatomsColumn::Entity);
        let d0_v = QualifiedAlias("datoms00".to_string(), DatomsColumn::Value);

        assert!(!cc.is_known_empty);
        assert_eq!(cc.from, vec![SourceAlias(DatomsTable::Datoms, "datoms00".to_string())]);

        // ?x must be a ref.
        assert_eq!(cc.known_type(&x).unwrap(), ValueType::Ref);

        // ?x is bound to datoms0.e.
        assert_eq!(cc.column_bindings.get(&x).unwrap(), &vec![d0_e.clone()]);

        // Our 'where' clauses are two:
        // - datoms0.v = true
        // - datoms0.value_type_tag = boolean
        // TODO: implement expand_type_tags.
        assert_eq!(cc.wheres, vec![
                   ColumnConstraint::Equals(d0_v, QueryValue::TypedValue(TypedValue::Boolean(true))),
                   ColumnConstraint::HasType("datoms00".to_string(), ValueType::Boolean),
        ].into());
    }

    /// This test ensures that we do less work if we know the attribute thanks to a var lookup.
    #[test]
    fn test_apply_unattributed_but_bound_pattern_with_returned() {
        let mut cc = ConjoiningClauses::default();
        let mut schema = Schema::default();
        associate_ident(&mut schema, NamespacedKeyword::new("foo", "bar"), 99);
        add_attribute(&mut schema, 99, Attribute {
            value_type: ValueType::Boolean,
            ..Default::default()
        });

        let x = Variable::from_valid_name("?x");
        let a = Variable::from_valid_name("?a");
        let v = Variable::from_valid_name("?v");

        cc.input_variables.insert(a.clone());
        cc.value_bindings.insert(a.clone(), TypedValue::Keyword(Rc::new(NamespacedKeyword::new("foo", "bar"))));
        cc.apply_pattern(&schema, Pattern {
            source: None,
            entity: PatternNonValuePlace::Variable(x.clone()),
            attribute: PatternNonValuePlace::Variable(a.clone()),
            value: PatternValuePlace::Variable(v.clone()),
            tx: PatternNonValuePlace::Placeholder,
        });

        // println!("{:#?}", cc);

        let d0_e = QualifiedAlias("datoms00".to_string(), DatomsColumn::Entity);
        let d0_a = QualifiedAlias("datoms00".to_string(), DatomsColumn::Attribute);

        assert!(!cc.is_known_empty);
        assert_eq!(cc.from, vec![SourceAlias(DatomsTable::Datoms, "datoms00".to_string())]);

        // ?x must be a ref.
        assert_eq!(cc.known_type(&x).unwrap(), ValueType::Ref);

        // ?x is bound to datoms0.e.
        assert_eq!(cc.column_bindings.get(&x).unwrap(), &vec![d0_e.clone()]);
        assert_eq!(cc.wheres, vec![
                   ColumnConstraint::Equals(d0_a, QueryValue::Entid(99)),
        ].into());
    }

    /// Queries that bind non-entity values to entity places can't return results.
    #[test]
    fn test_bind_the_wrong_thing() {
        let mut cc = ConjoiningClauses::default();
        let schema = Schema::default();

        let x = Variable::from_valid_name("?x");
        let a = Variable::from_valid_name("?a");
        let v = Variable::from_valid_name("?v");
        let hello = TypedValue::typed_string("hello");

        cc.input_variables.insert(a.clone());
        cc.value_bindings.insert(a.clone(), hello.clone());
        cc.apply_pattern(&schema, Pattern {
            source: None,
            entity: PatternNonValuePlace::Variable(x.clone()),
            attribute: PatternNonValuePlace::Variable(a.clone()),
            value: PatternValuePlace::Variable(v.clone()),
            tx: PatternNonValuePlace::Placeholder,
        });

        assert!(cc.is_known_empty);
        assert_eq!(cc.empty_because.unwrap(), EmptyBecause::InvalidBinding(DatomsColumn::Attribute, hello));
    }


    /// This test ensures that we query all_datoms if we're possibly retrieving a string.
    #[test]
    fn test_apply_unattributed_pattern_with_returned() {
        let mut cc = ConjoiningClauses::default();
        let schema = Schema::default();

        let x = Variable::from_valid_name("?x");
        let a = Variable::from_valid_name("?a");
        let v = Variable::from_valid_name("?v");
        cc.apply_pattern(&schema, Pattern {
            source: None,
            entity: PatternNonValuePlace::Variable(x.clone()),
            attribute: PatternNonValuePlace::Variable(a.clone()),
            value: PatternValuePlace::Variable(v.clone()),
            tx: PatternNonValuePlace::Placeholder,
        });

        // println!("{:#?}", cc);

        let d0_e = QualifiedAlias("all_datoms00".to_string(), DatomsColumn::Entity);

        assert!(!cc.is_known_empty);
        assert_eq!(cc.from, vec![SourceAlias(DatomsTable::AllDatoms, "all_datoms00".to_string())]);

        // ?x must be a ref.
        assert_eq!(cc.known_type(&x).unwrap(), ValueType::Ref);

        // ?x is bound to datoms0.e.
        assert_eq!(cc.column_bindings.get(&x).unwrap(), &vec![d0_e.clone()]);
        assert_eq!(cc.wheres, vec![].into());
    }

    /// This test ensures that we query all_datoms if we're looking for a string.
    #[test]
    fn test_apply_unattributed_pattern_with_string_value() {
        let mut cc = ConjoiningClauses::default();
        let schema = Schema::default();

        let x = Variable::from_valid_name("?x");
        cc.apply_pattern(&schema, Pattern {
            source: None,
            entity: PatternNonValuePlace::Variable(x.clone()),
            attribute: PatternNonValuePlace::Placeholder,
            value: PatternValuePlace::Constant(NonIntegerConstant::Text(Rc::new("hello".to_string()))),
            tx: PatternNonValuePlace::Placeholder,
        });

        // println!("{:#?}", cc);

        let d0_e = QualifiedAlias("all_datoms00".to_string(), DatomsColumn::Entity);
        let d0_v = QualifiedAlias("all_datoms00".to_string(), DatomsColumn::Value);

        assert!(!cc.is_known_empty);
        assert_eq!(cc.from, vec![SourceAlias(DatomsTable::AllDatoms, "all_datoms00".to_string())]);

        // ?x must be a ref.
        assert_eq!(cc.known_type(&x).unwrap(), ValueType::Ref);

        // ?x is bound to datoms0.e.
        assert_eq!(cc.column_bindings.get(&x).unwrap(), &vec![d0_e.clone()]);

        // Our 'where' clauses are two:
        // - datoms0.v = 'hello'
        // - datoms0.value_type_tag = string
        // TODO: implement expand_type_tags.
        assert_eq!(cc.wheres, vec![
                   ColumnConstraint::Equals(d0_v, QueryValue::TypedValue(TypedValue::String(Rc::new("hello".to_string())))),
                   ColumnConstraint::HasType("all_datoms00".to_string(), ValueType::String),
        ].into());
    }

    #[test]
    fn test_apply_two_patterns() {
        let mut cc = ConjoiningClauses::default();
        let mut schema = Schema::default();

        associate_ident(&mut schema, NamespacedKeyword::new("foo", "bar"), 99);
        associate_ident(&mut schema, NamespacedKeyword::new("foo", "roz"), 98);
        add_attribute(&mut schema, 99, Attribute {
            value_type: ValueType::Boolean,
            ..Default::default()
        });
        add_attribute(&mut schema, 98, Attribute {
            value_type: ValueType::String,
            ..Default::default()
        });

        let x = Variable::from_valid_name("?x");
        let y = Variable::from_valid_name("?y");
        cc.apply_pattern(&schema, Pattern {
            source: None,
            entity: PatternNonValuePlace::Variable(x.clone()),
            attribute: ident("foo", "roz"),
            value: PatternValuePlace::Constant(NonIntegerConstant::Text(Rc::new("idgoeshere".to_string()))),
            tx: PatternNonValuePlace::Placeholder,
        });
        cc.apply_pattern(&schema, Pattern {
            source: None,
            entity: PatternNonValuePlace::Variable(x.clone()),
            attribute: ident("foo", "bar"),
            value: PatternValuePlace::Variable(y.clone()),
            tx: PatternNonValuePlace::Placeholder,
        });

        // Finally, expand column bindings to get the overlaps for ?x.
        cc.expand_column_bindings();

        println!("{:#?}", cc);

        let d0_e = QualifiedAlias("datoms00".to_string(), DatomsColumn::Entity);
        let d0_a = QualifiedAlias("datoms00".to_string(), DatomsColumn::Attribute);
        let d0_v = QualifiedAlias("datoms00".to_string(), DatomsColumn::Value);
        let d1_e = QualifiedAlias("datoms01".to_string(), DatomsColumn::Entity);
        let d1_a = QualifiedAlias("datoms01".to_string(), DatomsColumn::Attribute);

        assert!(!cc.is_known_empty);
        assert_eq!(cc.from, vec![
                   SourceAlias(DatomsTable::Datoms, "datoms00".to_string()),
                   SourceAlias(DatomsTable::Datoms, "datoms01".to_string()),
        ]);

        // ?x must be a ref.
        assert_eq!(cc.known_type(&x).unwrap(), ValueType::Ref);

        // ?x is bound to datoms0.e and datoms1.e.
        assert_eq!(cc.column_bindings.get(&x).unwrap(),
                   &vec![
                       d0_e.clone(),
                       d1_e.clone(),
                   ]);

        // Our 'where' clauses are four:
        // - datoms0.a = 98 (:foo/roz)
        // - datoms0.v = "idgoeshere"
        // - datoms1.a = 99 (:foo/bar)
        // - datoms1.e = datoms0.e
        assert_eq!(cc.wheres, vec![
                   ColumnConstraint::Equals(d0_a, QueryValue::Entid(98)),
                   ColumnConstraint::Equals(d0_v, QueryValue::TypedValue(TypedValue::typed_string("idgoeshere"))),
                   ColumnConstraint::Equals(d1_a, QueryValue::Entid(99)),
                   ColumnConstraint::Equals(d0_e, QueryValue::Column(d1_e)),
        ].into());
    }

    #[test]
    fn test_value_bindings() {
        let mut schema = Schema::default();

        associate_ident(&mut schema, NamespacedKeyword::new("foo", "bar"), 99);
        add_attribute(&mut schema, 99, Attribute {
            value_type: ValueType::Boolean,
            ..Default::default()
        });

        let x = Variable::from_valid_name("?x");
        let y = Variable::from_valid_name("?y");

        let b: BTreeMap<Variable, TypedValue> =
            vec![(y.clone(), TypedValue::Boolean(true))].into_iter().collect();
        let mut cc = ConjoiningClauses::with_value_bindings(b);

        cc.apply_pattern(&schema, Pattern {
            source: None,
            entity: PatternNonValuePlace::Variable(x.clone()),
            attribute: ident("foo", "bar"),
            value: PatternValuePlace::Variable(y.clone()),
            tx: PatternNonValuePlace::Placeholder,
        });

        let d0_e = QualifiedAlias("datoms00".to_string(), DatomsColumn::Entity);
        let d0_a = QualifiedAlias("datoms00".to_string(), DatomsColumn::Attribute);
        let d0_v = QualifiedAlias("datoms00".to_string(), DatomsColumn::Value);

        // ?y has been expanded into `true`.
        assert_eq!(cc.wheres, vec![
                   ColumnConstraint::Equals(d0_a, QueryValue::Entid(99)),
                   ColumnConstraint::Equals(d0_v, QueryValue::TypedValue(TypedValue::Boolean(true))),
        ].into());

        // There is no binding for ?y.
        assert!(!cc.column_bindings.contains_key(&y));

        // ?x is bound to the entity.
        assert_eq!(cc.column_bindings.get(&x).unwrap(),
                   &vec![d0_e.clone()]);
    }

    #[test]
    /// Bind a value to a variable in a query where the type of the value disagrees with the type of
    /// the variable inferred from known attributes.
    fn test_value_bindings_type_disagreement() {
        let mut schema = Schema::default();

        associate_ident(&mut schema, NamespacedKeyword::new("foo", "bar"), 99);
        add_attribute(&mut schema, 99, Attribute {
            value_type: ValueType::Boolean,
            ..Default::default()
        });

        let x = Variable::from_valid_name("?x");
        let y = Variable::from_valid_name("?y");

        let b: BTreeMap<Variable, TypedValue> =
            vec![(y.clone(), TypedValue::Long(42))].into_iter().collect();
        let mut cc = ConjoiningClauses::with_value_bindings(b);

        cc.apply_pattern(&schema, Pattern {
            source: None,
            entity: PatternNonValuePlace::Variable(x.clone()),
            attribute: ident("foo", "bar"),
            value: PatternValuePlace::Variable(y.clone()),
            tx: PatternNonValuePlace::Placeholder,
        });

        // The type of the provided binding doesn't match the type of the attribute.
        assert!(cc.is_known_empty);
    }

    #[test]
    /// Bind a non-textual value to a variable in a query where the variable is used as the value
    /// of a fulltext-valued attribute.
    fn test_fulltext_type_disagreement() {
        let mut schema = Schema::default();

        associate_ident(&mut schema, NamespacedKeyword::new("foo", "bar"), 99);
        add_attribute(&mut schema, 99, Attribute {
            value_type: ValueType::String,
            fulltext: true,
            ..Default::default()
        });

        let x = Variable::from_valid_name("?x");
        let y = Variable::from_valid_name("?y");

        let b: BTreeMap<Variable, TypedValue> =
            vec![(y.clone(), TypedValue::Long(42))].into_iter().collect();
        let mut cc = ConjoiningClauses::with_value_bindings(b);

        cc.apply_pattern(&schema, Pattern {
            source: None,
            entity: PatternNonValuePlace::Variable(x.clone()),
            attribute: ident("foo", "bar"),
            value: PatternValuePlace::Variable(y.clone()),
            tx: PatternNonValuePlace::Placeholder,
        });

        // The type of the provided binding doesn't match the type of the attribute.
        assert!(cc.is_known_empty);
    }

    #[test]
    /// Apply two patterns with differently typed attributes, but sharing a variable in the value
    /// place. No value can bind to a variable and match both types, so the CC is known to return
    /// no results.
    fn test_apply_two_conflicting_known_patterns() {
        let mut cc = ConjoiningClauses::default();
        let mut schema = Schema::default();

        associate_ident(&mut schema, NamespacedKeyword::new("foo", "bar"), 99);
        associate_ident(&mut schema, NamespacedKeyword::new("foo", "roz"), 98);
        add_attribute(&mut schema, 99, Attribute {
            value_type: ValueType::Boolean,
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
            attribute: ident("foo", "roz"),
            value: PatternValuePlace::Variable(y.clone()),
            tx: PatternNonValuePlace::Placeholder,
        });
        cc.apply_pattern(&schema, Pattern {
            source: None,
            entity: PatternNonValuePlace::Variable(x.clone()),
            attribute: ident("foo", "bar"),
            value: PatternValuePlace::Variable(y.clone()),
            tx: PatternNonValuePlace::Placeholder,
        });

        // Finally, expand column bindings to get the overlaps for ?x.
        cc.expand_column_bindings();

        assert!(cc.is_known_empty);
        assert_eq!(cc.empty_because.unwrap(),
                   EmptyBecause::TypeMismatch(y.clone(), unit_type_set(ValueType::String), ValueType::Boolean));
    }

    #[test]
    #[should_panic(expected = "assertion failed: cc.is_known_empty")]
    /// This test needs range inference in order to succeed: we must deduce that ?y must
    /// simultaneously be a boolean-valued attribute and a ref-valued attribute, and thus
    /// the CC can never return results.
    fn test_apply_two_implicitly_conflicting_patterns() {
        let mut cc = ConjoiningClauses::default();
        let schema = Schema::default();

        // [:find ?x :where
        //  [?x ?y true]
        //  [?z ?y ?x]]
        let x = Variable::from_valid_name("?x");
        let y = Variable::from_valid_name("?y");
        let z = Variable::from_valid_name("?z");
        cc.apply_pattern(&schema, Pattern {
            source: None,
            entity: PatternNonValuePlace::Variable(x.clone()),
            attribute: PatternNonValuePlace::Variable(y.clone()),
            value: PatternValuePlace::Constant(NonIntegerConstant::Boolean(true)),
            tx: PatternNonValuePlace::Placeholder,
        });
        cc.apply_pattern(&schema, Pattern {
            source: None,
            entity: PatternNonValuePlace::Variable(z.clone()),
            attribute: PatternNonValuePlace::Variable(y.clone()),
            value: PatternValuePlace::Variable(x.clone()),
            tx: PatternNonValuePlace::Placeholder,
        });

        // Finally, expand column bindings to get the overlaps for ?x.
        cc.expand_column_bindings();

        assert!(cc.is_known_empty);
        assert_eq!(cc.empty_because.unwrap(),
                   EmptyBecause::TypeMismatch(x.clone(), unit_type_set(ValueType::Ref), ValueType::Boolean));
    }
}
