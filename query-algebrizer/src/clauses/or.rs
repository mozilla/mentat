// Copyright 2016 Mozilla
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

use std::collections::btree_map::Entry;
use std::collections::BTreeSet;

use mentat_core::{
    Schema,
};

use mentat_query::{
    OrJoin,
    OrWhereClause,
    Pattern,
    PatternValuePlace,
    PatternNonValuePlace,
    UnifyVars,
    Variable,
    WhereClause,
};

use clauses::ConjoiningClauses;

use errors::{
    Result,
};

use types::{
    ColumnConstraintOrAlternation,
    ColumnAlternation,
    ColumnIntersection,
    DatomsTable,
    EmptyBecause,
};

/// Return true if both left and right are the same variable or both are non-variable.
fn _simply_matches_place(left: &PatternNonValuePlace, right: &PatternNonValuePlace) -> bool {
    match (left, right) {
        (&PatternNonValuePlace::Variable(ref a), &PatternNonValuePlace::Variable(ref b)) => a == b,
        (&PatternNonValuePlace::Placeholder, &PatternNonValuePlace::Placeholder) => true,
        (&PatternNonValuePlace::Entid(_), &PatternNonValuePlace::Entid(_))       => true,
        (&PatternNonValuePlace::Entid(_), &PatternNonValuePlace::Ident(_))       => true,
        (&PatternNonValuePlace::Ident(_), &PatternNonValuePlace::Ident(_))       => true,
        (&PatternNonValuePlace::Ident(_), &PatternNonValuePlace::Entid(_))       => true,
        _ => false,
    }
}

/// Return true if both left and right are the same variable or both are non-variable.
fn _simply_matches_value_place(left: &PatternValuePlace, right: &PatternValuePlace) -> bool {
    match (left, right) {
        (&PatternValuePlace::Variable(ref a), &PatternValuePlace::Variable(ref b)) => a == b,
        (&PatternValuePlace::Placeholder, &PatternValuePlace::Placeholder) => true,
        (&PatternValuePlace::Variable(_), _) => false,
        (_, &PatternValuePlace::Variable(_)) => false,
        (&PatternValuePlace::Placeholder, _) => false,
        (_, &PatternValuePlace::Placeholder) => false,
        _ => true,
    }
}

pub enum DeconstructedOrJoin {
    KnownSuccess,
    KnownEmpty(EmptyBecause),
    Unit(OrWhereClause),
    UnitPattern(Pattern),
    Simple(Vec<Pattern>, BTreeSet<Variable>),
    Complex(OrJoin),
}

/// Application of `or`. Note that this is recursive!
impl ConjoiningClauses {
    fn apply_or_where_clause(&mut self, schema: &Schema, clause: OrWhereClause) -> Result<()> {
        match clause {
            OrWhereClause::Clause(clause) => self.apply_clause(schema, clause),

            // A query might be:
            // [:find ?x :where (or (and [?x _ 5] [?x :foo/bar 7]))]
            // which is equivalent to dropping the `or` _and_ the `and`!
            OrWhereClause::And(clauses) => {
                for clause in clauses {
                    self.apply_clause(schema, clause)?;
                }
                Ok(())
            },
        }
    }

    pub fn apply_or_join(&mut self, schema: &Schema, mut or_join: OrJoin) -> Result<()> {
        // Simple optimization. Empty `or` clauses disappear. Unit `or` clauses
        // are equivalent to just the inner clause.

        // Pre-cache mentioned variables. We use these in a few places.
        or_join.mentioned_variables();

        match or_join.clauses.len() {
            0 => Ok(()),
            1 if or_join.is_fully_unified() => {
                let clause = or_join.clauses.pop().expect("there's a clause");
                self.apply_or_where_clause(schema, clause)
            },
            // Either there's only one clause pattern, and it's not fully unified, or we
            // have multiple clauses.
            // In the former case we can't just apply it: it includes a variable that we don't want
            // to join with the rest of the query.
            // Notably, this clause might be an `and`, making this a complex pattern, so we can't
            // necessarily rewrite it in place.
            // In the latter case, we still need to do a bit more work.
            _ => self.apply_non_trivial_or_join(schema, or_join),
        }
    }

    /// Find out if the `OrJoin` is simple. A simple `or` is one in
    /// which:
    /// - Every arm is a pattern, so that we can use a single table alias for all.
    /// - Each pattern should run against the same table, for the same reason.
    /// - Each pattern uses the same variables. (That's checked by validation.)
    /// - Each pattern has the same shape, so we can extract bindings from the same columns
    ///   regardless of which clause matched.
    ///
    /// Like this:
    ///
    /// ```edn
    /// [:find ?x
    ///  :where (or [?x :foo/knows "John"]
    ///             [?x :foo/parent "Ámbar"]
    ///             [?x :foo/knows "Daphne"])]
    /// ```
    ///
    /// While we're doing this diagnosis, we'll also find out if:
    /// - No patterns can match: the enclosing CC is known-empty.
    /// - Some patterns can't match: they are discarded.
    /// - Only one pattern can match: the `or` can be simplified away.
    fn deconstruct_or_join(&self, schema: &Schema, or_join: OrJoin) -> DeconstructedOrJoin {
        // If we have explicit non-maximal unify-vars, we *can't* simply run this as a
        // single pattern --
        // ```
        // [:find ?x :where [?x :foo/bar ?y] (or-join [?x] [?x :foo/baz ?y])]
        // ```
        // is *not* equivalent to
        // ```
        // [:find ?x :where [?x :foo/bar ?y] [?x :foo/baz ?y]]
        // ```
        if !or_join.is_fully_unified() {
            // It's complex because we need to make sure that non-unified vars
            // mentioned in the body of the `or-join` do not unify with variables
            // outside the `or-join`. We can't naïvely collect clauses into the
            // same CC. TODO: pay attention to the unify list when generating
            // constraints. Temporarily shadow variables within each `or` branch.
            return DeconstructedOrJoin::Complex(or_join);
        }

        match or_join.clauses.len() {
            0 => DeconstructedOrJoin::KnownSuccess,

            // It's safe to simply 'leak' the entire clause, because we know every var in it is
            // supposed to unify with the enclosing form.
            1 => DeconstructedOrJoin::Unit(or_join.clauses.into_iter().next().unwrap()),
            _ => self._deconstruct_or_join(schema, or_join),
        }
    }

    /// This helper does the work of taking a known-non-trivial `or` or `or-join`,
    /// walking the contained patterns to decide whether it can be translated simply
    /// -- as a collection of constraints on a single table alias -- or if it needs to
    /// be implemented as a `UNION`.
    ///
    /// See the description of `deconstruct_or_join` for more details. This method expects
    /// to be called _only_ by `deconstruct_or_join`.
    fn _deconstruct_or_join(&self, schema: &Schema, or_join: OrJoin) -> DeconstructedOrJoin {
        // Preconditions enforced by `deconstruct_or_join`.
        assert_eq!(or_join.unify_vars, UnifyVars::Implicit);
        assert!(or_join.clauses.len() >= 2);

        // We're going to collect into this.
        // If at any point we hit something that's not a suitable pattern, we'll
        // reconstruct and return a complex `OrJoin`.
        let mut patterns: Vec<Pattern> = Vec::with_capacity(or_join.clauses.len());

        // Keep track of the table we need every pattern to use.
        let mut expected_table: Option<DatomsTable> = None;

        // Technically we might have several reasons, but we take the last -- that is, that's the
        // reason we don't have at least one pattern!
        // We'll return this as our reason if no pattern can return results.
        let mut empty_because: Option<EmptyBecause> = None;

        // Walk each clause in turn, bailing as soon as we know this can't be simple.
        let (join_clauses, mentioned_vars) = or_join.dismember();
        let mut clauses = join_clauses.into_iter();
        while let Some(clause) = clauses.next() {
            // If we fail half-way through processing, we want to reconstitute the input.
            // Keep a handle to the clause itself here to smooth over the moved `if let` below.
            let last: OrWhereClause;

            if let OrWhereClause::Clause(WhereClause::Pattern(p)) = clause {
                // Compute the table for the pattern. If we can't figure one out, it means
                // the pattern cannot succeed; we drop it.
                // Inside an `or` it's not a failure for a pattern to be unable to match, which
                // manifests as a table being unable to be found.
                let table = self.table_for_places(schema, &p.attribute, &p.value);
                match table {
                    Err(e) => {
                        empty_because = Some(e);

                        // Do not accumulate this pattern at all. Add lightness!
                        continue;
                    },
                    Ok(table) => {
                        // Check the shape of the pattern against a previous pattern.
                        let same_shape =
                            if let Some(template) = patterns.get(0) {
                                template.source == p.source &&     // or-arms all use the same source anyway.
                                _simply_matches_place(&template.entity, &p.entity) &&
                                _simply_matches_place(&template.attribute, &p.attribute) &&
                                _simply_matches_value_place(&template.value, &p.value) &&
                                _simply_matches_place(&template.tx, &p.tx)
                            } else {
                                // No previous pattern.
                                true
                            };

                        // All of our clauses that _do_ yield a table -- that are possible --
                        // must use the same table in order for this to be a simple `or`!
                        if same_shape {
                            if expected_table == Some(table) {
                                patterns.push(p);
                                continue;
                            }
                            if expected_table.is_none() {
                                expected_table = Some(table);
                                patterns.push(p);
                                continue;
                            }
                        }

                        // Otherwise, we need to keep this pattern so we can reconstitute.
                        // We'll fall through to reconstruction.
                    }
                }
                last = OrWhereClause::Clause(WhereClause::Pattern(p));
            } else {
                last = clause;
            }

            // If we get here, it means one of our checks above failed. Reconstruct and bail.
            let reconstructed: Vec<OrWhereClause> =
                // Non-empty patterns already collected…
                patterns.into_iter()
                        .map(|p| OrWhereClause::Clause(WhereClause::Pattern(p)))
                // … then the clause we just considered…
                        .chain(::std::iter::once(last))
                // … then the rest of the iterator.
                        .chain(clauses)
                        .collect();

            return DeconstructedOrJoin::Complex(OrJoin::new(
                UnifyVars::Implicit,
                reconstructed,
            ));
        }

        // If we got here without returning, then `patterns` is what we're working with.
        // If `patterns` is empty, it means _none_ of the clauses in the `or` could succeed.
        match patterns.len() {
            0 => {
                assert!(empty_because.is_some());
                DeconstructedOrJoin::KnownEmpty(empty_because.unwrap())
            },
            1 => DeconstructedOrJoin::UnitPattern(patterns.pop().unwrap()),
            _ => DeconstructedOrJoin::Simple(patterns, mentioned_vars),
        }
    }

    fn apply_non_trivial_or_join(&mut self, schema: &Schema, or_join: OrJoin) -> Result<()> {
        match self.deconstruct_or_join(schema, or_join) {
            DeconstructedOrJoin::KnownSuccess => {
                // The pattern came to us empty -- `(or)`. Do nothing.
                Ok(())
            },
            DeconstructedOrJoin::KnownEmpty(reason) => {
                // There were no arms of the join that could be mapped to a table.
                // The entire `or`, and thus the CC, cannot yield results.
                self.mark_known_empty(reason);
                Ok(())
            },
            DeconstructedOrJoin::Unit(clause) => {
                // There was only one clause. We're unifying all variables, so we can just apply here.
                self.apply_or_where_clause(schema, clause)
            },
            DeconstructedOrJoin::UnitPattern(pattern) => {
                // Same, but simpler.
                self.apply_pattern(schema, pattern);
                Ok(())
            },
            DeconstructedOrJoin::Simple(patterns, mentioned_vars) => {
                // Hooray! Fully unified and plain ol' patterns that all use the same table.
                // Go right ahead and produce a set of constraint alternations that we can collect,
                // using a single table alias.
                // TODO
                self.apply_simple_or_join(schema, patterns, mentioned_vars)
            },
            DeconstructedOrJoin::Complex(_) => {
                // Do this the hard way. TODO
                unimplemented!();
            },
        }
    }


    /// A simple `or` join is effectively a single pattern in which an individual column's bindings
    /// are not a single value. Rather than a pattern like
    ///
    /// ```edn
    /// [?x :foo/knows "John"]
    /// ```
    ///
    /// we have
    ///
    /// ```edn
    /// (or [?x :foo/knows "John"]
    ///     [?x :foo/hates "Peter"])
    /// ```
    ///
    /// but the generated SQL is very similar: the former is
    ///
    /// ```sql
    /// WHERE datoms00.a = 99 AND datoms00.v = 'John'
    /// ```
    ///
    /// with the latter growing to
    ///
    /// ```sql
    /// WHERE (datoms00.a = 99 AND datoms00.v = 'John')
    ///    OR (datoms00.a = 98 AND datoms00.v = 'Peter')
    /// ```
    ///
    fn apply_simple_or_join(&mut self, schema: &Schema, patterns: Vec<Pattern>, mentioned_vars: BTreeSet<Variable>) -> Result<()> {
        if self.is_known_empty() {
            return Ok(())
        }

        assert!(patterns.len() >= 2);

        // Begin by building a base CC that we'll use to produce constraints from each pattern.
        // Populate this base CC with whatever variables are already known from the CC to which
        // we're applying this `or`.
        // This will give us any applicable type constraints or column mappings.
        // Then generate a single table alias, based on the first pattern, and use that to make any
        // new variable mappings we will need to extract values.
        let template = self.use_as_template(&mentioned_vars);

        // We expect this to always work: if it doesn't, it means we should never have got to this
        // point.
        let source_alias = self.alias_table(schema, &patterns[0]).expect("couldn't get table");

        // This is where we'll collect everything we eventually add to the destination CC.
        let mut folded = ConjoiningClauses::default();

        // Scoped borrow of source_alias.
        {
            // Clone this CC once for each pattern.
            // Apply each pattern to its CC with the _same_ table alias.
            // Each pattern's derived types are intersected with any type constraints in the
            // template, sourced from the destination CC. If a variable cannot satisfy both type
            // constraints, the new CC cannot match. This prunes the 'or' arms:
            //
            // ```edn
            // [:find ?x
            //  :where [?a :some/int ?x]
            //         (or [_ :some/otherint ?x]
            //             [_ :some/string ?x])]
            // ```
            //
            // can simplify to
            //
            // ```edn
            // [:find ?x
            //  :where [?a :some/int ?x]
            //         [_ :some/otherint ?x]]
            // ```
            let mut receptacles =
                patterns.into_iter()
                        .map(|pattern| {
                            let mut receptacle = template.make_receptacle();
                            println!("Applying pattern with attribute {:?}", pattern.attribute);
                            receptacle.apply_pattern_clause_for_alias(schema, &pattern, &source_alias);
                            receptacle
                        })
                        .peekable();

            // Let's see if we can grab a reason if every pattern failed.
            // If every pattern failed, we can just take the first!
            let reason = receptacles.peek()
                                    .map(|r| r.empty_because.clone())
                                    .unwrap_or(None);

            // Filter out empties.
            let mut receptacles = receptacles.filter(|receptacle| !receptacle.is_known_empty())
                                             .peekable();

            // We need to copy the column bindings from one of the receptacles. Because this is a simple
            // or, we know that they're all the same.
            // Because we just made an empty template, and created a new alias from the destination CC,
            // we know that we can blindly merge: collisions aren't possible.
            if let Some(first) = receptacles.peek() {
                for (v, cols) in &first.column_bindings {
                    println!("Adding {:?}: {:?}", v, cols);
                    match self.column_bindings.entry(v.clone()) {
                        Entry::Vacant(e) => {
                            e.insert(cols.clone());
                        },
                        Entry::Occupied(mut e) => {
                            e.get_mut().append(&mut cols.clone());
                        },
                    }
                }
            } else {
                // No non-empty receptacles? The destination CC is known-empty, because or([]) is false.
                self.mark_known_empty(reason.unwrap_or(EmptyBecause::AttributeLookupFailed));
                return Ok(());
            }

            // Otherwise, we fold together the receptacles.
            //
            // Merge together the constraints from each receptacle. Each bundle of constraints is
            // combined into a `ConstraintIntersection`, and the collection of intersections is
            // combined into a `ConstraintAlternation`. (As an optimization, this collection can be
            // simplified.)
            //
            // Each receptacle's known types are _unioned_. Strictly speaking this is a weakening:
            // we might know that if `?x` is an integer then `?y` is a string, or vice versa, but at
            // this point we'll simply state that `?x` and `?y` can both be integers or strings.

            fn vec_for_iterator<T, I, U>(iter: &I) -> Vec<T> where I: Iterator<Item=U> {
                match iter.size_hint().1 {
                    None => Vec::new(),
                    Some(expected) => Vec::with_capacity(expected),
                }
            }

            let mut alternates: Vec<ColumnIntersection> = vec_for_iterator(&receptacles);
            for r in receptacles {
                folded.broaden_types(r.known_types);
                alternates.push(r.wheres);
            }

            if alternates.len() == 1 {
                // Simplify.
                folded.wheres = alternates.pop().unwrap();
            } else {
                let alternation = ColumnAlternation(alternates);
                let mut container = ColumnIntersection::default();
                container.add(ColumnConstraintOrAlternation::Alternation(alternation));
                folded.wheres = container;
            }
        }

        // Collect the source alias: we use a single table join to represent the entire `or`.
        self.from.push(source_alias);

        // Add in the known types and constraints.
        // Each constant attribute might _expand_ the set of possible types of the value-place
        // variable. We thus generate a set of possible types, and we intersect it with the
        // types already possible in the CC. If the resultant set is empty, the pattern cannot
        // match. If the final set isn't unit, we must project a type tag column.
        self.intersect(folded)
    }

    fn intersect(&mut self, mut cc: ConjoiningClauses) -> Result<()> {
        if cc.is_known_empty() {
            self.empty_because = cc.empty_because;
        }
        self.wheres.append(&mut cc.wheres);
        self.narrow_types(cc.known_types);
        Ok(())
    }
}

#[cfg(test)]
mod testing {
    extern crate mentat_query_parser;

    use super::*;

    use mentat_core::{
        Attribute,
        TypedValue,
        ValueType,
    };

    use mentat_query::{
        NamespacedKeyword,
        Variable,
    };

    use self::mentat_query_parser::{
        parse_find_string,
    };

    use clauses::{
        add_attribute,
        associate_ident,
    };

    use types::{
        ColumnConstraint,
        DatomsColumn,
        DatomsTable,
        NumericComparison,
        QualifiedAlias,
        QueryValue,
        SourceAlias,
    };

    use algebrize;

    fn alg(schema: &Schema, input: &str) -> ConjoiningClauses {
        let parsed = parse_find_string(input).expect("parse failed");
        algebrize(schema.into(), parsed).expect("algebrize failed").cc
    }

    fn compare_ccs(left: ConjoiningClauses, right: ConjoiningClauses) {
        assert_eq!(left.wheres, right.wheres);
        assert_eq!(left.from, right.from);
    }

    fn prepopulated_schema() -> Schema {
        let mut schema = Schema::default();
        associate_ident(&mut schema, NamespacedKeyword::new("foo", "name"), 65);
        associate_ident(&mut schema, NamespacedKeyword::new("foo", "knows"), 66);
        associate_ident(&mut schema, NamespacedKeyword::new("foo", "parent"), 67);
        associate_ident(&mut schema, NamespacedKeyword::new("foo", "age"), 68);
        associate_ident(&mut schema, NamespacedKeyword::new("foo", "height"), 69);
        add_attribute(&mut schema, 65, Attribute {
            value_type: ValueType::String,
            multival: false,
            ..Default::default()
        });
        add_attribute(&mut schema, 66, Attribute {
            value_type: ValueType::String,
            multival: true,
            ..Default::default()
        });
        add_attribute(&mut schema, 67, Attribute {
            value_type: ValueType::String,
            multival: true,
            ..Default::default()
        });
        add_attribute(&mut schema, 68, Attribute {
            value_type: ValueType::Long,
            multival: false,
            ..Default::default()
        });
        add_attribute(&mut schema, 69, Attribute {
            value_type: ValueType::Long,
            multival: false,
            ..Default::default()
        });
        schema
    }
    /// Test that if all the attributes in an `or` fail to resolve, the entire thing fails.
    #[test]
    fn test_schema_based_failure() {
        let schema = Schema::default();
        let query = r#"
            [:find ?x
             :where (or [?x :foo/nope1 "John"]
                        [?x :foo/nope2 "Ámbar"]
                        [?x :foo/nope3 "Daphne"])]"#;
        let cc = alg(&schema, query);
        assert!(cc.is_known_empty());
        assert_eq!(cc.empty_because, Some(EmptyBecause::InvalidAttributeIdent(NamespacedKeyword::new("foo", "nope3"))));
    }

    /// Test that if only one of the attributes in an `or` resolves, it's equivalent to a simple query.
    #[test]
    fn test_only_one_arm_succeeds() {
        let schema = prepopulated_schema();
        let query = r#"
            [:find ?x
             :where (or [?x :foo/nope "John"]
                        [?x :foo/parent "Ámbar"]
                        [?x :foo/nope "Daphne"])]"#;
        let cc = alg(&schema, query);
        assert!(!cc.is_known_empty());
        compare_ccs(cc, alg(&schema, r#"[:find ?x :where [?x :foo/parent "Ámbar"]]"#));
    }

    // Simple alternation.
    #[test]
    fn test_simple_alternation() {
        let schema = prepopulated_schema();
        let query = r#"
            [:find ?x
             :where (or [?x :foo/knows "John"]
                        [?x :foo/parent "Ámbar"]
                        [?x :foo/knows "Daphne"])]"#;
        let cc = alg(&schema, query);
        let vx = Variable::from_valid_name("?x");
        let d0 = "datoms00".to_string();
        let d0e = QualifiedAlias(d0.clone(), DatomsColumn::Entity);
        let d0a = QualifiedAlias(d0.clone(), DatomsColumn::Attribute);
        let d0v = QualifiedAlias(d0.clone(), DatomsColumn::Value);
        let knows = QueryValue::Entid(66);
        let parent = QueryValue::Entid(67);
        let john = QueryValue::TypedValue(TypedValue::typed_string("John"));
        let ambar = QueryValue::TypedValue(TypedValue::typed_string("Ámbar"));
        let daphne = QueryValue::TypedValue(TypedValue::typed_string("Daphne"));

        assert!(!cc.is_known_empty());
        assert_eq!(cc.wheres, ColumnIntersection(vec![
            ColumnConstraintOrAlternation::Alternation(
                ColumnAlternation(vec![
                    ColumnIntersection(vec![
                        ColumnConstraintOrAlternation::Constraint(ColumnConstraint::Equals(d0a.clone(), knows.clone())),
                        ColumnConstraintOrAlternation::Constraint(ColumnConstraint::Equals(d0v.clone(), john))]),
                    ColumnIntersection(vec![
                        ColumnConstraintOrAlternation::Constraint(ColumnConstraint::Equals(d0a.clone(), parent)),
                        ColumnConstraintOrAlternation::Constraint(ColumnConstraint::Equals(d0v.clone(), ambar))]),
                    ColumnIntersection(vec![
                        ColumnConstraintOrAlternation::Constraint(ColumnConstraint::Equals(d0a.clone(), knows)),
                        ColumnConstraintOrAlternation::Constraint(ColumnConstraint::Equals(d0v.clone(), daphne))]),
                    ]))]));
        assert_eq!(cc.column_bindings.get(&vx), Some(&vec![d0e]));
        assert_eq!(cc.from, vec![SourceAlias(DatomsTable::Datoms, d0)]);
    }

    // Alternation with a pattern.
    #[test]
    fn test_alternation_with_pattern() {
        let schema = prepopulated_schema();
        let query = r#"
            [:find [?x ?name]
             :where
             [?x :foo/name ?name]
             (or [?x :foo/knows "John"]
                 [?x :foo/parent "Ámbar"]
                 [?x :foo/knows "Daphne"])]"#;
        let cc = alg(&schema, query);
        let vx = Variable::from_valid_name("?x");
        let d0 = "datoms00".to_string();
        let d1 = "datoms01".to_string();
        let d0e = QualifiedAlias(d0.clone(), DatomsColumn::Entity);
        let d0a = QualifiedAlias(d0.clone(), DatomsColumn::Attribute);
        let d1e = QualifiedAlias(d1.clone(), DatomsColumn::Entity);
        let d1a = QualifiedAlias(d1.clone(), DatomsColumn::Attribute);
        let d1v = QualifiedAlias(d1.clone(), DatomsColumn::Value);
        let name = QueryValue::Entid(65);
        let knows = QueryValue::Entid(66);
        let parent = QueryValue::Entid(67);
        let john = QueryValue::TypedValue(TypedValue::typed_string("John"));
        let ambar = QueryValue::TypedValue(TypedValue::typed_string("Ámbar"));
        let daphne = QueryValue::TypedValue(TypedValue::typed_string("Daphne"));

        assert!(!cc.is_known_empty());
        assert_eq!(cc.wheres, ColumnIntersection(vec![
            ColumnConstraintOrAlternation::Constraint(ColumnConstraint::Equals(d0a.clone(), name.clone())),
            ColumnConstraintOrAlternation::Alternation(
                ColumnAlternation(vec![
                    ColumnIntersection(vec![
                        ColumnConstraintOrAlternation::Constraint(ColumnConstraint::Equals(d1a.clone(), knows.clone())),
                        ColumnConstraintOrAlternation::Constraint(ColumnConstraint::Equals(d1v.clone(), john))]),
                    ColumnIntersection(vec![
                        ColumnConstraintOrAlternation::Constraint(ColumnConstraint::Equals(d1a.clone(), parent)),
                        ColumnConstraintOrAlternation::Constraint(ColumnConstraint::Equals(d1v.clone(), ambar))]),
                    ColumnIntersection(vec![
                        ColumnConstraintOrAlternation::Constraint(ColumnConstraint::Equals(d1a.clone(), knows)),
                        ColumnConstraintOrAlternation::Constraint(ColumnConstraint::Equals(d1v.clone(), daphne))]),
                    ])),
            // The outer pattern joins against the `or`.
            ColumnConstraintOrAlternation::Constraint(ColumnConstraint::Equals(d0e.clone(), QueryValue::Column(d1e.clone()))),
        ]));
        assert_eq!(cc.column_bindings.get(&vx), Some(&vec![d0e, d1e]));
        assert_eq!(cc.from, vec![SourceAlias(DatomsTable::Datoms, d0),
                                 SourceAlias(DatomsTable::Datoms, d1)]);
    }

    // Alternation with a pattern and a predicate.
    #[test]
    fn test_alternation_with_pattern_and_predicate() {
        let schema = prepopulated_schema();
        let query = r#"
            [:find ?x ?age
             :where
             [?x :foo/age ?age]
             [[< ?age 30]]
             (or [?x :foo/knows "John"]
                 [?x :foo/knows "Daphne"])]"#;
        let cc = alg(&schema, query);
        let vx = Variable::from_valid_name("?x");
        let d0 = "datoms00".to_string();
        let d1 = "datoms01".to_string();
        let d0e = QualifiedAlias(d0.clone(), DatomsColumn::Entity);
        let d0a = QualifiedAlias(d0.clone(), DatomsColumn::Attribute);
        let d0v = QualifiedAlias(d0.clone(), DatomsColumn::Value);
        let d1e = QualifiedAlias(d1.clone(), DatomsColumn::Entity);
        let d1a = QualifiedAlias(d1.clone(), DatomsColumn::Attribute);
        let d1v = QualifiedAlias(d1.clone(), DatomsColumn::Value);
        let knows = QueryValue::Entid(66);
        let age = QueryValue::Entid(68);
        let john = QueryValue::TypedValue(TypedValue::typed_string("John"));
        let daphne = QueryValue::TypedValue(TypedValue::typed_string("Daphne"));

        assert!(!cc.is_known_empty());
        assert_eq!(cc.wheres, ColumnIntersection(vec![
            ColumnConstraintOrAlternation::Constraint(ColumnConstraint::Equals(d0a.clone(), age.clone())),
            ColumnConstraintOrAlternation::Constraint(ColumnConstraint::NumericInequality {
                operator: NumericComparison::LessThan,
                left: QueryValue::Column(d0v.clone()),
                right: QueryValue::TypedValue(TypedValue::Long(30)),
            }),
            ColumnConstraintOrAlternation::Alternation(
                ColumnAlternation(vec![
                    ColumnIntersection(vec![
                        ColumnConstraintOrAlternation::Constraint(ColumnConstraint::Equals(d1a.clone(), knows.clone())),
                        ColumnConstraintOrAlternation::Constraint(ColumnConstraint::Equals(d1v.clone(), john))]),
                    ColumnIntersection(vec![
                        ColumnConstraintOrAlternation::Constraint(ColumnConstraint::Equals(d1a.clone(), knows)),
                        ColumnConstraintOrAlternation::Constraint(ColumnConstraint::Equals(d1v.clone(), daphne))]),
                    ])),
            // The outer pattern joins against the `or`.
            ColumnConstraintOrAlternation::Constraint(ColumnConstraint::Equals(d0e.clone(), QueryValue::Column(d1e.clone()))),
        ]));
        assert_eq!(cc.column_bindings.get(&vx), Some(&vec![d0e, d1e]));
        assert_eq!(cc.from, vec![SourceAlias(DatomsTable::Datoms, d0),
                                 SourceAlias(DatomsTable::Datoms, d1)]);
    }

    // These two are not equivalent:
    // [:find ?x :where [?x :foo/bar ?y] (or-join [?x] [?x :foo/baz ?y])]
    // [:find ?x :where [?x :foo/bar ?y] [?x :foo/baz ?y]]
    #[test]
    #[should_panic(expected = "not yet implemented")]
    fn test_unit_or_join_doesnt_flatten() {
        let schema = prepopulated_schema();
        let query = r#"[:find ?x
                        :where [?x :foo/knows ?y]
                               (or-join [?x] [?x :foo/parent ?y])]"#;
        let cc = alg(&schema, query);
        let vx = Variable::from_valid_name("?x");
        let vy = Variable::from_valid_name("?y");
        let d0 = "datoms00".to_string();
        let d1 = "datoms01".to_string();
        let d0e = QualifiedAlias(d0.clone(), DatomsColumn::Entity);
        let d0a = QualifiedAlias(d0.clone(), DatomsColumn::Attribute);
        let d0v = QualifiedAlias(d0.clone(), DatomsColumn::Value);
        let d1e = QualifiedAlias(d1.clone(), DatomsColumn::Entity);
        let d1a = QualifiedAlias(d1.clone(), DatomsColumn::Attribute);
        let knows = QueryValue::Entid(66);
        let parent = QueryValue::Entid(67);

        assert!(!cc.is_known_empty());
        assert_eq!(cc.wheres, ColumnIntersection(vec![
            ColumnConstraintOrAlternation::Constraint(ColumnConstraint::Equals(d0a.clone(), knows.clone())),
            ColumnConstraintOrAlternation::Constraint(ColumnConstraint::Equals(d1a.clone(), parent.clone())),
            // The outer pattern joins against the `or` on the entity, but not value -- ?y means
            // different things in each place.
            ColumnConstraintOrAlternation::Constraint(ColumnConstraint::Equals(d0e.clone(), QueryValue::Column(d1e.clone()))),
        ]));
        assert_eq!(cc.column_bindings.get(&vx), Some(&vec![d0e, d1e]));

        // ?y does not have a binding in the `or-join` pattern.
        assert_eq!(cc.column_bindings.get(&vy), Some(&vec![d0v]));
        assert_eq!(cc.from, vec![SourceAlias(DatomsTable::Datoms, d0),
                                 SourceAlias(DatomsTable::Datoms, d1)]);
    }

    // These two are equivalent:
    // [:find ?x :where [?x :foo/bar ?y] (or [?x :foo/baz ?y])]
    // [:find ?x :where [?x :foo/bar ?y] [?x :foo/baz ?y]]
    #[test]
    fn test_unit_or_does_flatten() {
        let schema = prepopulated_schema();
        let or_query =   r#"[:find ?x
                             :where [?x :foo/knows ?y]
                                    (or [?x :foo/parent ?y])]"#;
        let flat_query = r#"[:find ?x
                             :where [?x :foo/knows ?y]
                                    [?x :foo/parent ?y]]"#;
        compare_ccs(alg(&schema, or_query),
                    alg(&schema, flat_query));
    }

    // Elision of `and`.
    #[test]
    fn test_unit_or_and_does_flatten() {
        let schema = prepopulated_schema();
        let or_query =   r#"[:find ?x
                             :where (or (and [?x :foo/parent ?y]
                                             [?x :foo/age 7]))]"#;
        let flat_query =   r#"[:find ?x
                               :where [?x :foo/parent ?y]
                                      [?x :foo/age 7]]"#;
        compare_ccs(alg(&schema, or_query),
                    alg(&schema, flat_query));
    }

    // Alternation with `and`.
    /// [:find ?x
    ///  :where (or (and [?x :foo/knows "John"]
    ///                  [?x :foo/parent "Ámbar"])
    ///             [?x :foo/knows "Daphne"])]
    /// Strictly speaking this can be implemented with a `NOT EXISTS` clause for the second pattern,
    /// but that would be a fair amount of analysis work, I think.
    #[test]
    #[should_panic(expected = "not yet implemented")]
    #[allow(dead_code, unused_variables)]
    fn test_alternation_with_and() {
        let schema = prepopulated_schema();
        let query = r#"
            [:find ?x
             :where (or (and [?x :foo/knows "John"]
                             [?x :foo/parent "Ámbar"])
                        [?x :foo/knows "Daphne"])]"#;
        let cc = alg(&schema, query);
    }

    #[test]
    fn test_type_based_or_pruning() {
        let schema = prepopulated_schema();
        // This simplifies to:
        // [:find ?x
        //  :where [?a :some/int ?x]
        //         [_ :some/otherint ?x]]
        let query = r#"
            [:find ?x
             :where [?a :foo/age ?x]
                    (or [_ :foo/height ?x]
                        [_ :foo/name ?x])]"#;
        let simple = r#"
            [:find ?x
             :where [?a :foo/age ?x]
                    [_ :foo/height ?x]]"#;
        compare_ccs(alg(&schema, query), alg(&schema, simple));
    }
}