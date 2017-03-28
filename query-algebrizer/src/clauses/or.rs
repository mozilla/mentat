// Copyright 2016 Mozilla
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

// WIP
#![allow(dead_code, unused_imports, unused_variables)]

use mentat_core::{
    Entid,
    Schema,
    TypedValue,
    ValueType,
};

use mentat_query::{
    NonIntegerConstant,
    OrJoin,
    OrWhereClause,
    Pattern,
    PatternValuePlace,
    PatternNonValuePlace,
    PlainSymbol,
    Predicate,
    SrcVar,
    UnifyVars,
    WhereClause,
};

use clauses::ConjoiningClauses;

use errors::{
    Result,
    Error,
    ErrorKind,
};

use types::{
    ColumnConstraint,
    ColumnIntersection,
    DatomsColumn,
    DatomsTable,
    EmptyBecause,
    NumericComparison,
    QualifiedAlias,
    QueryValue,
    SourceAlias,
    TableAlias,
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
    Simple(Vec<Pattern>),
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

    fn apply_or_join(&mut self, schema: &Schema, mut or_join: OrJoin) -> Result<()> {
        // Simple optimization. Empty `or` clauses disappear. Unit `or` clauses
        // are equivalent to just the inner clause.
        match or_join.clauses.len() {
            0 => Ok(()),
            1 => self.apply_or_where_clause(schema, or_join.clauses.pop().unwrap()),
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
        let mut clauses = or_join.clauses.into_iter();
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

            return DeconstructedOrJoin::Complex(OrJoin {
                unify_vars: UnifyVars::Implicit,
                clauses: reconstructed,
            });
        }

        // If we got here without returning, then `patterns` is what we're working with.
        // If `patterns` is empty, it means _none_ of the clauses in the `or` could succeed.
        match patterns.len() {
            0 => {
                assert!(empty_because.is_some());
                DeconstructedOrJoin::KnownEmpty(empty_because.unwrap())
            },
            1 => DeconstructedOrJoin::UnitPattern(patterns.pop().unwrap()),
            _ => DeconstructedOrJoin::Simple(patterns),
        }
    }

    /// Only call this with an `or_join` with 2 or more patterns.
    fn apply_non_trivial_or_join(&mut self, schema: &Schema, or_join: OrJoin) -> Result<()> {
        assert!(or_join.clauses.len() >= 2);

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
            DeconstructedOrJoin::Simple(patterns) => {
                // Hooray! Fully unified and plain ol' patterns that all use the same table.
                // Go right ahead and produce a set of constraint alternations that we can collect,
                // using a single table alias.
                // TODO
                self.apply_simple_or_join(schema, patterns)
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
    fn apply_simple_or_join(&mut self, schema: &Schema, patterns: Vec<Pattern>) -> Result<()> {
        assert!(patterns.len() >= 2);

        // Each constant attribute might _expand_ the set of possible types of the value-place
        // variable. We thus generate a set of possible types, and we intersect it with the
        // types already possible in the CC. If the resultant set is empty, the pattern cannot match.
        // If the final set isn't unit, we must project a type tag column.
        // If one of the alternations requires a type that is impossible in the CC, then we can
        // discard that alternate:
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
        //
        // Similarly, if the value place is constant, it must be of a type that doesn't determine
        // a different table for any of the patterns.
        // TODO

        // Begin by building a base CC that we'll use to produce constraints from each pattern.
        // Populate this base CC with whatever variables are already known from the CC to which
        // we're applying this `or`.
        // This will give us any applicable type constraints or column mappings.
        // Then generate a single table alias, based on the first pattern, and use that to make any
        // new variable mappings we will need to extract values.
        Ok(())
    }
}
