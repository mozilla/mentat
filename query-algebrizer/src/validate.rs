// Copyright 2016 Mozilla
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

use std::collections::BTreeSet;

use mentat_query::{
    ContainsVariables,
    OrJoin,
    Variable,
    WhereClause,
    UnifyVars,
};

use errors::{
    ErrorKind,
    Result,
};

/// In an `or` expression, every mentioned var is considered 'free'.
/// In an `or-join` expression, every var in the var list is 'required'.
///
/// Every extracted variable must be used in the clauses.
/// The extracted var list cannot be empty.
///
/// The original Datomic docs are poorly worded:
///
/// "All clauses used in an or clause must use the same set of variables, which will unify with the
/// surrounding query. This includes both the arguments to nested expression clauses as well as any
/// bindings made by nested function expressions. Datomic will attempt to push the or clause down
/// until all necessary variables are bound, and will throw an exception if that is not possible."
///
/// What this really means is: each pattern in the `or-join` clause must use the var list and unify
/// with the surrounding query. It does not mean that each leg must have the same set of vars.
///
/// An `or` pattern must, because the set of vars is defined as every var mentioned in any clause,
/// so naturally they must all be the same.
///
/// "As with rules, src-vars are not currently supported within the clauses of or, but are supported
/// on the or clause as a whole at top level."
pub fn validate_or_join(or_join: &OrJoin) -> Result<()> {
    // Grab our mentioned variables and ensure that the rules are followed.
    match or_join.unify_vars {
        UnifyVars::Implicit => {
            // Each 'leg' must have the same variable set.
            if or_join.clauses.len() < 2 {
                Ok(())
            } else {
                let mut clauses = or_join.clauses.iter();
                let template = clauses.next().unwrap().collect_mentioned_variables();
                for clause in clauses {
                    if template != clause.collect_mentioned_variables() {
                        bail!(ErrorKind::NonMatchingVariablesInOrClause);
                    }
                }
                Ok(())
            }
        },
        UnifyVars::Explicit(ref vars) => {
            // Each leg must use the joined vars.
            let var_set: BTreeSet<Variable> = vars.iter().cloned().collect();
            for clause in &or_join.clauses {
                if !var_set.is_subset(&clause.collect_mentioned_variables()) {
                    bail!(ErrorKind::NonMatchingVariablesInOrClause);
                }
            }
            Ok(())
        },
    }
}

#[cfg(test)]
mod tests {
    extern crate mentat_core;
    extern crate mentat_query;
    extern crate mentat_query_parser;

    use self::mentat_query::{
        FindQuery,
        NamespacedKeyword,
        OrWhereClause,
        Pattern,
        PatternNonValuePlace,
        PatternValuePlace,
        PlainSymbol,
        SrcVar,
        UnifyVars,
        Variable,
        WhereClause,
    };

    use self::mentat_query_parser::parse_find_string;

    use super::validate_or_join;

    /// Tests that the top-level form is a valid `or`, returning the clauses.
    fn valid_or_join(parsed: FindQuery, expected_unify: UnifyVars) -> Vec<OrWhereClause> {
        let mut wheres = parsed.where_clauses.into_iter();

        // There's only one.
        let clause = wheres.next().unwrap();
        assert_eq!(None, wheres.next());

        match clause {
            WhereClause::OrJoin(or_join) => {
                // It's valid: the variables are the same in each branch.
                assert_eq!((), validate_or_join(&or_join).unwrap());
                assert_eq!(expected_unify, or_join.unify_vars);
                or_join.clauses
            },
            _ => panic!(),
        }
    }

    /// Test that an `or` is valid if all of its arms refer to the same variables.
    #[test]
    fn test_success_or() {
        let query = r#"[:find [?artist ...]
                        :where (or [?artist :artist/type :artist.type/group]
                                   (and [?artist :artist/type :artist.type/person]
                                        [?artist :artist/gender :artist.gender/female]))]"#;
        let parsed = parse_find_string(query).expect("expected successful parse");
        let clauses = valid_or_join(parsed, UnifyVars::Implicit);

        // Let's do some detailed parse checks.
        let mut arms = clauses.into_iter();
        match (arms.next(), arms.next(), arms.next()) {
            (Some(left), Some(right), None) => {
                assert_eq!(
                    left,
                    OrWhereClause::Clause(WhereClause::Pattern(Pattern {
                        source: None,
                        entity: PatternNonValuePlace::Variable(Variable(PlainSymbol::new("?artist"))),
                        attribute: PatternNonValuePlace::Ident(NamespacedKeyword::new("artist", "type")),
                        value: PatternValuePlace::IdentOrKeyword(NamespacedKeyword::new("artist.type", "group")),
                        tx: PatternNonValuePlace::Placeholder,
                    })));
                assert_eq!(
                    right,
                    OrWhereClause::And(
                        vec![
                            WhereClause::Pattern(Pattern {
                                source: None,
                                entity: PatternNonValuePlace::Variable(Variable(PlainSymbol::new("?artist"))),
                                attribute: PatternNonValuePlace::Ident(NamespacedKeyword::new("artist", "type")),
                                value: PatternValuePlace::IdentOrKeyword(NamespacedKeyword::new("artist.type", "person")),
                                tx: PatternNonValuePlace::Placeholder,
                            }),
                            WhereClause::Pattern(Pattern {
                                source: None,
                                entity: PatternNonValuePlace::Variable(Variable(PlainSymbol::new("?artist"))),
                                attribute: PatternNonValuePlace::Ident(NamespacedKeyword::new("artist", "gender")),
                                value: PatternValuePlace::IdentOrKeyword(NamespacedKeyword::new("artist.gender", "female")),
                                tx: PatternNonValuePlace::Placeholder,
                            }),
                        ]));
            },
            _ => panic!(),
        };
    }

    /// Test that an `or` with differing variable sets in each arm will fail to validate.
    #[test]
    fn test_invalid_implicit_or() {
        let query = r#"[:find [?artist ...]
                        :where (or [?artist :artist/type :artist.type/group]
                                   [?artist :artist/type ?type])]"#;
        let parsed = parse_find_string(query).expect("expected successful parse");
        match parsed.where_clauses.into_iter().next().expect("expected at least one clause") {
            WhereClause::OrJoin(or_join) => assert!(validate_or_join(&or_join).is_err()),
            _ => panic!(),
        }
    }

    /// Test that two arms of an `or-join` can contain different variables if they both
    /// contain the required `or-join` list.
    #[test]
    fn test_success_differing_or_join() {
        let query = r#"[:find [?artist ...]
                        :where (or-join [?artist]
                                   [?artist :artist/type :artist.type/group]
                                   (and [?artist :artist/type ?type]
                                        [?type :artist/role :artist.role/parody]))]"#;
        let parsed = parse_find_string(query).expect("expected successful parse");
        let clauses = valid_or_join(parsed, UnifyVars::Explicit(vec![Variable(PlainSymbol::new("?artist"))]));

        // Let's do some detailed parse checks.
        let mut arms = clauses.into_iter();
        match (arms.next(), arms.next(), arms.next()) {
            (Some(left), Some(right), None) => {
                assert_eq!(
                    left,
                    OrWhereClause::Clause(WhereClause::Pattern(Pattern {
                        source: None,
                        entity: PatternNonValuePlace::Variable(Variable(PlainSymbol::new("?artist"))),
                        attribute: PatternNonValuePlace::Ident(NamespacedKeyword::new("artist", "type")),
                        value: PatternValuePlace::IdentOrKeyword(NamespacedKeyword::new("artist.type", "group")),
                        tx: PatternNonValuePlace::Placeholder,
                    })));
                assert_eq!(
                    right,
                    OrWhereClause::And(
                        vec![
                            WhereClause::Pattern(Pattern {
                                source: None,
                                entity: PatternNonValuePlace::Variable(Variable(PlainSymbol::new("?artist"))),
                                attribute: PatternNonValuePlace::Ident(NamespacedKeyword::new("artist", "type")),
                                value: PatternValuePlace::Variable(Variable(PlainSymbol::new("?type"))),
                                tx: PatternNonValuePlace::Placeholder,
                            }),
                            WhereClause::Pattern(Pattern {
                                source: None,
                                entity: PatternNonValuePlace::Variable(Variable(PlainSymbol::new("?type"))),
                                attribute: PatternNonValuePlace::Ident(NamespacedKeyword::new("artist", "role")),
                                value: PatternValuePlace::IdentOrKeyword(NamespacedKeyword::new("artist.role", "parody")),
                                tx: PatternNonValuePlace::Placeholder,
                            }),
                        ]));
            },
            _ => panic!(),
        };
    }
}