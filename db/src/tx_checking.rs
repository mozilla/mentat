// Copyright 2016 Mozilla
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

use std::collections::{
    BTreeMap,
    BTreeSet,
};

use mentat_core::{
    Entid,
    TypedValue,
    ValueType,
};

use mentat_tx::entities::{
    OpType,
};
use errors::{
    CardinalityConflict,
};

use internal_types::{
    AttributedTerm,
    Term,
};

/// Ensure that the given terms type check.
///
/// We try to be maximally helpful by yielding every malformed datom, rather than only the first.
/// In the future, we might change this choice, or allow the consumer to specify the robustness of
/// the type checking desired, since there is a cost to providing helpful diagnostics.
pub(crate) fn type_disagreements<'a, I>(terms: I) -> BTreeMap<(Entid, Entid, TypedValue), ValueType>
where I: IntoIterator<Item=&'a AttributedTerm<'a>> {
    let errors: BTreeMap<_, _> = terms.into_iter().filter_map(|term| {
        match term {
            &Term::AddOrRetract(_, e, (a, attribute), ref v) => {
                if attribute.value_type != v.value_type() {
                    Some(((e, a, v.clone()), attribute.value_type))
                } else {
                    None
                }
            },
        }
    }).collect();

    errors
}

/// Ensure that the given terms obey the cardinality restrictions of the given schema.
///
/// That is, ensure that any cardinality one attribute is added with at most one distinct value for
/// any specific entity (although that one value may be repeated for the given entity).
/// It is an error to:
///
/// - add two distinct values for the same cardinality one attribute and entity in a single transaction
/// - add and remove the same values for the same attribute and entity in a single transaction
/// - add any attribute to an entity that is also the subject of a `:db/retractEntity` term
///
/// We try to be maximally helpful by yielding every malformed set of datoms, rather than just the
/// first set, or even the first conflict.  In the future, we might change this choice, or allow the
/// consumer to specify the robustness of the cardinality checking desired.
pub(crate) fn cardinality_conflicts<'a, I>(terms: I) -> Vec<CardinalityConflict>
where I: IntoIterator<Item=&'a AttributedTerm<'a>> {
    let mut errors = vec![];

    let mut added: BTreeMap<Entid, BTreeMap<(Entid, bool), BTreeSet<&TypedValue>>> = BTreeMap::default();
    let mut retracted: BTreeMap<Entid, BTreeMap<(Entid, bool), BTreeSet<&TypedValue>>> = BTreeMap::default();

    for term in terms.into_iter() {
        match term {
            &Term::AddOrRetract(op, e, (a, attribute), ref v) => {
                let map = match op {
                    OpType::Add => &mut added,
                    OpType::Retract => &mut retracted,
                };
                map
                    .entry(e).or_insert(BTreeMap::default())
                    .entry((a, attribute.multival)).or_insert(BTreeSet::default())
                    .insert(v);
            },
        }
    }

    for (e, avs) in added.iter() {
        for (&(a, multival), added_vs) in avs.iter() {
            if !multival && added_vs.len() > 1 {
                let added = added_vs.iter().map(|v| (*e, a, (*v).clone())).collect();
                errors.push(CardinalityConflict::CardinalityOneAddConflict {
                    added: added,
                });
            }

            retracted.get(e).and_then(|avs| avs.get(&(a, multival))).map(|retracted_vs| {
                let intersection: Vec<_> = added_vs.intersection(&retracted_vs).cloned().collect();
                if !intersection.is_empty() {
                    let datoms = intersection.iter().map(|v| (*e, a, (*v).clone())).collect();
                    errors.push(CardinalityConflict::AddRetractConflict {
                        datoms: datoms,
                    });
                }
            });
        }
    }

    errors
}
