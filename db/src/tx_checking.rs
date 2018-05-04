// Copyright 2018 Mozilla
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

use std::collections::{
    BTreeSet,
    BTreeMap,
};

use mentat_core::{
    Entid,
    TypedValue,
    ValueType,
};

use errors::{
    CardinalityConflict,
};

use internal_types::{
    AEVTrie,
};

/// Map from found [e a v] to expected type.
pub(crate) type TypeDisagreements = BTreeMap<(Entid, Entid, TypedValue), ValueType>;

/// Ensure that the given terms type check.
///
/// We try to be maximally helpful by yielding every malformed datom, rather than only the first.
/// In the future, we might change this choice, or allow the consumer to specify the robustness of
/// the type checking desired, since there is a cost to providing helpful diagnostics.
pub(crate) fn type_disagreements<'schema>(aev_trie: &AEVTrie<'schema>) -> TypeDisagreements {
    let mut errors: TypeDisagreements = TypeDisagreements::default();

    for (&(a, attribute), evs) in aev_trie {
        for (&e, ref ars) in evs {
            for v in ars.add.iter().chain(ars.retract.iter()) {
                if attribute.value_type != v.value_type() {
                    errors.insert((e, a, v.clone()), attribute.value_type);
                }
            }
        }
    }

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
///
/// We try to be maximally helpful by yielding every malformed set of datoms, rather than just the
/// first set, or even the first conflict.  In the future, we might change this choice, or allow the
/// consumer to specify the robustness of the cardinality checking desired.
pub(crate) fn cardinality_conflicts<'schema>(aev_trie: &AEVTrie<'schema>) -> Vec<CardinalityConflict> {
    let mut errors = vec![];

    for (&(a, attribute), evs) in aev_trie {
        for (&e, ref ars) in evs {
            if !attribute.multival && ars.add.len() > 1 {
                let vs = ars.add.clone();
                errors.push(CardinalityConflict::CardinalityOneAddConflict { e, a, vs });
            }

            let vs: BTreeSet<_> = ars.retract.intersection(&ars.add).cloned().collect();
            if !vs.is_empty() {
                errors.push(CardinalityConflict::AddRetractConflict { e, a, vs })
            }
        }
    }

    errors
}
