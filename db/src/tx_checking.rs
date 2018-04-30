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
};

use mentat_core::{
    Entid,
    TypedValue,
    ValueType,
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
