// Copyright 2016 Mozilla
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

#![allow(dead_code)]

use std::collections::BTreeMap;

/// Witness assertions and retractions, folding (assertion, retraction) pairs into alterations.
/// Assumes that no assertion or retraction will be witnessed more than once.
///
/// This keeps track of when we see a :db/add, a :db/retract, or both :db/add and :db/retract in
/// some order.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialOrd, PartialEq)]
pub struct AddRetractAlterSet<K, V> {
    pub asserted: BTreeMap<K, V>,
    pub retracted: BTreeMap<K, V>,
    pub altered: BTreeMap<K, (V, V)>,
}

impl<K, V> Default for AddRetractAlterSet<K, V> where K: Ord {
    fn default() -> AddRetractAlterSet<K, V> {
        AddRetractAlterSet {
            asserted: BTreeMap::default(),
            retracted: BTreeMap::default(),
            altered: BTreeMap::default(),
        }
    }
}

impl<K, V> AddRetractAlterSet<K, V> where K: Ord {
    pub fn witness(&mut self, key: K, value: V, added: bool) {
        if added {
            if let Some(retracted_value) = self.retracted.remove(&key) {
                self.altered.insert(key, (retracted_value, value));
            } else {
                self.asserted.insert(key, value);
            }
        } else {
            if let Some(asserted_value) = self.asserted.remove(&key) {
                self.altered.insert(key, (value, asserted_value));
            } else {
                self.retracted.insert(key, value);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test() {
        let mut set: AddRetractAlterSet<i64, char> = AddRetractAlterSet::default();
        // Assertion.
        set.witness(1, 'a', true);
        // Retraction.
        set.witness(2, 'b', false);
        // Alteration.
        set.witness(3, 'c', true);
        set.witness(3, 'd', false);
        // Alteration, witnessed in the with the retraction before the assertion.
        set.witness(4, 'e', false);
        set.witness(4, 'f', true);

        let mut asserted = BTreeMap::default();
        asserted.insert(1, 'a');
        let mut retracted = BTreeMap::default();
        retracted.insert(2, 'b');
        let mut altered = BTreeMap::default();
        altered.insert(3, ('d', 'c'));
        altered.insert(4, ('e', 'f'));

        assert_eq!(set.asserted, asserted);
        assert_eq!(set.retracted, retracted);
        assert_eq!(set.altered, altered);
    }
}
