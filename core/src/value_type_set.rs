// Copyright 2018 Mozilla
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

use ::enum_set::{
    EnumSet,
};

use ::types::{
    ValueType,
};

trait EnumSetExtensions<T: ::enum_set::CLike + Clone> {
    /// Return a set containing both `x` and `y`.
    fn of_both(x: T, y: T) -> EnumSet<T>;

    /// Return a clone of `self` with `y` added.
    fn with(&self, y: T) -> EnumSet<T>;
}

impl<T: ::enum_set::CLike + Clone> EnumSetExtensions<T> for EnumSet<T> {
    /// Return a set containing both `x` and `y`.
    fn of_both(x: T, y: T) -> Self {
        let mut o = EnumSet::new();
        o.insert(x);
        o.insert(y);
        o
    }

    /// Return a clone of `self` with `y` added.
    fn with(&self, y: T) -> EnumSet<T> {
        let mut o = self.clone();
        o.insert(y);
        o
    }
}


#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ValueTypeSet(pub EnumSet<ValueType>);

impl Default for ValueTypeSet {
    fn default() -> ValueTypeSet {
        ValueTypeSet::any()
    }
}

impl ValueTypeSet {
    pub fn any() -> ValueTypeSet {
        ValueTypeSet(ValueType::all_enums())
    }

    pub fn none() -> ValueTypeSet {
        ValueTypeSet(EnumSet::new())
    }

    /// Return a set containing only `t`.
    pub fn of_one(t: ValueType) -> ValueTypeSet {
        let mut s = EnumSet::new();
        s.insert(t);
        ValueTypeSet(s)
    }

    /// Return a set containing `Double` and `Long`.
    pub fn of_numeric_types() -> ValueTypeSet {
        ValueTypeSet(EnumSet::of_both(ValueType::Double, ValueType::Long))
    }

    /// Return a set containing `Double`, `Long`, and `Instant`.
    pub fn of_numeric_and_instant_types() -> ValueTypeSet {
        let mut s = EnumSet::new();
        s.insert(ValueType::Double);
        s.insert(ValueType::Long);
        s.insert(ValueType::Instant);
        ValueTypeSet(s)
    }

    /// Return a set containing `Ref` and `Keyword`.
    pub fn of_keywords() -> ValueTypeSet {
        ValueTypeSet(EnumSet::of_both(ValueType::Ref, ValueType::Keyword))
    }

    /// Return a set containing `Ref` and `Long`.
    pub fn of_longs() -> ValueTypeSet {
        ValueTypeSet(EnumSet::of_both(ValueType::Ref, ValueType::Long))
    }
}

impl ValueTypeSet {
    pub fn insert(&mut self, vt: ValueType) -> bool {
        self.0.insert(vt)
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Returns a set containing all the types in this set and `other`.
    pub fn union(&self, other: &ValueTypeSet) -> ValueTypeSet {
        ValueTypeSet(self.0.union(other.0))
    }

    pub fn intersection(&self, other: &ValueTypeSet) -> ValueTypeSet {
        ValueTypeSet(self.0.intersection(other.0))
    }

    /// Returns the set difference between `self` and `other`, which is the
    /// set of items in `self` that are not in `other`.
    pub fn difference(&self, other: &ValueTypeSet) -> ValueTypeSet {
        ValueTypeSet(self.0 - other.0)
    }

    /// Return an arbitrary type that's part of this set.
    /// For a set containing a single type, this will be that type.
    pub fn exemplar(&self) -> Option<ValueType> {
        self.0.iter().next()
    }

    pub fn is_subset(&self, other: &ValueTypeSet) -> bool {
        self.0.is_subset(&other.0)
    }

    /// Returns true if `self` and `other` contain no items in common.
    pub fn is_disjoint(&self, other: &ValueTypeSet) -> bool {
        self.0.is_disjoint(&other.0)
    }

    pub fn contains(&self, vt: ValueType) -> bool {
        self.0.contains(&vt)
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn is_unit(&self) -> bool {
        self.0.len() == 1
    }

    pub fn iter(&self) -> ::enum_set::Iter<ValueType> {
        self.0.iter()
    }
}

impl From<ValueType> for ValueTypeSet {
    fn from(t: ValueType) -> Self {
        ValueTypeSet::of_one(t)
    }
}

impl ValueTypeSet {
    pub fn is_only_numeric(&self) -> bool {
        self.is_subset(&ValueTypeSet::of_numeric_types())
    }
}

impl IntoIterator for ValueTypeSet {
    type Item = ValueType;
    type IntoIter = ::enum_set::Iter<ValueType>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

impl ::std::iter::FromIterator<ValueType> for ValueTypeSet {
    fn from_iter<I: IntoIterator<Item = ValueType>>(iterator: I) -> Self {
        let mut ret = Self::none();
        ret.0.extend(iterator);
        ret
    }
}

impl ::std::iter::Extend<ValueType> for ValueTypeSet {
    fn extend<I: IntoIterator<Item = ValueType>>(&mut self, iter: I) {
        for element in iter {
            self.0.insert(element);
        }
    }
}
