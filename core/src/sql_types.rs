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
};

use core_traits::{
    ValueType,
    ValueTypeSet,
};

use types::{
    ValueTypeTag,
};

/// Type safe representation of the possible return values from SQLite's `typeof`
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialOrd, PartialEq)]
pub enum SQLTypeAffinity {
    Null,    // "null"
    Integer, // "integer"
    Real,    // "real"
    Text,    // "text"
    Blob,    // "blob"
}

// Put this here rather than in `db` simply because it's widely needed.
pub trait SQLValueType {
    fn value_type_tag(&self) -> ValueTypeTag;
    fn accommodates_integer(&self, int: i64) -> bool;

    /// Return a pair of the ValueTypeTag for this value type, and the SQLTypeAffinity required
    /// to distinguish it from any other types that share the same tag.
    ///
    /// Background: The tag alone is not enough to determine the type of a value, since multiple
    /// ValueTypes may share the same tag (for example, ValueType::Long and ValueType::Double).
    /// However, each ValueType can be determined by checking both the tag and the type's affinity.
    fn sql_representation(&self) -> (ValueTypeTag, Option<SQLTypeAffinity>);
}

impl SQLValueType for ValueType {
    fn sql_representation(&self) -> (ValueTypeTag, Option<SQLTypeAffinity>) {
        match *self {
            ValueType::Ref     => (0, None),
            ValueType::Boolean => (1, None),
            ValueType::Instant => (4, None),

            // SQLite distinguishes integral from decimal types, allowing long and double to share a tag.
            ValueType::Long    => (5, Some(SQLTypeAffinity::Integer)),
            ValueType::Double  => (5, Some(SQLTypeAffinity::Real)),
            ValueType::String  => (10, None),
            ValueType::Uuid    => (11, None),
            ValueType::Keyword => (13, None),
        }
    }

    #[inline]
    fn value_type_tag(&self) -> ValueTypeTag {
        self.sql_representation().0
    }

    /// Returns true if the provided integer is in the SQLite value space of this type. For
    /// example, `1` is how we encode `true`.
    ///
    /// ```
    /// extern crate core_traits;
    /// extern crate mentat_core;
    /// use core_traits::ValueType;
    /// use mentat_core::SQLValueType;
    /// assert!(!ValueType::Instant.accommodates_integer(1493399581314));
    /// assert!(!ValueType::Instant.accommodates_integer(1493399581314000));
    /// assert!(ValueType::Boolean.accommodates_integer(1));
    /// assert!(!ValueType::Boolean.accommodates_integer(-1));
    /// assert!(!ValueType::Boolean.accommodates_integer(10));
    /// assert!(!ValueType::String.accommodates_integer(10));
    /// ```
    fn accommodates_integer(&self, int: i64) -> bool {
        use ValueType::*;
        match *self {
            Instant                 => false,          // Always use #inst.
            Long | Double           => true,
            Ref                     => int >= 0,
            Boolean                 => (int == 0) || (int == 1),
            ValueType::String       => false,
            Keyword                 => false,
            Uuid                    => false,
        }
    }
}

/// We have an enum of types, `ValueType`. It can be collected into a set, `ValueTypeSet`. Each type
/// is associated with a type tag, which is how a type is represented in, e.g., SQL storage. Types
/// can share type tags, because backing SQL storage is able to differentiate between some types
/// (e.g., longs and doubles), and so distinct tags aren't necessary. That association is defined by
/// `SQLValueType`. That trait similarly extends to `ValueTypeSet`, which maps a collection of types
/// into a collection of tags.
pub trait SQLValueTypeSet {
    fn value_type_tags(&self) -> BTreeSet<ValueTypeTag>;
    fn has_unique_type_tag(&self) -> bool;
    fn unique_type_tag(&self) -> Option<ValueTypeTag>;
}

impl SQLValueTypeSet for ValueTypeSet {
    // This is inefficient, but it'll do for now.
    fn value_type_tags(&self) -> BTreeSet<ValueTypeTag> {
        let mut out = BTreeSet::new();
        for t in self.0.iter() {
            out.insert(t.value_type_tag());
        }
        out
    }

    fn unique_type_tag(&self) -> Option<ValueTypeTag> {
        if self.is_unit() || self.has_unique_type_tag() {
            self.exemplar().map(|t| t.value_type_tag())
        } else {
            None
        }
    }

    fn has_unique_type_tag(&self) -> bool {
        if self.is_unit() {
            return true;
        }

        let mut acc = BTreeSet::new();
        for t in self.0.iter() {
            if acc.insert(t.value_type_tag()) && acc.len() > 1 {
                // We inserted a second or subsequent value.
                return false;
            }
        }
        !acc.is_empty()
    }
}
