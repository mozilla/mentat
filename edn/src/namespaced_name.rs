// Copyright 2018 Mozilla
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

use std::cmp::{
    Ord,
    Ordering,
    PartialOrd,
};

use std::fmt;

#[cfg(feature = "serde_support")]
use serde::{
    de::{self, Deserialize, Deserializer},
    ser::{Serialize, Serializer}
};

// Data storage for both NamespacedKeyword and NamespacedSymbol.
#[derive(Clone, Eq, Hash, PartialEq)]
pub struct NamespacedName {
    // The bytes that make up the namespace followed directly by those
    // that make up the name.
    ns_and_name: String,

    // The index (in bytes) into `ns_and_name` where the namespace ends and
    // name begins.
    //
    // Important: The following invariants around `boundary` must be maintained
    // for memory safety.
    //
    // 1. `boundary` must always be less than or equal to `ns_and_name.len()`.
    // 2. `boundary` must be byte index that points to a character boundary,
    //     and not point into the middle of a utf8 codepoint. That is,
    //    `ns_and_name.is_char_boundary(boundary)` must always be true.
    //
    // These invariants are enforced by `NamespacedName::new()`, and since
    // we never mutate `NamespacedName`s, that's the only place we need to
    // worry about them.
    boundary: usize,
}

impl NamespacedName {
    #[inline]
    pub fn new<N, T>(namespace: N, name: T) -> Self where N: AsRef<str>, T: AsRef<str> {
        let n = name.as_ref();
        let ns = namespace.as_ref();

        // Note: These invariants are not required for safety. That is, if we
        // decide to allow these we can safely remove them.
        assert!(!n.is_empty(), "Symbols and keywords cannot be unnamed.");
        assert!(!ns.is_empty(), "Symbols and keywords cannot have an empty non-null namespace.");

        let mut dest = String::with_capacity(n.len() + ns.len());

        dest.push_str(ns);
        dest.push_str(n);

        let boundary = ns.len();

        NamespacedName {
            ns_and_name: dest,
            boundary: boundary,
        }
    }

    #[inline]
    pub fn namespace(&self) -> &str {
        &self.ns_and_name[0..self.boundary]
    }

    #[inline]
    pub fn name(&self) -> &str {
        &self.ns_and_name[self.boundary..]
    }

    #[inline]
    pub fn components<'a>(&'a self) -> (&'a str, &'a str) {
        self.ns_and_name.split_at(self.boundary)
    }
}

// We order by namespace then by name.
impl PartialOrd for NamespacedName {
    fn partial_cmp(&self, other: &NamespacedName) -> Option<Ordering> {
        // Just use a lexicographic ordering.
        self.components().partial_cmp(&other.components())
    }
}

impl Ord for NamespacedName {
    fn cmp(&self, other: &NamespacedName) -> Ordering {
        self.components().cmp(&other.components())
    }
}

// We could derive this, but it's really hard to make sense of as-is.
impl fmt::Debug for NamespacedName {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        fmt.debug_struct("NamespacedName")
           .field("namespace", &self.namespace())
           .field("name", &self.name())
           .finish()
    }
}

// This is convoluted, but the basic idea is that since we don't want to rely on our input being
// correct, we'll need to implement a custom serializer no matter what (e.g. we can't just
// `derive(Deserialize)` since `unsafe` code depends on `self.boundary` being a valid index).
//
// We'd also like for users consuming our serialized data as e.g. JSON not to have to learn how we
// store NamespacedName internally, since it's very much an implementation detail.
//
// We achieve both of these by implemeting a type that can serialize in way that's both user-
// friendly and automatic (e.g. `derive`d), and just pass all work off to it in our custom
// implementation of Serialize and Deserialize.
#[cfg(feature = "serde_support")]
#[cfg_attr(feature = "serde_support", serde(rename = "NamespacedName"))]
#[cfg_attr(feature = "serde_support", derive(Serialize, Deserialize))]
struct SerializedNamespacedName<'a> {
    namespace: &'a str,
    name: &'a str,
}

#[cfg(feature = "serde_support")]
impl<'de> Deserialize<'de> for NamespacedName {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error> where D: Deserializer<'de> {
        let separated = SerializedNamespacedName::deserialize(deserializer)?;
        if separated.name.len() == 0 {
            return Err(de::Error::custom("Empty name in keyword or symbol"));
        }
        if separated.namespace.len() == 0 {
            return Err(de::Error::custom("Empty namespace in keyword or symbol"));
        }
        Ok(NamespacedName::new(separated.namespace, separated.name))
    }
}

#[cfg(feature = "serde_support")]
impl Serialize for NamespacedName {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error> where S: Serializer {
        let ser = SerializedNamespacedName {
            namespace: self.namespace(),
            name: self.name(),
        };
        ser.serialize(serializer)
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use std::panic;

    #[test]
    fn test_new_invariants_maintained() {
        assert!(panic::catch_unwind(|| NamespacedName::new("", "foo")).is_err(),
                "Empty namespace should panic");
        assert!(panic::catch_unwind(|| NamespacedName::new("foo", "")).is_err(),
                "Empty name should panic");
        assert!(panic::catch_unwind(|| NamespacedName::new("", "")).is_err(),
                "Should panic if both fields are empty");
    }

    #[test]
    fn test_basic() {
        let s = NamespacedName::new("aaaaa", "b");
        assert_eq!(s.namespace(), "aaaaa");
        assert_eq!(s.name(), "b");
        assert_eq!(s.components(), ("aaaaa", "b"));

        let s = NamespacedName::new("b", "aaaaa");
        assert_eq!(s.name(), "aaaaa");
        assert_eq!(s.namespace(), "b");
        assert_eq!(s.components(), ("b", "aaaaa"));
    }

    #[test]
    fn test_order() {
        let n0 = NamespacedName::new("a", "aa");
        let n1 = NamespacedName::new("aa", "a");

        let n2 = NamespacedName::new("a", "ab");
        let n3 = NamespacedName::new("aa", "b");

        let n4 = NamespacedName::new("b", "ab");
        let n5 = NamespacedName::new("ba", "b");

        let n6 = NamespacedName::new("z", "zz");

        let mut arr = [
            n5.clone(),
            n6.clone(),
            n0.clone(),
            n3.clone(),
            n2.clone(),
            n1.clone(),
            n4.clone()
        ];

        arr.sort();

        assert_eq!(arr, [
            n0.clone(),
            n2.clone(),
            n1.clone(),
            n3.clone(),
            n4.clone(),
            n5.clone(),
            n6.clone(),
        ]);
    }
}
