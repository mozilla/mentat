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
use serde::de::{
    self,
    Deserialize,
    Deserializer
};
#[cfg(feature = "serde_support")]
use serde::ser::{
    Serialize,
    Serializer,
};

// Data storage for both NamespaceableKeyword and NamespaceableSymbol.
#[derive(Clone, Eq, Hash, PartialEq)]
pub struct NamespaceableName {
    // The bytes that make up the namespace followed directly by those
    // that make up the name. If there is a namespace, a solidus ('/') is between
    // the two parts.
    components: String,

    // The index (in bytes) into `components` of the dividing solidus — the character
    // between the namespace and the name.
    //
    // If this is zero, it means that this is _not_ a namespaced value!
    //
    // Important: The following invariants around `boundary` must be maintained:
    //
    // 1. `boundary` must always be less than or equal to `components.len()`.
    // 2. `boundary` must be a byte index that points to a character boundary,
    //     and not point into the middle of a UTF-8 codepoint. That is,
    //    `components.is_char_boundary(boundary)` must always be true.
    //
    // These invariants are enforced by `NamespaceableName::namespaced()`, and since
    // we never mutate `NamespaceableName`s, that's the only place we need to
    // worry about them.
    boundary: usize,
}

impl NamespaceableName {
    #[inline]
    pub fn plain<T>(name: T) -> Self where T: Into<String> {
        let n = name.into();
        assert!(!n.is_empty(), "Symbols and keywords cannot be unnamed.");

        NamespaceableName {
            components: n,
            boundary: 0,
        }
    }

    #[inline]
    pub fn namespaced<N, T>(namespace: N, name: T) -> Self where N: AsRef<str>, T: AsRef<str> {
        let n = name.as_ref();
        let ns = namespace.as_ref();

        // Note: These invariants are not required for safety. That is, if we
        // decide to allow these we can safely remove them.
        assert!(!n.is_empty(), "Symbols and keywords cannot be unnamed.");
        assert!(!ns.is_empty(), "Symbols and keywords cannot have an empty non-null namespace.");

        let mut dest = String::with_capacity(n.len() + ns.len());

        dest.push_str(ns);
        dest.push('/');
        dest.push_str(n);

        let boundary = ns.len();

        NamespaceableName {
            components: dest,
            boundary: boundary,
        }
    }

    fn new<N, T>(namespace: Option<N>, name: T) -> Self where N: AsRef<str>, T: AsRef<str> {
        if let Some(ns) = namespace {
            Self::namespaced(ns, name)
        } else {
            Self::plain(name.as_ref())
        }
    }

    pub fn is_namespaced(&self) -> bool {
        self.boundary > 0
    }

    #[inline]
    pub fn is_backward(&self) -> bool {
        self.name().starts_with('_')
    }

    #[inline]
    pub fn is_forward(&self) -> bool {
        !self.is_backward()
    }

    pub fn to_reversed(&self) -> NamespaceableName {
        let name = self.name();

        if name.starts_with('_') {
            Self::new(self.namespace(), &name[1..])
        } else {
            Self::new(self.namespace(), &format!("_{}", name))
        }
    }

    #[inline]
    pub fn namespace(&self) -> Option<&str> {
        if self.boundary > 0 {
            Some(&self.components[0..self.boundary])
        } else {
            None
        }
    }

    #[inline]
    pub fn name(&self) -> &str {
        if self.boundary == 0 {
            &self.components
        } else {
            &self.components[(self.boundary + 1)..]
        }
    }

    #[inline]
    pub fn components<'a>(&'a self) -> (&'a str, &'a str) {
        if self.boundary > 0 {
            (&self.components[0..self.boundary],
             &self.components[(self.boundary + 1)..])
        } else {
            (&self.components[0..0],
             &self.components)
        }
    }
}

// We order by namespace then by name.
// Non-namespaced values always sort before.
impl PartialOrd for NamespaceableName {
    fn partial_cmp(&self, other: &NamespaceableName) -> Option<Ordering> {
        match (self.boundary, other.boundary) {
            (0, 0) => self.components.partial_cmp(&other.components),
            (0, _) => Some(Ordering::Less),
            (_, 0) => Some(Ordering::Greater),
            (_, _) => {
                // Just use a lexicographic ordering.
                self.components().partial_cmp(&other.components())
            },
        }
    }
}

impl Ord for NamespaceableName {
    fn cmp(&self, other: &NamespaceableName) -> Ordering {
        self.components().cmp(&other.components())
    }
}

// We could derive this, but it's really hard to make sense of as-is.
impl fmt::Debug for NamespaceableName {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        fmt.debug_struct("NamespaceableName")
           .field("namespace", &self.namespace())
           .field("name", &self.name())
           .finish()
    }
}

impl fmt::Display for NamespaceableName {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        fmt.write_str(&self.components)
    }
}

// This is convoluted, but the basic idea is that since we don't want to rely on our input being
// correct, we'll need to implement a custom serializer no matter what (e.g. we can't just
// `derive(Deserialize)` since `unsafe` code depends on `self.boundary` being a valid index).
//
// We'd also like for users consuming our serialized data as e.g. JSON not to have to learn how we
// store NamespaceableName internally, since it's very much an implementation detail.
//
// We achieve both of these by implemeting a type that can serialize in way that's both user-
// friendly and automatic (e.g. `derive`d), and just pass all work off to it in our custom
// implementation of Serialize and Deserialize.
#[cfg(feature = "serde_support")]
#[cfg_attr(feature = "serde_support", serde(rename = "NamespaceableName"))]
#[cfg_attr(feature = "serde_support", derive(Serialize, Deserialize))]
struct SerializedNamespaceableName<'a> {
    namespace: Option<&'a str>,
    name: &'a str,
}

#[cfg(feature = "serde_support")]
impl<'de> Deserialize<'de> for NamespaceableName {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error> where D: Deserializer<'de> {
        let separated = SerializedNamespaceableName::deserialize(deserializer)?;
        if separated.name.len() == 0 {
            return Err(de::Error::custom("Empty name in keyword or symbol"));
        }
        if let Some(ns) = separated.namespace {
            if ns.len() == 0 {
                Err(de::Error::custom("Empty but present namespace in keyword or symbol"))
            } else {
                Ok(NamespaceableName::namespaced(ns, separated.name))
            }
        } else {
            Ok(NamespaceableName::plain(separated.name))
        }
    }
}

#[cfg(feature = "serde_support")]
impl Serialize for NamespaceableName {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error> where S: Serializer {
        let ser = SerializedNamespaceableName {
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
        assert!(panic::catch_unwind(|| NamespaceableName::namespaced("", "foo")).is_err(),
                "Empty namespace should panic");
        assert!(panic::catch_unwind(|| NamespaceableName::namespaced("foo", "")).is_err(),
                "Empty name should panic");
        assert!(panic::catch_unwind(|| NamespaceableName::namespaced("", "")).is_err(),
                "Should panic if both fields are empty");
    }

    #[test]
    fn test_basic() {
        let s = NamespaceableName::namespaced("aaaaa", "b");
        assert_eq!(s.namespace(), Some("aaaaa"));
        assert_eq!(s.name(), "b");
        assert_eq!(s.components(), ("aaaaa", "b"));

        let s = NamespaceableName::namespaced("b", "aaaaa");
        assert_eq!(s.namespace(), Some("b"));
        assert_eq!(s.name(), "aaaaa");
        assert_eq!(s.components(), ("b", "aaaaa"));
    }

    #[test]
    fn test_order() {
        let n0 = NamespaceableName::namespaced("a", "aa");
        let n1 = NamespaceableName::namespaced("aa", "a");

        let n2 = NamespaceableName::namespaced("a", "ab");
        let n3 = NamespaceableName::namespaced("aa", "b");

        let n4 = NamespaceableName::namespaced("b", "ab");
        let n5 = NamespaceableName::namespaced("ba", "b");

        let n6 = NamespaceableName::namespaced("z", "zz");

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
