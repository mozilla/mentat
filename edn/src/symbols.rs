// Copyright 2018 Mozilla
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

use std::fmt::{Display, Formatter};
use namespaceable_name::NamespaceableName;

#[macro_export]
macro_rules! ns_keyword {
    ($ns: expr, $name: expr) => {{
        $crate::Keyword::namespaced($ns, $name)
    }}
}

/// A simplification of Clojure's Symbol.
#[derive(Clone,Debug,Eq,Hash,Ord,PartialOrd,PartialEq)]
pub struct PlainSymbol(pub String);

#[derive(Clone,Debug,Eq,Hash,Ord,PartialOrd,PartialEq)]
pub struct NamespacedSymbol(NamespaceableName);

/// A keyword is a symbol, optionally with a namespace, that prints with a leading colon.
/// This concept is imported from Clojure, as it features in EDN and the query
/// syntax that we use.
///
/// Clojure's constraints are looser than ours, allowing empty namespaces or
/// names:
///
/// ```clojure
/// user=> (keyword "" "")
/// :/
/// user=> (keyword "foo" "")
/// :foo/
/// user=> (keyword "" "bar")
/// :/bar
/// ```
///
/// We think that's nonsense, so we only allow keywords like `:bar` and `:foo/bar`,
/// with both namespace and main parts containing no whitespace and no colon or slash:
///
/// ```rust
/// # use edn::symbols::Keyword;
/// let bar     = Keyword::plain("bar");                         // :bar
/// let foo_bar = Keyword::namespaced("foo", "bar");        // :foo/bar
/// assert_eq!("bar", bar.name());
/// assert_eq!(None, bar.namespace());
/// assert_eq!("bar", foo_bar.name());
/// assert_eq!(Some("foo"), foo_bar.namespace());
/// ```
///
/// If you're not sure whether your input is well-formed, you should use a
/// parser or a reader function first to validate. TODO: implement `read`.
///
/// Callers are expected to follow these rules:
/// http://www.clojure.org/reference/reader#_symbols
///
/// Future: fast equality (interning?) for keywords.
///
#[derive(Clone,Debug,Eq,Hash,Ord,PartialOrd,PartialEq)]
#[cfg_attr(feature = "serde_support", derive(Serialize, Deserialize))]
pub struct Keyword(NamespaceableName);

impl PlainSymbol {
    pub fn plain<T>(name: T) -> Self where T: Into<String> {
        let n = name.into();
        assert!(!n.is_empty(), "Symbols cannot be unnamed.");

        PlainSymbol(n)
    }

    /// Return the name of the symbol without any leading '?' or '$'.
    ///
    /// ```rust
    /// # use edn::symbols::PlainSymbol;
    /// assert_eq!("foo", PlainSymbol::plain("?foo").name());
    /// assert_eq!("foo", PlainSymbol::plain("$foo").name());
    /// assert_eq!("!foo", PlainSymbol::plain("!foo").name());
    /// ```
    pub fn name(&self) -> &str {
        if self.is_src_symbol() || self.is_var_symbol() {
            &self.0[1..]
        } else {
            &self.0
        }
    }

    #[inline]
    pub fn is_var_symbol(&self) -> bool {
        self.0.starts_with('?')
    }

    #[inline]
    pub fn is_src_symbol(&self) -> bool {
        self.0.starts_with('$')
    }
}

impl NamespacedSymbol {
    pub fn namespaced<N, T>(namespace: N, name: T) -> Self where N: AsRef<str>, T: AsRef<str> {
        let r = namespace.as_ref();
        assert!(!r.is_empty(), "Namespaced symbols cannot have an empty non-null namespace.");
        NamespacedSymbol(NamespaceableName::namespaced(r, name))
    }

    #[inline]
    pub fn name(&self) -> &str {
        self.0.name()
    }

    #[inline]
    pub fn namespace(&self) -> &str {
        self.0.namespace().unwrap()
    }

    #[inline]
    pub fn components<'a>(&'a self) -> (&'a str, &'a str) {
        self.0.components()
    }
}

impl Keyword {
    pub fn plain<T>(name: T) -> Self where T: Into<String> {
        Keyword(NamespaceableName::plain(name))
    }
}

impl Keyword {
    /// Creates a new `Keyword`.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use edn::symbols::Keyword;
    /// let keyword = Keyword::namespaced("foo", "bar");
    /// assert_eq!(keyword.to_string(), ":foo/bar");
    /// ```
    ///
    /// See also the `kw!` macro in the main `mentat` crate.
    pub fn namespaced<N, T>(namespace: N, name: T) -> Self where N: AsRef<str>, T: AsRef<str> {
        let r = namespace.as_ref();
        assert!(!r.is_empty(), "Namespaced keywords cannot have an empty non-null namespace.");
        Keyword(NamespaceableName::namespaced(r, name))
    }

    #[inline]
    pub fn name(&self) -> &str {
        self.0.name()
    }

    #[inline]
    pub fn namespace(&self) -> Option<&str> {
        self.0.namespace()
    }

    #[inline]
    pub fn components<'a>(&'a self) -> (&'a str, &'a str) {
        self.0.components()
    }

    /// Whether this `Keyword` should be interpreted in reverse order. For example,
    /// the two following snippets are identical:
    ///
    /// ```edn
    /// [?y :person/friend ?x]
    /// [?x :person/hired ?y]
    ///
    /// [?y :person/friend ?x]
    /// [?y :person/_hired ?x]
    /// ```
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use edn::symbols::Keyword;
    /// assert!(!Keyword::namespaced("foo", "bar").is_backward());
    /// assert!(Keyword::namespaced("foo", "_bar").is_backward());
    /// ```
    #[inline]
    pub fn is_backward(&self) -> bool {
        self.0.is_backward()
    }

    /// Whether this `Keyword` should be interpreted in forward order.
    /// See `symbols::Keyword::is_backward`.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use edn::symbols::Keyword;
    /// assert!(Keyword::namespaced("foo", "bar").is_forward());
    /// assert!(!Keyword::namespaced("foo", "_bar").is_forward());
    /// ```
    #[inline]
    pub fn is_forward(&self) -> bool {
        self.0.is_forward()
    }

    #[inline]
    pub fn is_namespaced(&self) -> bool {
        self.0.is_namespaced()
    }

    /// Returns a `Keyword` with the same namespace and a
    /// 'backward' name. See `symbols::Keyword::is_backward`.
    ///
    /// Returns a forward name if passed a reversed keyword; i.e., this
    /// function is its own inverse.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use edn::symbols::Keyword;
    /// let nsk = Keyword::namespaced("foo", "bar");
    /// assert!(!nsk.is_backward());
    /// assert_eq!(":foo/bar", nsk.to_string());
    ///
    /// let reversed = nsk.to_reversed();
    /// assert!(reversed.is_backward());
    /// assert_eq!(":foo/_bar", reversed.to_string());
    /// ```
    pub fn to_reversed(&self) -> Keyword {
        Keyword(self.0.to_reversed())
    }

    /// If this `Keyword` is 'backward' (see `symbols::Keyword::is_backward`),
    /// return `Some('forward name')`; otherwise, return `None`.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use edn::symbols::Keyword;
    /// let nsk = Keyword::namespaced("foo", "bar");
    /// assert_eq!(None, nsk.unreversed());
    ///
    /// let reversed = nsk.to_reversed();
    /// assert_eq!(Some(nsk), reversed.unreversed());
    /// ```
    pub fn unreversed(&self) -> Option<Keyword> {
        if self.is_backward() {
            Some(self.to_reversed())
        } else {
            None
        }
    }
}

//
// Note that we don't currently do any escaping.
//

impl Display for PlainSymbol {
    /// Print the symbol in EDN format.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use edn::symbols::PlainSymbol;
    /// assert_eq!("baz", PlainSymbol::plain("baz").to_string());
    /// ```
    fn fmt(&self, f: &mut Formatter) -> ::std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl Display for NamespacedSymbol {
    /// Print the symbol in EDN format.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use edn::symbols::NamespacedSymbol;
    /// assert_eq!("bar/baz", NamespacedSymbol::namespaced("bar", "baz").to_string());
    /// ```
    fn fmt(&self, f: &mut Formatter) -> ::std::fmt::Result {
        write!(f, "{}/{}", self.namespace(), self.name())
    }
}

impl Display for Keyword {
    /// Print the keyword in EDN format.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use edn::symbols::Keyword;
    /// assert_eq!(":baz", Keyword::plain("baz").to_string());
    /// assert_eq!(":bar/baz", Keyword::namespaced("bar", "baz").to_string());
    /// assert_eq!(":bar/_baz", Keyword::namespaced("bar", "baz").to_reversed().to_string());
    /// assert_eq!(":bar/baz", Keyword::namespaced("bar", "baz").to_reversed().to_reversed().to_string());
    /// ```
    fn fmt(&self, f: &mut Formatter) -> ::std::fmt::Result {
        if self.0.is_namespaced() {
            let (ns, name) = self.0.components();
            write!(f, ":{}/{}", ns, name)
        } else {
            write!(f, ":{}", self.0.name())
        }
    }
}

#[test]
fn test_ns_keyword_macro() {
    assert_eq!(ns_keyword!("test", "name").to_string(),
               Keyword::namespaced("test", "name").to_string());
    assert_eq!(ns_keyword!("ns", "_name").to_string(),
               Keyword::namespaced("ns", "_name").to_string());
}
