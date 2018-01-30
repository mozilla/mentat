// Copyright 2016 Mozilla
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

use std::fmt::{Display, Formatter};

#[macro_export]
macro_rules! ns_keyword {
    ($ns: expr, $name: expr) => {{
        $crate::NamespacedKeyword::new($ns, $name)
    }}
}

/// A simplification of Clojure's Symbol.
#[derive(Clone,Debug,Eq,Hash,Ord,PartialOrd,PartialEq)]
pub struct PlainSymbol(pub String);

#[derive(Clone,Debug,Eq,Hash,Ord,PartialOrd,PartialEq)]
pub struct NamespacedSymbol {
    // We derive PartialOrd, which implements a lexicographic based
    // on the order of members, so put namespace first.
    pub namespace: String,
    pub name: String,
}

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
/// # use edn::symbols::NamespacedKeyword;
/// let bar     = Keyword::new("bar");                         // :bar
/// let foo_bar = NamespacedKeyword::new("foo", "bar");        // :foo/bar
/// assert_eq!("bar", bar.0);
/// assert_eq!("bar", foo_bar.name);
/// assert_eq!("foo", foo_bar.namespace);
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
pub struct Keyword(pub String);

#[derive(Clone,Debug,Eq,Hash,Ord,PartialOrd,PartialEq)]
pub struct NamespacedKeyword {
    // We derive PartialOrd, which implements a lexicographic order based
    // on the order of members, so put namespace first.
    pub namespace: String,
    pub name: String,
}

impl PlainSymbol {
    pub fn new<T>(name: T) -> Self where T: Into<String> {
        let n = name.into();
        assert!(!n.is_empty(), "Symbols cannot be unnamed.");

        PlainSymbol(n)
    }

    /// Return the name of the symbol without any leading '?' or '$'.
    ///
    /// ```rust
    /// # use edn::symbols::PlainSymbol;
    /// assert_eq!("foo", PlainSymbol::new("?foo").plain_name());
    /// assert_eq!("foo", PlainSymbol::new("$foo").plain_name());
    /// assert_eq!("!foo", PlainSymbol::new("!foo").plain_name());
    /// ```
    pub fn plain_name(&self) -> &str {
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
    pub fn new<T>(namespace: T, name: T) -> Self where T: Into<String> {
        let n = name.into();
        let ns = namespace.into();

        assert!(!n.is_empty(), "Symbols cannot be unnamed.");
        assert!(!ns.is_empty(), "Symbols cannot have an empty non-null namespace.");

        NamespacedSymbol { name: n, namespace: ns }
    }
}

impl Keyword {
    pub fn new<T>(name: T) -> Self where T: Into<String> {
        let n = name.into();
        assert!(!n.is_empty(), "Keywords cannot be unnamed.");

        Keyword(n)
    }
}

impl NamespacedKeyword {
    /// Creates a new `NamespacedKeyword`.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use edn::symbols::NamespacedKeyword;
    /// let keyword = NamespacedKeyword::new("foo", "bar");
    /// assert_eq!(keyword.to_string(), ":foo/bar");
    /// ```
    ///
    /// See also the `kw!` macro in the main `mentat` crate.
    pub fn new<T>(namespace: T, name: T) -> Self where T: Into<String> {
        let n = name.into();
        let ns = namespace.into();
        assert!(!n.is_empty(), "Keywords cannot be unnamed.");
        assert!(!ns.is_empty(), "Keywords cannot have an empty non-null namespace.");

        // TODO: debug asserts to ensure that neither field matches [ :/].
        NamespacedKeyword {
            name: n,
            namespace: ns,
        }
    }

    /// Whether this `NamespacedKeyword` should be interpreted in reverse order. For example,
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
    /// # use edn::symbols::NamespacedKeyword;
    /// assert!(!NamespacedKeyword::new("foo", "bar").is_backward());
    /// assert!(NamespacedKeyword::new("foo", "_bar").is_backward());
    /// ```
    #[inline]
    pub fn is_backward(&self) -> bool {
        self.name.starts_with('_')
    }

    /// Whether this `NamespacedKeyword` should be interpreted in forward order.
    /// See `symbols::NamespacedKeyword::is_backward`.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use edn::symbols::NamespacedKeyword;
    /// assert!(NamespacedKeyword::new("foo", "bar").is_forward());
    /// assert!(!NamespacedKeyword::new("foo", "_bar").is_forward());
    /// ```
    #[inline]
    pub fn is_forward(&self) -> bool {
        !self.is_backward()
    }

    /// Returns a `NamespacedKeyword` with the same namespace and a
    /// 'backward' name. See `symbols::NamespacedKeyword::is_backward`.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use edn::symbols::NamespacedKeyword;
    /// let nsk = NamespacedKeyword::new("foo", "bar");
    /// assert!(!nsk.is_backward());
    /// assert_eq!(":foo/bar", nsk.to_string());
    ///
    /// let reversed = nsk.to_reversed();
    /// assert!(reversed.is_backward());
    /// assert_eq!(":foo/_bar", reversed.to_string());
    /// ```
    pub fn to_reversed(&self) -> NamespacedKeyword {
        let name = if self.is_backward() {
            self.name[1..].to_string()
        } else {
            format!("{}{}", "_", self.name)
        };

        NamespacedKeyword {
            name: name,
            namespace: self.namespace.clone(),
        }
    }

    /// If this `NamespacedKeyword` is 'backward' (see `symbols::NamespacedKeyword::is_backward`),
    /// return `Some('forward name')`; otherwise, return `None`.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use edn::symbols::NamespacedKeyword;
    /// let nsk = NamespacedKeyword::new("foo", "bar");
    /// assert_eq!(None, nsk.unreversed());
    ///
    /// let reversed = nsk.to_reversed();
    /// assert_eq!(Some(nsk), reversed.unreversed());
    /// ```
    pub fn unreversed(&self) -> Option<NamespacedKeyword> {
        if self.is_backward() {
            Some(NamespacedKeyword {
                name: self.name[1..].to_string(),
                namespace: self.namespace.clone(),
            })
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
    /// assert_eq!("baz", PlainSymbol::new("baz").to_string());
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
    /// assert_eq!("bar/baz", NamespacedSymbol::new("bar", "baz").to_string());
    /// ```
    fn fmt(&self, f: &mut Formatter) -> ::std::fmt::Result {
        write!(f, "{}/{}", self.namespace, self.name)
    }
}

impl Display for Keyword {
    /// Print the keyword in EDN format.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use edn::symbols::Keyword;
    /// assert_eq!(":baz", Keyword::new("baz").to_string());
    /// ```
    fn fmt(&self, f: &mut Formatter) -> ::std::fmt::Result {
        write!(f, ":{}", self.0)
    }
}

impl Display for NamespacedKeyword {
    /// Print the keyword in EDN format.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use edn::symbols::NamespacedKeyword;
    /// assert_eq!(":bar/baz", NamespacedKeyword::new("bar", "baz").to_string());
    /// assert_eq!(":bar/_baz", NamespacedKeyword::new("bar", "baz").to_reversed().to_string());
    /// assert_eq!(":bar/baz", NamespacedKeyword::new("bar", "baz").to_reversed().to_reversed().to_string());
    /// ```
    fn fmt(&self, f: &mut Formatter) -> ::std::fmt::Result {
        write!(f, ":{}/{}", self.namespace, self.name)
    }
}

#[test]
fn test_ns_keyword_macro() {
    assert_eq!(ns_keyword!("test", "name").to_string(),
               NamespacedKeyword::new("test", "name").to_string());
    assert_eq!(ns_keyword!("ns", "_name").to_string(),
               NamespacedKeyword::new("ns", "_name").to_string());
}
