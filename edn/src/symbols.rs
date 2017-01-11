// Copyright 2016 Mozilla
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

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
    // We derive PartialOrd, which implements a lexicographic based
    // on the order of members, so put namespace first.
    pub namespace: String,
    pub name: String,
}

impl PlainSymbol {
    pub fn new(name: &str) -> Self {
        assert!(!name.is_empty(), "Symbols cannot be unnamed.");

        return PlainSymbol(name.to_string());
    }
}

impl NamespacedSymbol {
    pub fn new(namespace: &str, name: &str) -> Self {
        assert!(!name.is_empty(), "Symbols cannot be unnamed.");
        assert!(!namespace.is_empty(), "Symbols cannot have an empty non-null namespace.");

        return NamespacedSymbol { name: name.to_string(), namespace: namespace.to_string() };
    }
}

impl Keyword {
    pub fn new(name: &str) -> Self {
        assert!(!name.is_empty(), "Keywords cannot be unnamed.");

        return Keyword(name.to_string());
    }
}

impl NamespacedKeyword {
    pub fn new(namespace: &str, name: &str) -> Self {
        assert!(!name.is_empty(), "Keywords cannot be unnamed.");
        assert!(!namespace.is_empty(), "Keywords cannot have an empty non-null namespace.");

        // TODO: debug asserts to ensure that neither field matches [ :/].
        return NamespacedKeyword { name: name.to_string(), namespace: namespace.to_string() };
    }
}

//
// Note that we don't currently do any escaping.
//

impl ToString for PlainSymbol {
    /// Print the symbol in EDN format.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use edn::symbols::PlainSymbol;
    /// assert_eq!("baz", PlainSymbol::new("baz").to_string());
    /// ```
    fn to_string(&self) -> String {
        return format!("{}", self.0);
    }
}

impl ToString for NamespacedSymbol {
    /// Print the symbol in EDN format.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use edn::symbols::NamespacedSymbol;
    /// assert_eq!("bar/baz", NamespacedSymbol::new("bar", "baz").to_string());
    /// ```
    fn to_string(&self) -> String {
        return format!("{}/{}", self.namespace, self.name);
    }
}

impl ToString for Keyword {
    /// Print the keyword in EDN format.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use edn::symbols::Keyword;
    /// assert_eq!(":baz", Keyword::new("baz").to_string());
    /// ```
    fn to_string(&self) -> String {
        return format!(":{}", self.0);
    }
}

impl ToString for NamespacedKeyword {
    /// Print the keyword in EDN format.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use edn::symbols::NamespacedKeyword;
    /// assert_eq!(":bar/baz", NamespacedKeyword::new("bar", "baz").to_string());
    /// ```
    fn to_string(&self) -> String {
        return format!(":{}/{}", self.namespace, self.name);
    }
}
