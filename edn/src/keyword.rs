// Copyright 2016 Mozilla
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

/// Just like Clojure's Keyword. We'll need this as part of EDN parsing,
/// but it's also used for identification within Mentat, so we'll define
/// it here first.
/// Callers are expected to follow these rules:
/// http://www.clojure.org/reference/reader#_symbols
#[derive(Clone,Debug,Eq,PartialEq)]
pub struct Keyword {
    pub name: String,
    pub namespace: Option<String>,
}

/// A symbol, optionally with a namespace, that prints with a leading colon.
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
/// # use edn::keyword::Keyword;
/// let bar     = Keyword::new("bar");                         // :bar
/// let foo_bar = Keyword::namespaced("foo", "bar");           // :foo/bar
/// assert_eq!("bar", bar.name);
/// assert_eq!(false, bar.namespace.is_some());
/// assert_eq!("bar", foo_bar.name);
/// assert_eq!(true, foo_bar.namespace.is_some());
/// assert_eq!("foo", foo_bar.namespace.expect("Should have a namespace"));
/// ```
///
/// If you're not sure whether your input is well-formed, you should use a
/// reader function first to validate. TODO: implement `read`.
///
impl Keyword {
    pub fn new(name: &str) -> Self {
        return Keyword { name: name.to_string(), namespace: None };
    }

    pub fn namespaced(namespace: &str, name: &str) -> Self {
        assert!(!name.is_empty(), "Keywords cannot be unnamed.");
        assert!(!namespace.is_empty(), "Keywords cannot have an empty non-null namespace.");

        // TODO: debug asserts to ensure that neither field matches [ :/].
        return Keyword { name: name.to_string(), namespace: Some(namespace.to_string()) };
    }
}

impl ToString for Keyword {
    /// Print the keyword in EDN format.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use edn::keyword::Keyword;
    /// assert_eq!(":baz", Keyword::new("baz").to_string());
    /// assert_eq!(":bar/baz", Keyword::namespaced("bar", "baz").to_string());
    /// ```
    fn to_string(&self) -> String {
        // Note that we don't currently do any escaping.
        if let Some(ref ns) = self.namespace {
            return format!(":{}/{}", ns, self.name);
        }
        return format!(":{}", self.name);
    }
}
