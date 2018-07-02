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

use std::collections::HashSet;
use std::hash::Hash;

use ::{
    ValueRc,
};

/// An `InternSet` allows to "intern" some potentially large values, maintaining a single value
/// instance owned by the `InternSet` and leaving consumers with lightweight ref-counted handles to
/// the large owned value.  This can avoid expensive clone() operations.
///
/// In Mentat, such large values might be strings or arbitrary [a v] pairs.
///
/// See https://en.wikipedia.org/wiki/String_interning for discussion.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct InternSet<T> where T: Eq + Hash {
    pub inner: HashSet<ValueRc<T>>,
}

impl<T> InternSet<T> where T: Eq + Hash {
    pub fn new() -> InternSet<T> {
        InternSet {
            inner: HashSet::new(),
        }
    }

    pub fn len(&self) -> usize {
        self.inner.len()
    }

    /// Intern a value, providing a ref-counted handle to the interned value.
    ///
    /// ```
    /// use edn::{InternSet, ValueRc};
    ///
    /// let mut s = InternSet::new();
    ///
    /// let one = "foo".to_string();
    /// let two = ValueRc::new("foo".to_string());
    ///
    /// let out_one = s.intern(one);
    /// assert_eq!(out_one, two);
    /// // assert!(!&out_one.ptr_eq(&two));      // Nightly-only.
    ///
    /// let out_two = s.intern(two);
    /// assert_eq!(out_one, out_two);
    /// assert_eq!(1, s.inner.len());
    /// // assert!(&out_one.ptr_eq(&out_two));   // Nightly-only.
    /// ```
    pub fn intern<R: Into<ValueRc<T>>>(&mut self, value: R) -> ValueRc<T> {
        let key: ValueRc<T> = value.into();
        if self.inner.insert(key.clone()) {
            key
        } else {
            self.inner.get(&key).unwrap().clone()
        }
    }
}
