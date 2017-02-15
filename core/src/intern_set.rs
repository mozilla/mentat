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
use std::rc::Rc;

/// An `InternSet` allows to "intern" some potentially large values, maintaining a single value
/// instance owned by the `InternSet` and leaving consumers with lightweight ref-counted handles to
/// the large owned value.  This can avoid expensive clone() operations.
///
/// In Mentat, such large values might be strings or arbitrary [a v] pairs.
///
/// See https://en.wikipedia.org/wiki/String_interning for discussion.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct InternSet<T> where T: Eq + Hash {
    pub inner: HashSet<Rc<T>>,
}

impl<T> InternSet<T> where T: Eq + Hash {
    pub fn new() -> InternSet<T> {
        InternSet {
            inner: HashSet::new(),
        }
    }

    /// Intern a value, providing a ref-counted handle to the interned value.
    pub fn intern(&mut self, value: T) -> Rc<T> {
        let key = Rc::new(value);
        if self.inner.insert(key.clone()) {
            key
        } else {
            self.inner.get(&key).unwrap().clone()
        }
    }
}
