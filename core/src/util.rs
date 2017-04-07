// Copyright 2016 Mozilla
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

use std::rc::Rc;

use std::sync::atomic::{
    AtomicUsize,
    Ordering,
};

/// Side-effect chaining on `Result`.
pub trait ResultEffect<T> {
    /// Invoke `f` if `self` is `Ok`, returning `self`.
    fn when_ok<F: FnOnce()>(self, f: F) -> Self;

    /// Invoke `f` if `self` is `Err`, returning `self`.
    fn when_err<F: FnOnce()>(self, f: F) -> Self;
}

impl<T, E> ResultEffect<T> for Result<T, E> {
    fn when_ok<F: FnOnce()>(self, f: F) -> Self {
        if self.is_ok() {
            f();
        }
        self
    }

    fn when_err<F: FnOnce()>(self, f: F) -> Self {
        if self.is_err() {
            f();
        }
        self
    }
}

/// Side-effect chaining on `Option`.
pub trait OptionEffect<T> {
    /// Invoke `f` if `self` is `None`, returning `self`.
    fn when_none<F: FnOnce()>(self, f: F) -> Self;

    /// Invoke `f` if `self` is `Some`, returning `self`.
    fn when_some<F: FnOnce()>(self, f: F) -> Self;
}

impl<T> OptionEffect<T> for Option<T> {
    fn when_none<F: FnOnce()>(self, f: F) -> Self {
        if self.is_none() {
            f();
        }
        self
    }

    fn when_some<F: FnOnce()>(self, f: F) -> Self {
        if self.is_some() {
            f();
        }
        self
    }
}

#[derive(Clone)]
pub struct RcCounter {
    c: Rc<AtomicUsize>,
}

impl RcCounter {
    pub fn new() -> Self {
        RcCounter { c: Rc::new(AtomicUsize::new(0)) }
    }

    pub fn next(&self) -> usize {
        self.c.fetch_add(1, Ordering::SeqCst)
    }
}

#[cfg(test)]
mod tests {
    use super::RcCounter;

    #[test]
    fn test_rc_counter() {
        let c = RcCounter::new();
        assert_eq!(c.next(), 0);
        assert_eq!(c.next(), 1);

        let d = c.clone();
        assert_eq!(d.next(), 2);
        assert_eq!(c.next(), 3);
    }
}