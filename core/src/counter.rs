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

#[derive(Clone)]
pub struct RcCounter {
    c: Rc<AtomicUsize>,
}

/// A simple shared counter.
impl RcCounter {
    pub fn with_initial(value: usize) -> Self {
        RcCounter { c: Rc::new(AtomicUsize::new(value)) }
    }

    pub fn new() -> Self {
        RcCounter { c: Rc::new(AtomicUsize::new(0)) }
    }

    /// Return the next value in the sequence.
    ///
    /// ```
    /// use mentat_core::counter::RcCounter;
    ///
    /// let c = RcCounter::with_initial(3);
    /// assert_eq!(c.next(), 3);
    /// assert_eq!(c.next(), 4);
    /// let d = c.clone();
    /// assert_eq!(d.next(), 5);
    /// assert_eq!(c.next(), 6);
    /// ```
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