// Copyright 2016 Mozilla
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

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